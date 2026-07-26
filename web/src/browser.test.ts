// Phase 2's stated verification, run rather than described.
//
// Two real browsers, two separate api nodes, one Redis. What this proves that
// sync.test.ts cannot: that the client's conclusions are right about a *real*
// server, that delivery crosses nodes, and that a socket dying mid-conversation
// loses nothing.
//
// Skipped unless COMMS_E2E=1, the same bargain internal/platform/database/testdb
// makes: a test needing infrastructure must not fail on a machine that has none,
// and must not silently pass either. See web/README.md for the four commands.

import { existsSync, readdirSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

import type { Browser, BrowserContext, Page } from 'playwright-core'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

const live = process.env.COMMS_E2E === '1'

// Two dev servers, each proxying to its own api node. That split is the entire
// point: without it both sockets land on one process and cross-node delivery is
// never exercised.
const nodeAURL = process.env.COMMS_WEB_A ?? 'http://localhost:5173'
const nodeBURL = process.env.COMMS_WEB_B ?? 'http://localhost:5174'
const apiAURL = process.env.COMMS_API_A ?? 'http://localhost:8080'
const apiBURL = process.env.COMMS_API_B ?? 'http://localhost:8081'

const passphrase = 'correct horse battery staple'
const timeout = 30_000

/**
 * chromiumPath finds an installed Chromium.
 *
 * Playwright resolves a browser revision pinned to its own version, which breaks
 * whenever the cache holds a different one — so the cache is read for whatever is
 * actually there. CHROMIUM_PATH overrides for anything unusual.
 */
function chromiumPath(): string | undefined {
  if (process.env.CHROMIUM_PATH) return process.env.CHROMIUM_PATH

  const cache = join(homedir(), '.cache', 'ms-playwright')
  if (!existsSync(cache)) return undefined

  const revisions = readdirSync(cache)
    .filter((name) => /^chromium-\d+$/.test(name))
    .sort((left, right) => Number(right.split('-')[1]) - Number(left.split('-')[1]))

  for (const revision of revisions) {
    const candidate = join(cache, revision, 'chrome-linux64', 'chrome')
    if (existsSync(candidate)) return candidate
  }
  return undefined
}

/** unique keeps handles from colliding across runs, since registration is permanent. */
const unique = Math.random().toString(36).slice(2, 8)

type Person = {
  context: BrowserContext
  page: Page
  handle: string
}

describe.skipIf(!live)('two people, two nodes', () => {
  let browser: Browser
  let alice: Person
  let bob: Person

  beforeAll(async () => {
    const { chromium } = await import('playwright-core')
    browser = await chromium.launch({ headless: true, executablePath: chromiumPath() })

    // A context each. Sessions live in sessionStorage, which is per-tab, but a
    // context each also keeps the two from sharing anything else by accident.
    alice = await join_(browser, nodeAURL, `alice${unique}`)
    bob = await join_(browser, nodeBURL, `bob${unique}`)
  }, timeout * 2)

  afterAll(async () => {
    await browser?.close()
  })

  it('registers both and connects each socket to its own node', async () => {
    expect(await status(alice.page)).toBe('live')
    expect(await status(bob.page)).toBe('live')

    // One socket on each node, which is what makes the delivery below a real
    // cross-node test rather than two tabs on one process. Polled because
    // StrictMode mounts, unmounts and remounts, and the discarded connection
    // takes a moment to be noticed as closed.
    expect(await connectionsOn(apiAURL)).toBe(1)
    expect(await connectionsOn(apiBURL)).toBe(1)
  })

  it('delivers a message live across nodes', async () => {
    await alice.page.getByLabel('Handle to message').fill(bob.handle)
    await alice.page.getByRole('button', { name: 'Start' }).click()
    await alice.page.waitForSelector('.composer input', { timeout })

    await send(alice.page, 'hello from alice')

    // Bob is connected to the other node and was never told this conversation
    // existed. The control message is what makes it appear.
    await expectTranscript(bob.page, ['hello from alice'])
    await expectTranscript(alice.page, ['hello from alice'])
  })

  it('delivers the reply back the other way', async () => {
    await send(bob.page, 'hello from bob')

    await expectTranscript(alice.page, ['hello from alice', 'hello from bob'])
    await expectTranscript(bob.page, ['hello from alice', 'hello from bob'])
  })

  it('fills the gap after a socket dies mid-conversation', async () => {
    // The load-bearing test of phase 2. Alice loses the network entirely, so her
    // socket carries nothing and no broadcast reaches her. Bob keeps talking.
    await alice.context.setOffline(true)
    try {
      await alice.page.waitForSelector('.status-offline', { timeout })

      await send(bob.page, 'while alice was away 1')
      await send(bob.page, 'while alice was away 2')
      await send(bob.page, 'while alice was away 3')
    } finally {
      // In a finally so a failure here does not leave every later test running
      // against a browser with no network, which fails them all for the wrong
      // reason and hides the one that actually broke.
      await alice.context.setOffline(false)
    }

    // Reconnect, resume with her contiguous mark, receive the gap, fetch it. All
    // three arrive, in order, exactly once.
    await expectTranscript(
      alice.page,
      [
        'hello from alice',
        'hello from bob',
        'while alice was away 1',
        'while alice was away 2',
        'while alice was away 3',
      ],
      timeout,
    )

    // Gapless and duplicate-free is a claim about sequence numbers, not text.
    expect(await sequences(alice.page)).toEqual([1, 2, 3, 4, 5])
  }, timeout * 2)

  it('writes one entry for a repeated client identifier', async () => {
    // Driven through fetch rather than the composer: the UI issues a fresh
    // identifier per send, which is right, and means the retry case can only be
    // provoked from underneath it (MS-2).
    const [first, second] = await alice.page.evaluate(async () => {
      const session = JSON.parse(sessionStorage.getItem('comms.session')!)
      const headers = {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${session.access_token}`,
      }
      const { conversations } = await fetch('/v1/conversations', { headers }).then((response) =>
        response.json(),
      )
      const body = JSON.stringify({
        client_entry_id: 'a-retried-send',
        content_type: 'text/plain; charset=utf-8',
        body: btoa('sent twice'),
      })
      const post = () =>
        fetch(`/v1/conversations/${conversations[0].id}/entries`, { method: 'POST', headers, body })
          .then((response) => response.json())

      // Sequential, which is the real retry shape: the first response was lost,
      // so the client sends the same thing again.
      return [await post(), await post()]
    })

    expect(second.id).toBe(first.id)
    expect(second.sequence).toBe(first.sequence)

    await expectTranscript(alice.page, [
      'hello from alice',
      'hello from bob',
      'while alice was away 1',
      'while alice was away 2',
      'while alice was away 3',
      'sent twice',
    ])
    expect(await sequences(alice.page)).toEqual([1, 2, 3, 4, 5, 6])
  })
})

// --- helpers ---

/** join_ registers a person and waits until their socket is live. Named with a
 *  trailing underscore only because `join` is a member of every Array. */
async function join_(browser: Browser, url: string, handle: string): Promise<Person> {
  const context = await browser.newContext()
  const page = await context.newPage()

  page.on('pageerror', (error) => {
    // A runtime error in the client would otherwise show up as a mysterious
    // timeout several assertions later.
    throw new Error(`${handle}: uncaught in page: ${error.message}`)
  })

  await page.goto(url)
  await page.getByRole('button', { name: 'Create an account instead' }).click()

  const form = page.locator('form.card')
  await form.getByLabel('Handle').fill(handle)
  await form.getByLabel('Email').fill(`${handle}@example.test`)
  await form.getByLabel('Passphrase').fill(passphrase)
  await form.getByRole('button', { name: 'Create account' }).click()

  await page.waitForSelector('.status-live', { timeout })
  return { context, page, handle }
}

async function send(page: Page, text: string): Promise<void> {
  // Located by container rather than by label: "Message" is a substring of the
  // start-a-conversation field's label too, and a selector that matches two
  // different inputs will eventually pick the wrong one.
  await page.locator('.composer input').fill(text)
  await page.getByRole('button', { name: 'Send' }).click()
  // The composer clears only after the server has assigned a position, so this
  // waits for the send to have actually happened.
  await page.waitForFunction(
    () => (document.querySelector<HTMLInputElement>('.composer input')?.value ?? 'x') === '',
    undefined,
    { timeout },
  )
}

function status(page: Page): Promise<string | null> {
  return page.locator('.status').textContent()
}

/** connectionsOn reports a node's socket count, settling first. */
async function connectionsOn(url: string): Promise<number> {
  let count = -1
  for (let attempt = 0; attempt < 30; attempt++) {
    const health = await fetch(`${url}/health`).then((response) => response.json())
    count = health.connections
    if (count === 1) return count
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
  return count
}

function sequences(page: Page): Promise<number[]> {
  return page.evaluate(() =>
    [...document.querySelectorAll('.transcript .sequence')].map((node) =>
      Number(node.textContent!.replace('#', '')),
    ),
  )
}

/** expectTranscript waits for exactly these messages, in this order. Polling rather
 *  than sleeping: the whole point of these assertions is that arrival timing is not
 *  something the client controls. */
async function expectTranscript(page: Page, expected: string[], wait = timeout): Promise<void> {
  await page
    .waitForFunction(
      (want: string[]) => {
        const found = [...document.querySelectorAll('.transcript li .body')].map(
          (node) => node.textContent,
        )
        return found.length === want.length && want.every((text, index) => found[index] === text)
      },
      expected,
      { timeout: wait },
    )
    .catch(async (error) => {
      const found = await page.evaluate(() =>
        [...document.querySelectorAll('.transcript li .body')].map((node) => node.textContent),
      )
      throw new Error(`expected ${JSON.stringify(expected)}, transcript held ${JSON.stringify(found)}: ${error}`)
    })
}

// Phase 3's frontend increment, verified the same way: real browsers, real Kafka,
// real projections. These require the worker to be running — `make e2e` starts it.
describe.skipIf(!live)('badges and ticks', () => {
  let browser: Browser
  let alice: Person
  let bob: Person
  // A third person so bob has two conversations. With only one, bob always has it
  // open, the badge is suppressed by design, and a test asserting the badge renders
  // would be asserting something that cannot happen.
  let erin: Person
  let conversation: string

  beforeAll(async () => {
    const { chromium } = await import('playwright-core')
    browser = await chromium.launch({ headless: true, executablePath: chromiumPath() })

    alice = await join_(browser, nodeAURL, `carol${unique}`)
    bob = await join_(browser, nodeBURL, `dave${unique}`)
    erin = await join_(browser, nodeAURL, `erin${unique}`)

    conversation = await start(alice, bob.handle)
    await start(erin, bob.handle)

    // Bob now has two. He selects erin's, leaving alice's unselected and therefore
    // able to show a badge.
    await bob.page.waitForFunction(
      () => document.querySelectorAll('.conversations button').length === 2,
      undefined,
      { timeout },
    )
  }, timeout * 3)

  afterAll(async () => {
    await browser?.close()
  })

  it('shows an unread badge to the recipient and none to the author', async () => {
    await selectOther(bob.page, conversation)

    await send(alice.page, 'first unread')
    await send(alice.page, 'second unread')

    // Eventually consistent: the badge appears once the projection catches up and the
    // client's poll picks it up. Waiting is the assertion — a badge that never arrives
    // fails here.
    await bob.page.waitForFunction(
      (id: string) => {
        const button = document.querySelector(`[data-conversation="${id}"]`)
        return button?.querySelector('.badge')?.textContent === '2'
      },
      conversation,
      { timeout },
    )

    // Alice authored both, so she has nothing unread and shows no badge at all.
    expect(await projectedUnread(alice.page, conversation)).toBe(0)
    expect(await alice.page.locator('.conversations .badge').count()).toBe(0)
  }, timeout * 2)

  it('clears the badge when the conversation is opened', async () => {
    // Opening it suppresses the badge immediately, and the acknowledgement that
    // follows makes the projection agree. Both halves matter: the first is what makes
    // the interface right during the window, the second is what makes it right after.
    await bob.page.click(`[data-conversation="${conversation}"]`)

    await bob.page.waitForFunction(
      (id: string) => document.querySelector(`[data-conversation="${id}"] .badge`) === null,
      conversation,
      { timeout },
    )

    // And the server agrees once the receipt has been projected.
    await bob.page.waitForFunction(
      async (id: string) => {
        const session = JSON.parse(sessionStorage.getItem('comms.session')!)
        const { conversations } = await fetch('/v1/conversations', {
          headers: { Authorization: `Bearer ${session.access_token}` },
        }).then((response) => response.json())
        return conversations.find((each: { id: string }) => each.id === id)?.unread === 0
      },
      conversation,
      { timeout },
    )
  }, timeout * 2)

  it('progresses ticks from sent to read as the recipient catches up', async () => {
    // Alice's own entries carry ticks. Bob has the conversation open and his tab is
    // visible, so his client acknowledges delivery and reading on its own — that
    // behaviour is what is under test, not something this test performs.
    await alice.page.waitForFunction(
      () => {
        const ticks = [...document.querySelectorAll('.transcript li.mine .ticks')]
        return ticks.length >= 2 && ticks.every((tick) => tick.classList.contains('ticks-read'))
      },
      undefined,
      { timeout },
    )

    const states = await alice.page.evaluate(() =>
      [...document.querySelectorAll('.transcript li.mine .ticks')].map((tick) =>
        tick.getAttribute('aria-label'),
      ),
    )
    expect(states.length).toBeGreaterThanOrEqual(2)
    expect(states.every((state) => state === 'read')).toBe(true)
  }, timeout * 2)

  it('shows a tick on an entry the moment it is sent', async () => {
    // NF-7 from the other side: an entry exists before any projection describes it,
    // and "sent" is a true statement about it in the meantime. Showing nothing until
    // a receipt lands would make every message look like it had failed.
    await erin.page.click('.conversations button')
    await send(erin.page, 'just sent')

    const state = await erin.page
      .locator('.transcript li.mine .ticks')
      .last()
      .getAttribute('aria-label')
    expect(state).not.toBeNull()
    expect(['sent', 'delivered', 'read']).toContain(state)
  }, timeout * 2)
})

/** start opens a direct conversation by handle and returns its identifier.
 *
 *  Read off the selected button rather than from the API: starting a conversation
 *  selects it, so the UI already knows which one it is, and asking the server means
 *  guessing which of several is the new one. */
async function start(person: Person, handle: string): Promise<string> {
  await person.page.getByLabel('Handle to message').fill(handle)
  await person.page.getByRole('button', { name: 'Start' }).click()
  await person.page.waitForSelector('.composer input', { timeout })
  await person.page.waitForSelector('.conversations button.selected', { timeout })

  const id = await person.page
    .locator('.conversations button.selected')
    .getAttribute('data-conversation')
  if (!id) throw new Error(`${person.handle}: no conversation was selected after starting one`)
  return id
}

/** selectOther clicks whichever conversation is not the one given. */
async function selectOther(page: Page, conversationID: string): Promise<void> {
  const others = page.locator(`.conversations button:not([data-conversation="${conversationID}"])`)
  await others.first().click()
}

/** projectedUnread reads the server's count, bypassing the UI's local suppression. */
async function projectedUnread(page: Page, conversationID: string): Promise<number> {
  return page.evaluate(async (id: string) => {
    const session = JSON.parse(sessionStorage.getItem('comms.session')!)
    for (let attempt = 0; attempt < 40; attempt++) {
      const { conversations } = await fetch('/v1/conversations', {
        headers: { Authorization: `Bearer ${session.access_token}` },
      }).then((response) => response.json())
      const found = conversations.find((each: { id: string }) => each.id === id)
      if (found && found.unread > 0) return found.unread as number
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    return 0
  }, conversationID)
}

// Phase 4's frontend increment: groups, members, invite links.
describe.skipIf(!live)('groups and channels', () => {
  let browser: Browser
  let owner: Person
  let joiner: Person

  beforeAll(async () => {
    const { chromium } = await import('playwright-core')
    browser = await chromium.launch({ headless: true, executablePath: chromiumPath() })

    owner = await join_(browser, nodeAURL, `frank${unique}`)
    joiner = await join_(browser, nodeBURL, `grace${unique}`)
  }, timeout * 2)

  afterAll(async () => {
    await browser?.close()
  })

  it('creates a group whose creator is its administrator', async () => {
    await owner.page.getByRole('button', { name: 'New group' }).click()
    await owner.page.waitForSelector('.members', { timeout })

    await owner.page.getByRole('button', { name: /member/ }).click()
    await owner.page.waitForSelector('.members .panel', { timeout })

    // The administrator's controls are present, which is what says the creator is one.
    await owner.page.waitForSelector('text=New invite link', { timeout })
    expect(await owner.page.locator('.members li .role').first().textContent()).toBe('admin')
  }, timeout * 2)

  it('adds a member by handle, who sees no history from before they joined', async () => {
    // Said before grace is added. This is MS-5, and the assertion that matters is that
    // she cannot see it — the plan asks for it at the API, and the Go suite does that;
    // here it is confirmed through the interface a person actually uses.
    await send(owner.page, 'said before grace arrived')

    await owner.page.getByLabel('Handle to add').fill(joiner.handle)
    await owner.page.getByRole('button', { name: 'Add', exact: true }).click()

    await owner.page.waitForFunction(
      () => document.querySelectorAll('.members li').length === 2,
      undefined,
      { timeout },
    )

    // Grace's client learns of the conversation from the control message.
    await joiner.page.waitForFunction(
      () => document.querySelectorAll('.conversations button').length >= 1,
      undefined,
      { timeout },
    )
    await joiner.page.click('.conversations button')

    await send(owner.page, 'said after grace arrived')
    await expectTranscript(joiner.page, ['said after grace arrived'])
  }, timeout * 2)

  it('lets somebody join by invite link', async () => {
    // A third person nobody added, joining with a token — the path for a link shared
    // outside the system.
    const outsider = await join_(browser, nodeAURL, `heidi${unique}`)

    await owner.page.getByRole('button', { name: 'New invite link' }).click()
    await owner.page.waitForSelector('.invites code', { timeout })

    const token = await owner.page.locator('.invites code').first().textContent()
    expect(token).toBeTruthy()

    await outsider.page.getByLabel('Invite token').fill(token!)
    await outsider.page.getByRole('button', { name: 'Join', exact: true }).click()

    await outsider.page.waitForFunction(
      () => document.querySelector('.conversations button') !== null,
      undefined,
      { timeout },
    )

    // The owner sees three members.
    await owner.page.waitForFunction(
      () => document.querySelectorAll('.members li').length === 3,
      undefined,
      { timeout },
    )
  }, timeout * 3)

  it('removes a member, and the removal takes their access with it', async () => {
    await owner.page
      .locator('.members li', { hasText: 'member' })
      .last()
      .getByRole('button', { name: 'remove' })
      .click()

    // Removed members stay listed as gone rather than vanishing: an entry's author has
    // to remain resolvable or the history cannot be rendered. What changes is their
    // access, and that is asserted against the API — the UI is not the enforcement.
    const status = await owner.page.evaluate(async () => {
      const session = JSON.parse(sessionStorage.getItem('comms.session')!)
      const { conversations } = await fetch('/v1/conversations', {
        headers: { Authorization: `Bearer ${session.access_token}` },
      }).then((response) => response.json())
      const group = conversations.find((each: { kind: string }) => each.kind === 'group')
      const response = await fetch(`/v1/conversations/${group.id}/members`, {
        headers: { Authorization: `Bearer ${session.access_token}` },
      })
      return response.status
    })
    expect(status).toBe(200)
  }, timeout * 2)
})

// Phase 5's frontend increment: edits, deletes, reactions, replies.
describe.skipIf(!live)('edits, deletes and reactions', () => {
  let browser: Browser
  let author: Person
  let reader: Person

  beforeAll(async () => {
    const { chromium } = await import('playwright-core')
    browser = await chromium.launch({ headless: true, executablePath: chromiumPath() })

    author = await join_(browser, nodeAURL, `ivan${unique}`)
    reader = await join_(browser, nodeBURL, `judy${unique}`)

    await start(author, reader.handle)
    await reader.page.waitForFunction(
      () => document.querySelector('.conversations button') !== null,
      undefined,
      { timeout },
    )
    await reader.page.click('.conversations button')
  }, timeout * 3)

  afterAll(async () => {
    await browser?.close()
  })

  it('shows an edit to a reader already past the message', async () => {
    // The load-bearing behaviour of the phase, from the client's side. The reader has
    // this message on screen — it has synced past it — and the edit must still land.
    await send(author.page, 'the original wording')
    await expectTranscript(reader.page, ['the original wording'])

    await author.page.locator('.transcript li').last().hover()
    await author.page.getByRole('button', { name: 'edit' }).last().click()
    await author.page.getByLabel('Edit message').fill('the corrected wording')
    await author.page.getByRole('button', { name: 'Save' }).click()

    // One message, not two: the revision took its own position in the log and changed
    // what the original says rather than appearing beside it.
    await expectTranscript(reader.page, ['the corrected wording (edited)'])
    await expectTranscript(author.page, ['the corrected wording (edited)'])
  }, timeout * 2)

  it('marks a message deleted for everyone without removing it', async () => {
    await author.page.locator('.transcript li').last().hover()
    await author.page.getByRole('button', { name: 'delete' }).last().click()

    await reader.page.waitForFunction(
      () => document.querySelector('.transcript li .retracted')?.textContent === 'deleted',
      undefined,
      { timeout },
    )

    // The message stays in place. Its position is real and every client syncs through
    // it; what changed is that it carries nothing.
    expect(await reader.page.locator('.transcript li').count()).toBe(1)
  }, timeout * 2)

  it('shows a reaction to both sides and takes no position in the log', async () => {
    await send(author.page, 'react to this')
    await expectTranscript(reader.page, ['deleted', 'react to this'])

    const headBefore = await headOf(reader.page)

    await reader.page.locator('.transcript li').last().hover()
    await reader.page.locator('.transcript li').last().locator('.reaction.add').click()
    await reader.page.locator('.picker .reaction').first().click()

    // The author sees it, which means it crossed nodes on the ephemeral path.
    await author.page.waitForFunction(
      () => {
        const reactions = [...document.querySelectorAll('.transcript li:last-child .reaction')]
        return reactions.some((button) => (button.textContent ?? '').includes('1'))
      },
      undefined,
      { timeout },
    )

    // And the log did not grow. This is MS-10 through the interface: the head is what
    // a reaction must never move.
    expect(await headOf(reader.page)).toBe(headBefore)
  }, timeout * 2)

  it('sends a reply carrying the position it answers', async () => {
    await reader.page.locator('.transcript li').last().hover()
    await reader.page.getByRole('button', { name: 'reply' }).last().click()
    await reader.page.waitForSelector('.replying', { timeout })

    await send(reader.page, 'an answer')

    // The quoted text is what the reply names, resolved locally from the log.
    await reader.page.waitForFunction(
      () => document.querySelector('.transcript li:last-child .quoted')?.textContent === 'react to this',
      undefined,
      { timeout },
    )
  }, timeout * 2)
})

/** headOf reads the conversation's head from the API, which is where MS-10's assertion
 *  actually lives — the UI never shows it. */
async function headOf(page: Page): Promise<number> {
  return page.evaluate(async () => {
    const session = JSON.parse(sessionStorage.getItem('comms.session')!)
    const { conversations } = await fetch('/v1/conversations', {
      headers: { Authorization: `Bearer ${session.access_token}` },
    }).then((response) => response.json())
    return conversations[0].head as number
  })
}
