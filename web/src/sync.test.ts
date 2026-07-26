// What is tested here is gap detection, because that is the client's half of the
// only delivery guarantee this system offers. Redis is allowed to drop a broadcast
// (ADR-0005); if these assertions stop holding, messages go missing silently.
//
// The socket is faked and the fetch is a stub. The equivalent assertions against
// real Postgres and real Redis live in internal/messaging/messaging_test.go — this
// file is about whether the *client* draws the right conclusion.

import { describe, expect, it } from 'vitest'

import { decodeBody, encodeBody, type Entry } from './api'
import { Sync, type Socket } from './sync'

const conversation = 'conversation-1'

function entry(sequence: number, text: string): Entry {
  return {
    id: `entry-${sequence}`,
    conversation_id: conversation,
    sequence,
    author_id: 'account-2',
    client_entry_id: `client-${sequence}`,
    kind: 'message',
    content_type: 'text/plain; charset=utf-8',
    body: encodeBody(text),
    created_at: '2026-01-01T00:00:00.000Z',
  }
}

function frameFor(item: Entry) {
  return {
    type: 'entry',
    conversation_id: item.conversation_id,
    entry_id: item.id,
    sequence: item.sequence,
    author_id: item.author_id,
    client_entry_id: item.client_entry_id,
    kind: item.kind,
    content_type: item.content_type,
    body: item.body,
    created_at: item.created_at,
  }
}

class FakeSocket implements Socket {
  sent: string[] = []
  onopen: ((event: unknown) => void) | null = null
  onmessage: ((event: { data: unknown }) => void) | null = null
  onclose: ((event: unknown) => void) | null = null
  onerror: ((event: unknown) => void) | null = null

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {}

  frames(): { type: string; [key: string]: unknown }[] {
    return this.sent.map((raw) => JSON.parse(raw))
  }

  frameOfType(type: string): Record<string, unknown> | undefined {
    return this.frames().find((frame) => frame.type === type)
  }

  deliver(frame: unknown): void {
    this.onmessage?.({ data: JSON.stringify(frame) })
  }
}

/** flush lets queued promises settle. The fill loop is several awaits deep, so one
 *  microtask turn is not enough. */
const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

type Harness = {
  sync: Sync
  socket: FakeSocket
  /** server is what a fetch would find, keyed by conversation. */
  server: Entry[]
  fetches: { conversationID: string; after: number }[]
  texts: () => string[]
  sequences: () => number[]
}

function harness(options: { pageSize?: number } = {}): Harness {
  const socket = new FakeSocket()
  const server: Entry[] = []
  const fetches: { conversationID: string; after: number }[] = []

  const sync = new Sync({
    token: async () => 'access-token',
    open: () => socket,
    fetchEntries: async (conversationID, after) => {
      fetches.push({ conversationID, after })
      const matching = server.filter((item) => item.sequence > after)
      return options.pageSize ? matching.slice(0, options.pageSize) : matching
    },
  })

  return {
    sync,
    socket,
    server,
    fetches,
    texts: () => (sync.getSnapshot().conversations.get(conversation) ?? []).map((item) => decodeBody(item.body)),
    sequences: () => (sync.getSnapshot().conversations.get(conversation) ?? []).map((item) => item.sequence),
  }
}

/** connected starts the socket and completes the handshake. */
async function connected(test: Harness): Promise<void> {
  test.sync.start()
  test.socket.onopen?.({})
  await flush()
  test.socket.deliver({ type: 'ready', account_id: 'account-1', device_id: 'device-1' })
  await flush()
}

describe('handshake', () => {
  it('authenticates in the first frame, never in the URL', async () => {
    const test = harness()
    await connected(test)

    expect(test.socket.frames()[0]).toEqual({ type: 'authenticate', token: 'access-token' })
    test.sync.stop()
  })

  it('resumes with an empty cursor on a first-ever connection', async () => {
    const test = harness()
    await connected(test)

    expect(test.socket.frameOfType('resume')).toEqual({ type: 'resume', cursor: {} })
    test.sync.stop()
  })

  it('reports live only once the server says ready', async () => {
    const test = harness()
    test.sync.start()
    test.socket.onopen?.({})
    await flush()

    expect(test.sync.getSnapshot().status).toBe('connecting')

    test.socket.deliver({ type: 'ready', account_id: 'account-1', device_id: 'device-1' })
    await flush()

    expect(test.sync.getSnapshot().status).toBe('live')
    test.sync.stop()
  })
})

describe('live delivery', () => {
  it('applies entries that arrive in order without fetching anything', async () => {
    const test = harness()
    await connected(test)

    test.socket.deliver(frameFor(entry(1, 'first')))
    test.socket.deliver(frameFor(entry(2, 'second')))
    await flush()

    expect(test.texts()).toEqual(['first', 'second'])
    // One fetch, from learning the conversation exists — and it starts after the
    // entry that revealed it rather than re-reading history it was just handed.
    // The second entry, being contiguous, costs nothing.
    expect(test.fetches).toEqual([{ conversationID: conversation, after: 1 }])
    test.sync.stop()
  })

  it('orders entries by sequence even when the socket delivers them backwards', async () => {
    const test = harness()
    await connected(test)
    test.server.push(entry(1, 'first'), entry(2, 'second'))

    test.socket.deliver(frameFor(entry(2, 'second')))
    test.socket.deliver(frameFor(entry(1, 'first')))
    await flush()

    expect(test.texts()).toEqual(['first', 'second'])
    test.sync.stop()
  })

  it('counts a repeated sequence once', async () => {
    const test = harness()
    await connected(test)

    test.socket.deliver(frameFor(entry(1, 'only')))
    test.socket.deliver(frameFor(entry(1, 'only')))
    await flush()

    expect(test.sequences()).toEqual([1])
    test.sync.stop()
  })

  it('does not show the sender their own entry twice', async () => {
    const test = harness()
    await connected(test)
    test.sync.follow(conversation)
    await flush()

    // The response to a send arrives, and the broadcast of the same entry arrives
    // separately. Order between them is not guaranteed and must not matter.
    test.sync.accept(entry(1, 'mine'))
    test.socket.deliver(frameFor(entry(1, 'mine')))
    await flush()

    expect(test.sequences()).toEqual([1])
    test.sync.stop()
  })
})

describe('gap detection', () => {
  it('fetches what is missing below a live entry, and keeps the live entry', async () => {
    const test = harness()
    await connected(test)
    test.sync.follow(conversation)
    await flush()

    test.sync.accept(entry(1, 'one'))
    // Two and three were published while Redis dropped them. Four arrives live.
    test.server.push(entry(1, 'one'), entry(2, 'two'), entry(3, 'three'), entry(4, 'four'))
    test.socket.deliver(frameFor(entry(4, 'four')))
    await flush()

    expect(test.texts()).toEqual(['one', 'two', 'three', 'four'])
    expect(test.sequences()).toEqual([1, 2, 3, 4])
    test.sync.stop()
  })

  it('resumes from the last contiguous sequence, not the highest one held', async () => {
    const test = harness()
    await connected(test)
    test.sync.accept(entry(1, 'one'))
    // Seven arrives with nothing in between and the fetch finds nothing yet, so
    // the hole stays open. Resuming at seven would abandon two through six.
    test.socket.deliver(frameFor(entry(7, 'seven')))
    await flush()

    test.socket.sent.length = 0
    test.socket.deliver({ type: 'ready', account_id: 'account-1', device_id: 'device-1' })
    await flush()

    expect(test.socket.frameOfType('resume')).toEqual({
      type: 'resume',
      cursor: { [conversation]: 1 },
    })
    test.sync.stop()
  })

  it('fills a gap the server reports on resume', async () => {
    const test = harness()
    await connected(test)
    test.server.push(entry(1, 'one'), entry(2, 'two'))

    test.socket.deliver({
      type: 'gaps',
      gaps: [{ conversation_id: conversation, from: 1, to: 2 }],
    })
    await flush()

    expect(test.texts()).toEqual(['one', 'two'])
    test.sync.stop()
  })

  it('pages through a gap larger than one response', async () => {
    const test = harness({ pageSize: 2 })
    await connected(test)
    for (let sequence = 1; sequence <= 5; sequence++) {
      test.server.push(entry(sequence, `entry ${sequence}`))
    }

    test.socket.deliver(frameFor(entry(5, 'entry 5')))
    await flush()

    expect(test.sequences()).toEqual([1, 2, 3, 4, 5])
    expect(test.fetches.map((fetch) => fetch.after)).toEqual([0, 2, 4, 5])
    test.sync.stop()
  })

  it('stops treating history it may not see as a gap', async () => {
    // A member joining an existing conversation is only entitled to entries from
    // their join point. The server returns nothing below it, forever. Without the
    // mark closing over that prefix, every later entry looks like a hole and the
    // client refetches for the life of the connection.
    const test = harness()
    await connected(test)
    test.server.push(entry(40, 'first visible'), entry(41, 'second visible'))

    test.socket.deliver(frameFor(entry(41, 'second visible')))
    await flush()

    const afterFirstFill = test.fetches.length
    test.server.push(entry(42, 'third visible'))
    test.socket.deliver(frameFor(entry(42, 'third visible')))
    await flush()

    expect(test.sequences()).toEqual([40, 41, 42])
    // The contiguous entry causes no fetch at all.
    expect(test.fetches.length).toBe(afterFirstFill)
    test.sync.stop()
  })

  it('does not run two fills over one conversation at once', async () => {
    const test = harness()
    await connected(test)
    test.server.push(entry(1, 'one'), entry(2, 'two'), entry(3, 'three'))

    // Three gap notices in the same tick. A concurrent pass per notice would fetch
    // the same pages three times and race on the mark.
    test.socket.deliver(frameFor(entry(3, 'three')))
    test.socket.deliver(frameFor(entry(3, 'three')))
    test.socket.deliver({ type: 'gaps', gaps: [{ conversation_id: conversation, from: 1, to: 3 }] })
    await flush()

    expect(test.sequences()).toEqual([1, 2, 3])
    expect(test.fetches.length).toBeLessThanOrEqual(3)
    test.sync.stop()
  })
})

describe('payloads', () => {
  it('survives text the server never interprets', async () => {
    // The server stores bytes and does not parse them (ADR-0001). Round-tripping
    // through base64 is the client's job to get right, including astral-plane
    // characters that a naive charCode loop truncates.
    const awkward = 'héllo 🌍 — ünicode\n\ttabbed'
    expect(decodeBody(encodeBody(awkward))).toBe(awkward)
  })
})
