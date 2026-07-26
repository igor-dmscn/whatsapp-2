// The browser client through phase 3: log in, hold conversations, send and receive,
// with unread badges and delivery ticks.
//
// Message state is in memory. A reload refetches, which is honest for now — the local
// store arrives in phase 6.
//
// This is the first UI that has to tolerate eventual consistency. Badges and ticks
// come from projections built asynchronously from the log (ADR-0002), so an entry is
// on screen before its badge moves and the interface must be right during that window
// rather than waiting for it to close (NF-7). Two places that shows up: the open
// conversation's badge is suppressed locally rather than waiting for the projection to
// clear it, and a just-sent entry renders as sent rather than as nothing at all.

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'

import { ApiError, Client, decodeBody, deliveryOf, login, register, SessionExpired } from './api'
import type { Conversation, DeliveryState, Entry, Invite, Member, Role, Session } from './api'
import { Sync } from './sync'

// sessionStorage, not localStorage, and the difference matters here: sessionStorage
// is per-tab, so two tabs are two independent logins. Sharing one session across
// tabs would make it impossible to watch two people talk to each other on one
// machine, which is how this phase is verified.
const sessionKey = 'comms.session'

function loadSession(): Session | null {
  const raw = sessionStorage.getItem(sessionKey)
  if (!raw) return null
  try {
    return JSON.parse(raw) as Session
  } catch {
    sessionStorage.removeItem(sessionKey)
    return null
  }
}

function describe(error: unknown): string {
  if (error instanceof ApiError) return error.message
  if (error instanceof Error) return error.message
  return 'something went wrong'
}

export function App() {
  const [session, setSession] = useState<Session | null>(loadSession)

  const remember = useCallback((next: Session | null) => {
    if (next) sessionStorage.setItem(sessionKey, JSON.stringify(next))
    else sessionStorage.removeItem(sessionKey)
    setSession(next)
  }, [])

  // Stable, because Workspace builds its socket from it. A fresh closure each
  // render would rebuild the socket on every render — see the ref in Workspace.
  const signOut = useCallback(() => remember(null), [remember])

  if (!session) return <SignIn onSignedIn={remember} />
  // Keyed by device so signing out and back in builds a fresh Client and Sync
  // rather than reusing ones holding a revoked token.
  return <Workspace key={session.device_id} session={session} onSignOut={signOut} />
}

// --- sign in ---

function SignIn({ onSignedIn }: { onSignedIn: (session: Session) => void }) {
  const [isNew, setIsNew] = useState(false)
  const [handle, setHandle] = useState('')
  const [email, setEmail] = useState('')
  const [passphrase, setPassphrase] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    setError(null)
    setBusy(true)
    try {
      const session = isNew
        ? await register(handle, email, passphrase, 'browser')
        : await login(handle, passphrase, 'browser')
      onSignedIn(session)
    } catch (failure) {
      setError(describe(failure))
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="centred">
      <form className="card" onSubmit={submit}>
        <h1>comms</h1>
        <p className="muted">{isNew ? 'Create an account.' : 'Sign in.'}</p>

        <label>
          Handle
          <input
            value={handle}
            onChange={(event) => setHandle(event.target.value)}
            autoComplete="username"
            autoFocus
            required
            minLength={3}
            maxLength={32}
          />
        </label>

        {isNew && (
          <label>
            Email
            <input
              type="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              autoComplete="email"
              required
            />
          </label>
        )}

        <label>
          Passphrase
          <input
            type="password"
            value={passphrase}
            onChange={(event) => setPassphrase(event.target.value)}
            autoComplete={isNew ? 'new-password' : 'current-password'}
            required
            minLength={isNew ? 12 : undefined}
          />
          {isNew && <span className="hint">At least 12 characters. Length beats punctuation.</span>}
        </label>

        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}

        <button type="submit" disabled={busy}>
          {busy ? 'Working…' : isNew ? 'Create account' : 'Sign in'}
        </button>

        <button
          type="button"
          className="link"
          onClick={() => {
            setIsNew(!isNew)
            setError(null)
          }}
        >
          {isNew ? 'I already have an account' : 'Create an account instead'}
        </button>
      </form>
    </main>
  )
}

// --- workspace ---

function Workspace({ session, onSignOut }: { session: Session; onSignOut: () => void }) {
  const [error, setError] = useState<string | null>(null)
  const [conversations, setConversations] = useState<Conversation[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  // Handles learned by looking them up. The API resolves handle to account, not the
  // reverse, so a conversation loaded from the list is labelled by its identifier
  // until someone is looked up. Proper titles come with phase 3's conversation list.
  const [handles, setHandles] = useState<Record<string, string>>({})

  const client = useMemo(
    () =>
      new Client(session, (refreshed) => sessionStorage.setItem(sessionKey, JSON.stringify(refreshed))),
    [session],
  )

  // The failure handler lives in a ref so the socket does not depend on a
  // callback's identity. Built into the Sync directly, an ordinary re-render that
  // produced a fresh closure would rebuild it — dropping the connection and every
  // entry held in it. That failure looks exactly like the server losing messages,
  // which is the most expensive thing it could be mistaken for.
  const fatal = useRef<(failure: unknown) => void>(() => {})
  fatal.current = (failure) => {
    if (failure instanceof SessionExpired) onSignOut()
    else setError(describe(failure))
  }

  const sync = useMemo(
    () =>
      new Sync({
        token: () => client.token(),
        fetchEntries: (conversationID, after) => client.entries(conversationID, after),
        onFatal: (failure) => fatal.current(failure),
      }),
    [client],
  )

  useEffect(() => {
    sync.start()
    return () => sync.stop()
  }, [sync])

  const snapshot = useSyncExternalStore(sync.subscribe, sync.getSnapshot)

  const load = useCallback(async () => {
    try {
      const found = await client.conversations()
      setConversations(found)
      for (const conversation of found) sync.follow(conversation.id)
      setSelected((current) => current ?? found[0]?.id ?? null)
    } catch (failure) {
      fatal.current(failure)
    }
  }, [client, sync])

  // The socket reports gaps for conversations that have entries; one with none
  // would go unmentioned. The list is what makes those visible.
  useEffect(() => {
    void load()
  }, [load])

  // Projections are polled, not pushed. There is no event telling a client that
  // somebody else's read mark moved, and inventing one would mean a second delivery
  // path with its own ordering and loss questions, for information that is
  // decorative. Three seconds is well inside NF-7's convergence window.
  //
  // ponytail: fixed interval, running whether the tab is visible or not. Pushing
  // receipts over the existing socket is phase 10's work, alongside presence.
  useEffect(() => {
    const timer = setInterval(() => void load(), 3000)
    return () => clearInterval(timer)
  }, [load])

  // Acknowledge what is on screen. Delivered always — the entries are on this device
  // either way. Read only while the tab is visible, because telling someone their
  // message was read when it was rendered into a background tab is exactly the lie a
  // read receipt exists not to tell.
  const acknowledged = useRef(new Map<string, number>())
  useEffect(() => {
    if (!selected) return
    const held = snapshot.conversations.get(selected) ?? []
    const highest = held.at(-1)?.sequence ?? 0
    if (highest === 0 || (acknowledged.current.get(selected) ?? 0) >= highest) return

    acknowledged.current.set(selected, highest)
    const readThrough = document.visibilityState === 'visible' ? highest : 0
    client
      .acknowledge(selected, highest, readThrough)
      .then(() => void load())
      .catch((failure) => {
        // Forgotten so the next change tries again. A lost receipt is not worth
        // failing anything over, but it is worth retrying.
        acknowledged.current.delete(selected)
        if (failure instanceof SessionExpired) onSignOut()
      })
  }, [selected, snapshot, client, load, onSignOut])

  // A conversation started while already connected arrives as an entry for a
  // conversation this list has never heard of — the socket carries entries, not
  // memberships. Asked once per identifier, so a list that comes back without it
  // cannot turn into a refetch loop.
  const asked = useRef(new Set<string>())
  useEffect(() => {
    const known = new Set(conversations.map((conversation) => conversation.id))
    const missing = [...snapshot.conversations.keys()].filter(
      (id) => !known.has(id) && !asked.current.has(id),
    )
    if (missing.length === 0) return

    for (const id of missing) asked.current.add(id)
    void load()
  }, [snapshot, conversations, load])

  const startDirect = useCallback(
    async (handle: string) => {
      setError(null)
      try {
        const account = await client.lookupHandle(handle)
        const conversation = await client.startDirect(account.id)
        setHandles((current) => ({ ...current, [account.id]: account.handle }))
        // StartDirect is idempotent on the pair, so this may be one that is
        // already open. Replacing rather than appending keeps the list unique.
        setConversations((current) => [
          conversation,
          ...current.filter((existing) => existing.id !== conversation.id),
        ])
        sync.follow(conversation.id)
        setSelected(conversation.id)
      } catch (failure) {
        fatal.current(failure)
      }
    },
    [client, sync],
  )

  const entries = selected ? (snapshot.conversations.get(selected) ?? []) : []
  const current = conversations.find((each) => each.id === selected)

  return (
    <div className="workspace">
      <header>
        <strong>{client.handle}</strong>
        <span className={`status status-${snapshot.status}`}>{snapshot.status}</span>
        <button type="button" className="link" onClick={onSignOut}>
          Sign out
        </button>
      </header>

      {error && (
        <p className="error banner" role="alert">
          {error}
          <button type="button" className="link" onClick={() => setError(null)}>
            dismiss
          </button>
        </p>
      )}

      <div className="panes">
        <aside>
          <StartDirect onStart={startDirect} />
          <NewConversation
            onGroup={async () => {
              const conversation = await client.startGroup()
              await load()
              setSelected(conversation.id)
              sync.follow(conversation.id)
            }}
            onChannel={async () => {
              const conversation = await client.startChannel()
              await load()
              setSelected(conversation.id)
              sync.follow(conversation.id)
            }}
            onJoin={async (token) => {
              const conversation = await client.redeemInvite(token)
              await load()
              setSelected(conversation.id)
              sync.follow(conversation.id)
            }}
            onError={setError}
          />
          <ul className="conversations">
            {conversations.map((conversation) => (
              <li key={conversation.id}>
                <button
                  type="button"
                  data-conversation={conversation.id}
                  className={conversation.id === selected ? 'selected' : ''}
                  onClick={() => setSelected(conversation.id)}
                >
                  <span className="name">{label(conversation, handles, client.accountID)}</span>
                  {/* Suppressed for the open conversation rather than waited on. The
                      projection clears it within a second or two, and showing "3
                      unread" against the conversation being read is the wrong answer
                      even while it is the freshly projected one. */}
                  {conversation.unread > 0 && conversation.id !== selected && (
                    <span className="badge" aria-label={`${conversation.unread} unread`}>
                      {conversation.unread}
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
          {conversations.length === 0 && <p className="muted">No conversations yet.</p>}
        </aside>

        <section className="conversation">
          {selected ? (
            <>
              {current && current.kind !== 'direct' && (
                <Members
                  client={client}
                  conversation={current}
                  onChanged={load}
                  onError={setError}
                />
              )}
              <Transcript
                entries={entries}
                me={client.accountID}
                handles={handles}
                conversation={current}
              />
              <Composer
                onSend={async (text, clientEntryID) => {
                  const entry = await client.send(selected, clientEntryID, text)
                  sync.accept(entry)
                }}
                onError={setError}
              />
            </>
          ) : (
            <p className="muted centred">Look up a handle to start a conversation.</p>
          )}
        </section>
      </div>
    </div>
  )
}

/** label names a conversation by the counterpart's handle when it is known, and by
 *  its identifier when it is not. Phase 3 replaces this with a real list. */
function label(conversation: Conversation, handles: Record<string, string>, me: string): string {
  // A group or channel has no single counterpart to name it after, so it is named by
  // what it is. Real titles are a field somebody sets, which this system has not been
  // asked for.
  if (conversation.kind !== 'direct') {
    return `${conversation.kind} ${conversation.id.slice(0, 8)}`
  }
  for (const [accountID, handle] of Object.entries(handles)) {
    if (accountID !== me) return handle
  }
  return `direct ${conversation.id.slice(0, 8)}`
}

/** NewConversation creates groups and channels, and joins by invite link. */
function NewConversation({
  onGroup,
  onChannel,
  onJoin,
  onError,
}: {
  onGroup: () => Promise<void>
  onChannel: () => Promise<void>
  onJoin: (token: string) => Promise<void>
  onError: (message: string) => void
}) {
  const [token, setToken] = useState('')
  const guard = (work: () => Promise<void>) => () => work().catch((failure) => onError(describe(failure)))

  return (
    <div className="new-conversation">
      <div className="row">
        <button type="button" onClick={guard(onGroup)}>
          New group
        </button>
        <button type="button" onClick={guard(onChannel)}>
          New channel
        </button>
      </div>
      <form
        className="start"
        onSubmit={(event) => {
          event.preventDefault()
          const wanted = token.trim()
          if (!wanted) return
          setToken('')
          void guard(() => onJoin(wanted))()
        }}
      >
        <input
          value={token}
          onChange={(event) => setToken(event.target.value)}
          placeholder="invite token"
          aria-label="Invite token"
        />
        <button type="submit">Join</button>
      </form>
    </div>
  )
}

/** Members shows who belongs to a group or channel, and lets an administrator
 *  change it.
 *
 *  The controls are shown only to an administrator, but that is presentation: the
 *  server refuses every one of these calls from anybody else, which is where the rule
 *  actually lives. */
function Members({
  client,
  conversation,
  onChanged,
  onError,
}: {
  client: Client
  conversation: Conversation
  onChanged: () => Promise<void>
  onError: (message: string) => void
}) {
  const [members, setMembers] = useState<Member[]>([])
  const [invites, setInvites] = useState<Invite[]>([])
  const [handle, setHandle] = useState('')
  const [open, setOpen] = useState(false)

  const administrator = conversation.role === 'admin'

  const refresh = useCallback(async () => {
    try {
      setMembers(await client.members(conversation.id))
      if (administrator) setInvites(await client.invites(conversation.id))
    } catch (failure) {
      onError(describe(failure))
    }
  }, [client, conversation.id, administrator, onError])

  useEffect(() => {
    if (open) void refresh()
  }, [open, refresh])

  const act = (work: () => Promise<unknown>) => async () => {
    try {
      await work()
      await refresh()
      await onChanged()
    } catch (failure) {
      onError(describe(failure))
    }
  }

  const active = members.filter((member) => member.left_at === null)

  return (
    <section className="members">
      <button type="button" className="link" onClick={() => setOpen(!open)}>
        {open ? 'Hide' : 'Show'} {active.length} member{active.length === 1 ? '' : 's'}
      </button>

      {open && (
        <div className="panel">
          <ul>
            {active.map((member) => (
              <li key={member.account_id}>
                <span className="who">{member.account_id.slice(0, 8)}</span>
                <span className="role">{member.role}</span>
                {/* Their join point is on screen because it is the whole of the
                    history policy, and the thing most likely to look like a bug when
                    a new member sees an empty conversation (MS-5). */}
                <span className="sequence">from #{member.visible_from}</span>
                {administrator && member.account_id !== client.accountID && (
                  <>
                    <button
                      type="button"
                      className="link"
                      onClick={act(() =>
                        client.changeRole(conversation.id, member.account_id, nextRole(member.role)),
                      )}
                    >
                      make {nextRole(member.role)}
                    </button>
                    {member.role !== 'admin' && (
                      <button
                        type="button"
                        className="link"
                        onClick={act(() => client.changeRole(conversation.id, member.account_id, 'admin'))}
                      >
                        make admin
                      </button>
                    )}
                    <button
                      type="button"
                      className="link"
                      onClick={act(() => client.removeMember(conversation.id, member.account_id))}
                    >
                      remove
                    </button>
                  </>
                )}
              </li>
            ))}
          </ul>

          {administrator && (
            <>
              <form
                className="start"
                onSubmit={(event) => {
                  event.preventDefault()
                  const wanted = handle.trim()
                  if (!wanted) return
                  setHandle('')
                  void act(async () => {
                    const account = await client.lookupHandle(wanted)
                    await client.addMember(conversation.id, account.id)
                  })()
                }}
              >
                <input
                  value={handle}
                  onChange={(event) => setHandle(event.target.value)}
                  placeholder="handle to add"
                  aria-label="Handle to add"
                />
                <button type="submit">Add</button>
              </form>

              <div className="row">
                <button type="button" onClick={act(() => client.createInvite(conversation.id))}>
                  New invite link
                </button>
              </div>

              <ul className="invites">
                {invites
                  .filter((invite) => !invite.revoked)
                  .map((invite) => (
                    <li key={invite.id}>
                      {/* Selectable rather than a copy button: a clipboard write needs
                          a permission prompt in some browsers, and the token is the
                          thing being shared. */}
                      <code>{invite.token}</code>
                      <span className="muted">
                        {invite.uses} use{invite.uses === 1 ? '' : 's'}
                        {invite.max_uses > 0 && ` of ${invite.max_uses}`}
                      </span>
                      <button
                        type="button"
                        className="link"
                        onClick={act(() => client.revokeInvite(invite.id))}
                      >
                        revoke
                      </button>
                    </li>
                  ))}
              </ul>
            </>
          )}

          <button type="button" className="link" onClick={act(() => client.leave(conversation.id))}>
            Leave this {conversation.kind}
          </button>
        </div>
      )}
    </section>
  )
}

/** nextRole is what the reader/member control toggles between.
 *
 *  Administration is deliberately not in this cycle. It is granted by its own
 *  labelled control, because a click that hands somebody the power to remove you is
 *  not a click anybody should make while cycling through options — and there is no
 *  undo for it. A group does need a second administrator eventually, which is why the
 *  control exists at all rather than being left to the API.
 */
function nextRole(role: Role): Role {
  return role === 'reader' ? 'member' : 'reader'
}

function StartDirect({ onStart }: { onStart: (handle: string) => Promise<void> }) {
  const [handle, setHandle] = useState('')

  return (
    <form
      className="start"
      onSubmit={async (event) => {
        event.preventDefault()
        const wanted = handle.trim()
        if (!wanted) return
        setHandle('')
        await onStart(wanted)
      }}
    >
      <input
        value={handle}
        onChange={(event) => setHandle(event.target.value)}
        placeholder="handle"
        aria-label="Handle to message"
      />
      <button type="submit">Start</button>
    </form>
  )
}

function Transcript({
  entries,
  me,
  handles,
  conversation,
}: {
  entries: Entry[]
  me: string
  handles: Record<string, string>
  conversation: Conversation | undefined
}) {
  return (
    <ol className="transcript">
      {entries.map((entry) => (
        <li key={entry.sequence} className={entry.author_id === me ? 'mine' : 'theirs'}>
          <span className="who">
            {entry.author_id === me ? 'you' : (handles[entry.author_id] ?? entry.author_id.slice(0, 8))}
          </span>
          <span className="body">{decodeBody(entry.body)}</span>
          {/* The sequence is on screen deliberately. It is what the sync protocol
              turns on, and seeing a hole appear and close is the fastest way to
              tell gap filling from a rendering bug. */}
          <span className="sequence">#{entry.sequence}</span>
          {entry.author_id === me && conversation && (
            <Ticks state={deliveryOf(entry.sequence, conversation)} />
          )}
        </li>
      ))}
    </ol>
  )
}

/** Ticks shows MS-13's three states.
 *
 *  A just-sent entry shows one tick immediately, derived from marks that have not
 *  moved yet, rather than showing nothing until a projection lands. "Sent" is a true
 *  statement about an entry the server has already assigned a position to — the
 *  uncertainty is only about what happened to it afterwards. */
function Ticks({ state }: { state: DeliveryState }) {
  return (
    <span className={`ticks ticks-${state}`} title={state} aria-label={state}>
      {state === 'sent' ? '✓' : '✓✓'}
    </span>
  )
}

function Composer({
  onSend,
  onError,
}: {
  onSend: (text: string, clientEntryID: string) => Promise<void>
  onError: (message: string) => void
}) {
  const [draft, setDraft] = useState('')
  // The identifier belongs to the draft, not to the attempt. A send that fails and
  // is tried again reuses it, so the server recognises the retry and returns the
  // entry the first attempt created rather than writing a second one (MS-2).
  const [clientEntryID, setClientEntryID] = useState(() => crypto.randomUUID())
  const [sending, setSending] = useState(false)

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const text = draft.trim()
    if (!text || sending) return

    setSending(true)
    try {
      await onSend(text, clientEntryID)
      setDraft('')
      setClientEntryID(crypto.randomUUID())
    } catch (failure) {
      // The draft and its identifier are kept, so pressing send again is a retry
      // of the same entry rather than a new one.
      onError(describe(failure))
    } finally {
      setSending(false)
    }
  }

  return (
    <form className="composer" onSubmit={submit}>
      <input
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        placeholder="Message"
        aria-label="Message"
        autoFocus
      />
      <button type="submit" disabled={sending || draft.trim() === ''}>
        Send
      </button>
    </form>
  )
}
