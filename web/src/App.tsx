// Phase 2's frontend increment: log in, hold one conversation, send and receive.
//
// State is in memory. A reload loses the messages and refetches them, which is the
// honest behaviour for this phase — the local store arrives in phase 6, and
// pretending to have it now would mean writing a cache this phase cannot verify.
//
// The conversation list, unread badges and delivery ticks are phase 3, because they
// depend on projections that do not exist yet.

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'

import { ApiError, Client, decodeBody, login, register, SessionExpired } from './api'
import type { Conversation, Entry, Session } from './api'
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
          <ul className="conversations">
            {conversations.map((conversation) => (
              <li key={conversation.id}>
                <button
                  type="button"
                  className={conversation.id === selected ? 'selected' : ''}
                  onClick={() => setSelected(conversation.id)}
                >
                  {label(conversation, handles, client.accountID)}
                </button>
              </li>
            ))}
          </ul>
          {conversations.length === 0 && <p className="muted">No conversations yet.</p>}
        </aside>

        <section className="conversation">
          {selected ? (
            <>
              <Transcript entries={entries} me={client.accountID} handles={handles} />
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
  for (const [accountID, handle] of Object.entries(handles)) {
    if (accountID !== me) return handle
  }
  return `${conversation.kind} ${conversation.id.slice(0, 8)}`
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
}: {
  entries: Entry[]
  me: string
  handles: Record<string, string>
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
        </li>
      ))}
    </ol>
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
