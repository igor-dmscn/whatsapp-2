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

import { ApiError, Client, deliveryOf, login, register, SessionExpired } from './api'
import type { Attachment, Conversation, DeliveryState, Invite, Member, Role, Session } from './api'
import { clientEntryID } from './ids'
import { Sync } from './sync'
import type { PresenceFrame } from './sync'
import { LocalStore, type Hit } from './store'
import { resolve, summarise, type Message, type ReactionsBySequence } from './transcript'
import { Call, type CallFrame, type CallState } from './call'

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
  // undefined while SQLite is still loading, null when it could not be opened.
  const [store, setStore] = useState<LocalStore | null | undefined>(undefined)

  useEffect(() => {
    let live = true
    LocalStore.open()
      .then((opened) => {
        if (live) setStore(opened)
        else opened.close()
      })
      .catch(() => {
        // No local store. The client then behaves as it did before phase 6 — in
        // memory, refetching on reload — which is a worse experience and a working
        // one. Refusing to start would be the wrong trade.
        if (live) setStore(null)
      })
    return () => {
      live = false
    }
  }, [])

  const remember = useCallback((next: Session | null) => {
    if (next) sessionStorage.setItem(sessionKey, JSON.stringify(next))
    else sessionStorage.removeItem(sessionKey)
    setSession(next)
  }, [])

  // Stable, because Workspace builds its socket from it. A fresh closure each
  // render would rebuild the socket on every render — see the ref in Workspace.
  const signOut = useCallback(() => remember(null), [remember])

  if (!session) return <SignIn onSignedIn={remember} />

  // Waiting on SQLite rather than rendering an empty conversation list and filling it
  // in: the whole point of the store is that the first paint is the real one, and a
  // flash of "no conversations" would undo it.
  if (store === undefined) {
    return (
      <main className="centred">
        <p className="muted">Opening local store…</p>
      </main>
    )
  }

  // Keyed by device so signing out and back in builds a fresh Client and Sync
  // rather than reusing ones holding a revoked token.
  return <Workspace key={session.device_id} session={session} store={store} onSignOut={signOut} />
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

function Workspace({
  session,
  store,
  onSignOut,
}: {
  session: Session
  store: LocalStore | null
  onSignOut: () => void
}) {
  const [error, setError] = useState<string | null>(null)
  const [conversations, setConversations] = useState<Conversation[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  // Handles learned by looking them up. The API resolves handle to account, not the
  // reverse, so a conversation loaded from the list is labelled by its identifier
  // until someone is looked up. Proper titles come with phase 3's conversation list.
  const [handles, setHandles] = useState<Record<string, string>>({})
  // The position the composer is replying to, zero for none.
  const [replyTo, setReplyTo] = useState(0)

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

  // Attachment epochs: a bump counter per attachment, so that "look at this again"
  // becomes a re-fetch in the one component showing it rather than a re-render of the
  // whole transcript. In a ref for the same reason as fatal above — the socket must not
  // depend on the identity of a callback that changes every render.
  const [epochs, setEpochs] = useState<Record<string, number>>({})
  const bump = useRef<(attachmentID: string) => void>(() => {})
  bump.current = (attachmentID) =>
    setEpochs((held) => ({ ...held, [attachmentID]: (held[attachmentID] ?? 0) + 1 }))

  // Presence, and it is deliberately not in the local store: both halves of it are false
  // within seconds, and a database is for things worth keeping. Held per conversation so
  // switching between two does not show the wrong people as typing.
  const [present, setPresent] = useState<Record<string, { online: string[]; typing: string[] }>>({})
  // When this client last said it was typing, so keystrokes do not become frames.
  const lastTyped = useRef(0)
  const presence = useRef<(frame: PresenceFrame) => void>(() => {})
  presence.current = (frame) =>
    setPresent((held) => {
      if (frame.type === 'presence') {
        return { ...held, [frame.conversation_id]: { online: frame.online, typing: frame.typing } }
      }

      // A typing change patches one name. The snapshot from the next poll is what
      // corrects this if a push was lost, which is why the poll exists at all.
      const current = held[frame.conversation_id] ?? { online: [], typing: [] }
      const typing = frame.typing
        ? [...new Set([...current.typing, frame.account_id])]
        : current.typing.filter((accountID) => accountID !== frame.account_id)
      return { ...held, [frame.conversation_id]: { ...current, typing } }
    })

  const sync = useMemo(
    () =>
      new Sync({
        token: () => client.token(),
        fetchEntries: (conversationID, after) => client.entries(conversationID, after),
        postEntry: (conversationID, clientEntryID, text, replyTo) =>
          client.send(conversationID, clientEntryID, text, replyTo),
        onFatal: (failure) => fatal.current(failure),
        onAttachmentChanged: (attachmentID) => bump.current(attachmentID),
        onCallFrame: (frame) => callFrame.current(frame),
        onCallChanged: (conversationID) => callChanged.current(conversationID),
        onPresence: (frame) => presence.current(frame),
        store: store ?? undefined,
      }),
    [client, store],
  )

  // Asked on a timer while a conversation is open, which is what makes presence soft
  // state rather than something to keep in step. Every answer is a whole snapshot, so a
  // missed push, a reconnect and a first render all repair themselves the same way.
  useEffect(() => {
    if (!selected) return

    const ask = () => sync.sendFrame({ type: 'presence.ask', conversation_id: selected })
    ask()
    const timer = setInterval(ask, presenceInterval)
    return () => clearInterval(timer)
  }, [selected, sync])

  // The call, and the same ref trick for the same reason: the socket must not be rebuilt
  // because a callback identity changed, and dropping it mid-call would look like the
  // server losing everyone.
  const [live, setLive] = useState<CallState | null>(null)
  const callFrame = useRef<(frame: CallFrame) => void>(() => {})
  const callChanged = useRef<(conversationID: string) => void>(() => {})
  const call = useMemo(
    () =>
      new Call({
        send: (frame) => {
          if (!sync.sendFrame(frame)) setError('not connected — cannot reach the call')
        },
        onChange: setLive,
        onError: (failure) => setError(describe(failure)),
      }),
    [sync],
  )
  callFrame.current = (frame) => call.apply(frame)
  // The broadcast says only that something changed, so the client asks. That indirection is
  // what makes a ring correct rather than merely fast: what is rendered came from a query
  // this client made, not from a frame it was handed.
  callChanged.current = (conversationID) => call.ask(conversationID)

  // Leaving on unmount is what turns the camera light off when somebody closes the tab.
  useEffect(() => () => void call.leave(), [call])

  useEffect(() => {
    // Hydrate first, connect second. NF-5 is exactly this ordering: what a previous
    // session stored is on screen before any request leaves.
    sync.hydrate()
    sync.start()
    return () => sync.stop()
  }, [sync])

  const snapshot = useSyncExternalStore(sync.subscribe, sync.getSnapshot)

  // Asked on every conversation change, and only once connected, because the notification
  // that a call started is ephemeral (ADR-0005): this is the durable answer to the same
  // question, and it is what makes a ring survive a client that was reloading when it
  // happened.
  useEffect(() => {
    if (!selected || snapshot.status !== 'live') return
    call.ask(selected)
  }, [call, selected, snapshot.status])

  const load = useCallback(async () => {
    try {
      const found = await client.conversations()
      sync.rememberConversations(found)
      setConversations(found)
      for (const conversation of found) sync.follow(conversation.id)
      setSelected((current) => current ?? found[0]?.id ?? null)
    } catch (failure) {
      fatal.current(failure)
    }
  }, [client, sync])

  // The stored list first, so an offline cold start shows conversations rather than
  // nothing while the request that will never succeed is in flight.
  useEffect(() => {
    if (!store) return
    const held = store.conversations()
    if (held.length === 0) return

    setConversations(
      held.map((conversation) => ({
        id: conversation.id,
        kind: conversation.kind,
        head: conversation.head,
        role: conversation.role,
        visible_from: conversation.visible_from,
        created_at: '',
        unread: conversation.unread,
        read_through: 0,
        delivered_through: 0,
        others_read_through: conversation.others_read,
        others_delivered_through: conversation.others_delivered,
      })),
    )
    setSelected((current) => current ?? held[0]?.id ?? null)
  }, [store])

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

  // Entries become messages here, not in the components. An edit occupies its own
  // position in the log but changes what an earlier message says (ADR-0008), so what
  // is on screen is derived from the log rather than being it.
  const messages = useMemo(() => resolve(entries), [entries])
  const reactions = (selected && snapshot.reactions.get(selected)) || new Map()

  // Reactions are read for the range on screen — no cursor, no history. They are not in
  // the log, so there is no gap to notice when one is missed; reading the range again
  // is the whole recovery mechanism (ADR-0008).
  const highest = entries.at(-1)?.sequence ?? 0
  useEffect(() => {
    if (!selected || highest === 0) return
    client
      .reactions(selected, 1, highest)
      .then((found) => sync.acceptReactions(selected, found))
      .catch((failure) => fatal.current(failure))
  }, [selected, highest, client, sync])

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
          <Search store={store} onOpen={setSelected} />
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
              <CallPanel
                call={live}
                me={session.account.id}
                handles={handles}
                onStart={() => void call.join(selected).catch((failure) => setError(describe(failure)))}
                onLeave={() => void call.leave()}
                onMute={(muted) => call.mute(muted)}
              />
              <Transcript
                messages={messages}
                reactions={reactions}
                me={client.accountID}
                handles={handles}
                conversation={current}
                onEdit={async (sequence, text) => {
                  const entry = await client.revise(selected, sequence, clientEntryID(), text)
                  sync.accept(entry)
                }}
                onDelete={async (sequence) => {
                  const entry = await client.retract(selected, sequence, clientEntryID())
                  sync.accept(entry)
                }}
                onReact={async (sequence, emoji, mine) => {
                  if (mine) await client.unreact(selected, sequence, emoji)
                  else await client.react(selected, sequence, emoji)
                  // Applied locally at once rather than waiting for the echo: the
                  // server has accepted it, and a tap that does nothing for a round
                  // trip gets tapped again.
                  sync.acceptReactions(selected, [])
                  const found = await client.reactions(selected, 1, highest)
                  sync.acceptReactions(selected, found)
                }}
                onReply={setReplyTo}
                onError={setError}
                client={client}
                epochs={epochs}
              />
              <Presence
                present={present[selected]}
                me={client.accountID}
                handles={handles}
              />
              <Composer
                replyTo={replyTo}
                onCancelReply={() => setReplyTo(0)}
                onSend={async (text, clientEntryID) => {
                  // Through the syncer, so the send is recorded as pending before it is
                  // attempted and retried on the next connection if it fails.
                  await sync.send(selected, clientEntryID, text, replyTo)
                  setReplyTo(0)
                }}
                onAttach={async (file, clientEntryID, text, onProgress) => {
                  // Not through the pending queue, deliberately. That queue exists so a
                  // typed message survives being offline; an attachment cannot be sent
                  // offline at all, because the bytes have to reach the store first. So
                  // there is nothing to queue — if the upload failed there is no
                  // attachment to reference, and the draft is kept for a retry.
                  const attachmentID = await client.attach(selected, file, onProgress)
                  const entry = await client.send(selected, clientEntryID, text, replyTo, attachmentID)
                  sync.accept(entry)
                  setReplyTo(0)
                }}
                onTyping={(typing) => {
                  // Coalesced by the client rather than sent per keystroke: the claim
                  // lasts several seconds on the server, so renewing it once a second
                  // is enough and a frame per character is not.
                  const now = Date.now()
                  if (typing && now - lastTyped.current < typingInterval) return
                  lastTyped.current = typing ? now : 0
                  sync.sendFrame({ type: 'typing', conversation_id: selected, typing })
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

/** Search queries the local store, and only the local store.
 *
 *  There is no server-side search and there never will be: the server does not read
 *  message text (ADR-0001). So this works offline, and it is the only search this system
 *  has. The CLI runs the same query against the same schema — see store.ts. */
function Search({ store, onOpen }: { store: LocalStore | null; onOpen: (id: string) => void }) {
  const [query, setQuery] = useState('')
  const [hits, setHits] = useState<Hit[]>([])

  useEffect(() => {
    if (!store || query.trim() === '') {
      setHits([])
      return
    }
    // Synchronous: SQLite here is WebAssembly on this thread, so there is nothing to
    // await and no loading state to render. That is a property of the choice, not an
    // oversight.
    setHits(store.search(query, 20))
  }, [store, query])

  if (!store) {
    return (
      <p className="muted hint">
        Search needs a local store, which this browser would not provide.
      </p>
    )
  }

  return (
    <div className="search">
      <input
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="search messages"
        aria-label="Search messages"
      />
      {query.trim() !== '' && (
        <ul className="hits">
          {hits.length === 0 && <li className="muted">no matches</li>}
          {hits.map((hit) => (
            <li key={`${hit.conversation_id}-${hit.sequence}`}>
              <button type="button" className="link" onClick={() => onOpen(hit.conversation_id)}>
                #{hit.sequence} {hit.body.slice(0, 40)}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
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
  messages,
  reactions,
  me,
  handles,
  conversation,
  onEdit,
  onDelete,
  onReact,
  onReply,
  onError,
  client,
  epochs,
}: {
  messages: Message[]
  reactions: ReactionsBySequence
  me: string
  handles: Record<string, string>
  conversation: Conversation | undefined
  onEdit: (sequence: number, text: string) => Promise<void>
  onDelete: (sequence: number) => Promise<void>
  onReact: (sequence: number, emoji: string, mine: boolean) => Promise<void>
  onReply: (sequence: number) => void
  onError: (message: string) => void
  client: Client
  /** epochs bumps when the server says an attachment changed, which is what makes a
   *  placeholder become a thumbnail without a poll. */
  epochs: Record<string, number>
}) {
  const [editing, setEditing] = useState(0)
  const byPosition = new Map(messages.map((message) => [message.sequence, message]))
  const guard = (work: () => Promise<void>) => work().catch((failure) => onError(describe(failure)))
  const administrator = conversation?.role === 'admin'

  return (
    <ol className="transcript">
      {messages.map((message) => (
        <li key={message.sequence} className={message.authorID === me ? 'mine' : 'theirs'}>
          {message.replyTo > 0 && (
            <span className="quoted">
              {/* The quoted text, or a note that it is not held. A reply is a bare
                  reference (ADR-0008), so the message it names may be before this
                  member's join point or simply not fetched yet. */}
              {byPosition.get(message.replyTo)?.text ?? `#${message.replyTo}`}
            </span>
          )}

          <span className="who">
            {message.authorID === me ? 'you' : (handles[message.authorID] ?? message.authorID.slice(0, 8))}
          </span>

          {message.attachmentID !== '' && (
            <AttachmentView
              client={client}
              attachmentID={message.attachmentID}
              epoch={epochs[message.attachmentID] ?? 0}
            />
          )}

          {message.retracted ? (
            <span className="body retracted">deleted</span>
          ) : editing === message.sequence ? (
            <EditBox
              initial={message.text}
              onCancel={() => setEditing(0)}
              onSave={async (text) => {
                await guard(() => onEdit(message.sequence, text))
                setEditing(0)
              }}
            />
          ) : (
            <span className="body">
              {message.text}
              {message.edited && <span className="muted edited"> (edited)</span>}
            </span>
          )}

          {/* The sequence is on screen deliberately. It is what the sync protocol
              turns on, and seeing a hole appear and close is the fastest way to
              tell gap filling from a rendering bug. */}
          <span className="sequence">#{message.sequence}</span>

          {message.authorID === me && conversation && (
            <Ticks state={deliveryOf(message.sequence, conversation)} />
          )}

          {!message.retracted && (
            <span className="actions">
              <button type="button" className="link" onClick={() => onReply(message.sequence)}>
                reply
              </button>
              {message.authorID === me && (
                <button type="button" className="link" onClick={() => setEditing(message.sequence)}>
                  edit
                </button>
              )}
              {/* Deleting somebody else's message is an administrator's power and only
                  theirs. Editing it is nobody's — see MS-9. */}
              {(message.authorID === me || administrator) && (
                <button
                  type="button"
                  className="link"
                  onClick={() => void guard(() => onDelete(message.sequence))}
                >
                  delete
                </button>
              )}
            </span>
          )}

          <Reactions
            summary={summarise(reactions, message.sequence, me)}
            onReact={(emoji, mine) => void guard(() => onReact(message.sequence, emoji, mine))}
          />
        </li>
      ))}
    </ol>
  )
}

function EditBox({
  initial,
  onSave,
  onCancel,
}: {
  initial: string
  onSave: (text: string) => Promise<void>
  onCancel: () => void
}) {
  const [text, setText] = useState(initial)

  return (
    <form
      className="edit"
      onSubmit={(event) => {
        event.preventDefault()
        const wanted = text.trim()
        if (!wanted) return
        void onSave(wanted)
      }}
    >
      <input value={text} onChange={(event) => setText(event.target.value)} aria-label="Edit message" autoFocus />
      <button type="submit">Save</button>
      <button type="button" className="link" onClick={onCancel}>
        cancel
      </button>
    </form>
  )
}

/** offered is the reaction picker's fixed set.
 *
 *  A fixed few rather than a full emoji keyboard. The server validates that a reaction
 *  is a symbol and not text, so an arbitrary picker would be safe — but six choices
 *  make the counts mean something, where an open set turns every message into a long
 *  tail of ones. */
const offered = ['👍', '❤️', '😂', '🎉', '😮', '😢']

function Reactions({
  summary,
  onReact,
}: {
  summary: { emoji: string; count: number; mine: boolean }[]
  onReact: (emoji: string, mine: boolean) => void
}) {
  const [picking, setPicking] = useState(false)

  return (
    <span className="reactions">
      {summary.map(({ emoji, count, mine }) => (
        <button
          key={emoji}
          type="button"
          className={mine ? 'reaction mine' : 'reaction'}
          title={mine ? 'remove your reaction' : 'react'}
          onClick={() => onReact(emoji, mine)}
        >
          {emoji} {count}
        </button>
      ))}

      <button type="button" className="reaction add" title="react" onClick={() => setPicking(!picking)}>
        +
      </button>

      {picking && (
        <span className="picker">
          {offered.map((emoji) => (
            <button
              key={emoji}
              type="button"
              className="reaction"
              onClick={() => {
                setPicking(false)
                // Always an add from the picker: the summary's own buttons are how a
                // reaction is removed, and a picker that toggled would make the same
                // tap mean different things depending on invisible state.
                onReact(emoji, false)
              }}
            >
              {emoji}
            </button>
          ))}
        </span>
      )}
    </span>
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

/**
 * CallPanel is the call, in whatever state it is.
 *
 * Four states and they are genuinely different things to a person: no call, a call ringing
 * that you could answer, a call you are in, and a call in progress you have not joined. The
 * temptation is to collapse the last two into "there is a call"; they are the difference
 * between a button that says join and one that says leave.
 */
function CallPanel({
  call,
  me,
  handles,
  onStart,
  onLeave,
  onMute,
}: {
  call: CallState | null
  me: string
  handles: Record<string, string>
  onStart: () => void
  onLeave: () => void
  onMute: (muted: boolean) => void
}) {
  if (!call) {
    return (
      <div className="call idle">
        <button type="button" onClick={onStart}>
          Start a call
        </button>
      </div>
    )
  }

  if (!call.joined) {
    // Somebody else's call, in progress. Who is in it is shown, because "join a call" and
    // "join a call with these three people" are different decisions.
    return (
      <div className="call ringing">
        <span className="who">
          {call.participants
            .map((participant) => handles[participant.account_id] ?? participant.account_id.slice(0, 8))
            .join(', ')}{' '}
          {call.state === 'ringing' ? 'is calling' : 'are in a call'}
        </span>
        <button type="button" onClick={onStart}>
          Join
        </button>
      </div>
    )
  }

  return (
    <div className="call joined" aria-label="Call in progress">
      <div className="tiles">
        {/* Muted and playsInline on the local tile, always: a browser that plays your own
            microphone back to you produces feedback, and a mobile browser that does not get
            playsInline takes the video fullscreen. */}
        <Tile stream={call.local} label={handles[me] ?? 'you'} muted />
        {[...call.remote.entries()].map(([id, stream]) => (
          <Tile key={id} stream={stream} label="" muted={false} />
        ))}
      </div>
      <div className="controls">
        <span className="muted">
          {call.state === 'ringing' ? 'ringing…' : `${call.participants.length} in the call`}
        </span>
        <button type="button" onClick={() => onMute(!call.muted)}>
          {call.muted ? 'Unmute' : 'Mute'}
        </button>
        <button type="button" className="hang-up" onClick={onLeave}>
          Hang up
        </button>
      </div>
    </div>
  )
}

/** Tile is one participant's video.
 *
 *  A ref rather than a src, because a MediaStream is attached to an element rather than
 *  addressed by URL — and attaching it in an effect is what keeps React from being asked to
 *  render an object it cannot serialise. */
function Tile({ stream, label, muted }: { stream: MediaStream | null; label: string; muted: boolean }) {
  const element = useRef<HTMLVideoElement>(null)

  useEffect(() => {
    if (element.current && element.current.srcObject !== stream) {
      element.current.srcObject = stream
    }
  }, [stream])

  return (
    <div className="tile">
      <video ref={element} autoPlay playsInline muted={muted} />
      {label && <span className="label">{label}</span>}
    </div>
  )
}

/**
 * AttachmentView shows one photo or video, whatever state it is in.
 *
 * It fetches its own metadata rather than being handed it, for two reasons. The URLs are
 * signed and expire within the hour, so they cannot be cached with the entry — a stored
 * URL is a broken image waiting to happen. And the entry arrives before the attachment
 * is ready (MD-1), so there is nothing to hand over at the moment it is rendered.
 */
function AttachmentView({
  client,
  attachmentID,
  epoch,
}: {
  client: Client
  attachmentID: string
  epoch: number
}) {
  const [attachment, setAttachment] = useState<Attachment | null>(null)
  const [failed, setFailed] = useState(false)
  const [open, setOpen] = useState(false)

  useEffect(() => {
    let current = true
    client
      .attachment(attachmentID)
      .then((found) => {
        if (current) setAttachment(found)
      })
      .catch(() => {
        // Not a fatal error and not worth a banner: an attachment this account may not
        // see, or one whose metadata could not be fetched, renders as unavailable. The
        // message it belongs to is still readable, which is the point of the two being
        // separate.
        if (current) setFailed(true)
      })
    return () => {
      current = false
    }
    // epoch is the dependency that matters: the server said this attachment changed, so
    // ask again.
  }, [client, attachmentID, epoch])

  if (failed) return <span className="attachment unavailable">attachment unavailable</span>

  if (!attachment || attachment.state === 'pending' || attachment.state === 'uploaded') {
    // The placeholder, and the whole reason an entry is decoupled from its attachment: a
    // message carrying a 90 MB video is readable now and displayable later (MD-1).
    return (
      <span className="attachment pending" aria-label="Attachment processing">
        <span className="spinner" /> preparing…
      </span>
    )
  }

  if (attachment.state === 'failed') {
    return (
      <span className="attachment failed" title={attachment.failure}>
        this attachment could not be processed
      </span>
    )
  }

  // Video has no derived renditions — deriving one needs a transcoder — so it is played
  // from the retained original with the browser's own controls. preload is metadata so
  // opening a conversation does not start downloading every video in it.
  if (attachment.content_type.startsWith('video/')) {
    return (
      <video className="attachment video" controls preload="metadata" src={attachment.original_url}>
        <track kind="captions" />
      </video>
    )
  }

  const thumbnail = attachment.variants.find((variant) => variant.name === 'thumbnail')
  const display = attachment.variants.find((variant) => variant.name === 'display')
  const shown = open ? (display ?? thumbnail) : thumbnail
  if (!shown) return <span className="attachment unavailable">attachment unavailable</span>

  return (
    <button
      type="button"
      className={open ? 'attachment photo open' : 'attachment photo'}
      onClick={() => setOpen(!open)}
      aria-label={open ? 'Close attachment' : 'Open attachment'}
    >
      {/* Dimensions are set so the transcript does not jump as thumbnails load. */}
      <img src={shown.url} width={shown.width} height={shown.height} alt="" loading="lazy" />
    </button>
  )
}

/** presenceInterval is how often a client asks who is present in the conversation it has open.
 *
 *  Well inside the server's thirty-second online window, so somebody who leaves is noticed
 *  within one poll rather than at the end of theirs. */
const presenceInterval = 8_000

/** typingInterval is the shortest gap between two typing claims from this client.
 *
 *  The server's claim lasts several seconds, so renewing it once a second keeps it true while
 *  keys are being pressed. A frame per keystroke would be the same statement forty times. */
const typingInterval = 1_000

/**
 * Presence shows who is here and who is typing.
 *
 * Typing takes precedence over the dots: somebody typing is present by definition, and showing
 * both lines at once would say the same thing twice. Nothing renders when nobody else is here,
 * because an empty row that appears and disappears is worse than one that is simply absent.
 */
function Presence({
  present,
  me,
  handles,
}: {
  present?: { online: string[]; typing: string[] }
  me: string
  handles: Record<string, string>
}) {
  // Excluding this account from both. Being told you are online is noise, and being told you
  // are typing while you type is the kind of detail that makes an interface feel wrong.
  const typing = (present?.typing ?? []).filter((accountID) => accountID !== me)
  const online = (present?.online ?? []).filter((accountID) => accountID !== me)

  if (typing.length === 0 && online.length === 0) return null

  const name = (accountID: string) => handles[accountID] ?? accountID.slice(0, 8)

  return (
    <p className="presence">
      {typing.length > 0 ? (
        <span className="typing">
          {typing.map(name).join(', ')} {typing.length === 1 ? 'is' : 'are'} typing…
        </span>
      ) : (
        <span className="online">
          <span className="dot" aria-hidden="true" />
          {online.map(name).join(', ')} {online.length === 1 ? 'is' : 'are'} here
        </span>
      )}
    </p>
  )
}

function Composer({
  replyTo,
  onCancelReply,
  onSend,
  onAttach,
  onTyping,
  onError,
}: {
  replyTo: number
  onCancelReply: () => void
  onSend: (text: string, clientEntryID: string) => Promise<void>
  onAttach: (
    file: File,
    clientEntryID: string,
    text: string,
    onProgress: (fraction: number) => void,
  ) => Promise<void>
  onTyping: (typing: boolean) => void
  onError: (message: string) => void
}) {
  const [draft, setDraft] = useState('')
  const [chosen, setChosen] = useState<File | null>(null)
  // -1 means no upload in flight, which is distinguishable from 0 — a hundred-megabyte
  // upload sits at zero for a moment and must not look idle.
  const [progress, setProgress] = useState(-1)
  const chooser = useRef<HTMLInputElement>(null)
  // The identifier belongs to the draft, not to the attempt. A send that fails and is
  // tried again reuses it, so the server recognises the retry and returns the entry the
  // first attempt created rather than writing a second one (MS-2).
  const [draftID, setDraftID] = useState(clientEntryID)
  const [sending, setSending] = useState(false)

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const text = draft.trim()
    // A photo needs no caption, so an empty draft is a valid send when a file is
    // chosen. An empty draft with no file is not a message.
    if ((!text && !chosen) || sending) return

    setSending(true)
    try {
      if (chosen) {
        await onAttach(chosen, draftID, text, setProgress)
        setChosen(null)
        if (chooser.current) chooser.current.value = ''
      } else {
        await onSend(text, draftID)
      }
      setDraft('')
      setDraftID(clientEntryID())
      // Sent, so no longer typing. Without this the indicator survives the message by
      // its whole window, which reads as a second message coming that never does.
      onTyping(false)
    } catch (failure) {
      // The draft, the file and the identifier are all kept, so pressing send again is a
      // retry of the same entry rather than a new one.
      onError(describe(failure))
    } finally {
      setSending(false)
      setProgress(-1)
    }
  }

  return (
    <form className="composer" onSubmit={submit}>
      {replyTo > 0 && (
        <span className="replying">
          replying to #{replyTo}
          <button type="button" className="link" onClick={onCancelReply}>
            cancel
          </button>
        </span>
      )}
      {chosen && (
        <span className="chosen">
          {chosen.name}
          {progress >= 0 ? ` — ${Math.round(progress * 100)}%` : ''}
          <button
            type="button"
            className="link"
            onClick={() => {
              setChosen(null)
              if (chooser.current) chooser.current.value = ''
            }}
          >
            remove
          </button>
        </span>
      )}
      {/* A plain file input, which is what a browser already knows how to do: a picker,
          a camera on a phone, and drag and drop, none of it written here. */}
      <input
        ref={chooser}
        type="file"
        accept="image/jpeg,image/png,image/gif,image/webp,video/mp4,video/webm,video/quicktime"
        aria-label="Attach a photo or video"
        onChange={(event) => setChosen(event.target.files?.[0] ?? null)}
      />
      <input
        value={draft}
        onChange={(event) => {
          setDraft(event.target.value)
          // Typing while there is something in the box, not typing when it is empty:
          // a cleared draft is somebody who changed their mind, and leaving the
          // indicator up for the remaining seconds of its window says otherwise.
          onTyping(event.target.value.trim() !== '')
        }}
        placeholder={chosen ? 'Caption (optional)' : 'Message'}
        aria-label="Message"
        autoFocus
      />
      <button type="submit" disabled={sending || (draft.trim() === '' && !chosen)}>
        Send
      </button>
    </form>
  )
}

