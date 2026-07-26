// The socket half of the client: connect, resume, fill gaps, stay live.
//
// This file is the client side of MS-3 and it is the part most worth getting
// right. Redis Pub/Sub is allowed to drop a broadcast (ADR-0005), so live delivery
// is an optimisation and *gap detection is the actual delivery guarantee*. A client
// that trusts the socket to be complete will lose messages silently, which is the
// worst failure this system can have.
//
// Deliberately free of React. The protocol is stateful and must survive re-renders,
// StrictMode's double-mounted effects, and being tested with no DOM at all.

import { decodeBody, encodeBody } from './api'
import type { Conversation, Entry, Reaction } from './api'
import type { LocalStore } from './store'
import type { ReactionsBySequence } from './transcript'

export type SyncStatus = 'connecting' | 'live' | 'offline'

/** Snapshot is what the UI renders. Entries are in sequence order per conversation. */
export type Snapshot = {
  status: SyncStatus
  conversations: Map<string, Entry[]>
  /** reactions are held beside the log, not in it (ADR-0008). They carry no sequence
   *  of their own, so there is no gap to detect and no order to preserve — a client
   *  that misses one learns of it the next time it reads the range. */
  reactions: Map<string, ReactionsBySequence>
}

/** Socket is the part of WebSocket this client uses, so tests can supply a fake. */
export interface Socket {
  send(data: string): void
  close(): void
  onopen: ((event: unknown) => void) | null
  onmessage: ((event: { data: unknown }) => void) | null
  onclose: ((event: unknown) => void) | null
  onerror: ((event: unknown) => void) | null
}

export type SyncOptions = {
  /** token supplies a fresh access token for each connection attempt. */
  token: () => Promise<string>
  /** fetchEntries returns what follows a sequence. This is how gaps are filled. */
  fetchEntries: (conversationID: string, after: number) => Promise<Entry[]>
  /** postEntry sends an entry.
   *
   *  A dependency rather than an argument to send(), because a *reload* has to be able
   *  to flush what a previous session left pending — and a poster remembered from an
   *  earlier send() is null in a process that has not sent anything yet. That was the
   *  first version of this, and it meant a message queued before a refresh stayed queued
   *  forever. */
  postEntry?: (conversationID: string, clientEntryID: string, text: string, replyTo: number) => Promise<Entry>
  /** onAttachmentChanged fires when an attachment became ready or failed.
   *
   *  Carries only the identifier, because the state and the URLs have to be fetched to
   *  be trusted — a frame is not a source of truth about something signed and expiring. */
  onAttachmentChanged?: (attachmentID: string) => void
  /** open connects. Defaults to a real WebSocket at the current origin.
   *
   *  It takes no URL: the default computes one from location, and a caller that
   *  supplies its own knows where it is connecting. Passing the URL in would make
   *  every caller — including the tests — depend on a browser global to build an
   *  argument the fake then ignores. */
  open?: () => Socket
  /** onFatal reports a failure reconnecting cannot fix, such as an expired session. */
  onFatal?: (error: unknown) => void
  /** store persists what is synced, so a cold start renders before connecting.
   *
   *  Optional. Without it the client behaves exactly as it did before phase 6 —
   *  everything in memory, a reload refetches — which is also what happens when OPFS is
   *  unavailable. Persistence is an optimisation of the cold start, not a requirement
   *  for correctness, and the code says so by making it possible to leave out. */
  store?: LocalStore
}

/** heartbeat keeps the connection warm through anything that drops idle
 *  connections.
 *
 *  ponytail: sends pings without requiring pongs, so it keeps a connection alive
 *  but does not detect one that is dead while still open. The network listeners
 *  below cover the case a browser can see; a socket a proxy silently discarded
 *  needs a pong deadline, which is phase 10's liveness work. */
const heartbeatInterval = 30_000

/** Reconnection backs off to a ceiling and stays there. Jittered, so a node
 *  restarting does not have every client it dropped return in the same instant. */
const firstRetryDelay = 500
const maxRetryDelay = 10_000

/** A conversation's local state.
 *
 *  contiguous is the highest sequence with nothing missing below it — the only
 *  number safe to resume from. Holding entries 1, 2 and 7 means contiguous is 2,
 *  because resuming at 7 would silently abandon 3 through 6. */
type Log = {
  entries: Map<number, Entry>
  contiguous: number
  filling: boolean
  /** refill records a gap noticed while a fill was already running, so the second
   *  gap is not lost when the first fill finishes. */
  refill: boolean
}

type entryFrame = {
  type: 'entry'
  conversation_id: string
  entry_id: string
  sequence: number
  author_id: string
  client_entry_id: string
  kind: string
  content_type: string
  body: string
  created_at: string
  /** target_sequence and reply_to travel with the broadcast so an edit can be applied
   *  without a round trip. Dropping them here would leave a live edit looking like an
   *  entry that amends nothing, which resolve() correctly ignores — a silent failure. */
  target_sequence?: number
  reply_to?: number
  /** attachment_id travels for the same reason: a live recipient renders a placeholder
   *  immediately rather than discovering the reference on a later fetch. */
  attachment_id?: string
}

type gapsFrame = {
  type: 'gaps'
  gaps: { conversation_id: string; from: number; to: number }[]
}

type reactionFrame = {
  type: 'reaction'
  conversation_id: string
  sequence: number
  account_id: string
  emoji: string
  removed: boolean
}

/** attachmentFrame says an attachment changed, and nothing about how.
 *
 *  Deliberately stateless: the variant URLs are signed and short-lived, so a frame
 *  carrying them would be stale by the time a client acted on it. "Look again" is the
 *  whole message, and the client re-fetches — which it must do for the URLs anyway. */
type attachmentFrame = {
  type: 'attachment'
  conversation_id: string
  attachment_id: string
}

type readyFrame = { type: 'ready'; account_id: string; device_id: string }
type errorFrame = { type: 'error'; code: string; message: string }

type serverFrame =
  | entryFrame
  | gapsFrame
  | reactionFrame
  | attachmentFrame
  | readyFrame
  | errorFrame
  | { type: string }

/** openAtCurrentOrigin is the browser default: same origin as the page, so the
 *  server's same-origin check passes with no configuration to keep in step. */
function openAtCurrentOrigin(): Socket {
  const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:'
  return new WebSocket(`${scheme}//${location.host}/v1/socket`) as unknown as Socket
}

function toEntry(frame: entryFrame): Entry {
  return {
    id: frame.entry_id,
    conversation_id: frame.conversation_id,
    sequence: frame.sequence,
    author_id: frame.author_id,
    client_entry_id: frame.client_entry_id,
    kind: frame.kind,
    content_type: frame.content_type,
    body: frame.body,
    created_at: frame.created_at,
    target_sequence: frame.target_sequence,
    reply_to: frame.reply_to,
    attachment_id: frame.attachment_id,
  }
}

export class Sync {
  private readonly options: SyncOptions
  private readonly logs = new Map<string, Log>()
  private readonly listeners = new Set<() => void>()

  private socket: Socket | null = null
  private status: SyncStatus = 'offline'
  private readonly reactions = new Map<string, ReactionsBySequence>()
  private snapshot: Snapshot = { status: 'offline', conversations: new Map(), reactions: new Map() }

  private attempt = 0
  private retryTimer: ReturnType<typeof setTimeout> | null = null
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private stopped = false

  constructor(options: SyncOptions) {
    this.options = options
  }

  // --- what the UI talks to ---

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  /** getSnapshot returns the same object until something changes, which is what
   *  useSyncExternalStore requires to avoid rendering forever. */
  getSnapshot = (): Snapshot => this.snapshot

  /** record adds an entry the UI already holds — the response to its own send.
   *  Keyed by sequence like everything else, so the socket echo of the same entry
   *  is not a second message. */
  accept(entry: Entry): void {
    if (this.store(entry.conversation_id, entry)) this.publish()
  }

  /** acceptReactions merges what a fetch returned.
   *
   *  Merged rather than replaced: a live reaction can arrive while the fetch is in
   *  flight, and replacing would discard it with nothing to bring it back — there is
   *  no gap to notice for something that has no sequence. */
  acceptReactions(conversationID: string, reactions: Reaction[]): void {
    for (const reaction of reactions) {
      this.setReaction(conversationID, reaction.sequence, reaction.emoji, reaction.account_id, false)
    }
    this.publish()
  }

  private onReaction(frame: reactionFrame): void {
    this.setReaction(frame.conversation_id, frame.sequence, frame.emoji, frame.account_id, frame.removed)
    this.publish()
  }

  private setReaction(
    conversationID: string,
    sequence: number,
    emoji: string,
    accountID: string,
    removed: boolean,
  ): void {
    this.options.store?.setReaction(conversationID, sequence, accountID, emoji, removed)

    let bySequence = this.reactions.get(conversationID)
    if (!bySequence) {
      bySequence = new Map()
      this.reactions.set(conversationID, bySequence)
    }

    let byEmoji = bySequence.get(sequence)
    if (!byEmoji) {
      byEmoji = new Map()
      bySequence.set(sequence, byEmoji)
    }

    const accounts = byEmoji.get(emoji) ?? new Set<string>()
    if (removed) accounts.delete(accountID)
    else accounts.add(accountID)
    byEmoji.set(emoji, accounts)
  }

  /**
   * send records a pending entry, then attempts it.
   *
   * Recorded first, deliberately, and this is the browser half of what the CLI does. A
   * send attempted before being written is lost if the tab closes waiting for the
   * response — and its client identifier with it, so the retry would create a second
   * entry rather than being recognised as the same one (MS-2).
   *
   * The poster is passed in rather than the API being a dependency here: this file owns
   * the protocol, not the HTTP surface.
   */
  async send(conversationID: string, clientEntryID: string, text: string, replyTo: number): Promise<void> {
    const post = this.options.postEntry
    if (!post) throw new Error('this client was not built with a way to send')

    this.options.store?.addPending({ clientEntryID, conversationID, body: text, replyTo })

    const entry = await post(conversationID, clientEntryID, text, replyTo)
    // Storing it clears the pending row, matched by client identifier — so the socket
    // echo reconciles it too, even if this response were lost.
    this.accept(entry)
  }

  /**
   * flushPending retries everything unacknowledged.
   *
   * Every retry carries its original identifier, which is what makes "send while the
   * server is unreachable, come back, and the message arrives exactly once" true rather
   * than hopeful.
   */
  async flushPending(): Promise<void> {
    const store = this.options.store
    const post = this.options.postEntry
    if (!store || !post) return

    for (const pending of store.pendingSends()) {
      try {
        const entry = await post(
          pending.conversation_id, pending.client_entry_id, pending.body, pending.reply_to)
        this.accept(entry)
      } catch (failure) {
        // Still unreachable, or refused. Left pending: the next connection tries again,
        // and a permanently refused send is dropped by the CLI's rule rather than
        // silently blocking the queue — which is phase 10's work for this client.
        this.options.onFatal?.(failure)
        return
      }
    }
  }

  /** hydrate loads what a previous session stored, before any network call.
   *
   *  This is NF-5: the conversation list and the open conversation render from local
   *  storage with the network disabled. Called before start(), so the first paint owes
   *  nothing to a socket.
   *
   *  The mark comes back with the entries, which is the part that matters beyond
   *  rendering — resuming from what was stored rather than from zero is what stops a
   *  cold start refetching the whole history. */
  hydrate(): void {
    const store = this.options.store
    if (!store) return

    for (const [conversationID, contiguous] of Object.entries(store.cursor())) {
      const log = this.logOf(conversationID)
      log.contiguous = contiguous
      for (const held of store.entries(conversationID)) {
        log.entries.set(held.sequence, {
          id: held.id,
          conversation_id: held.conversation_id,
          sequence: held.sequence,
          author_id: held.author_id,
          client_entry_id: held.client_entry_id,
          kind: held.kind,
          content_type: 'text/plain; charset=utf-8',
          // Stored decoded, so re-encoded on the way in: the snapshot's entries carry
          // the wire form, and one representation through the render path is worth an
          // encode per cold start.
          body: encodeBody(held.body),
          created_at: held.created_at,
          target_sequence: held.target_sequence || undefined,
          reply_to: held.reply_to || undefined,
        })
      }

      for (const reaction of store.reactions(conversationID)) {
        this.setReaction(conversationID, reaction.sequence, reaction.emoji, reaction.account_id, false)
      }
    }

    this.publish()
  }

  /** rememberConversations persists what the server said about the list. */
  rememberConversations(conversations: Conversation[]): void {
    const store = this.options.store
    if (!store) return
    for (const conversation of conversations) store.saveConversation(conversation)
  }

  /** follow makes a conversation known before any entry arrives, so opening one
   *  with existing history loads it rather than waiting for someone to speak. */
  follow(conversationID: string): void {
    if (this.logs.has(conversationID)) return
    this.logOf(conversationID)
    this.publish()
    void this.fill(conversationID)
  }

  // --- connection lifecycle ---

  start(): void {
    this.stopped = false
    this.watchNetwork(true)
    this.connect()
  }

  stop(): void {
    this.stopped = true
    this.watchNetwork(false)
    this.clearTimers()
    this.socket?.close()
    this.socket = null
    this.setStatus('offline')
  }

  /**
   * watchNetwork listens for the browser losing and regaining connectivity.
   *
   * A socket whose network has gone is not closed by anything — it simply stops
   * carrying traffic, and the client goes on reporting itself live and believing it
   * is current. That is precisely the silent-loss failure gap detection exists to
   * prevent, so the moment the browser says the network is gone, the connection is
   * treated as gone.
   *
   * The reverse matters as much: on regaining the network, reconnect immediately
   * rather than serving out a backoff delay earned while there was no point trying.
   */
  private watchNetwork(listening: boolean): void {
    if (typeof globalThis.addEventListener !== 'function') return
    const bind = listening ? globalThis.addEventListener : globalThis.removeEventListener
    bind('offline', this.onNetworkLost)
    bind('online', this.onNetworkFound)
  }

  private onNetworkLost = (): void => {
    // Closing takes the normal path: onclose schedules a reconnect, which will
    // keep failing harmlessly until the network returns.
    this.socket?.close()
  }

  private onNetworkFound = (): void => {
    if (this.stopped || this.socket) return
    if (this.retryTimer) clearTimeout(this.retryTimer)
    this.retryTimer = null
    this.attempt = 0
    this.connect()
  }

  private connect(): void {
    if (this.stopped || this.socket) return
    this.setStatus('connecting')

    const open = this.options.open ?? openAtCurrentOrigin
    let socket: Socket
    try {
      socket = open()
    } catch (error) {
      this.retry(error)
      return
    }
    this.socket = socket

    // Every handler below checks that it is still the current socket first.
    //
    // Events outlive the connection that produced them: a socket closed during its
    // handshake reports that close after the replacement has been assigned, and an
    // unguarded onclose would then null out the *replacement* and open a third
    // connection — leaving the second one authenticated, subscribed and orphaned on
    // the server for as long as the tab is open. Found by asserting a node held one
    // connection per client and finding two.
    const current = () => this.socket === socket

    socket.onopen = () => {
      if (!current()) return
      // Credentials go in the first frame, not the URL: a token in a query string
      // ends up in proxy logs and browser history, and a browser cannot put an
      // Authorization header on a WebSocket handshake.
      void this.options
        .token()
        .then((token) => socket.send(JSON.stringify({ type: 'authenticate', token })))
        .catch((error) => {
          // A token that cannot be obtained is not a connection problem, and
          // reconnecting would produce the same result at a slower rate.
          this.options.onFatal?.(error)
          this.stop()
        })
    }

    socket.onmessage = (event) => {
      if (!current() || typeof event.data !== 'string') return
      let frame: serverFrame
      try {
        frame = JSON.parse(event.data) as serverFrame
      } catch {
        return
      }
      this.handle(frame)
    }

    socket.onclose = () => {
      if (!current()) return
      this.retry(null)
    }
    socket.onerror = () => {
      // Not a retry on its own: a browser fires error then close, and treating
      // both as failures would double the backoff on every attempt.
    }
  }

  private handle(frame: serverFrame): void {
    switch (frame.type) {
      case 'ready':
        this.setStatus('live')
        this.attempt = 0
        this.resume()
        this.startHeartbeat()
        // A connection is the moment to retry what could not be sent while there was
        // none.
        void this.flushPending()
        break

      case 'gaps':
        // The server has said what is missing. It is authoritative about the
        // shape of the log, so its answer replaces any local guess.
        for (const gap of (frame as gapsFrame).gaps) {
          void this.fill(gap.conversation_id)
        }
        break

      case 'entry':
        this.onEntry(frame as entryFrame)
        break

      case 'reaction':
        this.onReaction(frame as reactionFrame)
        break

      case 'attachment':
        // Forwarded rather than stored. Nothing about the log changed, so no sequence
        // moves and no gap opens — the only thing to do is tell whoever is rendering
        // that attachment to ask again.
        this.options.onAttachmentChanged?.((frame as attachmentFrame).attachment_id)
        break

      case 'error':
        this.options.onFatal?.(new Error((frame as errorFrame).message))
        break
    }
  }

  private resume(): void {
    const cursor: Record<string, number> = {}
    for (const [conversationID, log] of this.logs) {
      cursor[conversationID] = log.contiguous
    }
    this.socket?.send(JSON.stringify({ type: 'resume', cursor }))
  }

  private retry(error: unknown): void {
    this.socket = null
    this.clearTimers()
    if (this.stopped) return

    this.setStatus('offline')
    if (error) this.options.onFatal?.(error)

    const delay = Math.min(firstRetryDelay * 2 ** this.attempt, maxRetryDelay)
    this.attempt++
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null
      this.connect()
    }, delay * (0.5 + Math.random() / 2))
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    this.heartbeatTimer = setInterval(() => {
      this.socket?.send(JSON.stringify({ type: 'ping' }))
    }, heartbeatInterval)
  }

  private clearTimers(): void {
    if (this.retryTimer) clearTimeout(this.retryTimer)
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    this.retryTimer = null
    this.heartbeatTimer = null
  }

  // --- the log ---

  private logOf(conversationID: string): Log {
    let log = this.logs.get(conversationID)
    if (!log) {
      log = { entries: new Map(), contiguous: 0, filling: false, refill: false }
      this.logs.set(conversationID, log)
    }
    return log
  }

  /** store inserts an entry, reporting whether it was new. Duplicates are the
   *  normal case, not an error: an entry arrives over the socket, in the response
   *  to its own send, and again in any gap fetch that spans it. */
  private store(conversationID: string, entry: Entry): boolean {
    const log = this.logOf(conversationID)
    if (log.entries.has(entry.sequence)) return false

    log.entries.set(entry.sequence, entry)
    while (log.entries.has(log.contiguous + 1)) log.contiguous++

    this.persist(conversationID, entry)
    return true
  }

  /** persist mirrors an entry into the local store, amendments included.
   *
   *  The in-memory map stays the render source because React needs a synchronous read;
   *  the store is what survives a reload. Mirrored rather than read back through, so a
   *  storage failure degrades to the pre-phase-6 behaviour instead of breaking the
   *  screen. */
  private persist(conversationID: string, entry: Entry): void {
    const store = this.options.store
    if (!store) return

    try {
      store.saveEntries(conversationID, [
        {
          conversation_id: conversationID,
          sequence: entry.sequence,
          id: entry.id,
          author_id: entry.author_id,
          client_entry_id: entry.client_entry_id,
          kind: entry.kind,
          body: decodeBody(entry.body),
          target_sequence: entry.target_sequence ?? 0,
          reply_to: entry.reply_to ?? 0,
          attachment_id: entry.attachment_id ?? '',
          created_at: entry.created_at,
        },
      ])

      // An amendment updates its target's stored row, so search reflects what a message
      // now says rather than what it said (ADR-0008).
      if (entry.target_sequence) {
        store.applyAmendment(conversationID, entry.target_sequence, entry.kind, decodeBody(entry.body))
      }
    } catch (failure) {
      this.options.onFatal?.(failure)
    }
  }

  private onEntry(frame: entryFrame): void {
    const unknown = !this.logs.has(frame.conversation_id)
    const added = this.store(frame.conversation_id, toEntry(frame))
    const log = this.logOf(frame.conversation_id)

    // A live entry above the contiguous mark means something below it never
    // arrived — the dropped-broadcast case ADR-0005 accepts. The entry is kept and
    // rendered; the hole below it is fetched.
    if (unknown || frame.sequence > log.contiguous) {
      void this.fill(frame.conversation_id)
    }
    if (added) this.publish()
  }

  /**
   * fill pages forward from the contiguous mark until the server has nothing more.
   *
   * One fill per conversation at a time. A second gap noticed while one is running
   * sets refill rather than starting a concurrent pass, because two passes would
   * fetch the same pages and race on the same mark.
   */
  private async fill(conversationID: string): Promise<void> {
    const log = this.logOf(conversationID)
    if (log.filling) {
      log.refill = true
      return
    }
    log.filling = true

    try {
      do {
        log.refill = false
        let after = log.contiguous

        for (;;) {
          const page = await this.options.fetchEntries(conversationID, after)
          if (page.length === 0) break

          // Positions between the request and the first entry returned are ones
          // this account may not see, or that never existed — the server would
          // have sent them otherwise. Closing the mark over them is what stops a
          // member who joined a conversation late from treating the history
          // before their join point as a permanent gap and refetching forever.
          const first = page[0]!
          if (first.sequence > log.contiguous + 1) {
            log.contiguous = first.sequence - 1
            this.options.store?.closeOver(conversationID, log.contiguous)
          }

          for (const entry of page) this.store(conversationID, entry)

          const last = page[page.length - 1]!
          // A page that does not advance would loop forever. Not expected; a
          // client loop against a server is the wrong place to assume that.
          if (last.sequence <= after) break
          after = last.sequence
        }
      } while (log.refill)
    } catch (error) {
      // Left to the next reconnect or the next live entry, both of which trigger
      // another fill. Retrying here would hammer a server that is already unwell.
      this.options.onFatal?.(error)
    } finally {
      log.filling = false
      this.publish()
    }
  }

  // --- notification ---

  private setStatus(status: SyncStatus): void {
    if (this.status === status) return
    this.status = status
    this.publish()
  }

  private publish(): void {
    // ponytail: rebuilds and re-sorts every conversation on every change. Fine for
    // phase 2's in-memory scope; phase 6 replaces this with the local store, which
    // will read ranges rather than whole conversations.
    const conversations = new Map<string, Entry[]>()
    for (const [conversationID, log] of this.logs) {
      conversations.set(
        conversationID,
        [...log.entries.values()].sort((left, right) => left.sequence - right.sequence),
      )
    }
    // The reaction maps are mutated in place; what changes on every publish is the
    // snapshot's own identity, which is what useSyncExternalStore compares.
    this.snapshot = { status: this.status, conversations, reactions: new Map(this.reactions) }
    for (const listener of this.listeners) listener()
  }
}
