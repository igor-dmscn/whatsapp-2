// The HTTP half of the client. The socket half is in sync.ts.
//
// Sending is a request, not a socket frame: a send needs a response its author can
// act on — the assigned sequence, or a rejection. The socket carries only what the
// server initiates.

export type Account = {
  id: string
  handle: string
  created_at: string
}

export type Session = {
  account: Account
  device_id: string
  access_token: string
  access_expires_at: string
  refresh_token: string
  refresh_expires_at: string
}

export type Conversation = {
  id: string
  kind: string
  head: number
  role: string
  visible_from: number
  created_at: string

  // Projected fields, and every one of them is eventually consistent (ADR-0002).
  // An entry can be on screen before these move, and the UI has to be right during
  // that window rather than waiting for it to close (NF-7).
  unread: number
  read_through: number
  delivered_through: number
  // The lowest marks among the other members: what everyone else has received and
  // read. A client derives each of its own entries' delivery state by comparing the
  // entry's sequence against these, rather than the server keeping a state per entry
  // per recipient (MS-13).
  others_read_through: number
  others_delivered_through: number
}

/** Role is what a membership may do. Reader is every channel subscriber. */
export type Role = 'member' | 'admin' | 'reader'

export type Member = {
  account_id: string
  role: Role
  visible_from: number
  joined_at: string
  left_at: string | null
}

export type Invite = {
  id: string
  role: Role
  max_uses: number
  uses: number
  revoked: boolean
  created_at: string
  expires_at: string | null
  /** token is the shareable secret. Returned only to an administrator. */
  token: string
}

/** DeliveryState is what a sender can observe about one of their own entries. */
export type DeliveryState = 'sent' | 'delivered' | 'read'

/** deliveryOf derives an entry's state from the conversation's marks.
 *
 *  The same three-way comparison the server would do, done here because the marks
 *  answer the question for every entry at once and shipping them per entry would be
 *  the fan-out this design exists to avoid. */
export function deliveryOf(sequence: number, conversation: Conversation): DeliveryState {
  if (sequence <= conversation.others_read_through) return 'read'
  if (sequence <= conversation.others_delivered_through) return 'delivered'
  return 'sent'
}

export type Entry = {
  id: string
  conversation_id: string
  sequence: number
  author_id: string
  client_entry_id: string
  /** kind is 'message', 'revision' or 'retraction' — and may be something this client
   *  has never heard of. Anything unrecognised must be ignored, not rendered and not
   *  fatal (ADR-0008). */
  kind: string
  content_type: string
  body: string
  created_at: string
  /** target_sequence is the position this entry amends, absent on a message. */
  target_sequence?: number
  /** reply_to is the position this entry replies to, absent for none. */
  reply_to?: number
  /** attachment_id is the photo or video this entry carries, absent for none.
   *
   *  An identifier only. Whether it is ready is fetched separately, which is what lets
   *  a message carrying a 90 MB video be readable the instant it arrives (MD-1). */
  attachment_id?: string
}

/** AttachmentState is where an attachment is in its lifecycle.
 *
 *  Three states before it can be shown, and they mean different things to a viewer:
 *  pending is still uploading, uploaded is waiting for a thumbnail, failed will never
 *  have one. */
export type AttachmentState = 'pending' | 'uploaded' | 'ready' | 'failed'

export type Variant = {
  name: 'thumbnail' | 'display' | string
  content_type: string
  url: string
  width: number
  height: number
  byte_size: number
}

export type Attachment = {
  id: string
  conversation_id: string
  owner_id: string
  content_type: string
  byte_size: number
  state: AttachmentState
  failure?: string
  original_url?: string
  variants: Variant[]
  /** expires_at is when the URLs above stop working. Signed and short-lived, so a
   *  client that holds one for an hour re-fetches rather than showing a broken image. */
  expires_at: string
  created_at: string
}

type UploadTarget = {
  attachment_id: string
  url: string
  method: string
  headers: Record<string, string>
  expires_at: string
}

export type Reaction = {
  sequence: number
  account_id: string
  emoji: string
  created_at: string
}

/** ApiError carries the server's error code, which is what the UI branches on. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

/** SessionExpired means both tokens are unusable and the person must log in again. */
export class SessionExpired extends Error {
  constructor() {
    super('your session has expired')
    this.name = 'SessionExpired'
  }
}

// Payloads cross the wire base64-encoded because the server does not interpret
// them (ADR-0001). Encoding byte-by-byte rather than spreading into
// String.fromCharCode: the spread form overflows the argument limit on a long
// message, which would turn a large paste into a crash.
export function encodeBody(text: string): string {
  const bytes = new TextEncoder().encode(text)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary)
}

export function decodeBody(body: string): string {
  const binary = atob(body)
  const bytes = new Uint8Array(binary.length)
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index)
  return new TextDecoder().decode(bytes)
}

/** textContentType is what this client sends. Kept as a constant because phase 7
 *  adds others and every one of them must be a deliberate choice. */
export const textContentType = 'text/plain; charset=utf-8'

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...init.headers },
  })

  if (response.status === 204) return undefined as T

  const body = await response.json().catch(() => null)
  if (!response.ok) {
    const error = body?.error ?? {}
    throw new ApiError(response.status, error.code ?? 'unknown', error.message ?? response.statusText)
  }
  return body as T
}

export function register(handle: string, email: string, passphrase: string, deviceName: string): Promise<Session> {
  return request<Session>('/v1/accounts', {
    method: 'POST',
    body: JSON.stringify({ handle, email, passphrase, device_name: deviceName }),
  })
}

export function login(handle: string, passphrase: string, deviceName: string): Promise<Session> {
  return request<Session>('/v1/sessions', {
    method: 'POST',
    body: JSON.stringify({ handle, passphrase, device_name: deviceName }),
  })
}

// refreshMargin is how early a token is replaced. Access tokens live fifteen
// minutes, so refreshing a minute out costs one extra request an hour and removes
// the class of failure where a request leaves with a token that expires in flight.
const refreshMargin = 60_000

/**
 * Client is an authenticated caller.
 *
 * It owns the token pair, because the alternative is every caller remembering to
 * check expiry — and the one that forgets fails intermittently, fifteen minutes
 * into a session, which is the hardest kind of bug to attribute.
 */
export class Client {
  private session: Session
  private refreshing: Promise<void> | null = null

  constructor(
    session: Session,
    private readonly onSession: (session: Session) => void = () => {},
  ) {
    this.session = session
  }

  get accountID(): string {
    return this.session.account.id
  }

  get handle(): string {
    return this.session.account.handle
  }

  get deviceID(): string {
    return this.session.device_id
  }

  /** token returns an access token good for at least refreshMargin longer. */
  async token(): Promise<string> {
    if (Date.parse(this.session.access_expires_at) - Date.now() > refreshMargin) {
      return this.session.access_token
    }
    // Collapsed into one in-flight refresh: several concurrent calls arriving
    // just after expiry would otherwise each rotate the pair, and every rotation
    // but the last would invalidate the token the others just took.
    this.refreshing ??= this.refresh().finally(() => {
      this.refreshing = null
    })
    await this.refreshing
    return this.session.access_token
  }

  private async refresh(): Promise<void> {
    try {
      const refreshed = await request<Omit<Session, 'account'>>('/v1/sessions/refresh', {
        method: 'POST',
        body: JSON.stringify({ refresh_token: this.session.refresh_token }),
      })
      this.session = { ...refreshed, account: this.session.account }
      this.onSession(this.session)
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) throw new SessionExpired()
      throw error
    }
  }

  private async authorized<T>(path: string, init: RequestInit = {}): Promise<T> {
    const token = await this.token()
    return request<T>(path, { ...init, headers: { Authorization: `Bearer ${token}`, ...init.headers } })
  }

  lookupHandle(handle: string): Promise<Account> {
    return this.authorized<Account>(`/v1/accounts/${encodeURIComponent(handle)}`)
  }

  revise(conversationID: string, target: number, clientEntryID: string, text: string): Promise<Entry> {
    return this.authorized<Entry>(`/v1/conversations/${conversationID}/entries/${target}/revision`, {
      method: 'POST',
      body: JSON.stringify({
        client_entry_id: clientEntryID,
        content_type: textContentType,
        body: encodeBody(text),
      }),
    })
  }

  retract(conversationID: string, target: number, clientEntryID: string): Promise<Entry> {
    return this.authorized<Entry>(`/v1/conversations/${conversationID}/entries/${target}/retraction`, {
      method: 'POST',
      body: JSON.stringify({ client_entry_id: clientEntryID }),
    })
  }

  react(conversationID: string, sequence: number, emoji: string): Promise<void> {
    return this.authorized<void>(`/v1/conversations/${conversationID}/entries/${sequence}/reactions`, {
      method: 'PUT',
      body: JSON.stringify({ emoji }),
    })
  }

  unreact(conversationID: string, sequence: number, emoji: string): Promise<void> {
    return this.authorized<void>(`/v1/conversations/${conversationID}/entries/${sequence}/reactions`, {
      method: 'DELETE',
      body: JSON.stringify({ emoji }),
    })
  }

  async reactions(conversationID: string, from: number, to: number): Promise<Reaction[]> {
    const body = await this.authorized<{ reactions: Reaction[] }>(
      `/v1/conversations/${conversationID}/reactions?from=${from}&to=${to}`,
    )
    return body.reactions
  }

  startGroup(): Promise<Conversation> {
    return this.authorized<Conversation>('/v1/conversations/group', { method: 'POST' })
  }

  startChannel(): Promise<Conversation> {
    return this.authorized<Conversation>('/v1/conversations/channel', { method: 'POST' })
  }

  async members(conversationID: string): Promise<Member[]> {
    const body = await this.authorized<{ members: Member[] }>(`/v1/conversations/${conversationID}/members`)
    return body.members
  }

  addMember(conversationID: string, accountID: string): Promise<Member> {
    return this.authorized<Member>(`/v1/conversations/${conversationID}/members`, {
      method: 'POST',
      body: JSON.stringify({ account_id: accountID }),
    })
  }

  removeMember(conversationID: string, accountID: string): Promise<void> {
    return this.authorized<void>(`/v1/conversations/${conversationID}/members/${accountID}`, {
      method: 'DELETE',
    })
  }

  changeRole(conversationID: string, accountID: string, role: Role): Promise<void> {
    return this.authorized<void>(`/v1/conversations/${conversationID}/members/${accountID}/role`, {
      method: 'PUT',
      body: JSON.stringify({ role }),
    })
  }

  leave(conversationID: string): Promise<void> {
    return this.authorized<void>(`/v1/conversations/${conversationID}/membership`, { method: 'DELETE' })
  }

  createInvite(conversationID: string, maxUses = 0): Promise<Invite> {
    return this.authorized<Invite>(`/v1/conversations/${conversationID}/invites`, {
      method: 'POST',
      body: JSON.stringify({ max_uses: maxUses, expires_at: null }),
    })
  }

  async invites(conversationID: string): Promise<Invite[]> {
    const body = await this.authorized<{ invites: Invite[] }>(`/v1/conversations/${conversationID}/invites`)
    return body.invites
  }

  revokeInvite(inviteID: string): Promise<void> {
    return this.authorized<void>(`/v1/invites/${inviteID}`, { method: 'DELETE' })
  }

  /** redeemInvite joins by link. Idempotent: clicking a link twice lands you in the
   *  conversation rather than reporting an error. */
  redeemInvite(token: string): Promise<Conversation> {
    return this.authorized<Conversation>(`/v1/invites/${encodeURIComponent(token)}/redeem`, {
      method: 'POST',
    })
  }

  startDirect(accountID: string): Promise<Conversation> {
    return this.authorized<Conversation>('/v1/conversations/direct', {
      method: 'POST',
      body: JSON.stringify({ account_id: accountID }),
    })
  }

  async conversations(): Promise<Conversation[]> {
    const body = await this.authorized<{ conversations: Conversation[] }>('/v1/conversations')
    return body.conversations
  }

  /** entries returns what follows a sequence, oldest first, up to the server's page size. */
  async entries(conversationID: string, after: number): Promise<Entry[]> {
    const body = await this.authorized<{ entries: Entry[] }>(
      `/v1/conversations/${conversationID}/entries?after=${after}`,
    )
    return body.entries
  }

  /**
   * acknowledge reports how far this account has received and read a conversation.
   *
   * Both marks in one call because a client that has just rendered messages knows
   * both answers at once. Neither is stored synchronously — the server answers 202
   * and the projection catches up, so nothing here should be read back expecting to
   * see its own effect immediately.
   */
  async acknowledge(conversationID: string, deliveredThrough: number, readThrough: number): Promise<void> {
    await this.authorized<void>(`/v1/conversations/${conversationID}/receipt`, {
      method: 'POST',
      body: JSON.stringify({ delivered_through: deliveredThrough, read_through: readThrough }),
    })
  }

  /**
   * send is idempotent on clientEntryID.
   *
   * A retry with the same identifier returns the entry the first attempt created,
   * with the sequence it already has — which is what makes retrying a send safe
   * when the response was lost rather than the request.
   */
  send(
    conversationID: string,
    clientEntryID: string,
    text: string,
    replyTo = 0,
    attachmentID = '',
  ): Promise<Entry> {
    return this.authorized<Entry>(`/v1/conversations/${conversationID}/entries`, {
      method: 'POST',
      body: JSON.stringify({
        client_entry_id: clientEntryID,
        content_type: textContentType,
        body: encodeBody(text),
        reply_to: replyTo,
        attachment_id: attachmentID,
      }),
    })
  }

  /**
   * attach uploads a file and returns the attachment identifier to reference it by.
   *
   * Three steps, and the middle one does not touch the api: the bytes go straight to
   * the object store with a signed URL, so a hundred megabytes never passes through the
   * server. That is the whole reason for the round trip before it.
   *
   * The identifier is returned as soon as the upload completes, not when the attachment
   * is ready — the caller sends its message immediately and the photo appears when it
   * has been processed (MD-1).
   */
  async attach(conversationID: string, file: File, onProgress?: (fraction: number) => void): Promise<string> {
    const contentType = file.type || 'application/octet-stream'

    const target = await this.authorized<UploadTarget>('/v1/attachments', {
      method: 'POST',
      body: JSON.stringify({
        conversation_id: conversationID,
        content_type: contentType,
        // Declared before anything is sent, and signed into the URL. A client that
        // lies is refused by the store, not by the api (MD-4).
        byte_size: file.size,
      }),
    })

    await this.transfer(target, file, contentType, onProgress)

    await this.authorized<{ state: string }>(
      `/v1/attachments/${target.attachment_id}/completion`, { method: 'POST' })

    return target.attachment_id
  }

  /**
   * transfer PUTs the bytes to the store.
   *
   * XMLHttpRequest rather than fetch, and only for this: upload progress. A request
   * body stream is the fetch equivalent and is still not available everywhere, while a
   * hundred-megabyte upload with no progress bar looks broken.
   *
   * No credentials are attached, deliberately — the signature is the authorisation, and
   * sending a bearer token to the object store would leak it there.
   */
  private transfer(
    target: UploadTarget,
    file: File,
    contentType: string,
    onProgress?: (fraction: number) => void,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest()
      request.open(target.method, target.url)
      // Content-Length is set by the browser from the body and cannot be set here;
      // it matches file.size, which is what was signed.
      request.setRequestHeader('Content-Type', contentType)

      request.upload.addEventListener('progress', (event) => {
        if (event.lengthComputable) onProgress?.(event.loaded / event.total)
      })
      request.addEventListener('load', () => {
        if (request.status >= 200 && request.status < 300) {
          resolve()
          return
        }
        // The store's refusals are XML, and the status is what distinguishes a
        // signature mismatch from an expired URL. Both mean: ask for another.
        reject(new ApiError(request.status, 'upload_refused', 'the store refused the upload'))
      })
      request.addEventListener('error', () =>
        reject(new ApiError(0, 'upload_failed', 'the upload could not be completed')))
      request.send(file)
    })
  }

  /** attachment returns an attachment's state and freshly signed URLs. */
  attachment(attachmentID: string): Promise<Attachment> {
    return this.authorized<Attachment>(`/v1/attachments/${attachmentID}`)
  }
}
