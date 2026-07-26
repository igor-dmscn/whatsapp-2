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
}

export type Entry = {
  id: string
  conversation_id: string
  sequence: number
  author_id: string
  client_entry_id: string
  kind: string
  content_type: string
  body: string
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
   * send is idempotent on clientEntryID.
   *
   * A retry with the same identifier returns the entry the first attempt created,
   * with the sequence it already has — which is what makes retrying a send safe
   * when the response was lost rather than the request.
   */
  send(conversationID: string, clientEntryID: string, text: string): Promise<Entry> {
    return this.authorized<Entry>(`/v1/conversations/${conversationID}/entries`, {
      method: 'POST',
      body: JSON.stringify({
        client_entry_id: clientEntryID,
        content_type: textContentType,
        body: encodeBody(text),
      }),
    })
  }
}
