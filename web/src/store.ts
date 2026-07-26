// The browser client's local store: SQLite compiled to WebAssembly, persisted in OPFS.
//
// This is the browser half of phase 6, and it is deliberately the *same* store as the
// CLI's — same schema, same queries, same search expression. The server cannot search
// (ADR-0001), so search is a client feature, and two clients that search differently
// give two different products. Holding both to one schema is the cheapest way to keep
// them honest.
//
// How it persists, and why it is not a VFS. SQLite's OPFS backends both need
// `createSyncAccessHandle`, which browsers expose **only inside a Worker** — so an OPFS
// VFS means the database lives in a worker and every read becomes asynchronous. That
// ripples all the way into rendering: `useSyncExternalStore` needs a synchronous
// snapshot, so an async store means a loading state on every screen that reads one.
//
// Instead the database is in memory on this thread and its bytes are snapshotted into
// OPFS after writes settle. Reads stay synchronous, a reload restores the snapshot, and
// the worst case is losing the last second of writes — which sync repairs on the next
// connection, because the mark is in the snapshot too and the server is asked for
// whatever follows it.
//
// ponytail: the whole database is exported per snapshot, so cost grows with total size
// rather than with what changed. Fine while a client's store is megabytes. The upgrade
// is the worker-based SAH pool VFS with an async store API, which is a bigger change to
// the render path than to the storage.

import sqlite3InitModule, { type Database, type Sqlite3Static } from '@sqlite.org/sqlite-wasm'

/** snapshotDelay is how long writes settle before the database is written to OPFS.
 *
 *  Debounced because a gap fill is hundreds of inserts in a burst, and snapshotting each
 *  one would export the whole database hundreds of times. A second of unsnapshotted
 *  writes is recoverable: the mark is in the snapshot, so sync asks for whatever follows
 *  it. */
const snapshotDelay = 250

/** Held is one entry as the client stores it. Bodies are stored decoded: this is where
 *  they are read, and search needs text rather than base64. */
export type Held = {
  conversation_id: string
  sequence: number
  id: string
  author_id: string
  client_entry_id: string
  kind: string
  body: string
  target_sequence: number
  reply_to: number
  created_at: string
}

export type HeldConversation = {
  id: string
  kind: string
  head: number
  role: string
  visible_from: number
  unread: number
  contiguous: number
  others_read: number
  others_delivered: number
}

export type Hit = {
  conversation_id: string
  sequence: number
  body: string
}

/** migrations are applied in order and recorded.
 *
 *  Identical to the CLI's list, and that is not a coincidence to be tidied away: the
 *  two stores must answer the same questions the same way, and the cheapest guarantee
 *  of that is one schema written twice rather than two schemas that drift. */
const migrations = [
  `CREATE TABLE conversations (
    id TEXT PRIMARY KEY, kind TEXT NOT NULL, head INTEGER NOT NULL DEFAULT 0,
    role TEXT NOT NULL DEFAULT 'member', visible_from INTEGER NOT NULL DEFAULT 1,
    unread INTEGER NOT NULL DEFAULT 0, contiguous INTEGER NOT NULL DEFAULT 0,
    others_read INTEGER NOT NULL DEFAULT 0, others_delivered INTEGER NOT NULL DEFAULT 0,
    label TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT ''
  )`,
  `CREATE TABLE entries (
    conversation_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT NOT NULL,
    author_id TEXT NOT NULL, client_entry_id TEXT NOT NULL, kind TEXT NOT NULL,
    body TEXT NOT NULL, target_sequence INTEGER NOT NULL DEFAULT 0,
    reply_to INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
    PRIMARY KEY (conversation_id, sequence)
  )`,
  `CREATE VIRTUAL TABLE entries_fts USING fts5(
    body, content = 'entries', content_rowid = 'rowid',
    tokenize = 'unicode61 remove_diacritics 2'
  )`,
  `CREATE TRIGGER entries_fts_insert AFTER INSERT ON entries BEGIN
    INSERT INTO entries_fts (rowid, body) VALUES (new.rowid, new.body);
  END`,
  `CREATE TRIGGER entries_fts_delete AFTER DELETE ON entries BEGIN
    INSERT INTO entries_fts (entries_fts, rowid, body) VALUES ('delete', old.rowid, old.body);
  END`,
  `CREATE TRIGGER entries_fts_update AFTER UPDATE ON entries BEGIN
    INSERT INTO entries_fts (entries_fts, rowid, body) VALUES ('delete', old.rowid, old.body);
    INSERT INTO entries_fts (rowid, body) VALUES (new.rowid, new.body);
  END`,
  `CREATE TABLE pending (
    client_entry_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, body TEXT NOT NULL,
    reply_to INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0
  )`,
  `CREATE TABLE reactions (
    conversation_id TEXT NOT NULL, sequence INTEGER NOT NULL, account_id TEXT NOT NULL,
    emoji TEXT NOT NULL, PRIMARY KEY (conversation_id, sequence, account_id, emoji)
  )`,
  `CREATE INDEX entries_by_conversation ON entries (conversation_id, sequence)`,
]

/**
 * ftsQuery turns what somebody typed into an FTS5 expression.
 *
 * Character-for-character the CLI's implementation. Two reasons it is not the raw
 * string: FTS5's syntax would otherwise leak, so a query containing a quote is a syntax
 * error rather than a search — and to the person typing, an error is indistinguishable
 * from "no results". And prefixing the final term is what makes results appear while
 * typing rather than after.
 */
export function ftsQuery(raw: string): string {
  const terms = raw
    .split(/\s+/)
    .filter((word) => word !== '')
    .map((word) => `"${word.replaceAll('"', '""')}"`)

  if (terms.length === 0) return ''
  // Prefix on the last term only: prefixing every term would make "a b" match far
  // too much.
  terms[terms.length - 1] += '*'
  return terms.join(' AND ')
}

/** restore loads a snapshot's bytes into an open database.
 *
 *  sqlite3_deserialize, which hands SQLite the buffer directly rather than replaying
 *  statements — so restoring a store is one memcpy rather than a rebuild.
 */
function restore(sqlite3: Sqlite3Static, db: Database, bytes: Uint8Array): void {
  const { capi, wasm } = sqlite3
  const pointer = wasm.allocFromTypedArray(bytes)
  const result = capi.sqlite3_deserialize(
    db,
    'main',
    pointer,
    bytes.byteLength,
    bytes.byteLength,
    capi.SQLITE_DESERIALIZE_FREEONCLOSE | capi.SQLITE_DESERIALIZE_RESIZEABLE,
  )
  if (result !== 0) {
    // A snapshot that will not load is a corrupt one. Freed and ignored: the schema is
    // then created fresh and sync refills it, which is slower than a restore and better
    // than a client that cannot start.
    wasm.dealloc(pointer)
  }
}

/** LocalStore is the browser's persisted copy of what it has synced. */
export class LocalStore {
  private snapshotTimer: ReturnType<typeof setTimeout> | null = null
  private snapshotting = false

  private constructor(
    private readonly sqlite3: Sqlite3Static,
    private readonly db: Database,
    /** file is the OPFS handle the snapshot is written to, absent when unavailable. */
    private readonly file: FileSystemFileHandle | null,
  ) {}

  /**
   * open initialises SQLite and brings the schema up to date.
   *
   * Falls back to an in-memory database when OPFS is unavailable — a private window, an
   * older browser, a context where storage is denied. The app then works exactly as it
   * did before this phase: nothing persists, everything else is the same. Refusing to
   * start would be the wrong trade for a feature that is an optimisation of a cold
   * start.
   */
  static async open(name = 'comms.db'): Promise<LocalStore> {
    const sqlite3 = await sqlite3InitModule()
    const db = new sqlite3.oo1.DB(':memory:')

    let file: FileSystemFileHandle | null = null
    try {
      const root = await navigator.storage.getDirectory()
      file = await root.getFileHandle(name, { create: true })

      const existing = new Uint8Array(await (await file.getFile()).arrayBuffer())
      if (existing.byteLength > 0) restore(sqlite3, db, existing)
    } catch {
      // No OPFS: a private window, storage denied, an older browser. The client then
      // behaves as it did before phase 6 — everything in memory, a reload refetches —
      // which is a worse experience and a working one. Refusing to start would be the
      // wrong trade for what is an optimisation of a cold start.
      file = null
    }

    const store = new LocalStore(sqlite3, db, file)
    store.migrate()

    // A closing tab must not lose the last quarter-second of writes. pagehide rather
    // than beforeunload: it fires on mobile backgrounding too, which is where a tab is
    // most likely to be discarded without warning.
    //
    // Best effort — the browser may not wait for the write. That is acceptable because
    // the mark is in the snapshot: whatever was lost is refetched on the next
    // connection, which is the same recovery every other gap uses.
    if (typeof addEventListener === 'function') {
      addEventListener('pagehide', () => void store.flush())
    }

    return store
  }

  /** snapshot writes the database to OPFS, debounced.
   *
   *  Called after every write. Serialised against itself with a flag rather than a
   *  queue: a snapshot that starts while one is running would interleave two writes to
   *  one file, and skipping is harmless because the next write schedules another. */
  private snapshot(): void {
    if (!this.file) return

    if (this.snapshotTimer) clearTimeout(this.snapshotTimer)
    this.snapshotTimer = setTimeout(() => {
      this.snapshotTimer = null
      void this.writeSnapshot()
    }, snapshotDelay)
  }

  private async writeSnapshot(): Promise<void> {
    if (!this.file || this.snapshotting) return
    this.snapshotting = true

    try {
      const bytes = this.sqlite3.capi.sqlite3_js_db_export(this.db)
      const writable = await this.file.createWritable()
      await writable.write(bytes)
      await writable.close()
    } catch (failure) {
      // A failed snapshot costs durability, not correctness: the in-memory database is
      // unaffected and the next write tries again. Reported, though — a store that
      // silently stops persisting looks exactly like one that was never persisting, and
      // an empty catch here is what made that hard to find the first time.
      console.error('local store snapshot failed', failure)
    } finally {
      this.snapshotting = false
    }
  }

  /** flush writes the snapshot immediately. For a page about to be unloaded, and for
   *  tests that need to assert what a reload would find. */
  async flush(): Promise<void> {
    if (this.snapshotTimer) {
      clearTimeout(this.snapshotTimer)
      this.snapshotTimer = null
    }
    await this.writeSnapshot()
  }

  /** persistent reports whether writes survive a reload. Shown in the interface, because
   *  "your messages are cached" and "they are not" is a difference somebody should be
   *  able to see rather than infer from a slow cold start. */
  get persistent(): boolean {
    return this.file !== null
  }

  close(): void {
    this.db.close()
  }

  private migrate(): void {
    this.db.exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`)
    const applied = Number(
      this.db.selectValue(`SELECT coalesce(max(version), 0) FROM schema_version`) ?? 0,
    )

    for (let index = applied; index < migrations.length; index++) {
      // One transaction per migration, so a failure halfway leaves the version behind
      // rather than the schema half-applied.
      this.db.transaction(() => {
        this.db.exec(migrations[index]!)
        this.db.exec({
          sql: `INSERT INTO schema_version (version) VALUES (?)`,
          bind: [index + 1],
        })
      })
    }
  }

  // --- conversations ---

  /** saveConversation upserts what the server said.
   *
   *  contiguous is deliberately not written: it is the client's own bookkeeping and the
   *  server has no opinion about what this device holds. */
  saveConversation(conversation: {
    id: string
    kind: string
    head: number
    role: string
    visible_from: number
    unread: number
    others_read_through: number
    others_delivered_through: number
  }): void {
    this.db.exec({
      sql: `INSERT INTO conversations
              (id, kind, head, role, visible_from, unread, others_read, others_delivered, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT (id) DO UPDATE SET
              kind = excluded.kind,
              head = max(conversations.head, excluded.head),
              role = excluded.role,
              visible_from = excluded.visible_from,
              unread = excluded.unread,
              others_read = excluded.others_read,
              others_delivered = excluded.others_delivered,
              updated_at = datetime('now')`,
      bind: [
        conversation.id,
        conversation.kind,
        conversation.head,
        conversation.role,
        conversation.visible_from,
        conversation.unread,
        conversation.others_read_through,
        conversation.others_delivered_through,
      ],
    })
    this.snapshot()
  }

  conversations(): HeldConversation[] {
    return this.db.selectObjects(
      `SELECT id, kind, head, role, visible_from, unread, contiguous, others_read, others_delivered
         FROM conversations ORDER BY head DESC, id`,
    ) as HeldConversation[]
  }

  /** cursor is the resume frame's contents: the contiguous mark per conversation. */
  cursor(): Record<string, number> {
    const rows = this.db.selectObjects(`SELECT id, contiguous FROM conversations`) as {
      id: string
      contiguous: number
    }[]
    return Object.fromEntries(rows.map((row) => [row.id, row.contiguous]))
  }

  // --- entries ---

  /** saveEntries stores entries and advances the mark, in one transaction.
   *
   *  One transaction for both, and it matters: a mark saved without its entries makes
   *  the client resume past messages it does not hold, and unlike the in-memory case
   *  that loss survives a reload. */
  saveEntries(conversationID: string, entries: Held[]): void {
    if (entries.length === 0) return

    this.db.transaction(() => {
      // A conversation row for anything we hold entries of, even if the list has not
      // been fetched yet. Without it the mark cannot advance and hydration finds
      // nothing — entries were stored, the conversation was not, and a cold start
      // rendered an empty screen over a full database.
      //
      // The kind is left empty and the join point assumed to be the first position: both
      // are corrected by the next refresh, and a join point that is too low costs a
      // refetch where one too high would skip messages.
      this.db.exec({
        sql: `INSERT INTO conversations (id, kind, visible_from) VALUES (?, '', 1)
              ON CONFLICT (id) DO NOTHING`,
        bind: [conversationID],
      })

      for (const entry of entries) {
        this.db.exec({
          sql: `INSERT INTO entries
                  (conversation_id, sequence, id, author_id, client_entry_id, kind, body, target_sequence, reply_to, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (conversation_id, sequence) DO NOTHING`,
          bind: [
            entry.conversation_id,
            entry.sequence,
            entry.id,
            entry.author_id,
            entry.client_entry_id,
            entry.kind,
            entry.body,
            entry.target_sequence,
            entry.reply_to,
            entry.created_at,
          ],
        })
        // A stored entry is one no longer pending. Cleared by client identifier, so an
        // entry arriving over the socket before its own send response still reconciles.
        this.db.exec({
          sql: `DELETE FROM pending WHERE client_entry_id = ?`,
          bind: [entry.client_entry_id],
        })
      }
      this.advanceContiguous(conversationID)
    })
    this.snapshot()
  }

  /** advanceContiguous walks the mark forward over what is now held.
   *
   *  Recomputed from the rows rather than incremented, so it cannot drift. */
  private advanceContiguous(conversationID: string): void {
    const row = this.db.selectObject(
      `SELECT contiguous, visible_from FROM conversations WHERE id = ?`,
      [conversationID],
    ) as { contiguous: number; visible_from: number } | undefined
    if (!row) return

    // Positions below the join point are not this member's to hold, so the mark starts
    // at the point before it rather than at zero.
    let mark = Math.max(row.contiguous, row.visible_from - 1)

    for (;;) {
      const held = this.db.selectValue(
        `SELECT 1 FROM entries WHERE conversation_id = ? AND sequence = ?`,
        [conversationID, mark + 1],
      )
      if (!held) break
      mark++
    }

    this.db.exec({
      sql: `UPDATE conversations SET contiguous = ?, head = max(head, ?) WHERE id = ?`,
      bind: [mark, mark, conversationID],
    })
  }

  /** closeOver moves the mark over positions the server will never send.
   *
   *  The other rule from docs/client-sync.md: a fetch that skips positions has said
   *  those positions are not this member's to see. */
  closeOver(conversationID: string, through: number): void {
    this.db.exec({
      sql: `UPDATE conversations SET contiguous = max(contiguous, ?) WHERE id = ?`,
      bind: [through, conversationID],
    })
    this.snapshot()
  }

  entries(conversationID: string): Held[] {
    return this.db.selectObjects(
      `SELECT conversation_id, sequence, id, author_id, client_entry_id, kind, body, target_sequence, reply_to, created_at
         FROM entries WHERE conversation_id = ? ORDER BY sequence`,
      [conversationID],
    ) as Held[]
  }

  /** applyAmendment records a revision or retraction against its target.
   *
   *  The target's row is updated locally, which is not a contradiction of ADR-0008: the
   *  log is on the server and this is a cache of what the log currently means. The
   *  amendment keeps its own row, so the mark still passes through its position. */
  applyAmendment(conversationID: string, target: number, kind: string, body: string): void {
    if (kind === 'revision') {
      this.db.exec({
        sql: `UPDATE entries SET body = ? WHERE conversation_id = ? AND sequence = ?`,
        bind: [body, conversationID, target],
      })
      this.snapshot()
      return
    }
    if (kind === 'retraction') {
      // Emptied rather than deleted: the position stays held so the mark develops no
      // hole, and the trigger drops the old text from the index — a withdrawn message
      // must not be findable by what it used to say.
      this.db.exec({
        sql: `UPDATE entries SET body = '', kind = 'retracted' WHERE conversation_id = ? AND sequence = ?`,
        bind: [conversationID, target],
      })
    }
    this.snapshot()
  }

  // --- search ---

  /** search finds entries matching a query, most recent first.
   *
   *  The same SQL the CLI runs. Retractions and amendments are excluded: an amendment is
   *  not a message, and a withdrawn one has no text to find. */
  search(query: string, limit = 50): Hit[] {
    const term = ftsQuery(query)
    if (term === '') return []

    return this.db.selectObjects(
      `SELECT e.conversation_id, e.sequence, e.body
         FROM entries_fts f
         JOIN entries e ON e.rowid = f.rowid
        WHERE entries_fts MATCH ?
          AND e.kind = 'message'
        ORDER BY e.sequence DESC
        LIMIT ?`,
      [term, limit],
    ) as Hit[]
  }

  // --- pending sends ---

  /** addPending records a send before it is attempted.
   *
   *  Written first, deliberately. A send that goes out unrecorded is lost if the tab
   *  closes waiting for the response — and its client identifier with it, so the retry
   *  would create a second entry rather than being recognised as the same one (MS-2). */
  addPending(pending: { clientEntryID: string; conversationID: string; body: string; replyTo: number }): void {
    this.db.exec({
      sql: `INSERT INTO pending (client_entry_id, conversation_id, body, reply_to, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT (client_entry_id) DO UPDATE SET attempts = pending.attempts + 1`,
      bind: [pending.clientEntryID, pending.conversationID, pending.body, pending.replyTo],
    })
    this.snapshot()
  }

  pendingSends(): { client_entry_id: string; conversation_id: string; body: string; reply_to: number }[] {
    return this.db.selectObjects(
      `SELECT client_entry_id, conversation_id, body, reply_to FROM pending ORDER BY created_at`,
    ) as { client_entry_id: string; conversation_id: string; body: string; reply_to: number }[]
  }

  clearPending(clientEntryID: string): void {
    this.db.exec({ sql: `DELETE FROM pending WHERE client_entry_id = ?`, bind: [clientEntryID] })
    this.snapshot()
  }

  // --- reactions ---

  setReaction(conversationID: string, sequence: number, accountID: string, emoji: string, removed: boolean): void {
    if (removed) {
      this.db.exec({
        sql: `DELETE FROM reactions WHERE conversation_id = ? AND sequence = ? AND account_id = ? AND emoji = ?`,
        bind: [conversationID, sequence, accountID, emoji],
      })
      return
    }
    this.db.exec({
      sql: `INSERT INTO reactions (conversation_id, sequence, account_id, emoji) VALUES (?, ?, ?, ?)
            ON CONFLICT DO NOTHING`,
      bind: [conversationID, sequence, accountID, emoji],
    })
    this.snapshot()
  }

  reactions(conversationID: string): { sequence: number; account_id: string; emoji: string }[] {
    return this.db.selectObjects(
      `SELECT sequence, account_id, emoji FROM reactions WHERE conversation_id = ?`,
      [conversationID],
    ) as { sequence: number; account_id: string; emoji: string }[]
  }
}
