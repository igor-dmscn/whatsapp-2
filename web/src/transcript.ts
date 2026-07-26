// Turning a log into something renderable.
//
// ADR-0008's consequence for clients, stated as code: "the log's element is an Entry,
// not a Message. A message is one kind of entry; a revision is another. Clients
// interpret entries rather than rendering them one-to-one."
//
// So this is where a conversation's entries become messages — each one carrying the
// latest amendment made to it. Kept pure and outside both the socket and React,
// because it is the piece most likely to be wrong and the easiest to test in
// isolation.

import { decodeBody, type Entry } from './api'

/** Message is one thing on screen, after amendments have been applied. */
export type Message = {
  /** sequence is the *original* entry's position, which is its identity. An edit does
   *  not move a message; it changes what it says. */
  sequence: number
  authorID: string
  text: string
  createdAt: string
  /** edited is true when a revision has been applied over the original. */
  edited: boolean
  /** retracted is true when the message has been withdrawn. Its text is empty, and no
   *  amount of later editing can bring content back — the server refuses it. */
  retracted: boolean
  replyTo: number
}

/**
 * resolve turns a conversation's entries into the messages to render.
 *
 * The rule is one line: apply the highest-sequenced amendment for each target. That is
 * correct because amendments always name the original rather than each other, so there
 * is no chain to walk and no order to reconstruct.
 *
 * Unknown kinds are skipped. A client from a future phase will publish entries this
 * code has never heard of, and the right response is to render what it does
 * understand — never to crash, and never to guess.
 */
export function resolve(entries: Entry[]): Message[] {
  const messages = new Map<number, Message>()
  // Amendments can arrive before the entry they amend: a client syncing a gap gets
  // them in order, but a live edit can land while the original is still being fetched.
  // Held aside rather than dropped, and applied when the original shows up.
  const pending = new Map<number, Entry[]>()

  const apply = (message: Message, amendment: Entry) => {
    if (message.retracted) return
    if (amendment.kind === 'retraction') {
      message.retracted = true
      message.text = ''
      return
    }
    message.text = decodeBody(amendment.body)
    message.edited = true
  }

  for (const entry of entries) {
    if (entry.kind === 'message') {
      const message: Message = {
        sequence: entry.sequence,
        authorID: entry.author_id,
        text: decodeBody(entry.body),
        createdAt: entry.created_at,
        edited: false,
        retracted: false,
        replyTo: entry.reply_to ?? 0,
      }
      messages.set(entry.sequence, message)

      // Amendments held aside, applied in sequence order so the last one wins.
      const waiting = pending.get(entry.sequence)
      if (waiting) {
        for (const amendment of waiting.sort((left, right) => left.sequence - right.sequence)) {
          apply(message, amendment)
        }
        pending.delete(entry.sequence)
      }
      continue
    }

    if (entry.kind === 'revision' || entry.kind === 'retraction') {
      const target = entry.target_sequence ?? 0
      if (target === 0) continue

      const message = messages.get(target)
      if (message) {
        apply(message, entry)
      } else {
        pending.set(target, [...(pending.get(target) ?? []), entry])
      }
      continue
    }

    // Anything else is from a newer server than this client. Ignored, deliberately:
    // failing here would turn a forward-compatible addition into a broken screen.
  }

  return [...messages.values()].sort((left, right) => left.sequence - right.sequence)
}

/** Reactions on one conversation: emoji to the accounts holding it, by position. */
export type ReactionsBySequence = Map<number, Map<string, Set<string>>>

/** summarise counts reactions on one entry, marking which the reader holds. */
export function summarise(
  reactions: ReactionsBySequence,
  sequence: number,
  me: string,
): { emoji: string; count: number; mine: boolean }[] {
  const held = reactions.get(sequence)
  if (!held) return []

  return [...held.entries()]
    .filter(([, accounts]) => accounts.size > 0)
    .map(([emoji, accounts]) => ({ emoji, count: accounts.size, mine: accounts.has(me) }))
    // Most-reacted first, then by emoji so the order is stable as counts change —
    // buttons that reorder themselves under the cursor get mis-clicked.
    .sort((left, right) => right.count - left.count || left.emoji.localeCompare(right.emoji))
}
