// The client half of ADR-0008: whether a log of entries becomes the right screen.
//
// Fast and hermetic — no server, no socket. The behaviour these protect is the one the
// whole decision exists for, and the one that breaks silently: an edit applied to the
// wrong message, or not applied at all, looks like nothing went wrong.

import { describe, expect, it } from 'vitest'

import { encodeBody, type Entry } from './api'
import { resolve, summarise, type ReactionsBySequence } from './transcript'

function entry(sequence: number, kind: string, text: string, extra: Partial<Entry> = {}): Entry {
  return {
    id: `entry-${sequence}`,
    conversation_id: 'conversation-1',
    sequence,
    author_id: 'account-1',
    client_entry_id: `client-${sequence}`,
    kind,
    content_type: 'text/plain; charset=utf-8',
    body: encodeBody(text),
    created_at: '2026-01-01T00:00:00.000Z',
    ...extra,
  }
}

describe('resolve', () => {
  it('renders messages in sequence order', () => {
    const messages = resolve([entry(1, 'message', 'first'), entry(2, 'message', 'second')])

    expect(messages.map((message) => message.text)).toEqual(['first', 'second'])
  })

  it('applies an edit to the message it names, not to its own position', () => {
    // The heart of ADR-0008 on the client: a revision occupies position 2 but changes
    // what position 1 says, and is not itself a message on screen.
    const messages = resolve([
      entry(1, 'message', 'the original'),
      entry(2, 'revision', 'the correction', { target_sequence: 1 }),
    ])

    expect(messages).toHaveLength(1)
    expect(messages[0]!.sequence).toBe(1)
    expect(messages[0]!.text).toBe('the correction')
    expect(messages[0]!.edited).toBe(true)
  })

  it('applies the last of several edits', () => {
    const messages = resolve([
      entry(1, 'message', 'first wording'),
      entry(2, 'revision', 'second wording', { target_sequence: 1 }),
      entry(3, 'revision', 'third wording', { target_sequence: 1 }),
    ])

    expect(messages[0]!.text).toBe('third wording')
  })

  it('withdraws content on a retraction and keeps it withdrawn', () => {
    // Terminal on the client as well as on the server. An edit arriving after a
    // retraction — from a replay, or from a server that allowed it — must not put
    // content back on screen.
    const messages = resolve([
      entry(1, 'message', 'regretted'),
      entry(2, 'retraction', '', { target_sequence: 1 }),
      entry(3, 'revision', 'second thoughts', { target_sequence: 1 }),
    ])

    expect(messages[0]!.retracted).toBe(true)
    expect(messages[0]!.text).toBe('')
  })

  it('applies an amendment that arrives before the message it amends', () => {
    // A live edit can land while the original is still being fetched. Dropping it
    // would leave the message showing its original wording forever, with nothing to
    // trigger a correction.
    const messages = resolve([
      entry(2, 'revision', 'the correction', { target_sequence: 1 }),
      entry(1, 'message', 'the original'),
    ])

    expect(messages[0]!.text).toBe('the correction')
    expect(messages[0]!.edited).toBe(true)
  })

  it('ignores a kind it has never heard of rather than failing', () => {
    // A newer server publishing something new must not break an older client. This is
    // the same requirement the Go suite states from the server's side.
    const messages = resolve([
      entry(1, 'message', 'still fine'),
      entry(2, 'something_from_a_later_phase', 'unknown', { target_sequence: 1 }),
      entry(3, 'message', 'also fine'),
    ])

    expect(messages.map((message) => message.text)).toEqual(['still fine', 'also fine'])
    // And critically the message it referenced is untouched, not half-applied.
    expect(messages[0]!.edited).toBe(false)
  })

  it('carries a reply reference through', () => {
    const messages = resolve([
      entry(1, 'message', 'a question'),
      entry(2, 'message', 'an answer', { reply_to: 1 }),
    ])

    expect(messages[1]!.replyTo).toBe(1)
  })

  it('ignores an amendment naming no target', () => {
    const messages = resolve([entry(1, 'message', 'fine'), entry(2, 'revision', 'orphan')])

    expect(messages).toHaveLength(1)
    expect(messages[0]!.text).toBe('fine')
  })
})

describe('summarise', () => {
  const reactions: ReactionsBySequence = new Map([
    [
      1,
      new Map([
        ['👍', new Set(['account-1', 'account-2'])],
        ['🎉', new Set(['account-2'])],
      ]),
    ],
  ])

  it('counts each emoji and marks the reader’s own', () => {
    expect(summarise(reactions, 1, 'account-1')).toEqual([
      { emoji: '👍', count: 2, mine: true },
      { emoji: '🎉', count: 1, mine: false },
    ])
  })

  it('reports nothing for an entry with no reactions', () => {
    expect(summarise(reactions, 2, 'account-1')).toEqual([])
  })

  it('drops an emoji whose last holder removed it', () => {
    // The socket clears one account at a time, so an emoji legitimately reaches zero
    // holders and must stop being rendered rather than showing a count of nothing.
    const emptied: ReactionsBySequence = new Map([[1, new Map([['👍', new Set<string>()]])]])

    expect(summarise(emptied, 1, 'account-1')).toEqual([])
  })
})
