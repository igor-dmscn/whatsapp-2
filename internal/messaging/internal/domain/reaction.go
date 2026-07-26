package domain

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Reaction is somebody's reaction to an entry.
//
// Deliberately not in the log (ADR-0008, MS-10). Reactions are high-churn: a popular
// message in a large group would burn a sequence number per tap and wake every
// connected client each time. So they are separate state, keyed by what they are
// about, with no position of their own — and the conversation's head does not move
// when one is added.
//
// Not an aggregate root either, and worth being explicit about why. A reaction has no
// invariant spanning anything: the rule "one account holds one of each emoji on one
// entry" is a primary key, not behaviour, and there is no state transition it can
// undergo — it exists or it does not. It is a value, and the storage enforces its one
// rule better than an aggregate could against concurrent taps.
type Reaction struct {
	ConversationID ConversationID
	Sequence       Sequence
	AccountID      AccountID
	Emoji          Emoji
	CreatedAt      time.Time
}

// Emoji is a validated reaction symbol.
//
// A value object rather than a string because the validation is the entire point:
// this field is rendered verbatim into every member's client, and "any short string"
// is a way to put arbitrary text on somebody else's message without it counting as a
// message.
type Emoji string

// emojiMaxBytes bounds one reaction. Enough for a multi-codepoint sequence — a
// family, a flag, a skin-toned gesture — and far short of a sentence.
const emojiMaxBytes = 32

// ParseEmoji validates a reaction symbol.
//
// The rule is "symbols and marks only, no letters, digits, spaces or punctuation".
// That admits the emoji people actually use, including sequences joined by
// zero-width joiners and modified by variation selectors, while excluding text.
// Enumerating permitted codepoints would be the alternative and would be a list that
// is out of date the day a Unicode revision ships.
func ParseEmoji(raw string) (Emoji, error) {
	if raw == "" {
		return "", ValidationError{"emoji", "must not be empty"}
	}
	if len(raw) > emojiMaxBytes {
		return "", ValidationError{"emoji", "is too long to be a reaction"}
	}
	if !utf8.ValidString(raw) {
		return "", ValidationError{"emoji", "is not valid text"}
	}

	for _, character := range raw {
		switch {
		case unicode.Is(unicode.So, character), // other symbols: most emoji
			unicode.Is(unicode.Sk, character), // modifier symbols: skin tones
			unicode.Is(unicode.Mn, character), // non-spacing marks: variation selectors
			unicode.Is(unicode.Cf, character), // format: zero-width joiner
			unicode.Is(unicode.Sm, character), // maths symbols: a few legacy emoji
			unicode.Is(unicode.Sc, character), // currency: likewise
			character >= 0x1F000:              // supplementary planes, where most emoji live
		default:
			return "", ValidationError{"emoji", "must be a symbol, not text"}
		}
	}

	return Emoji(strings.TrimSpace(raw)), nil
}

// NewReaction validates and returns a reaction.
//
// Takes the membership rather than an account identifier, so that reacting is subject
// to the same visibility rule as reading: reacting to an entry from before your join
// point would tell its author you had read something you cannot see.
func NewReaction(
	member *Membership,
	sequence Sequence,
	head Sequence,
	raw string,
	now time.Time,
) (Reaction, error) {
	if member == nil || !member.Active() {
		return Reaction{}, ErrNotAMember
	}
	if sequence < FirstSequence || sequence > head {
		return Reaction{}, ValidationError{"sequence", "is not a position in this conversation"}
	}
	if !member.CanSee(sequence) {
		return Reaction{}, ErrEntryNotFound
	}

	emoji, err := ParseEmoji(raw)
	if err != nil {
		return Reaction{}, err
	}

	return Reaction{
		ConversationID: member.ConversationID(),
		Sequence:       sequence,
		AccountID:      member.AccountID(),
		Emoji:          emoji,
		CreatedAt:      now,
	}, nil
}

// A reader may react. Reading and reacting are the same entitlement: a channel
// subscriber who could not react would be unable to do the one thing a broadcast
// audience does. Writing is a separate permission and is not required here — which is
// why NewReaction asks CanSee rather than MayWrite.
