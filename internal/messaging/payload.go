package messaging

import "time"

// Payload is a message's content.
//
// Opaque by construction: the body is bytes and the content type is recorded but
// never interpreted. This is ADR-0001 expressed as a type — there is no accessor
// that parses, indexes or inspects a body, so no server-side feature can come to
// depend on reading one, and end-to-end encryption stays a client-side change.
type Payload struct {
	contentType string
	body        []byte
}

// maxPayloadBytes bounds a single entry. Large content is an attachment, which
// goes through Media and is referenced rather than inlined.
const maxPayloadBytes = 64 * 1024

// NewPayload validates and returns a payload.
func NewPayload(contentType string, body []byte) (Payload, error) {
	if contentType == "" {
		return Payload{}, ValidationError{"content_type", "must not be empty"}
	}
	if len(body) == 0 {
		return Payload{}, ValidationError{"body", "must not be empty"}
	}
	if len(body) > maxPayloadBytes {
		return Payload{}, ValidationError{"body", "must be at most 64 KiB"}
	}

	// Copied so a caller cannot alter the payload after it has been validated,
	// or after it has been handed to an aggregate.
	return Payload{contentType: contentType, body: append([]byte(nil), body...)}, nil
}

func (p Payload) ContentType() string { return p.contentType }

// Body returns a copy of the bytes.
func (p Payload) Body() []byte {
	return append([]byte(nil), p.body...)
}

// Size is what projections and quotas need, and is deliberately the only thing
// derivable from the content without copying it.
func (p Payload) Size() int { return len(p.body) }

// String reports the shape of the payload, never the content — a log line
// containing message bodies would defeat the point of the type.
func (p Payload) String() string {
	return "Payload(" + p.contentType + ", " + itoa(len(p.body)) + " bytes)"
}

// itoa avoids importing strconv for one call in a String method.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for n > 0 {
		index--
		digits[index] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[index:])
}

// ClientEntryID is an identifier the sending client generates before it sends.
//
// It exists so that a retry over a flaky network is a lookup rather than a
// duplicate (MS-2). Without it, at-least-once delivery from the client produces
// double-sent messages, which users notice immediately.
type ClientEntryID string

// ParseClientEntryID validates a client-supplied identifier.
func ParseClientEntryID(raw string) (ClientEntryID, error) {
	if raw == "" {
		return "", ValidationError{"client_entry_id", "must not be empty"}
	}
	if len(raw) > 64 {
		return "", ValidationError{"client_entry_id", "must be at most 64 characters"}
	}
	return ClientEntryID(raw), nil
}

// Sequence is an entry's position in its conversation's log.
//
// Monotonic and gapless within a conversation, meaningless across conversations.
// Gaplessness is what makes sync tractable: a client holding up to n that learns
// the head is m knows precisely which numbers it is missing (MS-3).
type Sequence int64

// FirstSequence is the position of a conversation's first entry. Sequences start
// at one so that zero can mean "nothing yet" without a separate flag.
const FirstSequence Sequence = 1

// Next returns the position after s.
func (s Sequence) Next() Sequence { return s + 1 }

// Clock is a timestamp source. The domain never reads the clock itself; callers
// pass the time in, which is what makes expiry and ordering testable.
type Clock func() time.Time
