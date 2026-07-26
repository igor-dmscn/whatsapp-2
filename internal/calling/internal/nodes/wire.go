package nodes

// The wire between an api node and a media node. Both halves are in this package and this
// file holds what they must agree on, because the failure of a protocol whose two ends are
// written in different places is silence — an offer that goes nowhere, a call that works in
// one direction, no error anywhere. Phase 9 has already produced one of those.
//
// Deliberately not versioned and deliberately not Kafka. This is one internal hop between
// two processes deployed together: an offer is only useful for as long as the transport it
// belongs to exists, so durability, replay and schema evolution would all be machinery in
// service of nothing.

// Paths on a media node.
const (
	// participantPath is join (POST), answer (POST .../answer) and leave (DELETE).
	//
	// The call and the participant are in the path rather than the body because they name
	// the resource being acted on, and because that makes an access log of a media node
	// readable without decoding anything.
	participantPath = "/v1/calls/{call}/participants/{participant}"

	// offersPath is the reverse channel: a stream of offers the node has produced.
	offersPath = "/v1/offers"
)

// sdpBody carries an offer or an answer. One shape for both directions, because SDP is all
// either of them is.
type sdpBody struct {
	SDP string `json:"sdp"`
}

// offerMessage is one server-initiated offer, on its way to whichever api node holds the
// participant's socket.
//
// The participant is a device identifier, which is the only name the media plane knows a
// participant by — turning that into a connection is the api node's job, and the reason this
// is a broadcast rather than a delivery.
type offerMessage struct {
	CallID        string `json:"call_id"`
	ParticipantID string `json:"participant_id"`
	SDP           string `json:"sdp"`
}
