package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"comms/internal/calling/internal/sfu"
)

// Media is a forwarding server's signalling surface, for the process that holds it.
//
// The other side of Remote. An api node asks this to admit a participant and gets an answer;
// this asks api nodes to admit a *track*, over the stream below, because the media plane
// discovers a new publisher and the api nodes are the ones holding sockets.
type Media struct {
	server *sfu.Server
	offers *fanout
	logger *slog.Logger
}

// NewMedia returns the surface, with the reverse channel already connected.
//
// Connected here rather than by the caller: a media node whose offers reach nobody is a node
// on which every call silently stops at two participants, and that is not a state worth
// making possible.
func NewMedia(server *sfu.Server, logger *slog.Logger) *Media {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	media := &Media{
		server: server,
		offers: &fanout{subscribers: make(map[int]chan offerMessage), logger: logger},
		logger: logger,
	}

	server.SetRenegotiator(func(_ context.Context, callID, participantID, offer string) error {
		message := offerMessage{CallID: callID, ParticipantID: participantID, SDP: offer}
		if delivered := media.offers.publish(message); delivered == 0 {
			// Reported as a failure so the media plane retries. No api node is subscribed,
			// which happens while this process is starting and while a subscriber
			// reconnects — a second later there is somewhere for it to go, and the
			// alternative is a participant who never sees the joiner.
			return ErrNoSubscribers
		}
		return nil
	})

	return media
}

// ErrNoSubscribers means no api node was listening for the offers this node produces.
var ErrNoSubscribers = errors.New("nodes: no api node is subscribed to offers")

// Subscribers reports how many api nodes are listening for offers, for health reporting.
func (m *Media) Subscribers() int { return m.offers.count() }

// Routes registers the node's surface.
func (m *Media) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+participantPath, m.join)
	mux.HandleFunc("POST "+participantPath+"/answer", m.answer)
	mux.HandleFunc("DELETE "+participantPath, m.leave)
	mux.HandleFunc("GET "+offersPath, m.stream)
}

func (m *Media) join(w http.ResponseWriter, r *http.Request) {
	body, ok := m.read(w, r)
	if !ok {
		return
	}

	answer, err := m.server.Join(r.PathValue("call"), r.PathValue("participant"), body.SDP)
	if err != nil {
		// A refused join is this node's fault or the offer's, and either way the api node
		// can do nothing but report it. 502 rather than 400 would hide which.
		m.fail(w, r, http.StatusBadRequest, "join", err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(sdpBody{SDP: answer}); err != nil {
		m.logger.Warn("write answer", slog.Any("error", err))
	}
}

func (m *Media) answer(w http.ResponseWriter, r *http.Request) {
	body, ok := m.read(w, r)
	if !ok {
		return
	}

	if err := m.server.Answer(r.PathValue("call"), r.PathValue("participant"), body.SDP); err != nil {
		// A participant this node has never heard of is a stale answer, not a broken node:
		// the transport was released while the client was still composing its reply.
		status := http.StatusBadRequest
		if errors.Is(err, sfu.ErrParticipantNotFound) {
			status = http.StatusNotFound
		}
		m.fail(w, r, status, "answer", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *Media) leave(w http.ResponseWriter, r *http.Request) {
	// No error and no body. Leaving is idempotent all the way down — a participant who is
	// already gone is the desired state, and an api node cleaning up after a closed socket
	// has nothing to do with a refusal.
	m.server.Leave(r.PathValue("call"), r.PathValue("participant"))
	w.WriteHeader(http.StatusNoContent)
}

// stream sends this node's offers to one api node, for as long as it is connected.
//
// The reverse channel, and the one part of moving media out of process that is not just a
// port with a second implementation. An offer is produced by the media plane and has to
// reach the api node holding one particular socket — which this node cannot know, because
// sockets are the thing it deliberately does not have.
//
// So every subscriber gets every offer and drops the ones that are not theirs. That is a
// broadcast whose cost is a few kilobytes per join per api node, against a registry mapping
// devices to nodes, which is a second source of truth about where a client is and one more
// thing to be stale.
func (m *Media) stream(w http.ResponseWriter, r *http.Request) {
	id, messages := m.offers.subscribe()
	defer m.offers.unsubscribe(id)

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	control := http.NewResponseController(w)
	// Headers first, so that a subscriber knows it is subscribed rather than assuming it
	// after a request that has not reached a handler yet.
	if err := control.Flush(); err != nil {
		m.logger.Warn("offer stream cannot be flushed", slog.Any("error", err))
		return
	}

	m.logger.Info("an api node subscribed to offers", slog.Int("subscribers", m.offers.count()))
	defer func() {
		m.logger.Info("an api node stopped reading offers",
			slog.Int("subscribers", m.offers.count()-1))
	}()

	encoder := json.NewEncoder(w)
	// A keepalive, because the failure this stream can have is silence. A subscriber that
	// has gone away without closing is indistinguishable from one that is simply idle until
	// something is written to it, and until then this node believes its offers are being
	// delivered. A bare newline is whitespace between JSON values and the decoder on the
	// other side never sees it.
	keepalive := time.NewTicker(streamKeepalive)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case message := <-messages:
			if err := encoder.Encode(message); err != nil {
				m.logger.Warn("send offer", slog.Any("error", err))
				return
			}
			if err := control.Flush(); err != nil {
				return
			}

		case <-keepalive.C:
			if _, err := w.Write([]byte("\n")); err != nil {
				return
			}
			if err := control.Flush(); err != nil {
				return
			}
		}
	}
}

// streamKeepalive is how often an idle offer stream is proven. Well under the idle timeout
// of anything likely to sit between two of these processes.
const streamKeepalive = 20 * time.Second

func (m *Media) read(w http.ResponseWriter, r *http.Request) (sdpBody, bool) {
	var body sdpBody
	// Bounded because it is a network input. An SDP is a few kilobytes; a megabyte is
	// already absurd and is the point at which refusing costs nothing.
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSDPBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		m.fail(w, r, http.StatusBadRequest, "decode", err)
		return sdpBody{}, false
	}
	if body.SDP == "" {
		m.fail(w, r, http.StatusBadRequest, "decode", errors.New("no sdp"))
		return sdpBody{}, false
	}
	return body, true
}

const maxSDPBytes = 1 << 20

// fail answers with plain text and says which participant it was about.
//
// Plain text rather than the api's error shape: the only client of this surface is another
// one of these processes, and what it does with a failure is log it. What matters is that
// the log line names the call.
func (m *Media) fail(w http.ResponseWriter, r *http.Request, status int, what string, err error) {
	m.logger.Warn("media node refused a request",
		slog.String("step", what),
		slog.String("call", r.PathValue("call")),
		slog.String("participant", r.PathValue("participant")),
		slog.Any("error", err))
	http.Error(w, fmt.Sprintf("%s: %v", what, err), status)
}

// fanout delivers each offer to every subscribed api node.
type fanout struct {
	logger *slog.Logger

	mutex       sync.Mutex
	next        int
	subscribers map[int]chan offerMessage
}

func (f *fanout) subscribe() (int, <-chan offerMessage) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.next++
	// Buffered, so that publishing does not wait on a subscriber. Publishing happens on the
	// path that has just discovered a new publisher, and blocking there would hold up
	// forwarding for everyone in the call.
	messages := make(chan offerMessage, offerBuffer)
	f.subscribers[f.next] = messages
	return f.next, messages
}

func (f *fanout) unsubscribe(id int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	delete(f.subscribers, id)
}

// publish hands a message to every subscriber, returning how many took it.
func (f *fanout) publish(message offerMessage) int {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	delivered := 0
	for id, messages := range f.subscribers {
		select {
		case messages <- message:
			delivered++
		default:
			// Dropped rather than waited for. A subscriber this far behind is an api node
			// that is not reading, and the participant recovers: the media plane gives up on
			// an unanswered offer after a grace period and offers again on the next change.
			f.logger.Warn("an api node is not keeping up with offers", slog.Int("subscriber", id))
		}
	}
	return delivered
}

func (f *fanout) count() int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return len(f.subscribers)
}

// offerBuffer is how many offers may be queued for one api node. A burst of joins into one
// call is the shape that fills it, and eight is the participant limit.
const offerBuffer = 8
