// Package httpx holds the HTTP plumbing shared by every context: JSON encoding,
// a single error shape, and the middleware that gives each request a correlation
// identifier.
//
// It is deliberately not a framework. Handlers stay plain http.HandlerFunc so
// that anyone who knows net/http can read them without learning anything else.
package httpx

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"comms/internal/platform/id"
	"comms/internal/platform/logging"
)

// CorrelationHeader lets a client supply its own identifier so that a report of
// "this failed for me at 14:02" can be found without guessing.
const CorrelationHeader = "X-Correlation-ID"

// ErrorBody is the only error shape the API returns. One shape means clients
// write one error path instead of one per endpoint.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// WriteJSON writes value as JSON with the given status.
func WriteJSON(w http.ResponseWriter, logger *slog.Logger, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if value == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(value); err != nil {
		// The status line is already sent, so this cannot become an error
		// response. Logging it is all that is left.
		logger.Error("write json response", slog.Any("error", err))
	}
}

// WriteError writes a single error with the given status.
func WriteError(w http.ResponseWriter, logger *slog.Logger, status int, body ErrorBody) {
	WriteJSON(w, logger, status, map[string]ErrorBody{"error": body})
}

// DecodeJSON reads a JSON request body into target.
//
// The body is length-limited and unknown fields are rejected: a client sending
// "passphrase" when the field is "password" should be told, not silently
// registered with an empty value.
func DecodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	return nil
}

// Correlate assigns each request a correlation identifier, reusing the client's
// if it supplied one, and echoes it back so a client can quote it in a report.
func Correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := r.Header.Get(CorrelationHeader)
		if correlationID == "" {
			correlationID = id.New()
		}
		w.Header().Set(CorrelationHeader, correlationID)
		next.ServeHTTP(w, r.WithContext(logging.Correlate(r.Context(), correlationID)))
	})
}

// LogRequests logs one line per completed request.
func LogRequests(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(recorder, r)

			logging.With(r.Context(), logger).Info("request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", recorder.status),
				slog.Duration("duration", time.Since(started)),
			)
		})
	}
}

// statusRecorder captures the status code for logging.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(status int) {
	if !s.wroteHeader {
		s.status = status
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(status)
}

// Unwrap exposes the wrapped writer so http.ResponseController can find
// optional interfaces such as Flusher and Hijacker. Without it, wrapping this
// middleware around a WebSocket upgrade would break the upgrade.
func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// Chain applies middleware in the order given, so the first listed is outermost.
func Chain(handler http.Handler, middleware ...func(http.Handler) http.Handler) http.Handler {
	for index := len(middleware) - 1; index >= 0; index-- {
		handler = middleware[index](handler)
	}
	return handler
}
