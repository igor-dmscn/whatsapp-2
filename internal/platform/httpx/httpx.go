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
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"comms/internal/platform/id"
	"comms/internal/platform/logging"
	"comms/internal/platform/ratelimit"
	"comms/internal/platform/tracing"
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

// RateLimited refuses a request that exceeds a caller's allowance.
//
// Middleware rather than a check inside each handler, because a limit is a property of the
// endpoint and not of what the endpoint does — and because a handler that has to remember to
// call a limiter is a handler somebody will add without remembering.
//
// It must be wrapped *inside* whatever authenticates, since the key is usually the caller and
// there is no caller before authentication. A limit keyed on nothing is a global limit, which is
// a different and much blunter thing.
//
// 429 with Retry-After, which is what a client can act on: the header is a number of seconds a
// well-behaved client waits, and the alternative — a bare refusal — invites an immediate retry
// that is refused again.
func RateLimited(
	limiter *ratelimit.Limiter,
	name string,
	limit int,
	window time.Duration,
	key func(*http.Request) string,
	logger *slog.Logger,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			caller := key(r)
			if caller == "" {
				// Nothing to key on. Allowed rather than refused: an unidentifiable
				// caller here means the authentication layer let something through, and
				// that is a different bug to report than a rate limit.
				next.ServeHTTP(w, r)
				return
			}

			decision := limiter.Allow(r.Context(), name+":"+caller, limit, window)
			if !decision.Allowed {
				seconds := int(decision.RetryAfter.Seconds())
				if seconds < 1 {
					seconds = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(seconds))
				WriteError(w, logger, http.StatusTooManyRequests, ErrorBody{
					Code:    "rate_limited",
					Message: fmt.Sprintf("too many requests; try again in %ds", seconds),
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
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

		ctx := logging.Correlate(r.Context(), correlationID)

		// A trace continued from the caller if it sent one, and begun here if not — which is
		// what makes a browser's request and the worker's projection of it one trace rather
		// than two (NF-16). Extracted before the span is started, or the span would be a
		// root and the caller's half would be orphaned.
		ctx = tracing.Extract(ctx, headerCarrier(r.Header))
		ctx, span := tracing.Start(ctx, r.Method+" "+r.URL.Path,
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
		)
		defer span.End()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// headerCarrier reads trace context out of HTTP headers.
//
// Canonical form matters: Go stores headers canonicalised and the propagator asks for
// "traceparent" in lower case, so Header.Get is used rather than indexing the map.
func headerCarrier(header http.Header) tracing.Carrier {
	carrier := tracing.Carrier{}
	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		if value := header.Get(key); value != "" {
			carrier[key] = value
		}
	}
	return carrier
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
