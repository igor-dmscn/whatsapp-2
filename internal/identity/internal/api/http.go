// Package transport exposes identity over HTTP.
//
// Handlers translate between JSON and application calls and do nothing else. Any
// rule that survives a change of protocol belongs in application or domain.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"comms/internal/identity/internal/app"
	"comms/internal/identity/internal/domain"
	"comms/internal/platform/httpx"
	"comms/internal/platform/logging"
	"comms/internal/platform/ratelimit"
)

// Handler serves the identity endpoints.
type Handler struct {
	service *app.Service
	logger  *slog.Logger
	limiter *ratelimit.Limiter
}

// NewHandler returns a handler over service.
func NewHandler(service *app.Service, limiter *ratelimit.Limiter, logger *slog.Logger) *Handler {
	return &Handler{service: service, logger: logger, limiter: limiter}
}

// Handle lookups allowed per account per window (ID-5).
//
// Thirty a minute is far above what using the app requires and far below what enumerating a
// namespace would need. Shared across nodes now, which is the change phase 10 makes: counted
// per process, this limit was multiplied by the number of api nodes and loosened every time
// the deployment grew.
const (
	lookupLimit  = 30
	lookupWindow = time.Minute
)

// limitLookups bounds how often one account may resolve a handle.
//
// Keyed by caller, not by the handle being looked up: limiting per target would let one caller
// sweep the whole namespace one handle at a time, which is precisely what ID-5 is about.
func (h *Handler) limitLookups(next http.Handler) http.Handler {
	return httpx.RateLimited(h.limiter, "handle-lookup", lookupLimit, lookupWindow,
		func(r *http.Request) string {
			accountID, _ := Caller(r.Context())
			return string(accountID)
		}, h.logger)(next)
}

// Routes registers identity endpoints on mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/accounts", h.register)
	mux.HandleFunc("POST /v1/sessions", h.login)
	mux.HandleFunc("POST /v1/sessions/refresh", h.refresh)

	mux.Handle("GET /v1/me", h.Authenticated(http.HandlerFunc(h.me)))
	mux.Handle("GET /v1/devices", h.Authenticated(http.HandlerFunc(h.listDevices)))
	mux.Handle("DELETE /v1/devices/{deviceID}", h.Authenticated(http.HandlerFunc(h.revokeDevice)))
	mux.Handle("GET /v1/accounts/{handle}", h.Authenticated(h.limitLookups(http.HandlerFunc(h.lookupHandle))))
}

// --- request and response bodies ---

type registerRequest struct {
	Handle     string `json:"handle"`
	Email      string `json:"email"`
	Passphrase string `json:"passphrase"`
	DeviceName string `json:"device_name"`
}

type loginRequest struct {
	Handle     string `json:"handle"`
	Passphrase string `json:"passphrase"`
	DeviceName string `json:"device_name"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type accountResponse struct {
	ID        string    `json:"id"`
	Handle    string    `json:"handle"`
	CreatedAt time.Time `json:"created_at"`
}

// sessionResponse deliberately omits the account's email. Contexts and clients
// receive a handle; a recovery address is nobody else's business.
type sessionResponse struct {
	Account          accountResponse `json:"account"`
	DeviceID         string          `json:"device_id"`
	AccessToken      string          `json:"access_token"`
	AccessExpiresAt  time.Time       `json:"access_expires_at"`
	RefreshToken     string          `json:"refresh_token"`
	RefreshExpiresAt time.Time       `json:"refresh_expires_at"`
}

type deviceResponse struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

func newAccountResponse(account *domain.Account) accountResponse {
	return accountResponse{
		ID:        string(account.ID()),
		Handle:    string(account.Handle()),
		CreatedAt: account.CreatedAt(),
	}
}

func newSessionResponse(account *domain.Account, pair app.Session) sessionResponse {
	return sessionResponse{
		Account:          newAccountResponse(account),
		DeviceID:         string(pair.DeviceID),
		AccessToken:      pair.AccessToken,
		AccessExpiresAt:  pair.AccessExpiresAt,
		RefreshToken:     pair.RefreshToken,
		RefreshExpiresAt: pair.RefreshExpiresAt,
	}
}

// --- handlers ---

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var request registerRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.badRequest(w, r, "malformed_body", err.Error())
		return
	}

	account, pair, err := h.service.Register(r.Context(), request.Handle, request.Email, request.Passphrase, request.DeviceName)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, newSessionResponse(account, pair))
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	var request loginRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.badRequest(w, r, "malformed_body", err.Error())
		return
	}

	account, pair, err := h.service.Login(r.Context(), request.Handle, request.Passphrase, request.DeviceName)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, newSessionResponse(account, pair))
}

func (h *Handler) refresh(w http.ResponseWriter, r *http.Request) {
	var request refreshRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		h.badRequest(w, r, "malformed_body", err.Error())
		return
	}

	pair, err := h.service.Refresh(r.Context(), request.RefreshToken)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusCreated, map[string]any{
		"device_id":          string(pair.DeviceID),
		"access_token":       pair.AccessToken,
		"access_expires_at":  pair.AccessExpiresAt,
		"refresh_token":      pair.RefreshToken,
		"refresh_expires_at": pair.RefreshExpiresAt,
	})
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	accountID, _ := Caller(r.Context())

	account, err := h.service.LookupByID(r.Context(), accountID)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{
		"id":         string(account.ID()),
		"handle":     string(account.Handle()),
		"email":      string(account.Email()),
		"created_at": account.CreatedAt(),
	})
}

func (h *Handler) listDevices(w http.ResponseWriter, r *http.Request) {
	accountID, _ := Caller(r.Context())

	devices, err := h.service.Devices(r.Context(), accountID)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	responses := make([]deviceResponse, 0, len(devices))
	for _, device := range devices {
		responses = append(responses, deviceResponse{
			ID:        string(device.ID()),
			Name:      device.Name(),
			CreatedAt: device.CreatedAt(),
			RevokedAt: device.RevokedAt(),
		})
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, map[string]any{"devices": responses})
}

func (h *Handler) revokeDevice(w http.ResponseWriter, r *http.Request) {
	accountID, _ := Caller(r.Context())

	err := h.service.RevokeDevice(r.Context(), accountID, domain.DeviceID(r.PathValue("deviceID")))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusNoContent, nil)
}

func (h *Handler) lookupHandle(w http.ResponseWriter, r *http.Request) {
	account, err := h.service.LookupHandle(r.Context(), r.PathValue("handle"))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}

	httpx.WriteJSON(w, h.logger, http.StatusOK, newAccountResponse(account))
}

// --- authentication ---

type callerKey struct{}

// caller is what authentication resolved the bearer token to.
type caller struct {
	accountID domain.AccountID
	deviceID  domain.DeviceID
}

// Caller returns the authenticated account and device carried by ctx. It is only
// meaningful inside a handler wrapped by Authenticated.
func Caller(ctx context.Context) (domain.AccountID, domain.DeviceID) {
	resolved, _ := ctx.Value(callerKey{}).(caller)
	return resolved.accountID, resolved.deviceID
}

// WithCaller puts an authenticated caller on ctx. Exported for other contexts'
// transports, which authenticate the same way but route elsewhere.
func WithCaller(ctx context.Context, accountID domain.AccountID, deviceID domain.DeviceID) context.Context {
	return context.WithValue(ctx, callerKey{}, caller{accountID, deviceID})
}

// Authenticated rejects requests without a valid access token.
func (h *Handler) Authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			h.unauthorized(w, r, "missing_token", "an access token is required")
			return
		}

		accountID, deviceID, err := h.service.Authenticate(r.Context(), token)
		if err != nil {
			switch {
			case errors.Is(err, domain.ErrInvalidToken):
				h.unauthorized(w, r, "invalid_token", "the access token is not valid")
			case errors.Is(err, domain.ErrTokenExpired):
				h.unauthorized(w, r, "expired_token", "the access token has expired")
			case errors.Is(err, domain.ErrDeviceRevoked):
				h.unauthorized(w, r, "device_revoked", "this device has been revoked")
			default:
				h.internal(w, r, err)
			}
			return
		}

		next.ServeHTTP(w, r.WithContext(WithCaller(r.Context(), accountID, deviceID)))
	})
}

// bearerToken extracts a token from the Authorization header.
func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(header[len(prefix):]), true
}

// --- error mapping ---

// writeDomainError maps a domain error to a status. The default is 500: an error
// this function does not recognise is a bug here, not a client mistake, and
// reporting it as 400 would hide it.
func (h *Handler) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	var validation domain.ValidationError
	switch {
	case errors.As(err, &validation):
		httpx.WriteError(w, h.logger, http.StatusUnprocessableEntity, httpx.ErrorBody{
			Code:    "invalid_field",
			Message: validation.Reason,
			Field:   validation.Field,
		})
	case errors.Is(err, domain.ErrHandleTaken):
		h.conflict(w, r, "handle_taken", "that handle is already taken")
	case errors.Is(err, domain.ErrEmailTaken):
		h.conflict(w, r, "email_taken", "that email is already registered")
	case errors.Is(err, domain.ErrPasswordAlreadySet):
		h.conflict(w, r, "password_already_set", "this account already has a password")
	case errors.Is(err, domain.ErrInvalidCredential):
		h.unauthorized(w, r, "invalid_credentials", "the handle or passphrase is wrong")
	case errors.Is(err, domain.ErrInvalidToken):
		h.unauthorized(w, r, "invalid_token", "the token is not valid")
	case errors.Is(err, domain.ErrTokenExpired):
		h.unauthorized(w, r, "expired_token", "the token has expired")
	case errors.Is(err, domain.ErrDeviceRevoked):
		h.unauthorized(w, r, "device_revoked", "this device has been revoked")
	case errors.Is(err, domain.ErrAccountNotFound):
		h.notFound(w, r, "account_not_found", "no such account")
	case errors.Is(err, domain.ErrDeviceNotFound):
		h.notFound(w, r, "device_not_found", "no such device")
	default:
		h.internal(w, r, err)
	}
}

func (h *Handler) badRequest(w http.ResponseWriter, r *http.Request, code, message string) {
	logging.With(r.Context(), h.logger).Debug("bad request", slog.String("code", code))
	httpx.WriteError(w, h.logger, http.StatusBadRequest, httpx.ErrorBody{Code: code, Message: message})
}

func (h *Handler) unauthorized(w http.ResponseWriter, r *http.Request, code, message string) {
	logging.With(r.Context(), h.logger).Debug("unauthorized", slog.String("code", code))
	httpx.WriteError(w, h.logger, http.StatusUnauthorized, httpx.ErrorBody{Code: code, Message: message})
}

func (h *Handler) notFound(w http.ResponseWriter, r *http.Request, code, message string) {
	httpx.WriteError(w, h.logger, http.StatusNotFound, httpx.ErrorBody{Code: code, Message: message})
}

func (h *Handler) conflict(w http.ResponseWriter, r *http.Request, code, message string) {
	httpx.WriteError(w, h.logger, http.StatusConflict, httpx.ErrorBody{Code: code, Message: message})
}

// internal logs the cause and returns a body that reveals nothing about it. The
// correlation identifier is how the two get connected afterwards.
func (h *Handler) internal(w http.ResponseWriter, r *http.Request, err error) {
	logging.With(r.Context(), h.logger).Error("unhandled error", slog.Any("error", err))
	httpx.WriteError(w, h.logger, http.StatusInternalServerError, httpx.ErrorBody{
		Code:    "internal_error",
		Message: "something went wrong",
	})
}
