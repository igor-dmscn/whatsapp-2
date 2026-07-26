package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// textContentType is what this client sends.
const textContentType = "text/plain; charset=utf-8"

// Account is who somebody is.
type Account struct {
	ID     string `json:"id"`
	Handle string `json:"handle"`
}

// Session is a token pair and who it belongs to.
type Session struct {
	Account          Account   `json:"account"`
	DeviceID         string    `json:"device_id"`
	AccessToken      string    `json:"access_token"`
	AccessExpiresAt  time.Time `json:"access_expires_at"`
	RefreshToken     string    `json:"refresh_token"`
	RefreshExpiresAt time.Time `json:"refresh_expires_at"`
}

// Conversation is a conversation with this member's projected state.
type Conversation struct {
	ID                     string    `json:"id"`
	Kind                   string    `json:"kind"`
	Head                   int64     `json:"head"`
	Role                   string    `json:"role"`
	VisibleFrom            int64     `json:"visible_from"`
	CreatedAt              time.Time `json:"created_at"`
	Unread                 int64     `json:"unread"`
	ReadThrough            int64     `json:"read_through"`
	DeliveredThrough       int64     `json:"delivered_through"`
	OthersReadThrough      int64     `json:"others_read_through"`
	OthersDeliveredThrough int64     `json:"others_delivered_through"`
}

// Entry is one position in a conversation's log.
type Entry struct {
	ID             string    `json:"id"`
	ConversationID string    `json:"conversation_id"`
	Sequence       int64     `json:"sequence"`
	AuthorID       string    `json:"author_id"`
	ClientEntryID  string    `json:"client_entry_id"`
	Kind           string    `json:"kind"`
	ContentType    string    `json:"content_type"`
	Body           string    `json:"body"`
	CreatedAt      time.Time `json:"created_at"`
	TargetSequence int64     `json:"target_sequence"`
	ReplyTo        int64     `json:"reply_to"`
	// AttachmentID is the photo or video this entry carries, empty for none.
	//
	// Carried so the terminal can say a message has one. It cannot show it, and does not
	// try: rendering a photo in a terminal is a different project. What matters is that
	// the two clients agree about what a conversation contains — a caption-less photo
	// must not read as a blank line.
	AttachmentID string `json:"attachment_id"`
}

// Text decodes the payload. The wire form is base64 because the server does not
// interpret it (ADR-0001).
func (e Entry) Text() string {
	decoded, err := base64.StdEncoding.DecodeString(e.Body)
	if err != nil {
		// A body that will not decode is a body this client cannot show. Reported as
		// empty rather than as an error: one unreadable entry must not stop a
		// conversation from rendering.
		return ""
	}
	return string(decoded)
}

// Reaction is somebody's reaction to an entry.
type Reaction struct {
	Sequence  int64  `json:"sequence"`
	AccountID string `json:"account_id"`
	Emoji     string `json:"emoji"`
}

// Error is what the server said went wrong.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s (%d)", e.Code, e.Message, e.Status)
}

// Unauthorised reports whether the session needs renewing or replacing.
func (e Error) Unauthorised() bool { return e.Status == http.StatusUnauthorized }

// API is an authenticated caller.
//
// It owns the token pair for the same reason the browser client's does: the
// alternative is every caller remembering to check expiry, and the one that forgets
// fails fifteen minutes into a session.
type API struct {
	baseURL string
	http    *http.Client

	mutex   sync.Mutex
	session Session
	// onSession is called whenever the pair rotates, so the store keeps up.
	onSession func(Session) error
}

// NewAPI returns a client for a base URL such as http://localhost:8080.
func NewAPI(baseURL string, session Session, onSession func(Session) error) *API {
	if onSession == nil {
		onSession = func(Session) error { return nil }
	}
	return &API{
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		http:      &http.Client{Timeout: 30 * time.Second},
		session:   session,
		onSession: onSession,
	}
}

// Register creates an account and returns its first session.
func Register(ctx context.Context, baseURL, handle, email, passphrase, deviceName string) (Session, error) {
	var session Session
	err := call(ctx, &http.Client{Timeout: 30 * time.Second}, http.MethodPost,
		strings.TrimSuffix(baseURL, "/")+"/v1/accounts", "",
		map[string]string{
			"handle": handle, "email": email,
			"passphrase": passphrase, "device_name": deviceName,
		}, &session)
	return session, err
}

// Login authenticates and returns a session.
func Login(ctx context.Context, baseURL, handle, passphrase, deviceName string) (Session, error) {
	var session Session
	err := call(ctx, &http.Client{Timeout: 30 * time.Second}, http.MethodPost,
		strings.TrimSuffix(baseURL, "/")+"/v1/sessions", "",
		map[string]string{
			"handle": handle, "passphrase": passphrase, "device_name": deviceName,
		}, &session)
	return session, err
}

// Session returns the current pair.
func (a *API) Session() Session {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.session
}

// AccountID is who this client is acting as.
func (a *API) AccountID() string { return a.Session().Account.ID }

// refreshMargin is how early a token is replaced. Access tokens live fifteen minutes.
const refreshMargin = time.Minute

// Token returns an access token good for at least refreshMargin longer.
func (a *API) Token(ctx context.Context) (string, error) {
	a.mutex.Lock()
	session := a.session
	a.mutex.Unlock()

	if time.Until(session.AccessExpiresAt) > refreshMargin {
		return session.AccessToken, nil
	}

	var refreshed struct {
		DeviceID         string    `json:"device_id"`
		AccessToken      string    `json:"access_token"`
		AccessExpiresAt  time.Time `json:"access_expires_at"`
		RefreshToken     string    `json:"refresh_token"`
		RefreshExpiresAt time.Time `json:"refresh_expires_at"`
	}
	err := call(ctx, a.http, http.MethodPost, a.baseURL+"/v1/sessions/refresh", "",
		map[string]string{"refresh_token": session.RefreshToken}, &refreshed)
	if err != nil {
		return "", err
	}

	a.mutex.Lock()
	a.session.AccessToken = refreshed.AccessToken
	a.session.AccessExpiresAt = refreshed.AccessExpiresAt
	a.session.RefreshToken = refreshed.RefreshToken
	a.session.RefreshExpiresAt = refreshed.RefreshExpiresAt
	updated := a.session
	a.mutex.Unlock()

	if err := a.onSession(updated); err != nil {
		return "", err
	}
	return updated.AccessToken, nil
}

func (a *API) authorized(ctx context.Context, method, path string, body, out any) error {
	token, err := a.Token(ctx)
	if err != nil {
		return err
	}
	return call(ctx, a.http, method, a.baseURL+path, token, body, out)
}

// Conversations lists what this account belongs to, with projected state.
func (a *API) Conversations(ctx context.Context) ([]Conversation, error) {
	var response struct {
		Conversations []Conversation `json:"conversations"`
	}
	if err := a.authorized(ctx, http.MethodGet, "/v1/conversations", nil, &response); err != nil {
		return nil, err
	}
	return response.Conversations, nil
}

// Entries returns what follows a sequence.
func (a *API) Entries(ctx context.Context, conversationID string, after int64) ([]Entry, error) {
	var response struct {
		Entries []Entry `json:"entries"`
	}
	path := "/v1/conversations/" + conversationID + "/entries?after=" + strconv.FormatInt(after, 10)
	if err := a.authorized(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, err
	}
	return response.Entries, nil
}

// Send appends an entry, idempotently on clientEntryID.
func (a *API) Send(ctx context.Context, conversationID, clientEntryID, text string, replyTo int64) (Entry, error) {
	var entry Entry
	err := a.authorized(ctx, http.MethodPost, "/v1/conversations/"+conversationID+"/entries",
		map[string]any{
			"client_entry_id": clientEntryID,
			"content_type":    textContentType,
			"body":            base64.StdEncoding.EncodeToString([]byte(text)),
			"reply_to":        replyTo,
		}, &entry)
	return entry, err
}

// Revise edits an entry by appending a revision (MS-8).
func (a *API) Revise(ctx context.Context, conversationID string, target int64, clientEntryID, text string) (Entry, error) {
	var entry Entry
	path := "/v1/conversations/" + conversationID + "/entries/" + strconv.FormatInt(target, 10) + "/revision"
	err := a.authorized(ctx, http.MethodPost, path, map[string]string{
		"client_entry_id": clientEntryID,
		"content_type":    textContentType,
		"body":            base64.StdEncoding.EncodeToString([]byte(text)),
	}, &entry)
	return entry, err
}

// Retract deletes an entry for everyone by appending a retraction (MS-9).
func (a *API) Retract(ctx context.Context, conversationID string, target int64, clientEntryID string) (Entry, error) {
	var entry Entry
	path := "/v1/conversations/" + conversationID + "/entries/" + strconv.FormatInt(target, 10) + "/retraction"
	err := a.authorized(ctx, http.MethodPost, path, map[string]string{"client_entry_id": clientEntryID}, &entry)
	return entry, err
}

// LookupHandle resolves a handle to an account.
func (a *API) LookupHandle(ctx context.Context, handle string) (Account, error) {
	var account Account
	err := a.authorized(ctx, http.MethodGet, "/v1/accounts/"+handle, nil, &account)
	return account, err
}

// StartDirect opens or returns the direct conversation with an account.
func (a *API) StartDirect(ctx context.Context, accountID string) (Conversation, error) {
	var conversation Conversation
	err := a.authorized(ctx, http.MethodPost, "/v1/conversations/direct",
		map[string]string{"account_id": accountID}, &conversation)
	return conversation, err
}

// Acknowledge reports how far this account has received and read a conversation.
func (a *API) Acknowledge(ctx context.Context, conversationID string, delivered, read int64) error {
	return a.authorized(ctx, http.MethodPost, "/v1/conversations/"+conversationID+"/receipt",
		map[string]int64{"delivered_through": delivered, "read_through": read}, nil)
}

// Reactions returns the reactions on a stretch of entries.
func (a *API) Reactions(ctx context.Context, conversationID string, from, to int64) ([]Reaction, error) {
	var response struct {
		Reactions []Reaction `json:"reactions"`
	}
	path := "/v1/conversations/" + conversationID + "/reactions?from=" +
		strconv.FormatInt(from, 10) + "&to=" + strconv.FormatInt(to, 10)
	if err := a.authorized(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, err
	}
	return response.Reactions, nil
}

// React adds a reaction.
func (a *API) React(ctx context.Context, conversationID string, sequence int64, emoji string) error {
	path := "/v1/conversations/" + conversationID + "/entries/" + strconv.FormatInt(sequence, 10) + "/reactions"
	return a.authorized(ctx, http.MethodPut, path, map[string]string{"emoji": emoji}, nil)
}

// SocketURL is where the WebSocket lives, derived from the base URL.
func (a *API) SocketURL() string {
	url := a.baseURL
	switch {
	case strings.HasPrefix(url, "https://"):
		url = "wss://" + strings.TrimPrefix(url, "https://")
	case strings.HasPrefix(url, "http://"):
		url = "ws://" + strings.TrimPrefix(url, "http://")
	}
	return url + "/v1/socket"
}

// call performs one request.
func call(ctx context.Context, httpClient *http.Client, method, url, token string, body, out any) error {
	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	var request *http.Request
	var err error
	if reader != nil {
		request, err = http.NewRequestWithContext(ctx, method, url, reader)
	} else {
		request, err = http.NewRequestWithContext(ctx, method, url, nil)
	}
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer response.Body.Close()

	if response.StatusCode >= 400 {
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.NewDecoder(response.Body).Decode(&failure)
		return Error{Status: response.StatusCode, Code: failure.Error.Code, Message: failure.Error.Message}
	}

	if out == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
