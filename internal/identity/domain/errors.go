package domain

import "errors"

// Errors callers are expected to distinguish. Anything not listed here is an
// internal failure and should surface as one, not as a validation message.
var (
	ErrHandleTaken        = errors.New("handle already taken")
	ErrEmailTaken         = errors.New("email already registered")
	ErrAccountNotFound    = errors.New("account not found")
	ErrDeviceNotFound     = errors.New("device not found")
	ErrDeviceRevoked      = errors.New("device revoked")
	ErrSessionNotFound    = errors.New("session not found")
	ErrInvalidToken       = errors.New("token is not valid")
	ErrTokenExpired       = errors.New("token expired")
	ErrInvalidCredential  = errors.New("invalid credential")
	ErrPasswordAlreadySet = errors.New("account already has a password")
)

// ValidationError names the field that was wrong. Handles, emails and
// passphrases are chosen by people, so telling them which value to fix is part
// of the job.
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return e.Field + ": " + e.Reason
}
