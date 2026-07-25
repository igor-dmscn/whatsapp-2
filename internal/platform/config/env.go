// Package config reads configuration from the environment.
//
// There is no config struct shared between binaries on purpose: api, worker and
// sfu need different things, and a struct holding the union of all three would
// let each one silently depend on values it has no business knowing about. Each
// binary declares what it needs, at its main.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// MustEnv returns the value of key, or exits if it is unset or empty.
//
// Failing at startup is deliberate. A missing database URL discovered on the
// first request is a much worse outcome than a process that refuses to boot.
func MustEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		fmt.Fprintf(os.Stderr, "required environment variable %s is not set\n", key)
		os.Exit(1)
	}
	return value
}

// EnvOr returns the value of key, or fallback if it is unset or empty.
func EnvOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// EnvIntOr returns the value of key parsed as an integer, or fallback if it is
// unset, empty, or not a number.
func EnvIntOr(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
