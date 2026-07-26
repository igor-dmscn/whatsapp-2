// Package nodes carries signalling to the SFU holding a call.
//
// Two implementations, and the choice between them is a deployment decision rather than a
// design one. Local forwards in this process, which is what a development machine and every
// test wants. Remote reaches a media node over HTTP (ADR-0007), which is what a deployment
// wants, because forwarding is CPU-bound and holding sockets is not — they scale differently,
// and one process means scaling both together.
//
// Nothing above here knows which it has. MediaNodes names a node by address on every call, so
// the second implementation was a package rather than a change, and the domain, the use cases
// and the signalling are untouched by it.
//
// Media is the other side of Remote: the surface a forwarding process serves. It lives here,
// beside its client, because the two must agree on a wire and the way a protocol goes wrong
// when its ends are written apart is silence rather than an error.
package nodes

import (
	"context"
	"fmt"

	"comms/internal/calling/internal/domain"
	"comms/internal/calling/internal/sfu"
)

// Local forwards media inside this process.
type Local struct {
	// address is what this node is called. Recorded on every call (CL-4), so that when
	// media does move out of process the calls already in the database name something
	// resolvable rather than "local".
	address string
	server  *sfu.Server
}

// NewLocal returns a node backed by server, advertising itself as address.
func NewLocal(address string, server *sfu.Server) *Local {
	return &Local{address: address, server: server}
}

var _ domain.MediaNodes = (*Local)(nil)

// Allocate returns this process's node.
//
// One node, so there is nothing to choose between. A deployment with several needs a
// policy — least-loaded, or hashed by conversation so a reconnecting participant lands
// where their call already is — and inventing one now would be inventing it without the
// measurements that would settle it.
func (l *Local) Allocate(context.Context) (string, error) {
	if l.server == nil {
		return "", domain.ErrNoMediaNode
	}
	return l.address, nil
}

// Join hands an offer to the local server.
func (l *Local) Join(
	_ context.Context,
	node string,
	call domain.CallID,
	participant domain.DeviceID,
	offer string,
) (string, error) {
	if err := l.mine(node); err != nil {
		return "", err
	}

	answer, err := l.server.Join(string(call), string(participant), offer)
	if err != nil {
		return "", fmt.Errorf("sfu join: %w", err)
	}
	return answer, nil
}

// Answer applies a participant's answer.
func (l *Local) Answer(
	_ context.Context,
	node string,
	call domain.CallID,
	participant domain.DeviceID,
	answer string,
) error {
	if err := l.mine(node); err != nil {
		return err
	}

	if err := l.server.Answer(string(call), string(participant), answer); err != nil {
		return fmt.Errorf("sfu answer: %w", err)
	}
	return nil
}

// Leave releases a participant's transport.
func (l *Local) Leave(
	_ context.Context,
	node string,
	call domain.CallID,
	participant domain.DeviceID,
) error {
	if err := l.mine(node); err != nil {
		return err
	}

	l.server.Leave(string(call), string(participant))
	return nil
}

// mine refuses to act for a call allocated elsewhere.
//
// The check that makes CL-4 visible rather than assumed. A call recorded against another
// node reaching this one means either a stale row or a deployment that grew a second node
// without a way to route to it, and forwarding it here would produce a call whose
// participants are split across two nodes and cannot see each other.
func (l *Local) mine(node string) error {
	if node != l.address {
		return fmt.Errorf("%w: this call is on %s and this node is %s",
			domain.ErrNoMediaNode, node, l.address)
	}
	return nil
}
