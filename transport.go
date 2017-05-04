package redis

import (
	"context"
	"net"
	"runtime"
	"sync"
	"time"
)

// RoundTripper is an interface representing the ability to execute a single
// Redis transaction, obtaining the Response for a given Request.
//
// A RoundTripper must be safe for concurrent use by multiple goroutines.
type RoundTripper interface {
	// RoundTrip executes a single Redis transaction, returning/ a Response for
	// the provided Request.
	//
	// RoundTrip should not attempt to interpret the response. In particular,
	// RoundTrip must return err == nil if it obtained a response, regardless of
	// whether the response carries a protocol error. A non-nil err should be
	// reserved for failure to obtain a response.
	//
	// RoundTrip should not modify the request, except for consuming and closing
	// the Request's Args.
	//
	// RoundTrip must always close the argument list, including on errors, but
	// depending on the implementation may do so in a separate goroutine even
	// after RoundTrip returns. This means that callers wanting to reuse the
	// argument list for subsequent requests must arrange to wait for the Close
	// call before doing so.
	//
	// The Request's Addr and Cmd fields must be initialized.
	RoundTrip(*Request) (*Response, error)
}

// Transport is an implementation of RoundTripper.
//
// By default, Transport caches connections for future re-use. This may leave
// many open connections when accessing many hosts. This behavior can be managed
// using Transport's CloseIdleConnections method and ConnsPerHost field.
//
// Transports should be reused instead of created as needed. Transports are safe
// for concurrent use by multiple goroutines.
//
// A Transport is a low-level primitive for making Redis requests. For high-level
// functionality, see Client.
type Transport struct {
	// DialContext specifies the dial function for creating network connections.
	// If DialContext is nil, then the transport dials using package net.
	DialContext func(context.Context, string, string) (net.Conn, error)

	// ConnsPerHost controls the number of connections that are opened to each
	// host that is accessed by the transport.
	//
	// The default value used by this traansport is one.
	ConnsPerHost int

	// PingInterval is the amount of time between pings that the transport sends
	// to the hosts it connects to.
	PingInterval time.Duration

	// PingTimeout is the amount of time that the transport waits for responses
	// to ping requests before discarding connections.
	PingTimeout time.Duration

	mutex sync.RWMutex
	conns map[string]conn
}

// CloseIdleConnections closes any connections which were previously connected
// from previous requests but are now sitting idle. It does not interrupt any
// connections currently in use.
func (t *Transport) CloseIdleConnections() {
	var conns map[string]conn

	t.mutex.Lock()
	conns, t.conns = t.conns, nil
	t.mutex.Unlock()

	for _, c := range conns {
		c.close()
	}
}

// RoundTrip implements the RoundTripper interface.
//
// For higher-level Redis client support, see Exec, Query, and the Client type.
func (t *Transport) RoundTrip(req *Request) (*Response, error) {
	ctx := req.Context

	if ctx == nil {
		ctx = context.Background()
	}

	tx := makeConnTx(ctx, req)

	if err := t.conn(req.Addr).send(tx); err != nil {
		return nil, err
	}

	return tx.recv()
}

func (t *Transport) conn(addr string) conn {
	t.mutex.RLock()
	c := t.conns[addr]
	t.mutex.RUnlock()

	if c == nil {
		t.mutex.Lock()

		if c = t.conns[addr]; c == nil {
			if t.conns == nil {
				t.conns = make(map[string]conn)
				runtime.SetFinalizer(t, (*Transport).CloseIdleConnections)
			}
			c = makePool(t.poolConfig(addr))
			t.conns[addr] = c
		}

		t.mutex.Unlock()
	}

	return c
}

func (t *Transport) poolConfig(addr string) poolConfig {
	config := poolConfig{
		addr:         addr,
		size:         t.ConnsPerHost,
		dialContext:  t.DialContext,
		pingTimeout:  t.PingTimeout,
		pingInterval: t.PingInterval,
	}

	if config.size == 0 {
		config.size = 1
	}

	if config.dialContext == nil {
		config.dialContext = DefaultDialer.DialContext
	}

	if config.pingTimeout == 0 {
		config.pingTimeout = 10 * time.Second
	}

	if config.pingInterval == 0 {
		config.pingInterval = 30 * time.Second
	}

	return config
}

// DefaultTransport is the default implementation of Transport and is used by
// DefaultClient. It establishes network connections as needed and caches them
// for reuse by subsequent calls.
var DefaultTransport RoundTripper = &Transport{
	ConnsPerHost: 4,
	PingTimeout:  10 * time.Second,
	PingInterval: 15 * time.Second,
}

// DefaultDialer is the default dialer used by Transports when no DialContext
// is set.
var DefaultDialer = &net.Dialer{
	Timeout:   10 * time.Second,
	KeepAlive: 30 * time.Second,
	DualStack: true,
}
