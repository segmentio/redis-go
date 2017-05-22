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
	// RoundTrip executes a single Redis transaction, returning a Response for
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

// Subscribe uses the transport's configuration to open a connection to a redis
// server that subscrribes to the given channels.
func (t *Transport) Subscribe(ctx context.Context, network string, address string, channels ...string) (*SubConn, error) {
	return t.sub(ctx, network, address, "SUBSCRIBE", channels...)
}

// Subscribe uses the transport's configuration to open a connection to a redis
// server that subscrribes to the given patterns.
func (t *Transport) PSubscribe(ctx context.Context, network string, address string, patterns ...string) (*SubConn, error) {
	return t.sub(ctx, network, address, "PSUBSCRIBE", patterns...)
}

func (t *Transport) sub(ctx context.Context, network string, address string, command string, channels ...string) (*SubConn, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		// Assuming the ping timeout should be a good enough approximation of
		// how long it should take at most to connect.
		deadline = time.Now().Add(t.pingTimeout())
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	conn, err := t.dialContext()(ctx, network, address)
	if err != nil {
		return nil, err
	}

	subch := make(chan *SubConn, 1)
	errch := make(chan error, 1)

	go func() {
		sub := NewSubConn(conn)
		sub.SetWriteDeadline(deadline)

		if err := sub.WriteCommand(command, channels...); err != nil {
			sub.Close()
			errch <- err
			return
		}

		sub.SetDeadline(time.Time{})
		subch <- sub
	}()

	select {
	case sub := <-subch:
		return sub, nil

	case err := <-errch:
		return nil, err

	case <-ctx.Done():
		conn.Close()
		return nil, ctx.Err()
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
	return poolConfig{
		addr:         addr,
		size:         t.connsPerHost(),
		dialContext:  t.dialContext(),
		pingTimeout:  t.pingTimeout(),
		pingInterval: t.pingInterval(),
	}
}

func (t *Transport) connsPerHost() int {
	if connsPerHost := t.ConnsPerHost; connsPerHost != 0 {
		return connsPerHost
	}
	return 1
}

func (t *Transport) dialContext() func(context.Context, string, string) (net.Conn, error) {
	dialContext := t.DialContext
	if dialContext == nil {
		dialContext = DefaultDialer.DialContext
	}
	return dialContext
}

func (t *Transport) pingTimeout() time.Duration {
	if pingTimeout := t.PingTimeout; pingTimeout != 0 {
		return pingTimeout
	}
	return 10 * time.Second
}

func (t *Transport) pingInterval() time.Duration {
	if pingInterval := t.PingInterval; pingInterval != 0 {
		return pingInterval
	}
	return 30 * time.Second
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
