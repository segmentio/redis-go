package redis

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/objconv/resp"
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

	// MaxIdleConns controls the maximum number of idle (keep-alive) connections
	// across all hosts. Zero means no limit.
	MaxIdleConns int

	// MaxIdleConnsPerHost, if non-zero, controls the maximum idle
	// (keep-alive) connections to keep per-host. Zero means no limit.
	MaxIdleConnsPerHost int

	// PingInterval is the amount of time between pings that the transport sends
	// to the hosts it connects to.
	PingInterval time.Duration

	// PingTimeout is the amount of time that the transport waits for responses
	// to ping requests before discarding connections.
	PingTimeout time.Duration

	once sync.Once
	pool *connPool
}

// CloseIdleConnections closes any connections which were previously connected
// from previous requests but are now sitting idle. It does not interrupt any
// connections currently in use.
func (t *Transport) CloseIdleConnections() {
	t.once.Do(t.init)
	t.pool.closeIdleConnections()
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

	conn, err := t.dialContext(ctx, network, address)
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
	t.once.Do(t.init)

	ctx := req.Context
	if ctx == nil {
		ctx = context.Background()
	}

	conn := t.pool.getConn(req.Addr)
	if conn == nil {
		network, address := splitNetworkAddress(req.Addr)
		c, err := t.dialContext(ctx, network, address)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = &net.OpError{Op: "dial", Net: "redis", Err: ctxErr}
			}
			return nil, err
		}
		conn = NewClientConn(c)
	}

	resch := make(chan *Response, 1)
	errch := make(chan error, 1)

	go t.roundTrip(conn, req, resch, errch)

	var res *Response
	var err error

	select {
	case res = <-resch:
	case err = <-errch:
	case <-ctx.Done():
		err = ctx.Err()
	}

	if err != nil {
		laddr := conn.LocalAddr()
		raddr := conn.RemoteAddr()
		conn.Close()
		err = &net.OpError{Op: "request", Net: "redis", Source: laddr, Addr: raddr, Err: err}
	}

	return res, err
}

func (t *Transport) roundTrip(conn *Conn, req *Request, resch chan<- *Response, errch chan<- error) {
	if err := conn.WriteCommands(Command{Cmd: req.Cmd, Args: req.Args}); err != nil {
		if req.Args != nil {
			req.Args.Close()
		}
		errch <- err
		return
	}

	args := conn.ReadArgs()
	args.Len() // waits for the first bytes of the response to arrive

	resch <- &Response{
		Args: &transportArgs{
			Args: args,
			host: req.Addr,
			conn: conn,
			pool: t.pool,
		},
		Request: req,
	}
}

func (t *Transport) init() {
	pool := &connPool{
		maxIdleConns:        t.MaxIdleConns,
		maxIdleConnsPerHost: t.MaxIdleConnsPerHost,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func(pingInterval time.Duration, pingTimeout time.Duration) {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
			pool.pingIdleConnections(pingTimeout)
		}
	}(t.pingInterval(), t.pingTimeout())

	runtime.SetFinalizer(pool, func(*connPool) { cancel() })
	t.pool = pool
}

func (t *Transport) dialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	dialContext := t.DialContext
	if dialContext == nil {
		dialContext = DefaultDialer.DialContext
	}
	return dialContext(ctx, network, address)
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

type transportArgs struct {
	Args
	host string
	conn *Conn
	pool *connPool
	once sync.Once
}

func (a *transportArgs) Close() error {
	err := a.Args.Close()

	if err != nil {
		if _, stable := err.(*resp.Error); !stable {
			a.once.Do(func() { a.conn.Close() })
			return err
		}
	}

	a.once.Do(func() { a.pool.putConn(a.host, a.conn) })
	return err
}

func splitNetworkAddress(s string) (string, string) {
	if i := strings.Index(s, "://"); i >= 0 {
		return s[:i], s[i+3:]
	}
	return "tcp", s
}
