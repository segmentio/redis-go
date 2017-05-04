package redis

import (
	"context"
	"time"
)

// A Client is a Redis client. Its zero value (DefaultClient) is a usable client
// that uses DefaultTransport and connects to a redis server on localhost:6379.
//
// The Client's Transport typically has internal state (cached TCP connections),
// so Clients should be reused instead of created as needed. Clients are safe
// for concurrent use by multiple goroutines.
type Client struct {
	// Addr is the server address used by the client's Exec or Query methods
	// are called.
	Addr string

	// Transport specifies the mechanism by which individual requests are made.
	// If nil, DefaultTransport is used.
	Transport RoundTripper

	// Timeout specifies a time limit for requests made by this Client. The
	// timeout includes connection time, any redirects, and reading the response.
	// The timer remains running after Exec, Query, or Do return and will
	// interrupt reading of the Response.Args.
	//
	// A Timeout of zero means no timeout.
	Timeout time.Duration
}

// Do sends an Redis request and returns an Redis response.
//
// An error is returned if the transport failed to contact the Redis server, or
// if a timeout occurs. Redis protocol errors are returned by the Response.Args'
// Close method.
//
// If the error is nil, the Response will contain a non-nil Args which the user
// is expected to close. If the Body is not closed, the Client's underlying
// RoundTripper (typically Transport) may not be able to re-use a persistent TCP
// connection to the server for a subsequent request.
//
// The request Args, if non-nil, will be closed by the underlying Transport, even
// on errors.
//
// Generally Exec or Query will be used instead of Do.
func (c *Client) Do(req *Request) (*Response, error) {
	transport := c.Transport

	if transport == nil {
		transport = DefaultTransport
	}

	if c.Timeout != 0 {
		var ctx = req.Context
		var cancel context.CancelFunc

		if ctx == nil {
			ctx = context.Background()
		}

		req.Context, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}

	return transport.RoundTrip(req)
}

// Exec issues a request with cmd and args to the Redis server at the address
// set on the client.
//
// An error is returned if the request couldn't be sent or if the command was
// refused by the Redis server.
//
// The context passed as first argument allows the operation to be canceled
// asynchronously.
func (c *Client) Exec(ctx context.Context, cmd string, args ...interface{}) error {
	return ParseArgs(c.Query(ctx, cmd, args...), nil)
}

// Query issues a request with cmd and args to the Redis server at the address
// set on the client, returning the response's Args (which is never nil).
//
// Any error occurring while querying the Redis server will be returned by the
// Args.Close method of the returned value.
//
// The context passed as first argument allows the operation to be canceled
// asynchronously.
func (c *Client) Query(ctx context.Context, cmd string, args ...interface{}) Args {
	addr := c.Addr
	if len(addr) == 0 {
		addr = "localhost:6379"
	}

	r, err := c.Do(&Request{
		Addr:    addr,
		Cmd:     cmd,
		Args:    List(args...),
		Context: ctx,
	})
	if err != nil {
		return newArgsError(err)
	}

	return r.Args
}

// DefaultClient is the default client and is used by Exec and Query.
var DefaultClient = &Client{}

// Exec is a wrapper around DefaultClient.Exec.
func Exec(ctx context.Context, cmd string, args ...interface{}) error {
	return DefaultClient.Exec(ctx, cmd, args...)
}

// Query is a wrapper around DefaultClient.Query.
func Query(ctx context.Context, cmd string, args ...interface{}) Args {
	return DefaultClient.Query(ctx, cmd, args...)
}
