package redis

import (
	"context"
	"fmt"
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

// MultiExec issues a transaction composed of the given list of commands.
//
// An error is returned if the request couldn't be sent or if the command was
// refused by the Redis server.
//
// The context passed as first argument allows the operation to be canceled
// asynchronously.
func (c *Client) MultiExec(ctx context.Context, cmds ...Command) error {
	return c.MultiQuery(ctx, cmds...).Close()
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
		Cmds:    []Command{{cmd, List(args...)}},
		Context: ctx,
	})
	if err != nil {
		return newArgsError(err)
	}

	return r.Args
}

// MultiQuery issues a transaction composed of the given list of commands to the
// Redis server at the address set on the client, returning the response's TxArgs
// (which is never nil).
//
// The method automatically wraps the list of commands with MULTI and EXEC, it
// is an error to put those in the command list.
//
// Any error occurring while querying the Redis server will be returned by the
// TxArgs.Close method of the returned value.
//
// The context passed as first argument allows the operation to be canceled
// asynchronously.
func (c *Client) MultiQuery(ctx context.Context, cmds ...Command) TxArgs {
	addr := c.Addr
	if len(addr) == 0 {
		addr = "localhost:6379"
	}

	for _, cmd := range cmds {
		switch cmd.Cmd {
		case "MULTI", "EXEC", "DISCARD":
			return newTxArgsError(fmt.Errorf("commands passed to redis.(*Client).MultiQuery cannot contain MULTI, EXEC, or DISCARD"))
		}
	}

	txCmds := make([]Command, 0, len(cmds))
	txCmds = append(txCmds, Command{Cmd: "MULTI"})
	txCmds = append(txCmds, cmds...)
	txCmds = append(txCmds, Command{Cmd: "EXEC"})

	r, err := c.Do(&Request{
		Addr:    addr,
		Cmds:    txCmds,
		Context: ctx,
	})
	if err != nil {
		return newTxArgsError(err)
	}

	return r.TxArgs
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
