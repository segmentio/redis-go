package redis

import (
	"context"
	"fmt"
	"sync"
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

type MultiError []error

func (err MultiError) Error() string {
	if len(err) > 0 {
		return fmt.Sprintf("%d errors. First one: '%s'.", len(err), err[0])
	} else {
		return fmt.Sprintf("No errors (weird).")
	}
}

// WriteTestPattern writes a test pattern to a Redis client. The objective is to read the test pattern back
// at a later stage using ReadTestPattern.
func WriteTestPattern(client *Client, n int, keyTempl string, timeout time.Duration) (numSuccess int, numFailure int, err MultiError) {
	writeErrs := make(chan error, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(keyTempl, i)
		val := "1"
		waiter.Add(1)
		go func(key, val string) {
			defer waiter.Done()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := client.Exec(ctx, "SET", key, val); err != nil {
				writeErrs <- err
			}
		}(key, val)
	}
	waiter.Wait()
	close(writeErrs)
	numFailure = len(writeErrs)
	numSuccess = n - numFailure
	if len(writeErrs) > 0 {
		err = make(MultiError, 0)
		for writeErr := range writeErrs {
			err = append(err, writeErr)
		}
	}
	return
}

// ReadTestPattern reads a test pattern (previously writen using WriteTestPattern) from a Redis client and returns hit statistics.
func ReadTestPattern(client *Client, n int, keyTempl string, timeout time.Duration) (numHits, numMisses int, err error) {
	type tresult struct {
		hit bool
		err error
	}
	results := make(chan tresult, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < cap(results); i++ {
		key := fmt.Sprintf(keyTempl, i)
		waiter.Add(1)
		go func(key string, rq chan<- tresult) {
			defer waiter.Done()
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			args := client.Query(ctx, "GET", key)
			if args.Len() != 1 {
				rq <- tresult{err: fmt.Errorf("Unexpected response: 1 arg expected; contains %d. ", args.Len())}
				return
			}
			var v string
			args.Next(&v)
			if err := args.Close(); err != nil {
				rq <- tresult{err: err}
			} else {
				rq <- tresult{hit: v == "1"}
			}
		}(key, results)
	}
	waiter.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			return 0, 0, result.err
		}
		if result.hit {
			numHits++
		} else {
			numMisses++
		}
	}
	return
}
