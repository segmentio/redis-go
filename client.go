package redis

import (
	"context"
	"time"

	"github.com/segmentio/objconv/resp"
)

type Client struct {
	Address string

	Transport RoundTripper

	Timeout time.Duration
}

func (c *Client) Do(req *Request) (*Response, error) {
	transport := c.Transport

	if transport == nil {
		transport = DefaultTransport
	}

	if c.Timeout != 0 {
		ctx, cancel := context.WithTimeout(req.Context(), c.Timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	return transport.RoundTrip(req)
}

func (c *Client) Exec(ctx context.Context, cmd string, args ...interface{}) error {
	var val interface{}
	var err error
	var it = c.Query(ctx, cmd, args...)

	if it.Next(&val) {
		if e, ok := val.(*resp.Error); ok {
			err = e
		}
	}

	if e := it.Close(); e != nil {
		err = e
	}

	return err
}

func (c *Client) Query(ctx context.Context, cmd string, args ...interface{}) Args {
	res, err := c.Do(NewRequest(c.Address, cmd, args...).WithContext(ctx))
	if err != nil {
		return &argsError{err: err}
	}
	return res.Args
}

var DefaultClient = &Client{
	Address: "localhost:6379",
}

func Do(req *Request) (*Response, error) {
	return DefaultClient.Do(req)
}

func Exec(ctx context.Context, cmd string, args ...interface{}) error {
	return DefaultClient.Exec(ctx, cmd, args...)
}

func Query(ctx context.Context, cmd string, args ...interface{}) Args {
	return DefaultClient.Query(ctx, cmd, args...)
}
