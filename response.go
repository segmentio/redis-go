package redis

import (
	"io"
	"strings"

	"github.com/segmentio/objconv/resp"
)

// Response represents the response from a Redis request.
type Response struct {
	// Args is the arguments list of the response.
	//
	// When the response is obtained from Transport.RoundTrip or from
	// Client.Do the Args field is never nil.
	Args []Args

	// Request is the request that was sent to obtain this Response.
	Request *Request
}

// Write writes the response to w.
//
// If the argument list is not nil, it is closed after being written.
func (res *Response) Write(w io.Writer) error {
	enc := resp.NewStreamEncoder(w)

	for _, arg := range res.Args {
		var val interface{}
		for arg.Next(&val) {
			if err := enc.Encode(val); err != nil {
				return err
			}
			val = nil
		}
		arg.Close()
	}

	return enc.Close()
}

func (res *Response) Close() error {
	for _, arg := range res.Args {
		if err := arg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func newResponse(parser *resp.Parser, req *Request, done chan<- error) *Response {
	argsDone := make(chan error, len(req.Cmds))

	args := make([]Args, len(req.Cmds))
	for i := 0; i < len(req.Cmds); i++ {
		args[i] = newArgsReader(parser, argsDone)
	}

	go func() {
		var err error
		for range req.Cmds {
			if e := <-argsDone; err == nil && e != nil {
				err = e
				if redisErr, ok := e.(*resp.Error); ok {
					if strings.HasPrefix(redisErr.Error(), "EXECABORT") {
						break
					}
				}
			}

		}
		done <- err
	}()

	return &Response{
		Args:    args,
		Request: req,
	}
}
