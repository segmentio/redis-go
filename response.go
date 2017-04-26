package redis

import (
	"io"

	"github.com/segmentio/objconv/resp"
)

// Response represents the response from a Redis request.
type Response struct {
	// Args is the arguments list of the response.
	//
	// When the response is obtained from Transport.RoundTrip or from
	// Client.Do the Args field is never nil.
	Args Args

	// Request is the request that was sent to obtain this Response.
	Request *Request
}

// Write writes the response to w.
//
// If the argument list is not nil, it is closed after being written.
func (res *Response) Write(w io.Writer) error {
	enc := resp.NewStreamEncoder(w)

	if res.Args != nil {
		var val interface{}
		for res.Args.Next(&val) {
			if err := enc.Encode(val); err != nil {
				return err
			}
			val = nil
		}
		res.Args.Close()
	}

	return enc.Close()
}

func newResponse(parser *resp.Parser, req *Request, done chan<- error) *Response {
	return &Response{
		Args:    newArgsReader(parser, done),
		Request: req,
	}
}
