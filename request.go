package redis

import (
	"context"
	"io"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// A Request represents a Redis request received by a server or to be sent by
// a client.
//
// The field semantics differ slightly between client and server usage.
// In addition to the notes on the fields below, see the documentation for
// Request.Write and RoundTripper.
type Request struct {
	// For client requests, Addr is set to the address of the server to which
	// the request is sent.
	//
	// For server requests (when received in a Handler's ServeRedis method),
	// the Addr field contains the remote address of the client that sent the
	// request.
	Addr string

	// Cmd is the Redis command that's being sent with this request.
	Cmd string

	// Args is the list of arguments for the request's command. This field
	// may be nil for client requests if there are no arguments to send with
	// the request.
	//
	// For server request, Args is never nil, even if there are no values in
	// the argument list.
	Args Args

	// If not nil, this context is used to control asynchronous cancellation of
	// the request when it is passed to a RoundTripper.
	Context context.Context
}

// NewRequest returns a new Request, given an address, command, and list of
// arguments.
func NewRequest(addr string, cmd string, args Args) *Request {
	return &Request{
		Addr: addr,
		Cmd:  cmd,
		Args: args,
	}
}

// ParseArgs parses the list of arguments from the request into the destination
// pointers, returning an error if something went wrong.
func (req *Request) ParseArgs(dsts ...interface{}) error {
	return ParseArgs(req.Args, dsts...)
}

// Write writes the request to w.
//
// If the argument list is not nil, it is closed after being written.
func (req *Request) Write(w io.Writer) error {
	enc := objconv.StreamEncoder{Emitter: resp.NewClientEmitter(w)}

	if err := enc.Encode(req.Cmd); err != nil {
		return err
	}

	if req.Args != nil {
		var val interface{}
		for req.Args.Next(&val) {
			if err := enc.Encode(val); err != nil {
				return err
			}
			val = nil
		}
		req.Args.Close()
	}

	return enc.Close()
}
