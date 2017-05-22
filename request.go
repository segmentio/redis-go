package redis

import (
	"context"
	"io"
	"sync"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// A Command represent a Redis command used withing a Request.
type Command struct {
	// Cmd is the Redis command that's being sent with this request.
	Cmd string

	// Args is the list of arguments for the request's command. This field
	// may be nil for client requests if there are no arguments to send with
	// the request.
	//
	// For server request, Args is never nil, even if there are no values in
	// the argument list.
	Args Args

	key *string
}

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

	// Cmds is a list of commands that are part of the transaction.
	Cmds []Command

	// If not nil, this context is used to control asynchronous cancellation of
	// the request when it is passed to a RoundTripper.
	Context context.Context

	once sync.Once
}

// NewRequest returns a new Request, given an address, command, and list of
// arguments.
func NewRequest(addr string, cmds ...Command) *Request {
	return &Request{
		Addr: addr,
		Cmds: cmds,
	}
}

func NewCommand(cmd string, args Args) *Command {
	return &Command{
		Cmd:  cmd,
		Args: args,
	}
}

// ParseArgs parses the list of arguments from the request into the destination
// pointers, returning an error if something went wrong.
func (req *Request) ParseArgs(dsts ...interface{}) error {
	for _, cmd := range req.Cmds {
		if err := ParseArgs(cmd.Args, dsts...); err != nil {
			return err
		}
	}

	return nil
}

// Write writes the request to w.
//
// If the argument list is not nil, it is closed after being written.
func (req *Request) Write(w io.Writer) error {
	for _, cmd := range req.Cmds {
		enc := objconv.StreamEncoder{Emitter: resp.NewClientEmitter(w)}

		if err := enc.Encode(cmd.Cmd); err != nil {
			return err
		}

		if cmd.Args != nil {
			var val interface{}
			for cmd.Args.Next(&val) {
				if err := enc.Encode(val); err != nil {
					return err
				}
				val = nil
			}
			cmd.Args.Close()
		}

		if err := enc.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (req *Request) getKey() (string, bool) {
	// getKey should look at all the Command.key to figure out if the Request can be proxied.
	idx := 0

	req.once.Do(func() {
		var key string

		if req.Cmds[idx].Args.Next(&key) {
			req.Cmds[idx].key = &key
			req.Cmds[idx].Args = MultiArgs(List(key), req.Cmds[idx].Args)
		}
	})

	if req.Cmds[idx].key == nil {
		return "", false
	}

	return *req.Cmds[idx].key, true
}

func (req *Request) parseKey() (string, bool) {
	return "", false
}
