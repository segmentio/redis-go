package redis

import "context"

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

	// Cmds is the list of commands submitted by the request.
	Cmds []Command

	// If not nil, this context is used to control asynchronous cancellation of
	// the request when it is passed to a RoundTripper.
	Context context.Context
}

// NewRequest returns a new Request, given an address, command, and list of
// arguments.
func NewRequest(addr string, cmd string, args Args) *Request {
	return &Request{
		Addr: addr,
		Cmds: []Command{{cmd, args}},
	}
}

// Close closes all arguments of the request command list.
func (req *Request) Close() error {
	var err error

	for _, cmd := range req.Cmds {
		if cmd.Args != nil {
			if e := cmd.Args.Close(); e != nil && err == nil {
				err = e
			}
		}
	}

	return err
}

// IsTransaction returns true if the request is configured to run as a
// transaction, false otherwise.
func (req *Request) IsTransaction() bool {
	return len(req.Cmds) == 0 || req.Cmds[0].Cmd == "MULTI"
}
