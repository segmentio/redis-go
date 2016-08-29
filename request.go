package redis

import (
	"io"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// Request represents a redis request.
type Request struct {
	// Cmd the name of the command.
	Cmd Command

	// Args is a list of arguments to the command.
	Args Arguments
}

// NewRequest returns a Request initialized with cmd and args.
func NewRequest(cmd Command, args ...interface{}) *Request {
	return &Request{Cmd: cmd, Args: Args(args...)}
}

// Read loads the request arguments in memory and closes the argument stream,
// returns an error if something went wrong.
func (req *Request) Read() (err error) {
	var args []interface{}
	var list []string

	if list, err = ReadStrings(req.Args); err == nil {
		if list == nil {
			req.Args = Args()
		} else {
			args = make([]interface{}, len(list))
			for i, s := range list {
				args[i] = s
			}
			req.Args = Args(args...)
		}
	}

	return
}

// Write writes the request to w, returning an error if something went wrong.
//
// The method automatically closes req.Args.
func (req *Request) Write(w io.Writer) (err error) {
	if req.Args != nil {
		defer req.Args.Close()
	}
	return objconv.NewEncoder(objconv.EncoderConfig{
		Output:  w,
		Emitter: &resp.Emitter{EmitBulkStringsOnly: true},
	}).Encode(objconv.MultiArray(makeCommandArray(req.Cmd), makeArgsArray(req.Args)))
}

// ReadRequest reads and return a request from r, or returns a nil request
// and an error if something went wrong.
func ReadRequest(r io.Reader) (req *Request, err error) {
	cmd := ""
	dec := objconv.NewStreamDecoder(objconv.DecoderConfig{
		Input:  r,
		Parser: &resp.Parser{},
	})

	if err = dec.Decode(&cmd); err == nil {
		req = &Request{
			Cmd:  Command(cmd),
			Args: argsDecoder{dec},
		}
	}

	return
}
