package redis

import (
	"io"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// A ResponseWriter is exposed to server request handlers in order to send back
// the response to a client request.
type ResponseWriter interface {
	// WriteLen should be called before Write to indicate how many values will
	// be sent as part of the response.
	//
	// If WriteLen is not called before writing the first value the writer will
	// assume that a single value will be sent.
	WriteLen(n int)

	// Write should be called repeatedly by the server request handler to send
	// the response values.
	//
	// If Write was never called by a request handler a default "+OK\r\n"
	// response will be sent to the client, assuming everything went well.
	Write(v interface{}) error
}

// Response represents a redis response.
type Response struct {
	// Args is a the list of arguments that compose the response.
	Args Arguments

	// Request is the request that this response is for, may be nil.
	Request *Request
}

// NewResponse returns a Response initialized with req and args.
func NewResponse(req *Request, args ...interface{}) *Response {
	return &Response{Args: Args(args...), Request: req}
}

// Read loads the response arguments in memory and closes the argument stream,
// returns an error if something went wrong.
func (res *Response) Read() (err error) {
	var args []interface{}

	if args, err = ReadValues(res.Args); err == nil {
		res.Args = Args(args...)
	}

	return
}

// Write writes the response to w, returning an error if something went wrong.
//
// The method automatically closes res.Args.
func (res *Response) Write(w io.Writer) (err error) {
	if res.Args != nil {
		defer res.Args.Close()
	}
	return objconv.NewEncoder(objconv.EncoderConfig{
		Output:  w,
		Emitter: &resp.Emitter{},
	}).Encode(makeArgsArray(res.Args))
}

// ReadResponse reads and return a response from r, or returns a nil response
// and an error if something went wrong.
func ReadResponse(r io.Reader, req *Request) (res *Response, err error) {
	res = &Response{
		Args: argsDecoder{objconv.NewStreamDecoder(objconv.DecoderConfig{
			Input:  r,
			Parser: &resp.Parser{},
		})},
		Request: req,
	}
	return
}
