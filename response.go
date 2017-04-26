package redis

import (
	"io"

	"github.com/segmentio/objconv/resp"
)

type Response struct {
	Args Args

	Request *Request
}

func NewResponse(args ...interface{}) *Response {
	return &Response{
		Args: List(args...),
	}
}

func (res *Response) Write(w io.Writer) error {
	var enc = resp.NewStreamEncoder(w)
	var val interface{}

	for res.Args.Next(&val) {
		if err := enc.Encode(val); err != nil {
			return err
		}
		val = nil
	}

	return enc.Close()
}

func newResponse(parser *resp.Parser, req *Request, done chan<- error) *Response {
	return &Response{
		Args:    newArgsReader(parser, done),
		Request: req,
	}
}
