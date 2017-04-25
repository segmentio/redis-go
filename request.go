package redis

import (
	"context"
	"io"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

type Request struct {
	Addr string

	Cmd string

	Args Args

	ctx context.Context
}

func NewRequest(addr string, cmd string, args ...interface{}) *Request {
	return &Request{
		Addr: addr,
		Cmd:  cmd,
		Args: List(args...),
	}
}

func (req *Request) Context() context.Context {
	ctx := req.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx
}

func (req *Request) WithContext(ctx context.Context) *Request {
	r := *req
	r.ctx = ctx
	return &r
}

func (req *Request) Write(w io.Writer) error {
	var enc = objconv.StreamEncoder{Emitter: resp.NewClientEmitter(w)}
	var val interface{}

	if err := enc.Encode(req.Cmd); err != nil {
		return err
	}

	if req.Args != nil {
		for req.Args.Next(&val) {
			if err := enc.Encode(val); err != nil {
				return err
			}
			val = nil
		}
	}

	return enc.Close()
}
