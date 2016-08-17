package redis

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime/debug"
	"time"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// A Server defines parameters for running an redis server. The zero value for
// Server is a valid configuration.
type Server struct {
	Network      string
	Address      string
	Handler      Handler
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	ErrorLog     *log.Logger
}

// ListenAndServe listens on the server's network address and then calls Serve
// to handle requests on incoming connections.
// By default the server listens on all network interfaces on port 6379.
func (s *Server) ListenAndServe() (err error) {
	var lstn net.Listener
	var network string
	var address string

	if network = s.Network; len(network) == 0 {
		network = "tcp"
	}

	if address = s.Address; len(address) == 0 {
		address = ":6379"
	}

	if lstn, err = net.Listen(network, address); err == nil {
		err = s.Serve(lstn)
	}

	return
}

// Serve accepts incoming connections on the Listener, creating a new service
// goroutine for each. The service goroutines read requests and then call the
// Handler to reply to them.
func (s *Server) Serve(lstn net.Listener) (err error) {
	// mostly copied from https://golang.org/src/net/http/server.go?s=69176:69222#L2246
	defer lstn.Close()
	var tempDelay time.Duration // how long to sleep on accept failure

	for err == nil {
		c, e := lstn.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				s.logf("redis: Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0
		go s.serve(NewConn(c))
	}

	return
}

func (s *Server) logf(msg string, args ...interface{}) {
	if s.ErrorLog != nil {
		s.ErrorLog.Printf(msg, args...)
	}
}

func (s *Server) serve(c Conn) {
	defer c.Close()
	defer func() {
		if x := recover(); x != nil {
			fmt.Fprintf(os.Stderr, "%s\n", x)
			debug.PrintStack()
		}
	}()

	raddr := c.RemoteAddr()
	laddr := c.LocalAddr()
	rtimeout := s.ReadTimeout
	wtimeout := s.WriteTimeout

	for {
		var req *Request
		var err error

		if rtimeout != 0 {
			if err := c.SetReadDeadline(time.Now().Add(rtimeout)); err != nil {
				s.logf("redis: failed to set read deadline on %s->%s: %s", raddr, laddr, err)
				return
			}
		}

		if req, err = c.ReadRequest(); err != nil {
			if err != io.EOF {
				s.logf("redis: failed to read request on %s->%s: %s", raddr, laddr, err)
			}
			return
		}

		if wtimeout != 0 {
			if err := c.SetWriteDeadline(time.Now().Add(wtimeout)); err != nil {
				s.logf("redis: failed to set write deadline on %s->%s: %s", raddr, laddr, err)
				return
			}
		}

		if err = s.serveRequest(c, req); err != nil {
			s.logf("redis: encountered an error while serving %s on %s->%s: %s", req.Cmd, raddr, laddr, err)
			return
		}
	}
}

func (s *Server) serveRequest(c Conn, req *Request) error {
	defer req.Args.Close()

	// delegate to the handler
	res := &responseWriter{w: c}
	s.Handler.ServeRedis(res, req)

	// when the handler didn't call any of the write methods we assume the
	// operation was a success.
	if res.e == nil && res.s == nil {
		res.Write("OK")
	}

	// report any error.
	if res.err != nil {
		return res.err
	}

	// Always make sure we close the output stream.
	if res.s != nil {
		res.s.Close()
	}

	// don't forget to flush the buffered connection.
	return c.Flush()
}

type responseWriter struct {
	err error
	w   io.Writer
	e   objconv.Encoder
	s   objconv.StreamEncoder
	n   int
	i   int
}

func (r *responseWriter) WriteLen(n int) {
	if n < 0 {
		panic("redis.ResponseWriter.WriteLen: invalid negative length")
	}

	if r.e != nil {
		panic("redis.ResponseWriter.WriteLen: called after Write")
	}

	if r.s != nil {
		panic("redis.ResponseWriter.WriteLen: called multiple times")
	}

	r.s = objconv.NewStreamEncoder(objconv.EncoderConfig{
		Output:  r.w,
		Emitter: &resp.Emitter{},
		Tag:     "redis",
	})
	r.err = r.s.Open(n)
	r.n = n
}

func (r *responseWriter) Write(v interface{}) error {
	if r.err != nil {
		return r.err
	}

	if r.s != nil {
		if r.i++; r.i > r.n {
			panic("redis.ResponseWriter.Write: called too many times")
		}

		r.err = r.s.Encode(v)
		return r.err
	}

	if r.e != nil {
		panic("redis.ResponseWriter.Write: called multiple time and not length was set")
	}

	r.e = objconv.NewEncoder(objconv.EncoderConfig{
		Output:  r.w,
		Emitter: &resp.Emitter{},
		Tag:     "redis",
	})
	r.err = r.e.Encode(v)
	return r.err
}
