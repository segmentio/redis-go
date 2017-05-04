package redis

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/objconv/resp"
)

func TestServer(t *testing.T) {
	n := int32(0)

	getKey := func() string {
		i := atomic.AddInt32(&n, 1)
		return fmt.Sprintf("redis-go.test.server.%d", i)
	}

	t.Run("close a server right after starting it", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(nil)

		if err := srv.Close(); err != nil {
			t.Error(err)
		}
	})

	t.Run("gracefully shutdown", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(nil)
		defer srv.Close()

		if err := srv.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	t.Run("cancel a graceful shutdown", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(nil)
		defer srv.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := srv.Shutdown(ctx); err != context.Canceled {
			t.Error(err)
		}
	})

	t.Run("start a server with a listener that returns always errors", func(t *testing.T) {
		t.Parallel()

		e := &testError{temporary: false}
		l := &testErrorListener{err: e}

		srv := &Server{}

		if err := srv.Serve(l); err != e {
			t.Error(err)
		}
	})

	t.Run("set a key, then gracefully shutdown", func(t *testing.T) {
		t.Parallel()
		key := getKey()

		srv, url := newServer(HandlerFunc(func(res ResponseWriter, req *Request) {
			if req.Cmd != "SET" {
				t.Error("invalid command received by the server:", req.Cmd)
				return
			}

			var k string
			var v string
			req.ParseArgs(&k, &v)

			if k != key {
				t.Error("invalid key received by the server:", k)
			}

			if v != "0123456789" {
				t.Error("invalid value received by the server:", v)
			}

			res.Write("OK")
		}))
		defer srv.Close()

		tr := &Transport{}
		defer tr.CloseIdleConnections()

		cli := &Client{Addr: url, Transport: tr}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := cli.Exec(ctx, "SET", key, "0123456789"); err != nil {
			t.Error(err)
		}

		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})

	t.Run("fetch a stream of values, then gracefully shutdown", func(t *testing.T) {
		t.Parallel()
		key := getKey()

		srv, url := newServer(HandlerFunc(func(res ResponseWriter, req *Request) {
			if req.Cmd != "LRANGE" {
				t.Error("invalid command received by the server:", req.Cmd)
				return
			}

			var k string
			var i int
			var j int
			req.ParseArgs(&k, &i, &j)

			if k != key {
				t.Error("invalid key received by the server:", k)
			}

			if i != 0 {
				t.Error("invalid start offset received by the server:", i)
			}

			if j != 10 {
				t.Error("invalid stop offset received by the server:", j)
			}

			res.WriteStream(3)
			res.Write(1)
			res.Write(2)
			res.Write(3)
		}))
		defer srv.Close()

		tr := &Transport{}
		defer tr.CloseIdleConnections()

		cli := &Client{Addr: url, Transport: tr}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		it := cli.Query(ctx, "LRANGE", key, 0, 10)

		if n := it.Len(); n != 3 {
			t.Error("invalid value count received by the client:", n)
		}

		for i := 0; i != 3; i++ {
			var v int
			if !it.Next(&v) {
				t.Error("not enough values read in the response:", i)
			}
			if v != i+1 {
				t.Error("invalid value received by the client:", v)
			}
		}

		if err := it.Close(); err != nil {
			t.Error("error received by the client:", err)
		}

		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})

	t.Run("fetch multiple streams of values, then gracefully shutdown", func(t *testing.T) {
		t.Parallel()

		srv, url := newServer(HandlerFunc(func(res ResponseWriter, req *Request) {
			var i int
			var j int
			req.ParseArgs(nil, &i, &j)

			res.WriteStream(j - i)

			for i != j {
				i++
				res.Write(i)
			}
		}))
		defer srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		wg := sync.WaitGroup{}

		tr := &Transport{ConnsPerHost: 2}
		defer tr.CloseIdleConnections()

		for i := 0; i != 5; i++ {
			wg.Add(1)

			go func(i int, key string) {
				defer wg.Done()

				cli := &Client{Addr: url, Transport: tr}

				it := cli.Query(ctx, "LRANGE-"+strconv.Itoa(i), key, 0, i)

				if n := it.Len(); n != i {
					t.Error("invalid value count received by the client:", key, n, "!=", i)
				}

				for j := 0; j != i; j++ {
					var v int
					if !it.Next(&v) {
						t.Error("not enough values read in the response:", key, j)
					}
					if v != j+1 {
						t.Error("invalid value received by the client:", key, v, "!=", j+1)
					}
				}

				if err := it.Close(); err != nil {
					t.Error(err)
				}
			}(i, getKey())
		}

		wg.Wait()

		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})

	t.Run("hijack connections, ensure that the response writer is unusable afterward", func(t *testing.T) {
		t.Parallel()

		srv, url := newServer(HandlerFunc(func(res ResponseWriter, req *Request) {
			conn, _, err := res.(Hijacker).Hijack()

			if err != nil {
				t.Error("Hijack failed:", err)
				return
			}

			if err := res.WriteStream(1); err != errHijacked {
				t.Error("expected an error on the server after the connection was hijacked but got", err)
			}

			if err := res.Write(nil); err != errHijacked {
				t.Error("expected an error on the server after the connection was hijacked but got", err)
			}

			if err := res.(Flusher).Flush(); err != errHijacked {
				t.Error("expected an error on the server after the connection was hijacked but got", err)
			}

			if _, _, err := res.(Hijacker).Hijack(); err != errHijacked {
				t.Error("expected an error on the server after the connection was hijacked but got", err)
			}

			conn.Close()
		}))
		defer srv.Close()

		tr := &Transport{ConnsPerHost: 2}
		defer tr.CloseIdleConnections()

		cli := &Client{Addr: url, Transport: tr}

		if err := cli.Exec(context.Background(), "SET", "hello", "world"); err == nil {
			t.Error("expected an error on the client when the connection is hijacked and closed but got <nil>")
		}
	})

	t.Run("return a redis protocol error", func(t *testing.T) {
		t.Parallel()

		respErr := resp.NewError("ERR something went wrong")

		srv, url := newServer(HandlerFunc(func(res ResponseWriter, req *Request) {
			res.Write(respErr)
		}))
		defer srv.Close()

		tr := &Transport{ConnsPerHost: 1}
		defer tr.CloseIdleConnections()

		cli := &Client{Addr: url, Transport: tr}

		if err := cli.Exec(context.Background(), "SET", "hello", "world"); err == nil {
			t.Error("expected a redis protocol error but got <nil>")

		} else if e, ok := err.(*resp.Error); !ok {
			t.Error("unexpected error type:", err)

		} else if s := e.Error(); s != respErr.Error() {
			t.Error("unexpected error string:", s)
		}
	})
}

func newServer(handler Handler) (srv *Server, url string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	srv = &Server{
		Handler:      handler,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		IdleTimeout:  100 * time.Millisecond,
		ErrorLog:     log.New(os.Stderr, "", 0),
	}

	go srv.Serve(l)

	addr := l.Addr()
	url = addr.Network() + "://" + addr.String()
	return
}

type testAddr struct {
	network string
	address string
}

func (a *testAddr) Network() string { return a.network }
func (a *testAddr) String() string  { return a.address }

type testError struct {
	timeout   bool
	temporary bool
}

func (e *testError) Error() string   { return "error" }
func (e *testError) Timeout() bool   { return e.timeout }
func (e *testError) Temporary() bool { return e.temporary }

type testErrorListener struct {
	err error
}

func (l *testErrorListener) Accept() (net.Conn, error) { return nil, l.err }
func (l *testErrorListener) Addr() net.Addr            { return &testAddr{} }
func (l *testErrorListener) Close() error              { return nil }
