package redis_test

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/objconv/resp"
	redis "github.com/segmentio/redis-go"
)

func TestServer(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context)
	}{
		{
			scenario: "close a server right after starting it",
			function: testServerCloseAfterStart,
		},
		{
			scenario: "gracefully shutdown the server when no connection has been received",
			function: testServerGracefulShutdown,
		},
		{
			scenario: "cancelling a graceful shutdown returns context.Canceled",
			function: testServerCancelGracefulShutdown,
		},
		{
			scenario: "listener errors are reported by the Serve method",
			function: testServerServeError,
		},
		{
			scenario: "gracefully shutdown after setting a key produces no errors",
			function: testServerSetAndGracefulShutdown,
		},
		{
			scenario: "fetch a stream of values and gracefully shutdown produces no errors",
			function: testServerSingleLrangeAndGracefulShutdown,
		},
		{
			scenario: "fetch multiple streams of values and gracefully shutdown procudes no errors",
			function: testServerManyLrangeAndGracefulShutdown,
		},
		{
			scenario: "the response writer is unusable after being hijacked",
			function: testServerHijackResponseWriter,
		},
		{
			scenario: "redis protocol errors written to the response writer are made visible by the client",
			function: testServerWriteErrorToResponseWriter,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()

			testFunc(t, ctx)
		})
	}
}

func testServerCloseAfterStart(t *testing.T, ctx context.Context) {
	srv, _ := newServer(nil)

	if err := srv.Close(); err != nil {
		t.Error(err)
	}
}

func testServerGracefulShutdown(t *testing.T, ctx context.Context) {
	srv, _ := newServer(nil)
	defer srv.Close()

	if err := srv.Shutdown(ctx); err != nil {
		t.Error(err)
	}
}

func testServerCancelGracefulShutdown(t *testing.T, ctx context.Context) {
	srv, _ := newServer(nil)
	defer srv.Close()

	ctx, cancel := context.WithCancel(ctx)
	cancel()

	if err := srv.Shutdown(ctx); err != context.Canceled {
		t.Error(err)
	}
}

func testServerServeError(t *testing.T, ctx context.Context) {
	e := &testError{temporary: false}
	l := &testErrorListener{err: e}

	srv := &redis.Server{}

	if err := srv.Serve(l); err != e {
		t.Error(err)
	}
}

func testServerSetAndGracefulShutdown(t *testing.T, ctx context.Context) {
	key := generateKey()

	srv, url := newServer(redis.HandlerFunc(func(res redis.ResponseWriter, req *redis.Request) {
		if req.Cmds[0].Cmd != "SET" {
			t.Error("invalid command received by the server:", req.Cmds[0].Cmd)
			return
		}

		var k string
		var v string
		req.Cmds[0].ParseArgs(&k, &v)

		if k != key {
			t.Error("invalid key received by the server:", k)
		}

		if v != "0123456789" {
			t.Error("invalid value received by the server:", v)
		}

		res.Write("OK")
	}))
	defer srv.Close()

	tr := &redis.Transport{}
	defer tr.CloseIdleConnections()

	cli := &redis.Client{Addr: url, Transport: tr}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := cli.Exec(ctx, "SET", key, "0123456789"); err != nil {
		t.Error(err)
	}

	if err := srv.Shutdown(ctx); err != nil {
		t.Error(err)
	}
}

func testServerSingleLrangeAndGracefulShutdown(t *testing.T, ctx context.Context) {
	key := generateKey()

	srv, url := newServer(redis.HandlerFunc(func(res redis.ResponseWriter, req *redis.Request) {
		if req.Cmds[0].Cmd != "LRANGE" {
			t.Error("invalid command received by the server:", req.Cmds[0].Cmd)
			return
		}

		var k string
		var i int
		var j int
		req.Cmds[0].ParseArgs(&k, &i, &j)

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

	tr := &redis.Transport{}
	defer tr.CloseIdleConnections()

	cli := &redis.Client{Addr: url, Transport: tr}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
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
}

func testServerManyLrangeAndGracefulShutdown(t *testing.T, ctx context.Context) {
	srv, url := newServer(redis.HandlerFunc(func(res redis.ResponseWriter, req *redis.Request) {
		var i int
		var j int
		req.Cmds[0].ParseArgs(nil, &i, &j)

		res.WriteStream(j - i)

		for i != j {
			i++
			res.Write(i)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}

	tr := &redis.Transport{MaxIdleConns: 2}
	defer tr.CloseIdleConnections()

	for i := 0; i != 5; i++ {
		wg.Add(1)

		go func(i int, key string) {
			defer wg.Done()

			cli := &redis.Client{Addr: url, Transport: tr}

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
		}(i, generateKey())
	}

	wg.Wait()

	if err := srv.Shutdown(ctx); err != nil {
		t.Error(err)
	}
}

func testServerHijackResponseWriter(t *testing.T, ctx context.Context) {
	srv, url := newServer(redis.HandlerFunc(func(res redis.ResponseWriter, req *redis.Request) {
		conn, _, err := res.(redis.Hijacker).Hijack()

		if err != nil {
			t.Error("Hijack failed:", err)
			return
		}

		if err := res.WriteStream(1); err != redis.ErrHijacked {
			t.Error("expected an error on the server after the connection was hijacked but got", err)
		}

		if err := res.Write(nil); err != redis.ErrHijacked {
			t.Error("expected an error on the server after the connection was hijacked but got", err)
		}

		if err := res.(redis.Flusher).Flush(); err != redis.ErrHijacked {
			t.Error("expected an error on the server after the connection was hijacked but got", err)
		}

		if _, _, err := res.(redis.Hijacker).Hijack(); err != redis.ErrHijacked {
			t.Error("expected an error on the server after the connection was hijacked but got", err)
		}

		conn.Close()
	}))
	defer srv.Close()

	tr := &redis.Transport{MaxIdleConns: 2}
	defer tr.CloseIdleConnections()

	cli := &redis.Client{Addr: url, Transport: tr}

	if err := cli.Exec(ctx, "SET", "hello", "world"); err == nil {
		t.Error("expected an error on the client when the connection is hijacked and closed but got <nil>")
	}
}

func testServerWriteErrorToResponseWriter(t *testing.T, ctx context.Context) {
	respErr := resp.NewError("ERR something went wrong")

	srv, url := newServer(redis.HandlerFunc(func(res redis.ResponseWriter, req *redis.Request) {
		res.Write(respErr)
	}))
	defer srv.Close()

	tr := &redis.Transport{MaxIdleConns: 1}
	defer tr.CloseIdleConnections()

	cli := &redis.Client{Addr: url, Transport: tr}

	if err := cli.Exec(ctx, "SET", "hello", "world"); err == nil {
		t.Error("expected a redis protocol error but got <nil>")

	} else if e, ok := err.(*resp.Error); !ok {
		t.Error("unexpected error type:", err)

	} else if s := e.Error(); s != respErr.Error() {
		t.Error("unexpected error string:", s)
	}
}

func newServer(handler redis.Handler) (srv *redis.Server, url string) {
	return newServerTimeout(handler, 100*time.Millisecond)
}

func newServerTimeout(handler redis.Handler, timeout time.Duration) (srv *redis.Server, url string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	srv = &redis.Server{
		Handler:      handler,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		IdleTimeout:  timeout,
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
