package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/segmentio/objconv/resp"
)

// ReverseProxy is the implementation of a redis reverse proxy.
type ReverseProxy struct {
	// Transport specifies the mechanism by which individual requests are made.
	// If nil, DefaultTransport is used.
	Transport RoundTripper

	// The registry exposing the set of redis servers that the proxy routes
	// requests to.
	Registry ServerRegistry

	// ErrorLog specifies an optional logger for errors accepting connections
	// and unexpected behavior from handlers. If nil, logging goes to os.Stderr
	// via the log package's standard logger.
	ErrorLog *log.Logger
}

// ServeRedis satisfies the Handler interface.
func (proxy *ReverseProxy) ServeRedis(w ResponseWriter, r *Request) {
	for _, cmd := range r.Cmds {
		switch cmd.Cmd {
		case "SUBSCRIBE", "PSUBSCRIBE":
			var channels []string
			var channel string

			for cmd.Args.Next(&channel) {
				// TOOD: limit the number of channels read here
				channels = append(channels, channel)
			}

			if err := cmd.Args.Close(); err != nil {
				proxy.log(err)
				return
			}

			conn, rw, err := w.(Hijacker).Hijack()
			if err != nil {
				proxy.log(err)
				return
			}

			proxy.servePubSub(conn, rw, cmd.Cmd, channels...)
			// TOD: figure out a way to pass the connection back in regular mode

		default:
			proxy.serveRequest(w, r)
		}
	}
}

func (proxy *ReverseProxy) serveRequest(w ResponseWriter, req *Request) {
	key, ok := req.getKey()

	if !ok {
		w.Write(errorf("no valid key found for the request and therefore cannot be proxied"))
		return
	}

	servers, err := proxy.lookupServers(req.Context)
	if err != nil {
		w.Write(errorf("bad gateway"))
		proxy.log(err)
		return
	}

	// TODO: looking up servers and rebuilding the hash ring for every request
	// is not efficient, we should cache and reuse the state.
	hashring := makeHashRing(servers...)

	req.Addr = hashring.lookup(key)
	res, err := proxy.roundTrip(req)

	switch err.(type) {
	case nil:
	case *resp.Error:
		w.Write(err)
		return
	default:
		w.Write(errorf("bad gateway"))
		proxy.log(err)
		return
	}

	defer func() {
		if err := res.Close(); err != nil {
			proxy.log(err)
		}
	}()

	for _, args := range res.Args {
		if err := w.WriteStream(args.Len()); err != nil {
			w.Write(errorf("internal server error"))
			proxy.log(err)
			return
		}

		var v interface{}
		for args.Next(&v) {
			w.Write(v)
			v = nil
		}
	}
}

func (proxy *ReverseProxy) servePubSub(conn net.Conn, rw *bufio.ReadWriter, command string, channels ...string) {
	defer conn.Close()

	// TODO:
	// - select the backend server to subscribe to by hashing the channel
	// - refresh the list of servers periodically so we can rebalance when new servers are added
}

func (proxy *ReverseProxy) lookupServers(ctx context.Context) ([]ServerEndpoint, error) {
	r := proxy.Registry
	if r == nil {
		return nil, errors.New("a redis proxy needs a non-nil registry to lookup the list of avaiable servers")
	}
	return r.LookupServers(ctx)
}

func (proxy *ReverseProxy) roundTrip(req *Request) (*Response, error) {
	t := proxy.Transport
	if t == nil {
		t = DefaultTransport
	}
	return t.RoundTrip(req)
}

func (proxy *ReverseProxy) log(err error) {
	switch err {
	case io.EOF, io.ErrUnexpectedEOF, io.ErrClosedPipe:
		// Don't log these errors because they are very common and it doesn't
		// bring any value to know that a client disconnected.
		return
	}
	print := log.Print
	if logger := proxy.ErrorLog; logger != nil {
		print = logger.Print
	}
	print(err)
}

func (proxy *ReverseProxy) transport() RoundTripper {
	if transport := proxy.Transport; transport != nil {
		return transport
	}
	return DefaultTransport
}

func errorf(format string, args ...interface{}) error {
	return resp.NewError(fmt.Sprintf(format, args...))
}
