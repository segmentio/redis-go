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
	proxy.serveRequest(w, r)
}

func (proxy *ReverseProxy) serveRequest(w ResponseWriter, req *Request) {
	keys := make([]string, 0, 10)
	cmds := req.Cmds

	for i := range cmds {
		keys = cmds[i].getKeys(keys)
	}

	servers, err := proxy.lookupServers(req.Context)
	if err != nil {
		w.Write(errorf("ERR No upstream server were found to route the request to."))
		proxy.log(err)
		return
	}

	// TODO: looking up servers and rebuilding the hash ring for every request
	// is not efficient, we should cache and reuse the state.
	hashring := makeHashRing(servers...)
	upstream := ""

	for _, key := range keys {
		addr := hashring.lookup(key)

		if len(upstream) == 0 {
			upstream = addr
		} else if upstream != addr {
			w.Write(errorf("EXECABORT The transaction contains keys that hash to different upstream servers."))
			return
		}
	}

	req.Addr = upstream
	res, err := proxy.roundTrip(req)

	switch err.(type) {
	case nil:
	case *resp.Error:
		w.Write(err)
		return
	default:
		w.Write(errorf("ERR Connecting to the upstream server failed."))
		proxy.log(err)
		return
	}

	if err := w.WriteStream(res.Args.Len()); err != nil {
		panic(err)
	}

	var v interface{}
	for res.Args.Next(&v) {
		w.Write(v)
		v = nil
	}

	if err := res.Args.Close(); err != nil {
		if e, ok := err.(*resp.Error); ok {
			w.Write(e)
		} else {
			// Get caught by the server, that way the connection is closed and not
			// left in an unpredictable state.
			panic(err)
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
