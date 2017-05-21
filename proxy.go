package redis

import (
	"bufio"
	"io"
	"log"
	"net"
)

// ReverseProxy is the implementation of a redis reverse proxy.
type ReverseProxy struct {
	// Transport specifies the mechanism by which individual requests are made.
	// If nil, DefaultTransport is used.
	Transport RoundTripper

	// ErrorLog specifies an optional logger for errors accepting connections
	// and unexpected behavior from handlers. If nil, logging goes to os.Stderr
	// via the log package's standard logger.
	ErrorLog *log.Logger
}

// ServeRedis satisfies the Handler interface.
func (proxy *ReverseProxy) ServeRedis(w ResponseWriter, r *Request) {
	switch r.Cmd {
	case "SUBSCRIBE", "PSUBSCRIBE":
		var channels []string
		var channel string

		for r.Args.Next(&channel) {
			channels = append(channels, channel)
		}

		if err := r.Args.Close(); err != nil {
			proxy.log(err)
			return
		}

		conn, rw, err := w.(Hijacker).Hijack()
		if err != nil {
			proxy.log(err)
			return
		}

		proxy.servePubSub(conn, rw, req.Cmd, channels...)
		return
	}

	proxy.serveRequest(w, r)
}

func (proxy *ReverseProxy) serveRequest(w ResponseWriter, r *Request) {
	// TODO:
	// - select the backend server to send the request to
	// - forward the response
}

func (proxy *ReverseProxy) servePubSub(conn net.Conn, rw *bufio.ReadWriter, command string, channels ...string) {
	defer conn.Close()

	// TODO:
	// - select the backend server to subscribe to by hashing the channel
	// - refresh the list of servers periodically so we can rebalance when new servers are added
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

func (proxy *ReverseProxy) transport() *Transport {
	if transport := proxy.Transport; transport != nil {
		return transport
	}
	return DefaultTransport
}
