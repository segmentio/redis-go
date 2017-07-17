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
		cmd := &cmds[i]
		getter, ok := proxyCommands[cmd.Cmd]
		if !ok {
			w.Write(errorf("ERR unknown command '%s'", cmd.Cmd))
			return
		}
		keys = getter.getKeys(keys, cmd)
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
		proxy.blacklistServer(upstream)
		proxy.log(err)
		return
	}

	if res.Args != nil {
		err = proxy.writeArgs(w, res.Args)
	} else {
		err = proxy.writeTxArgs(w, res.TxArgs)
	}

	if err != nil {
		// Get caught by the server, that way the connection is closed and not
		// left in an unpredictable state.
		panic(err)
	}
}

func (proxy *ReverseProxy) writeTxArgs(w ResponseWriter, tx TxArgs) (err error) {
	w.WriteStream(tx.Len())
	var v []interface{} // TODO: figure out a way to avoid loading values in memory

	for a := tx.Next(); a != nil; a = tx.Next() {
		n := 0
		v = append(v, nil)

		for a.Next(&v[n]) {
			v = append(v, nil)
			n++
		}

		err = a.Close()

		if _, ok := err.(*resp.Error); ok {
			v = append(v, err)
			n++
		}

		w.Write(v[:n])
	}

	if e := tx.Close(); e != nil && err == nil {
		err = e
	}

	return
}

func (proxy *ReverseProxy) writeArgs(w ResponseWriter, a Args) (err error) {
	var v interface{}
	w.WriteStream(a.Len())

	for a.Next(&v) {
		w.Write(v)
		v = nil
	}

	err = a.Close()

	if e, ok := err.(*resp.Error); ok {
		w.Write(e)
		err = nil
	}

	if f, ok := w.(Flusher); ok {
		err = f.Flush()
	}

	return
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

func (proxy *ReverseProxy) blacklistServer(upstream string) {
	if b, ok := proxy.Registry.(ServerBlacklist); !ok {
		b.BlacklistServer(ServerEndpoint{Addr: upstream})
	}
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

type keyGetter interface {
	getKeys(keys []string, cmd *Command) []string
}

type singleKey struct{}

func (*singleKey) getKeys(keys []string, cmd *Command) []string {
	lastIndex := len(keys)
	keys = append(keys, "")

	if cmd.Args.Next(&keys[lastIndex]) {
		cmd.Args = MultiArgs(List(keys[lastIndex]), cmd.Args)
	} else {
		keys = keys[:lastIndex]
	}

	return keys
}

type allKeys struct{}

func (*allKeys) getKeys(keys []string, cmd *Command) []string {
	var key string
	var args = make([]interface{}, 0, cmd.Args.Len())

	for cmd.Args.Next(&key) {
		keys = append(keys, key)
		args = append(args, key)
		key = ""
	}

	cmd.Args = List(args...)
	return keys
}

type alternateKeys []bool

func (pattern alternateKeys) getKeys(keys []string, cmd *Command) []string {
	var key string
	var val interface{}
	var args = make([]interface{}, 0, cmd.Args.Len())

	for i := 0; true; i++ {
		if pattern[i%len(pattern)] {
			if !cmd.Args.Next(&key) {
				break
			}
			keys = append(keys, key)
			args = append(args, key)
			key = ""
		} else {
			if !cmd.Args.Next(&val) {
				break
			}
			args = append(args, val)
			val = nil
		}
	}

	cmd.Args = List(args...)
	return keys
}

type skipAndGet struct {
	n int
	g keyGetter
}

func (s *skipAndGet) getKeys(keys []string, cmd *Command) []string {
	var val interface{}
	var args = make([]interface{}, 0, s.n)

	for i := 0; i != s.n; i++ {
		if !cmd.Args.Next(&val) {
			break
		}
		args = append(args, val)
		val = nil
	}

	keys = s.g.getKeys(keys, cmd)
	cmd.Args = MultiArgs(List(args...), cmd.Args)
	return keys
}

var proxyCommands = map[string]keyGetter{
	// Strings
	"APPEND":      &singleKey{},
	"BITCOUNT":    &singleKey{},
	"BITFIELD":    &singleKey{},
	"BITOP":       &skipAndGet{2, &allKeys{}},
	"BITPOS":      &singleKey{},
	"DECR":        &singleKey{},
	"DECRBY":      &singleKey{},
	"GET":         &singleKey{},
	"GETBIT":      &singleKey{},
	"GETRANGE":    &singleKey{},
	"GETSET":      &singleKey{},
	"INCR":        &singleKey{},
	"INCRBY":      &singleKey{},
	"INCRBYFLOAT": &singleKey{},
	"MGET":        &allKeys{},
	"MSET":        alternateKeys{true, false},
	"MSETNX":      alternateKeys{true, false},
	"PSETEX":      &singleKey{},
	"SET":         &singleKey{},
	"SETBIT":      &singleKey{},
	"SETEX":       &singleKey{},
	"SETNX":       &singleKey{},
	"SETRANGE":    &singleKey{},
	"STRLEN":      &singleKey{},
}
