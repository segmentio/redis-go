package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// A ResponseWriter interface is used by a Redis handler to construct an Redis
// response.
//
// A ResponseWriter may not be used after the Handler.ServeRedis method has
// returned.
type ResponseWriter interface {
	// WriteStream is called if the server handler is going to produce a list of
	// values by calling Write repeatedly n times.
	//
	// The method cannot be called more than once, or after Write was called.
	WriteStream(n int) error

	// Write is called by the server handler to send values back to the client.
	//
	// Write may not be called more than once, or more than n times, when n is
	// passed to a previous call to WriteStream.
	Write(v interface{}) error
}

// The Flusher interface is implemented by ResponseWriters that allow a Redis
// handler to flush buffered data to the client.
type Flusher interface {
	// Flush sends any buffered data to the client.
	Flush() error
}

// The Hijacker interface is implemented by ResponseWriters that allow a Redis
// handler to take over the connection.
type Hijacker interface {
	// Hijack lets the caller take over the connection. After a call to Hijack
	// the Redis server library will not do anything else with the connection.
	//
	// It becomes the caller's responsibility to manage and close the
	// connection.
	//
	// The returned net.Conn may have read or write deadlines already set,
	// depending on the configuration of the Server. It is the caller's
	// responsibility to set or clear those deadlines as needed.
	//
	// The returned bufio.Reader may contain unprocessed buffered data from the
	// client.
	Hijack() (net.Conn, *bufio.ReadWriter, error)
}

// A Handler responds to a Redis request.
//
// ServeRedis should write reply headers and data to the ResponseWriter and then
// return. Returning signals that the request is finished; it is not valid to
// use the ResponseWriter or read from the Request.Args after or concurrently with
// the completion of the ServeRedis call.
//
// Except for reading the argument list, handlers should not modify the provided
// Request.
type Handler interface {
	// ServeRedis is called by a Redis server to handle requests.
	ServeRedis(ResponseWriter, *Request)
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as
// Redis handlers. If f is a function with the appropriate signature.
type HandlerFunc func(ResponseWriter, *Request)

// ServeRedis implements the Handler interface, calling f.
func (f HandlerFunc) ServeRedis(res ResponseWriter, req *Request) {
	f(res, req)
}

// A Server defines parameters for running a Redis server.
type Server struct {
	// The address to listen on, ":6379" if empty.
	//
	// The address may be prefixed with "tcp://" or "unix://" to specify the
	// type of network to listen on.
	Addr string

	// Handler invoked to handle Redis requests, must not be nil.
	Handler Handler

	// ReadTimeout is the maximum duration for reading the entire request,
	// including the reading the argument list.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out writes of the
	// response. It is reset whenever a new request is read.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the next request.
	// If IdleTimeout is zero, the value of ReadTimeout is used. If both are
	// zero, there is no timeout.
	IdleTimeout time.Duration

	// ErrorLog specifies an optional logger for errors accepting connections
	// and unexpected behavior from handlers. If nil, logging goes to os.Stderr
	// via the log package's standard logger.
	ErrorLog *log.Logger

	mutex       sync.Mutex
	listeners   map[net.Listener]struct{}
	connections map[*Conn]struct{}
	context     context.Context
	shutdown    context.CancelFunc
}

// ListenAndServe listens on the network address s.Addr and then calls Serve to
// handle requests on incoming connections. If s.Addr is blank, ":6379" is used.
// ListenAndServe always returns a non-nil error.
func (s *Server) ListenAndServe() error {
	addr := s.Addr
	if len(addr) == 0 {
		addr = ":6379"
	}

	network, address := splitNetworkAddress(addr)
	if len(network) == 0 {
		network = "tcp"
	}

	l, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	return s.Serve(l)
}

// Close immediately closes all active net.Listeners and any connections.
// For a graceful shutdown, use Shutdown.
func (s *Server) Close() error {
	var err error
	s.mutex.Lock()

	if s.shutdown != nil {
		s.shutdown()
	}

	for l := range s.listeners {
		if cerr := l.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}

	for c := range s.connections {
		c.Close()
	}

	s.mutex.Unlock()
	return err
}

// Shutdown gracefully shuts down the server without interrupting any active
// connections. Shutdown works by first closing all open listeners, then closing
// all idle connections, and then waiting indefinitely for connections to return
// to idle and then shut down. If the provided context expires before the shutdown
// is complete, then the context's error is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	const maxPollInterval = 500 * time.Millisecond
	const minPollInterval = 10 * time.Millisecond

	s.mutex.Lock()

	if s.shutdown != nil {
		s.shutdown()
	}

	for l := range s.listeners {
		l.Close()
	}

	s.mutex.Unlock()

	for i := 0; s.numberOfActors() != 0; i++ {
		select {
		case <-ctx.Done():
		case <-time.After(backoff(i, minPollInterval, maxPollInterval)):
		}
	}

	return ctx.Err()
}

// Serve accepts incoming connections on the Listener l, creating a new service
// goroutine for each. The service goroutines read requests and then call
// s.Handler to reply to them.
//
// Serve always returns a non-nil error. After Shutdown or Close, the returned
// error is ErrServerClosed.
func (s *Server) Serve(l net.Listener) error {
	const maxBackoffDelay = 1 * time.Second
	const minBackoffDelay = 10 * time.Millisecond

	defer l.Close()
	defer s.untrackListener(l)

	s.trackListener(l)
	attempt := 0

	config := serverConfig{
		idleTimeout:  s.IdleTimeout,
		readTimeout:  s.ReadTimeout,
		writeTimeout: s.WriteTimeout,
	}

	if config.idleTimeout == 0 {
		config.idleTimeout = config.readTimeout
	}

	for {
		conn, err := l.Accept()

		if err != nil {
			select {
			default:
			case <-s.context.Done():
				return ErrServerClosed
			}
			switch {
			case isTimeout(err):
				continue
			case isTemporary(err):
				attempt++
				select {
				case <-time.After(backoff(attempt, minBackoffDelay, maxBackoffDelay)):
				case <-s.context.Done():
				}
				continue
			default:
				return err
			}
		}

		attempt = 0
		c := NewServerConn(conn)
		s.trackConnection(c)
		go s.serveConnection(s.context, c, config)
	}
}

func (s *Server) serveConnection(ctx context.Context, c *Conn, config serverConfig) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.Close()
	defer s.untrackConnection(c)

	var addr = c.RemoteAddr().String()
	var cmds []Command

	for {
		select {
		default:
		case <-ctx.Done():
			return
		}

		if c.waitReadyRead(config.idleTimeout) != nil {
			return
		}

		c.setTimeout(config.readTimeout)
		cmdReader := c.ReadCommands()

		cmds = cmds[:0]
		cmds = append(cmds, Command{})

		if !cmdReader.Read(&cmds[0]) {
			s.log(cmdReader.Close())
			return
		}

		if cmds[0].Cmd == "MULTI" {
			// Transactions have to be loaded in memory because the server has to
			// interleave responses between each command it receives.
			for {
				lastIndex := len(cmds)
				cmds[lastIndex].loadByteArgs()

				if lastIndex == 0 {
					c.WriteArgs(List("OK")) // response to MULTI
				} else {
					c.WriteArgs(List("QUEUED"))
				}

				cmds = append(cmds, Command{})
				cmd := &cmds[lastIndex]

				if !cmdReader.Read(cmd) {
					cmds = cmds[:lastIndex]
					break
				}
			}

			lastIndex := len(cmds) - 1

			if cmds[lastIndex].Cmd == "DISCARD" {
				cmds[lastIndex].Args.Close()

				if err := c.WriteArgs(List("OK")); err != nil {
					return
				}

				continue // discarded transactions are not passed to the handler
			}

			cmds = cmds[1:lastIndex]
		}

		if err := s.serveCommands(c, addr, cmds, config); err != nil {
			s.log(err)
			return
		}

		if err := cmdReader.Close(); err != nil {
			s.log(err)
			return
		}

		for i := range cmds {
			cmds[i] = Command{}
		}
	}
}

func (s *Server) serveCommands(c *Conn, addr string, cmds []Command, config serverConfig) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.readTimeout)

	req := &Request{
		Addr:    addr,
		Cmds:    cmds,
		Context: ctx,
	}

	res := &responseWriter{
		conn:    c,
		timeout: config.writeTimeout,
	}

	err = s.serveRequest(res, req)
	req.Close()
	cancel()
	return
}

func (s *Server) serveRequest(res *responseWriter, req *Request) (err error) {
	var preparedRes *preparedResponseWriter
	var w ResponseWriter = res
	var i int

	addPreparedResponse := func(i int, v interface{}) {
		if preparedRes == nil {
			preparedRes = &preparedResponseWriter{base: res}
		}
		preparedRes.responses = append(preparedRes.responses, preparedResponse{
			index: i,
			value: v,
		})
	}

	for _, cmd := range req.Cmds {
		switch cmd.Cmd {
		case "PING":
			msg := "PONG"
			cmd.ParseArgs(&msg)
			addPreparedResponse(i, msg)

		default:
			req.Cmds[i] = cmd
			i++
		}
	}

	if preparedRes != nil {
		w = preparedRes
		w.WriteStream(len(req.Cmds) + len(preparedRes.responses))
	}

	if req.Cmds = req.Cmds[:i]; len(req.Cmds) != 0 {
		s.serveRedis(w, req)
	}

	if err == nil && preparedRes != nil {
		err = preparedRes.writeRemainingValues()
	}

	if err == nil {
		err = res.Flush()
	}

	return
}

func (s *Server) serveRedis(res ResponseWriter, req *Request) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = convertPanicToError(v)
		}
	}()
	s.Handler.ServeRedis(res, req)
	return
}

func (s *Server) log(err error) {
	if err != ErrHijacked {
		print := log.Print
		if logger := s.ErrorLog; logger != nil {
			print = logger.Print
		}
		print(err)
	}
}

func (s *Server) trackListener(l net.Listener) {
	s.mutex.Lock()

	if s.listeners == nil {
		s.listeners = map[net.Listener]struct{}{}
		s.context, s.shutdown = context.WithCancel(context.Background())
	}

	s.listeners[l] = struct{}{}
	s.mutex.Unlock()
}

func (s *Server) untrackListener(l net.Listener) {
	s.mutex.Lock()
	delete(s.listeners, l)
	s.mutex.Unlock()
}

func (s *Server) trackConnection(c *Conn) {
	s.mutex.Lock()

	if s.connections == nil {
		s.connections = map[*Conn]struct{}{}
	}

	s.connections[c] = struct{}{}
	s.mutex.Unlock()
}

func (s *Server) untrackConnection(c *Conn) {
	s.mutex.Lock()
	delete(s.connections, c)
	s.mutex.Unlock()
}

func (s *Server) numberOfActors() int {
	s.mutex.Lock()
	n := len(s.connections) + len(s.listeners)
	s.mutex.Unlock()
	return n
}

// ListenAndServe listens on the network address addr and then calls Serve with
// handler to handle requests on incoming connections.
//
// ListenAndServe always returns a non-nil error.
func ListenAndServe(addr string, handler Handler) error {
	return (&Server{Addr: addr, Handler: handler}).ListenAndServe()
}

// Serve accepts incoming Redis connections on the listener l, creating a new
// service goroutine for each. The service goroutines read requests and then
// call handler to reply to them.
//
// Serve always returns a non-nil error.
func Serve(l net.Listener, handler Handler) error {
	return (&Server{Handler: handler}).Serve(l)
}

func isTimeout(err error) bool {
	e, ok := err.(timeoutError)
	return ok && e.Timeout()
}

func isTemporary(err error) bool {
	e, ok := err.(temporaryError)
	return ok && e.Temporary()
}

type timeoutError interface {
	Timeout() bool
}

type temporaryError interface {
	Temporary() bool
}

type serverConfig struct {
	idleTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func backoff(attempt int, minDelay time.Duration, maxDelay time.Duration) time.Duration {
	d := time.Duration(attempt*attempt) * minDelay
	if d > maxDelay {
		d = maxDelay
	}
	return d
}

func deadline(timeout time.Duration) time.Time {
	if timeout == 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}

func convertPanicToError(v interface{}) (err error) {
	switch x := v.(type) {
	case error:
		err = x
	default:
		err = fmt.Errorf("recovered from redis handler: %v", x)
	}
	return
}

type responseWriterType int

const (
	notype responseWriterType = iota
	oneshot
	stream
)

type responseWriter struct {
	conn    *Conn
	wtype   responseWriterType
	remain  int
	enc     objconv.Encoder
	stream  objconv.StreamEncoder
	timeout time.Duration
}

func (res *responseWriter) WriteStream(n int) error {
	if res.conn == nil {
		return ErrHijacked
	}

	if n < 0 {
		return ErrNegativeStreamCount
	}

	switch res.wtype {
	case oneshot:
		return ErrWriteStreamCalledAfterWrite
	case stream:
		return ErrWriteStreamCalledTooManyTimes
	}

	res.waitReadyWrite()
	res.wtype = stream
	res.remain = n
	res.stream = *resp.NewStreamEncoder(res.conn)
	return res.stream.Open(n)
}

func (res *responseWriter) Write(val interface{}) error {
	if res.conn == nil {
		return ErrHijacked
	}

	if res.wtype == notype {
		res.waitReadyWrite()
		res.wtype = oneshot
		res.remain = 1
		res.enc = *resp.NewEncoder(res.conn)
	}

	if res.remain == 0 {
		return ErrWriteCalledTooManyTimes
	}
	res.remain--

	if res.wtype == oneshot {
		return res.enc.Encode(val)
	}

	return res.stream.Encode(val)
}

func (res *responseWriter) Flush() error {
	if res.conn == nil {
		return ErrHijacked
	}

	if res.wtype == notype {
		if err := res.Write("OK"); err != nil {
			return err
		}
	}

	if res.remain != 0 {
		return ErrWriteCalledNotEnoughTimes
	}

	return res.conn.Flush()
}

func (res *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if res.conn == nil {
		return nil, nil, ErrHijacked
	}
	nc := res.conn.conn
	rw := &bufio.ReadWriter{
		Reader: &res.conn.rbuffer,
		Writer: &res.conn.wbuffer,
	}
	res.conn = nil
	return nc, rw, nil
}

func (res *responseWriter) waitReadyWrite() {
	// TODO: figure out here how to wait for the previous response to flush to
	// support pipelining.
	res.conn.setWriteTimeout(res.timeout)
}

type preparedResponseWriter struct {
	base      ResponseWriter
	index     int
	responses []preparedResponse
}

type preparedResponse struct {
	index int
	value interface{}
}

func (res *preparedResponseWriter) WriteStream(n int) error {
	return ErrWriteStreamCalledTooManyTimes
}

func (res *preparedResponseWriter) Write(v interface{}) error {
	if len(res.responses) != 0 && res.responses[0].index == res.index {
		if err := res.base.Write(res.responses[0].value); err != nil {
			return err
		}
		res.responses = res.responses[1:]
	}

	res.index++
	return res.base.Write(v)
}

func (res *preparedResponseWriter) Flush() (err error) {
	if w, ok := res.base.(Flusher); ok {
		err = w.Flush()
	}
	return
}

func (res *preparedResponseWriter) Hijack() (c net.Conn, rw *bufio.ReadWriter, err error) {
	if w, ok := res.base.(Hijacker); ok {
		c, rw, err = w.Hijack()
	} else {
		err = ErrNotHijackable
	}
	return
}

func (res *preparedResponseWriter) writeRemainingValues() (err error) {
	for _, r := range res.responses {
		if err = res.base.Write(r.value); err != nil {
			break
		}
	}
	res.responses = nil
	return
}

var (
	// ErrServerClosed is returned by Server.Serve when the server is closed.
	ErrNilArgs                       = errors.New("cannot parse values from a nil argument list")
	ErrServerClosed                  = errors.New("redis: Server closed")
	ErrNegativeStreamCount           = errors.New("invalid call to redis.ResponseWriter.WriteStream with a negative value")
	ErrWriteStreamCalledAfterWrite   = errors.New("invalid call to redis.ResponseWriter.WriteStream after redis.ResponseWriter.Write was called")
	ErrWriteStreamCalledTooManyTimes = errors.New("multiple calls to ResponseWriter.WriteStream")
	ErrWriteCalledTooManyTimes       = errors.New("too many calls to redis.ResponseWriter.Write")
	ErrWriteCalledNotEnoughTimes     = errors.New("not enough calls to redis.ResponseWriter.Write")
	ErrHijacked                      = errors.New("invalid use of a hijacked redis.ResponseWriter")
	ErrNotHijackable                 = errors.New("the response writer is not hijackable")
)
