package redis

import (
	"bufio"
	"bytes"
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

type ResponseWriter interface {
	Stream(int) error

	Write(interface{}) error
}

type Flusher interface {
	Flush() error
}

type Handler interface {
	ServeRedis(ResponseWriter, *Request)
}

type HandlerFunc func(ResponseWriter, *Request)

func (f HandlerFunc) ServeRedis(res ResponseWriter, req *Request) {
	f(res, req)
}

type Server struct {
	Addr string

	Handler Handler

	ReadTimeout time.Duration

	WriteTimeout time.Duration

	IdleTimeout time.Duration

	ErrorLog *log.Logger

	mutex       sync.Mutex
	listeners   map[net.Listener]bool
	connections map[*serverConn]bool
	context     context.Context
	shutdown    context.CancelFunc
}

func (s *Server) ListenAndServe() error {
	network, address := splitNetworkAddress(s.Addr)

	if len(network) == 0 {
		network = "tcp"
	}

	l, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	return s.Serve(l)
}

func (s *Server) Close() error {
	s.mutex.Lock()

	if s.shutdown != nil {
		s.shutdown()
	}

	for l := range s.listeners {
		l.Close()
	}

	for c := range s.connections {
		c.Close()
	}

	s.mutex.Unlock()
	return nil
}

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

func (s *Server) Serve(l net.Listener) error {
	const maxBackoffDelay = 1 * time.Second
	const minBackoffDelay = 10 * time.Millisecond

	defer l.Close()
	defer s.untrackListener(l)

	s.trackListener(l)
	attempt := 0

	config := serverConfig{
		idleTimeout:  s.idleTimeout(),
		readTimeout:  s.readTimeout(),
		writeTimeout: s.writeTimeout(),
	}

	for {
		conn, err := l.Accept()

		if err != nil {
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
		c := newServerConn(conn)
		s.trackConnection(c)
		go s.serveConnection(s.context, c, config)
	}
}

func (s *Server) serveConnection(ctx context.Context, c *serverConn, config serverConfig) {
	defer c.Close()
	defer s.untrackConnection(c)

	orderedLocks := quetex{}

	for {
		select {
		default:
		case <-ctx.Done():
			return
		}

		if err := c.waitReadyRead(config.idleTimeout); err != nil {
			return
		}

		c.setReadTimeout(config.readTimeout)

		reqLock := make(chan error, 1)
		resLock := orderedLocks.acquire()

		req, err := readRequest(ctx, c, reqLock)

		if err != nil {
			s.log(err)
			return
		}

		res := &responseWriter{
			conn:    c,
			lock:    resLock,
			timeout: config.writeTimeout,
		}

		go func() {
			if err := s.serveRequest(res, req); err != nil {
				c.Close()
				s.log(err)
			}
			orderedLocks.release()
		}()

		if err := <-reqLock; err != nil {
			s.log(err)
			return
		}
	}
}

func (s *Server) serveRequest(res *responseWriter, req *Request) (err error) {
	args := req.Args

	switch req.Cmd {
	case "PING":
		msg := "PONG"
		args.Next(&msg)
		args.Close()
		res.Write(msg)

	default:
		err = s.serveRedis(res, req)
		args.Close()
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

func (s *Server) idleTimeout() time.Duration {
	return defaultDuration(s.IdleTimeout, 20*time.Second)
}

func (s *Server) readTimeout() time.Duration {
	return defaultDuration(s.ReadTimeout, 10*time.Second)
}

func (s *Server) writeTimeout() time.Duration {
	return defaultDuration(s.WriteTimeout, 10*time.Second)
}

func (s *Server) log(err error) {
	if log := s.ErrorLog; log != nil {
		log.Print(err)
	}
}

func (s *Server) trackListener(l net.Listener) {
	s.mutex.Lock()

	if s.listeners == nil {
		s.listeners = map[net.Listener]bool{}
		s.context, s.shutdown = context.WithCancel(context.Background())
	}

	s.listeners[l] = true
	s.mutex.Unlock()
}

func (s *Server) untrackListener(l net.Listener) {
	s.mutex.Lock()
	delete(s.listeners, l)
	s.mutex.Unlock()
}

func (s *Server) trackConnection(c *serverConn) {
	s.mutex.Lock()

	if s.connections == nil {
		s.connections = map[*serverConn]bool{}
	}

	s.connections[c] = true
	s.mutex.Unlock()
}

func (s *Server) untrackConnection(c *serverConn) {
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

func ListenAndServe(addr string, handler Handler) error {
	return (&Server{Addr: addr, Handler: handler}).ListenAndServe()
}

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

func backoff(attempt int, minDelay time.Duration, maxDelay time.Duration) time.Duration {
	d := time.Duration(attempt*attempt) * minDelay
	if d > maxDelay {
		d = maxDelay
	}
	return d
}

func defaultDuration(d time.Duration, def time.Duration) time.Duration {
	if d == 0 {
		d = def
	}
	return d
}

type serverConfig struct {
	idleTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

type serverConn struct {
	net.Conn
	r bufio.Reader
	w bufio.Writer
	p resp.Parser
}

func newServerConn(conn net.Conn) *serverConn {
	c := &serverConn{
		Conn: conn,
		r:    *bufio.NewReader(conn),
		w:    *bufio.NewWriter(conn),
	}
	c.p = *resp.NewParser(&c.r)
	return c
}

func (c *serverConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

func (c *serverConn) Write(b []byte) (int, error) {
	return c.w.Write(b)
}

func (c *serverConn) Flush() error {
	return c.w.Flush()
}

func (c *serverConn) waitReadyRead(timeout time.Duration) (err error) {
	if br := c.p.Buffered().(*bytes.Reader); br.Len() == 0 {
		c.setReadTimeout(timeout)
		_, err = c.r.Peek(1)
	}
	return
}

func (c *serverConn) setReadTimeout(timeout time.Duration) error {
	return c.SetReadDeadline(time.Now().Add(timeout))
}

func (c *serverConn) setWriteTimeout(timeout time.Duration) error {
	return c.SetWriteDeadline(time.Now().Add(timeout))
}

func readRequest(ctx context.Context, conn *serverConn, done chan<- error) (*Request, error) {
	args := newByteArgsReader(&conn.p, done)

	req := &Request{
		Addr: conn.RemoteAddr().String(),
		Args: args,
		ctx:  ctx,
	}

	if !args.Next(&req.Cmd) {
		return nil, args.Close()
	}

	return req, nil
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
	conn    *serverConn
	lock    <-chan struct{}
	wtype   responseWriterType
	remain  int
	enc     objconv.Encoder
	stream  objconv.StreamEncoder
	timeout time.Duration
}

func (res *responseWriter) Write(val interface{}) error {
	if res.wtype == notype {
		res.waitReadyWrite()
		res.wtype = oneshot
		res.remain = 1
		res.enc = *resp.NewEncoder(res.conn)
	}

	if res.remain == 0 {
		return errWriteCalledTooManyTimes
	}
	res.remain--

	if res.wtype == oneshot {
		return res.enc.Encode(val)
	}

	return res.stream.Encode(val)
}

func (res *responseWriter) Stream(n int) error {
	if n < 0 {
		return errNegativeStreamCount
	}

	switch res.wtype {
	case oneshot:
		return errStreamCalledAfterWrite
	case stream:
		return errStreamCalledTooManyTimes
	}

	res.waitReadyWrite()
	res.wtype = stream
	res.remain = n
	res.stream = *resp.NewStreamEncoder(res.conn)
	return res.stream.Open(n)
}

func (res *responseWriter) Flush() error {
	if res.wtype == notype {
		if err := res.Write("OK"); err != nil {
			return err
		}
	}

	if res.remain != 0 && res.wtype == stream {
		return errWriteCalledNotEnoughTimes
	}

	return res.conn.Flush()
}

func (res *responseWriter) waitReadyWrite() {
	<-res.lock
	res.conn.setWriteTimeout(res.timeout)
}

var (
	errNegativeStreamCount       = errors.New("invalid call to redis.ResponseWriter.Stream with a negative value")
	errStreamCalledAfterWrite    = errors.New("invalid call to redis.ResponseWriter.Stream after redis.ResponseWriter.Write was called")
	errStreamCalledTooManyTimes  = errors.New("multiple calls to ResponseWriter.Stream")
	errWriteCalledTooManyTimes   = errors.New("too many calls to redis.ResponseWriter.Write")
	errWriteCalledNotEnoughTimes = errors.New("not enough calls to redis.ResponseWriter.Write")
)
