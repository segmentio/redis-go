package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

var (
	// ErrDiscard is the error returned to indicate that transactions are
	// discarded.
	ErrDiscard = resp.NewError("EXECABORT Transcation discarded.")
)

// Conn is a low-level API to represent client connections to redis.
type Conn struct {
	conn net.Conn

	rmutex  sync.Mutex
	rbuffer bufio.Reader
	decoder objconv.StreamDecoder
	parser  resp.Parser

	wmutex  sync.Mutex
	wbuffer bufio.Writer
	encoder objconv.StreamEncoder
	emitter resp.ClientEmitter

	cancelOnce sync.Once
	cancelFunc context.CancelFunc
}

// Dial connects to the redis server at the given address, returing a new client
// redis connection.
func Dial(network string, address string) (*Conn, error) {
	return DialContext(context.Background(), network, address)
}

// Dial connects to the redis server at the given address, returing a new client
// redis connection. Connecting may be asynchronously cancelled by the context
// passed as first argument.
func DialContext(ctx context.Context, network string, address string) (*Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewClientConn(c), nil
}

// NewClientConn creates a new redis connection from an already open client
// connections.
func NewClientConn(conn net.Conn) *Conn {
	c := &Conn{
		conn:       conn,
		rbuffer:    *bufio.NewReader(conn),
		wbuffer:    *bufio.NewWriter(conn),
		cancelFunc: func() {},
	}
	c.parser.Reset(&c.rbuffer)
	c.emitter.Reset(&c.wbuffer)
	c.decoder = objconv.StreamDecoder{Parser: &c.parser}
	c.encoder = objconv.StreamEncoder{Emitter: &c.emitter}
	return c
}

// NewServerConn creates a new redis connection from an already open server
// connections.
func NewServerConn(conn net.Conn) *Conn {
	c := &Conn{
		conn:       conn,
		rbuffer:    *bufio.NewReader(conn),
		wbuffer:    *bufio.NewWriter(conn),
		cancelFunc: func() {},
	}
	c.parser.Reset(&c.rbuffer)
	c.emitter.Reset(&c.wbuffer)
	c.decoder = objconv.StreamDecoder{Parser: &c.parser}
	c.encoder = objconv.StreamEncoder{Emitter: &c.emitter.Emitter}
	return c
}

// Close closes the kafka connection.
func (c *Conn) Close() error {
	c.cancelOnce.Do(c.cancelFunc)
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated with the connection.
// It is equivalent to calling both SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations fail with a timeout
// instead of blocking. The deadline applies to all future and pending I/O, not
// just the immediately following call to Read or Write. After a deadline has
// been exceeded, the connection may be closed if it was found to be in an
// unrecoverable state.
//
// A zero value for t means I/O operations will not time out.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls and any
// currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls and any
// currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that some of the
// data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Read reads upt to len(b) bytes from c, returning the number of bytes read,
// and an error if something went wrong while reading from the connection.
//
// The function is exposed for the sole purpose of satisfying the net.Conn
// interface, it doesn't check that the data read are leaving the connection in
// a valid state in reagrd to the semantics of the redis protocol.
func (c *Conn) Read(b []byte) (int, error) {
	c.rmutex.Lock()
	n, err := c.rbuffer.Read(b)
	c.rmutex.Unlock()
	return n, err
}

// Write writes b ot c, returning the number obytes written, and an error if
// something went wrong while writing to the connection.
//
// The function is exposed for the sole purpose of satsifying the net.Conn
// interface, it doesn't check that the data written to the connection are
// meaningful messages in the redis protocol.
func (c *Conn) Write(b []byte) (int, error) {
	c.wmutex.Lock()
	n, err := c.wbuffer.Write(b)

	if err != nil {
		c.conn.Close()
	} else {
		err = c.wbuffer.Flush()
	}

	c.wmutex.Unlock()
	return n, err
}

/*
// ReadCommands returns a new CommandReader which reads the next set of commands
// from c.
//
// The new CommandReader holds the connection's read lock, which is released
// only when its Close method is called, so a program must make sure to call
// that method or the connection will be left in an unusable state.
func (c *Conn) ReadCommands() (*CommandReader, error) {
	c.rmutex.Lock()
	c.resetDecoder()

	cmd := ""
	dec := c.decoder

	if err := dec.Decode(&cmd); err != nil {
		return nil, err
	}

	r := &CommandReader{
		cmd:   Command{Cmd: cmd},
		conn:  c,
		multi: cmd == "MULTI",
	}

	r.cmd.Args = newCmdArgsReader(dec, r)
	return r, nil
}
*/

// ReadArgs opens a stream to read arguments from the redis connection.
//
// The method never returns a nil Args value, the program has to call the Args'
// Close method before calling any of the connection's read methods.
//
// If an error occurs while reading the list of arguments it will be returned by
// the call to the Args' Close method.
func (c *Conn) ReadArgs() Args {
	c.rmutex.Lock()
	c.resetDecoder()
	return &connArgs{
		conn:    c,
		decoder: c.decoder,
	}
}

// ReadTxArgs opens a stream to read the arguments in response to opening a
// transaction of size n (not including the opening MULTI and closing EXEC or
// DISCARD commands).
//
// If an error occurs while reading the transaction queuing responses it will be
// returned by the TxArgs' Close method, the ReadTxArgs method never returns a
// nil object, even if the connetion was closed.
func (c *Conn) ReadTxArgs(n int) *TxArgs {
	c.rmutex.Lock()
	c.resetDecoder()

	tx := &TxArgs{
		conn: c,
		args: make([]Args, n),
	}

	cnt := 0
	err := c.readMultiArgs(tx)

	if err == nil {
		for i := 0; i != n && err == nil; i++ {
			cnt, err = c.readTxArgs(tx, i, cnt)
		}
	}

	if err == nil {
		err = c.readTxExecArgs(tx, cnt)
	}

	if err != nil {
		c.conn.Close()
		c.rmutex.Unlock()

		tx.conn = nil
		tx.args = nil
		tx.err = err
	}

	return tx
}

func (c *Conn) readMultiArgs(tx *TxArgs) (err error) {
	status, error, err := c.readTxStatus()

	// The redis protocol says that MULTI only returns OK, but here we've
	// got a protocol error, it's safer to just close the connection in those
	// case.
	switch {
	case err != nil:

	case error != nil:
		err = fmt.Errorf("opening a transaction to the redis server failed: %s", error)

	case status != "OK":
		err = fmt.Errorf("opening a transaction to the redis server failed: %s", status)
	}

	return
}

func (c *Conn) readTxExecArgs(tx *TxArgs, n int) error {
	var decoder = objconv.StreamDecoder{Parser: c.decoder.Parser}
	var error *resp.Error
	var status string

	t, err := decoder.Parser.ParseType()
	if err != nil {
		return err
	}

	switch t {
	case objconv.Array:
		if l := decoder.Len(); l != n {
			return fmt.Errorf("%d received in a redis transaction response but the client expected %d", l, n)
		}

	case objconv.Error:
		if err := decoder.Decode(&error); err != nil {
			return err
		}
		if error.Type() == "EXECABORT" {
			error = ErrDiscard
		}

	case objconv.String:
		if err := decoder.Decode(&status); err != nil {
			return err
		}
		if status != "OK" { // OK is returned when a transcation is discarded
			return fmt.Errorf("unsupported transaction status received: %s", status)
		}
		error = ErrDiscard

	default:
		return fmt.Errorf("unsupported value of type %s returned while reading the status of a redis transaction", t)
	}

	if error != nil {
		for i := range tx.args {
			a := tx.args[i].(*connArgs)
			a.conn = nil
			if a.respErr == nil {
				a.respErr = error
			}
		}
		tx.err = error
	}

	return nil
}

func (c *Conn) readTxArgs(tx *TxArgs, i int, n int) (int, error) {
	status, error, err := c.readTxStatus()

	switch {
	case err != nil:

	case error != nil:
		tx.args[i] = &connArgs{tx: tx, respErr: error}

	case status == "QUEUED":
		tx.args[i] = &connArgs{conn: c, tx: tx, decoder: c.decoder}
		n++

	default:
		err = fmt.Errorf("unsupported status received in response to queuing a command to a redis transaction: %s", status)
	}

	return n, err
}

func (c *Conn) readTxStatus() (status string, error *resp.Error, err error) {
	var val interface{}
	var dec = objconv.Decoder{Parser: c.decoder.Parser}

	if err = dec.Decode(&val); err != nil {
		return
	}

	switch v := val.(type) {
	case string:
		status = v

	case *resp.Error:
		error = v

	default:
		err = fmt.Errorf("unsupported value of type %T returned while reading responses of a redis transaction", v)
	}

	return
}

// WriteArgs writes a sequence of arguments to the redis connection, returning
// nil on success or an error describing what went wrong.
//
// On error, the connection is closed because it's not possible to determine if
// it was left it a recoverable state.
func (c *Conn) WriteArgs(args Args) error {
	c.wmutex.Lock()
	c.resetEncoder()
	err := c.writeArgs(args)

	if err == nil {
		err = c.wbuffer.Flush()
	}

	if err != nil {
		c.conn.Close()
	}

	c.wmutex.Unlock()
	return err
}

// WriteCommands writes a set of commands to c.
//
// This is a low-level API intended to be called to write a set of client
// commands to a redis server, no check is done on the validity of the commands.
//
// Calling WriteCommands with no arguments has no effect and won't return an
// error even if the connection has already been closed.
//
// The argument list of each command is always fully consumed and closed, even
// if the method returns an error.
//
// It is invalid to call this method on server connections in the redis protocol
// however it is not enforced at the connection level, applications are expected
// to respect the protocol semantics.
func (c *Conn) WriteCommands(cmds ...Command) error {
	var err error
	c.wmutex.Lock()

	for i := range cmds {
		c.resetEncoder()
		if err = c.writeCommand(&cmds[i]); err != nil {
			break
		}
	}

	if err == nil {
		err = c.wbuffer.Flush()
	}

	if err != nil {
		c.conn.Close()

		for _, cmd := range cmds {
			if cmd.Args != nil {
				cmd.Args.Close()
			}
		}
	}

	c.wmutex.Unlock()
	return err
}

func (c *Conn) writeCommand(cmd *Command) (err error) {
	var n int

	if cmd.Args != nil {
		n = cmd.Args.Len()
	}

	if err = c.encoder.Open(n + 1); err != nil {
		return
	}

	if err = c.encoder.Encode(&cmd.Cmd); err != nil {
		return
	}

	if cmd.Args != nil {
		if err = c.encodeArgs(cmd.Args); err != nil {
			return
		}
	}

	err = c.encoder.Close()
	return
}

func (c *Conn) writeArgs(args Args) (err error) {
	if err = c.encoder.Open(args.Len()); err != nil {
		return
	}

	if err = c.encodeArgs(args); err != nil {
		return
	}

	err = c.encoder.Close()
	return
}

func (c *Conn) encodeArgs(args Args) (err error) {
	var val interface{}

	for args.Next(&val) {
		if err = c.encoder.Encode(val); err != nil {
			return
		}
		val = nil
	}

	err = args.Close()
	return
}

func (c *Conn) resetEncoder() {
	c.encoder = objconv.StreamEncoder{Emitter: c.encoder.Emitter}
}

func (c *Conn) resetDecoder() {
	c.decoder = objconv.StreamDecoder{Parser: c.decoder.Parser}
}

type connArgs struct {
	mutex   sync.Mutex
	decoder objconv.StreamDecoder
	conn    *Conn
	tx      *TxArgs
	respErr *resp.Error
}

func (args *connArgs) Close() error {
	var err error
	args.mutex.Lock()

	if args.conn != nil {
		for args.next(nil) == nil {
			// discard all remaining arguments in an attempt to maintain the
			// connection in a stable state
		}
		if err == nil {
			err = args.decoder.Err()
		}
	}

	if err == nil && args.respErr != nil {
		err = args.respErr
	}

	if args.conn != nil {
		if _, stable := err.(*resp.Error); err != nil && !stable {
			args.conn.Close()
		}
		if args.tx == nil { // no transcation, owner of the connection read lock
			args.conn.rmutex.Unlock()
		}
		args.conn = nil
	}

	if args.tx != nil {
		args.tx.mutex.Unlock()
		args.tx = nil
	}

	args.mutex.Unlock()
	return err
}

func (args *connArgs) Len() (n int) {
	args.mutex.Lock()
	if args.conn != nil {
		n = args.decoder.Len()
	}
	args.mutex.Unlock()
	return
}

func (args *connArgs) Next(dst interface{}) bool {
	var err error
	args.mutex.Lock()

	if args.respErr != nil {
		err = args.respErr
	} else {
		err = args.next(dst)
	}

	args.mutex.Unlock()
	return err == nil
}

func (args *connArgs) next(dst interface{}) (err error) {
	var typ objconv.Type

	if args.decoder.Len() == 0 {
		err = objconv.End
		return
	}

	if typ, err = args.decoder.Parser.ParseType(); err == nil {
		if typ != objconv.Error {
			err = args.decoder.Decode(dst)
		} else {
			args.decoder.Decode(&args.respErr)
			err = args.respErr
		}
	}

	return
}
