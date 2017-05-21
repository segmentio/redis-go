package redis

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/resp"
)

// SubConn represents a redis connection that has been switched to PUB/SUB mode.
//
// Instances of SubConn are safe for concurrent use by multiple goroutines.
type SubConn struct {
	conn net.Conn

	rmtx sync.Mutex
	rbuf bufio.Reader
	dec  objconv.Decoder

	wmtx sync.Mutex
	wbuf bufio.Writer
	enc  objconv.Encoder
}

// NewSubConn creates a new SubConn from a pre-existing network connection.
func NewSubConn(conn net.Conn) *SubConn {
	sub := &SubConn{conn: conn}
	sub.rbuf.Reset(conn)
	sub.wbuf.Reset(conn)
	sub.dec = *resp.NewDecoder(&sub.rbuf)
	sub.enc = *resp.NewEncoder(&sub.wbuf)
	return sub
}

// WriteCommand writes a PUB/SUB command to the connection. The command must be
// one of "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", or "PUNSUBSCRIBE".
func (sub *SubConn) WriteCommand(command string, channels ...string) (err error) {
	switch command {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
	default:
		err = fmt.Errorf("redis: %q is not a PUB/SUB command", command)
		return
	}

	args := make([][]byte, 0, 1+len(channels))
	args = append(args, []byte(command))

	for _, channel := range channels {
		args = append(args, []byte(channel))
	}

	defer sub.wmtx.Unlock()
	sub.wmtx.Lock()

	if err = sub.enc.Encode(args); err != nil {
		// We can't tell anymore if the connection is in a recoverable state,
		// we're better off closing it at this point.
		sub.conn.Close()
		return
	}

	if err = sub.wbuf.Flush(); err != nil {
		sub.conn.Close()
		return
	}

	// TODO: should WriteCommand block waiting for the ack on channels and
	// patterns that are being subscribed or unsubscribed?
	return
}

// ReadMessage reads the stream of PUB/SUB messages from the connection and
// returns the channel and payload of the first message it received.
//
// The program is expected to call ReadMessage in a loop to consume messages
// from the PUB/SUB channels that the connection was subscribed to.
func (sub *SubConn) ReadMessage() (channel string, message []byte, err error) {
	defer sub.rmtx.Unlock()
	sub.rmtx.Lock()

	for {
		var args []interface{}

		if err = sub.dec.Decode(&args); err != nil {
			sub.conn.Close()
			return
		}

		if len(args) != 3 {
			continue
		}

		arg0, _ := args[0].([]byte)
		arg1, _ := args[1].([]byte)
		arg2, _ := args[2].([]byte)

		if arg0 == nil || arg1 == nil || arg2 == nil {
			continue
		}

		if string(arg0) == "message" {
			channel, message = string(arg1), arg2
			return
		}
	}
}

// Close closes the connection, writing commands or reading messages from the
// connection after Close was called will return errors.
func (sub *SubConn) Close() error {
	return sub.conn.Close()
}

// SetReadDeadline sets the deadline for future ReadFrom calls and any
// currently-blocked ReadFrom call. A zero value for t means ReadFrom will
// not time out.
func (sub *SubConn) SetReadDeadline(t time.Time) error {
	return sub.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future WriteTo calls and any
// currently-blocked WriteTo call. Even if write times out, it may return n > 0,
// indicating that some of the data was successfully written.
// A zero value for t means WriteTo will not time out.
func (sub *SubConn) SetWriteDeadline(t time.Time) error {
	return sub.conn.SetWriteDeadline(t)
}

// SetDeadline sets the read and write deadlines associated with the connection.
// It is equivalent to calling both SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations fail with a timeout
// (see type Error) instead of blocking. The deadline applies to all future and
// pending I/O, not just the immediately following call to ReadMessage or
// WriteCommand. After a deadline has been exceeded, the connection can be
// refreshed by setting a deadline in the future.
//
// An idle timeout can be implemented by repeatedly extending the deadline after
// successful ReadFrom or WriteTo calls.
//
// A zero value for t means I/O operations will not time out.
func (sub *SubConn) SetDeadline(t time.Time) error {
	return sub.conn.SetDeadline(t)
}

// LocalAddr returns the local network address.
func (sub *SubConn) LocalAddr() net.Addr {
	return sub.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (sub *SubConn) RemoteAddr() net.Addr {
	return sub.conn.RemoteAddr()
}
