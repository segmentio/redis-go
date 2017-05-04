package redis

import (
	"bufio"
	"context"
	"io"
	"net"
	"time"

	"github.com/segmentio/objconv/resp"
)

type conn chan<- connTx

func makeConn(nc net.Conn, done chan<- struct{}) conn {
	txch1 := make(chan connTx)
	txch2 := make(chan connTx, 16)

	go writeLoop(nc, txch1, txch2)
	go readLoop(nc, txch2, done)

	return txch1
}

func (c conn) close() {
	close(c)
}

func (c conn) send(tx connTx) error {
	return sendTx(c, tx)
}

func writeLoop(conn net.Conn, txch1 <-chan connTx, txch2 chan<- connTx) {
	w := bufio.NewWriter(conn)

txLoop:
	for tx := range txch1 {
		conn.SetWriteDeadline(tx.deadline())

		for _, req := range tx.reqs {
			if err := req.Write(w); err != nil {
				tx.error(err)
				break txLoop
			}
		}

		if err := w.Flush(); err != nil {
			tx.error(err)
			break txLoop
		}

		if err := sendTx(txch2, tx); err != nil {
			tx.error(err)
			break txLoop
		}
	}

	close(txch2)

	for tx := range txch1 {
		tx.error(io.ErrClosedPipe)
	}
}

func readLoop(conn net.Conn, txch <-chan connTx, done chan<- struct{}) {
	r := bufio.NewReader(conn)
	p := resp.NewParser(r)

txLoop:
	for tx := range txch {
		conn.SetReadDeadline(tx.deadline())

		for _, req := range tx.reqs {
			done := make(chan error, 1)
			tx.send(newResponse(p, req, done))

			select {
			case err := <-done:
				if err != nil {
					tx.error(err)
					break txLoop
				}

			case <-tx.ctx.Done():
				tx.error(tx.ctx.Err())
				break txLoop
			}
		}

		tx.close()
	}

	conn.Close()

	if done != nil {
		close(done)
	}

	for tx := range txch {
		tx.error(io.ErrClosedPipe)
	}
}

func sendTx(ch chan<- connTx, tx connTx) error {
	select {
	case ch <- tx:
		return nil
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	}
}

type connTx struct {
	resch chan *Response
	errch chan error
	reqs  []*Request
	ctx   context.Context
}

func makeConnTx(ctx context.Context, reqs ...*Request) connTx {
	return connTx{
		resch: make(chan *Response, len(reqs)),
		errch: make(chan error, 1),
		reqs:  reqs,
		ctx:   ctx,
	}
}

func (tx *connTx) close() {
	for _, req := range tx.reqs {
		if req.Args != nil {
			req.Args.Close()
		}
	}
	close(tx.resch)
	close(tx.errch)
}

func (tx *connTx) deadline() time.Time {
	t, _ := tx.ctx.Deadline()
	return t
}

func (tx *connTx) send(res *Response) {
	select {
	case tx.resch <- res:
	case <-tx.ctx.Done():
	}
}

func (tx *connTx) error(err error) {
	select {
	case tx.errch <- err:
	case <-tx.ctx.Done():
	}
	tx.close()
}

func (tx *connTx) recv() (res *Response, err error) {
	var ok bool
	select {
	case res, ok = <-tx.resch:
	case err, ok = <-tx.errch:
	}
	if !ok {
		if err, ok = <-tx.errch; !ok {
			err = io.ErrUnexpectedEOF
		}
	}
	return
}

func contextWithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
