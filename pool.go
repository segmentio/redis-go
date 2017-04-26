package redis

import (
	"context"
	"net"
	"strings"
	"time"
)

type poolConfig struct {
	addr         string
	size         int
	dialContext  func(context.Context, string, string) (net.Conn, error)
	pingTimeout  time.Duration
	pingInterval time.Duration
}

func makePool(config poolConfig) conn {
	txch := make(chan connTx)

	for i := 0; i != config.size; i++ {
		go dialLoop(txch, config)
	}

	return txch
}

func dialLoop(txch <-chan connTx, config poolConfig) {
	var conn conn
	var closed <-chan struct{}
	var recvTx <-chan connTx
	var optime time.Time

	ticker := time.NewTicker(config.pingInterval / 2)
	defer ticker.Stop()

	connect := func() (err error) {
		if conn == nil {
			if conn, closed, err = dial(config); err == nil {
				recvTx = txch
			}
		}
		return
	}

	disconnect := func() {
		if conn != nil {
			conn.close()
			conn, closed, recvTx = nil, nil, nil
		}
	}

	connect()

	for {
		select {
		case tx, ok := <-recvTx:
			if !ok {
				disconnect()
				return
			}

			if err := conn.send(tx); err != nil {
				tx.error(err)
				disconnect()
				continue
			}

			optime = time.Now()

		case now := <-ticker.C:
			connect()

			if conn != nil && now.Sub(optime) >= config.pingInterval {
				ctx, cancel := contextWithTimeout(context.Background(), config.pingTimeout)
				tx := makeConnTx(ctx, &Request{Cmd: "PING"})
				conn.send(tx)

				go func(tx connTx) {
					defer cancel()
					if r, err := tx.recv(); err == nil {
						r.Args.Close()
					}
				}(tx)
			}

		case <-closed:
			disconnect()
		}
	}
}

func dial(config poolConfig) (conn, <-chan struct{}, error) {
	network, address := splitNetworkAddress(config.addr)
	if len(network) == 0 {
		network = "tcp"
	}

	c, err := config.dialContext(context.Background(), network, address)
	if err != nil {
		return nil, nil, err
	}

	done := make(chan struct{})
	return makeConn(c, done), done, nil
}

func splitNetworkAddress(s string) (string, string) {
	if i := strings.Index(s, "://"); i >= 0 {
		return s[:i], s[i+3:]
	}
	return "", s
}
