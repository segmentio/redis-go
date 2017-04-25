package redis

import (
	"context"
	"net"
	"sync"
	"time"
)

type RoundTripper interface {
	RoundTrip(*Request) (*Response, error)
}

type Transport struct {
	DialContext func(context.Context, string, string) (net.Conn, error)

	ConnsPerHost int

	PingInterval time.Duration

	PingTimeout time.Duration

	mutex sync.RWMutex
	conns map[string]conn
}

func (t *Transport) CloseIdleConnections() {
	var conns map[string]conn

	t.mutex.Lock()
	conns, t.conns = t.conns, nil
	t.mutex.Unlock()

	for _, c := range conns {
		c.close()
	}
}

func (t *Transport) RoundTrip(req *Request) (*Response, error) {
	tx := makeConnTx(req.Context(), req)

	if err := t.conn(req.Addr).send(tx); err != nil {
		return nil, err
	}

	return tx.recv()
}

func (t *Transport) conn(addr string) conn {
	t.mutex.RLock()
	c := t.conns[addr]
	t.mutex.RUnlock()

	if c == nil {
		t.mutex.Lock()

		if c = t.conns[addr]; c == nil {
			if t.conns == nil {
				t.conns = make(map[string]conn)
			}
			c = makePool(t.poolConfig(addr))
			t.conns[addr] = c
		}

		t.mutex.Unlock()
	}

	return c
}

func (t *Transport) poolConfig(addr string) poolConfig {
	config := poolConfig{
		addr:         addr,
		size:         t.ConnsPerHost,
		dialContext:  t.DialContext,
		pingTimeout:  t.PingTimeout,
		pingInterval: t.PingInterval,
	}

	if config.size == 0 {
		config.size = 1
	}

	if config.dialContext == nil {
		config.dialContext = (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext
	}

	if config.pingTimeout == 0 {
		config.pingTimeout = 10 * time.Second
	}

	if config.pingInterval == 0 {
		config.pingInterval = 30 * time.Second
	}

	return config
}

var DefaultTransport RoundTripper = &Transport{
	DialContext: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	ConnsPerHost: 10,
	PingTimeout:  10 * time.Second,
	PingInterval: 15 * time.Second,
}
