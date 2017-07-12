package redis

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"
)

type connPool struct {
	// immutable configuration
	maxIdleConns        int
	maxIdleConnsPerHost int

	// mutable state of the connection pool
	mutex sync.Mutex
	calls int
	idles int
	conns map[string]*connList
}

func (p *connPool) getConn(host string) *Conn {
	var list *connList
	var conn *Conn

	p.mutex.Lock()

	if list = p.conns[host]; list != nil {
		conn = list.pop()
	}

	if p.calls++; p.calls == 1000 {
		p.calls = 0
		// Every 1000 calls to getConn we cleanup empty entries in the host
		// map to avoid leaking memory.
		for host, list := range p.conns {
			if list.len() == 0 {
				delete(p.conns, host)
			}
		}
	}

	if conn != nil {
		p.idles--
	}

	p.mutex.Unlock()

	if conn != nil {
		conn.SetDeadline(time.Time{}) // don't leak deadlines
	}

	return conn
}

func (p *connPool) putConn(host string, conn *Conn) {
	if conn == nil {
		return
	}

	var list *connList
	p.mutex.Lock()

	if p.maxIdleConns == 0 || p.idles < p.maxIdleConns {
		if p.conns == nil {
			p.conns = make(map[string]*connList)
		}

		if list = p.conns[host]; list == nil {
			list = new(connList)
			p.conns[host] = list
		}
	}

	if list != nil && (p.maxIdleConnsPerHost == 0 || list.len() < p.maxIdleConnsPerHost) {
		list.push(conn)
		p.idles++
		conn = nil
	}

	p.mutex.Unlock()

	if conn != nil {
		conn.Close()
	}
}

func (p *connPool) closeIdleConnections() {
	p.mutex.Lock()

	for _, conns := range p.conns {
		for conn := conns.pop(); conn != nil; conn = conns.pop() {
			conn.Close()
		}
	}

	p.mutex.Unlock()
}

func (p *connPool) pingIdleConnections(timeout time.Duration) {
	for _, host := range p.hosts() {
		if conn := p.getConn(host); conn != nil {
			if ping(conn, timeout) != nil {
				conn.Close()
			} else {
				p.putConn(host, conn)
			}
		}
	}
}

func (p *connPool) hosts() (hosts []string) {
	p.mutex.Lock()
	hosts = make([]string, 0, len(p.conns))

	for host := range p.conns {
		hosts = append(hosts, host)
	}

	p.mutex.Unlock()
	return
}

func ping(conn *Conn, timeout time.Duration) (err error) {
	if err = conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	if err = conn.WriteCommands(Command{Cmd: "PING"}); err != nil {
		return
	}
	err = conn.ReadArgs().Close()
	return
}

type connList struct {
	popList  []*Conn
	pushList []*Conn
}

func (c *connList) len() int {
	return len(c.popList) + len(c.pushList)
}

func (c *connList) pop() (conn *Conn) {
	if len(c.popList) == 0 {
		c.popList, c.pushList = c.pushList, c.popList
		reverse(c.popList)
	}

	if n := len(c.popList); n != 0 {
		n--
		conn = c.popList[n]
		c.popList[n] = nil
		c.popList = c.popList[:n]
	}

	return
}

func (c *connList) push(conn *Conn) {
	c.pushList = append(c.pushList, conn)
}

func reverse(conns []*Conn) {
	for i, j := 0, len(conns)-1; i < j; {
		conns[i], conns[j] = conns[j], conns[i]
		i++
		j--
	}
}

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
