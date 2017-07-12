package redis

import (
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
