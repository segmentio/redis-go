package redis

import (
	"log"
	"net"
	"os"
	"testing"
	"time"
)

func TestServerDefaultValue(t *testing.T) {
	_, lstn := serve(t, HandlerFunc(func(res ResponseWriter, req *Request) {
		if req.Cmd != SET {
			t.Error("invalid command received by the server:", req.Cmd)
		}
		if s, err := ReadString(req.Args); s != "hello" {
			t.Error("invalid first argument received by the server:", s, err)
		}
		if s, err := ReadString(req.Args); s != "world" {
			t.Error("invalid second argument received by the server:", s, err)
		}
	}))
	defer lstn.Close()

	conn := dial(t, lstn.Addr())
	defer conn.Close()

	if err := conn.WriteRequest(NewRequest(SET, "hello", "world")); err != nil {
		t.Error(err)
		return
	}

	res, err := conn.ReadResponse(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer res.Args.Close()

	if s, err := ReadString(res.Args); s != "OK" {
		t.Error("invalid default server response:", s, err)
	}
}

func TestServerOneValue(t *testing.T) {
	_, lstn := serve(t, HandlerFunc(func(res ResponseWriter, req *Request) {
		if req.Cmd != GET {
			t.Error("invalid command received by the server:", req.Cmd)
		}
		if s, err := ReadString(req.Args); s != "hello" {
			t.Error("invalid argument received by the server:", s, err)
		}
		res.Write("world")
	}))
	defer lstn.Close()

	conn := dial(t, lstn.Addr())
	defer conn.Close()

	if err := conn.WriteRequest(NewRequest(GET, "hello")); err != nil {
		t.Error(err)
		return
	}

	res, err := conn.ReadResponse(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer res.Args.Close()

	if s, err := ReadString(res.Args); s != "world" {
		t.Error("invalid server response:", s, err)
	}
}

func TestServerStreamValue(t *testing.T) {
	_, lstn := serve(t, HandlerFunc(func(res ResponseWriter, req *Request) {
		res.WriteLen(4)
		res.Write(0)
		res.Write(1)
		res.Write(2)
		res.Write(3)
	}))
	defer lstn.Close()

	conn := dial(t, lstn.Addr())
	defer conn.Close()

	if err := conn.WriteRequest(NewRequest(GET)); err != nil {
		t.Error(err)
		return
	}

	res, err := conn.ReadResponse(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer res.Args.Close()

	n := res.Args.Len()
	if n != 4 {
		t.Errorf("invalid arg count: %d", n)
		return
	}

	for i := 0; i != n; i++ {
		if v, err := ReadInt(res.Args); v != i {
			t.Errorf("invalid value returned at index %d: %d %s", i, v, err)
		}
	}
}

func serve(t *testing.T, h Handler) (srv *Server, lstn net.Listener) {
	var err error

	if lstn, err = net.Listen("tcp", ":0"); err != nil {
		t.Error(err)
		t.FailNow()
	}

	srv = &Server{
		Handler:      h,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		ErrorLog:     log.New(os.Stderr, "server: ", 0),
	}
	go srv.Serve(lstn)
	return
}

func dial(t *testing.T, addr net.Addr) (conn Conn) {
	var err error

	if conn, err = Dial(addr.Network(), addr.String()); err != nil {
		t.Error(err)
		t.FailNow()
	}

	return
}
