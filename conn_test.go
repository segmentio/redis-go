package redis

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestConn(t *testing.T) {
	var v interface{}
	var nc net.Conn
	var err error

	if nc, err = net.Dial("tcp", "localhost:6379"); err != nil {
		t.Error(err)
		return
	}

	c := makeConn(nc, nil)
	defer c.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tx1 := makeConnTx(ctx, &Request{Cmd: "SET", Args: List("redis-go.test.conn", "0123456789")})
	tx2 := makeConnTx(ctx, &Request{Cmd: "GET", Args: List("redis-go.test.conn")})

	c <- tx1
	c <- tx2

	res1, err := tx1.recv()

	if err != nil {
		t.Error("receiving first response:", err)
		return
	}

	for res1.Args.Next(&v) {
		if v != "OK" {
			t.Error("bad value in the first response:", v)
			return
		}
	}

	if err := res1.Args.Close(); err != nil {
		t.Error("reading first response:", err)
		return
	}

	res2, err := tx2.recv()

	if err != nil {
		t.Error("second response:", err)
		return
	}

	for res2.Args.Next(&v) {
		if !reflect.DeepEqual(v, []byte("0123456789")) {
			t.Error("bad value in the second response:", v)
			return
		}
	}

	if err := res2.Args.Close(); err != nil {
		t.Error("reading second response:", err)
		return
	}
}
