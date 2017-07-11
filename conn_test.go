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

	req1 := &Request{
		Cmds: []Command{{Cmd: "SET", Args: List("redis-go.test.conn", "0123456789")}},
	}
	tx1 := makeConnTx(ctx, req1)

	req2 := &Request{
		Cmds: []Command{{Cmd: "GET", Args: List("redis-go.test.conn")}},
	}
	tx2 := makeConnTx(ctx, req2)

	c <- tx1
	c <- tx2

	res1, err := tx1.recv()

	if err != nil {
		t.Error("receiving first response:", err)
		return
	}

	for _, args := range res1.Args {
		for args.Next(&v) {
			if v != "OK" {
				t.Error("bad value in the first response:", v)
				return
			}
		}

		if err := args.Close(); err != nil {
			t.Error("reading first response:", err)
			return
		}
	}

	res2, err := tx2.recv()

	if err != nil {
		t.Error("second response:", err)
		return
	}

	for _, args := range res2.Args {
		for args.Next(&v) {
			if !reflect.DeepEqual(v, []byte("0123456789")) {
				t.Error("bad value in the second response:", v)
				return
			}
		}

		if err := args.Close(); err != nil {
			t.Error("reading second response:", err)
			return
		}
	}
}

func TestConnMulti(t *testing.T) {
	var v string
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

	set := &Request{
		Cmds: []Command{
			{Cmd: "MULTI"},
			{Cmd: "SET", Args: List("foo", "bar")},
			{Cmd: "SET", Args: List("bob", "alice")},
			{Cmd: "EXEC"},
		},
	}
	tx := makeConnTx(ctx, set)

	req1 := &Request{
		Cmds: []Command{
			{Cmd: "MULTI"},
			{Cmd: "GET", Args: List("foo")},
			{Cmd: "GET", Args: List("bob")},
			{Cmd: "EXEC"},
		},
	}
	tx1 := makeConnTx(ctx, req1)

	req2 := &Request{
		Cmds: []Command{
			{Cmd: "MULTI"},
			{Cmd: "SET", Args: List("a", "1")},
			{Cmd: "EXEC"},
		},
	}
	tx2 := makeConnTx(ctx, req2)

	c <- tx
	c <- tx1
	c <- tx2

	res, err := tx.recv()
	if err != nil {
		t.Error("multi set", err)
		return
	}
	res.Close()

	res1, err := tx1.recv()
	if err != nil {
		t.Error("multi get", err)
		return
	}

	result := []string{"OK", "QUEUED", "QUEUED", "bar", "alice"}

	var i int
	for _, arg := range res1.Args {
		for arg.Next(&v) {
			if !reflect.DeepEqual(v, result[i]) {
				t.Errorf("bad value in response: %s instead of %s", v, result[i])
				return
			}
			i++
		}
		if err := arg.Close(); err != nil {
			t.Error("reading response:", err)
			return
		}
	}

	res2, err := tx2.recv()
	if err != nil {
		t.Error("multi set", err)
		return
	}
	res2.Close()
}
