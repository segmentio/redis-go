package redis_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/objconv/resp"
	redis "github.com/segmentio/redis-go"
)

func TestConn(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, *redis.Conn)
	}{
		{
			scenario: "cleanly closing a connection doesn't return any error",
			function: testConnCloseHasNoError,
		},
		{
			scenario: "reading after closing a connection returns an error",
			function: testConnReadAfterClose,
		},
		{
			scenario: "reading arguments after closing a connection returns an error",
			function: testConnReadArgsAfterClose,
		},
		{
			scenario: "writing after closing a connection returns an error",
			function: testConnWriteAfterClose,
		},
		{
			scenario: "writing commands after closing a connection returns an error",
			function: testConnWriteCommandsAfterClose,
		},
		{
			scenario: "writing an empty list of commands after closing a connection returns no errors",
			function: testConnEmptyWriteCommandsAfterClose,
		},
		{
			scenario: "writing an empty list of commands returns no errors",
			function: testConnEmptyWriteCommandsBeforeClose,
		},
		{
			scenario: "sending a ping command as a list of arguments expects a pong as response from the server",
			function: testConnWriteArgsPingPong,
		},
		{
			scenario: "sending a ping command as a list of commands expects a pong as response from the server",
			function: testConnWriteCommandsPingPong,
		},
		{
			scenario: "sending sequential SET and GET retrieves the expected value",
			function: testConnSetAndGetSequential,
		},
		{
			scenario: "sending a pipeline of SET and GET retrieves the expected value",
			function: testConnSetAndGetPipeline,
		},
		{
			scenario: "sending sequential SET for multiple keys and MGET retrieves the expected value",
			function: testConnMultiSetAndMGetSequential,
		},
		{
			scenario: "sending a pipeline of SET for multiple keys and MGET retrieves the expected value",
			function: testConnMultiSetAndMGetPipeline,
		},
		{
			scenario: "setting keys sequentially and discarding the responses leaves the connection in a stable state",
			function: testConnSetAndDiscardResponsesSequential,
		},
		{
			scenario: "setting keys in a pipeline and discarding the responses leaves the connection in a stable state",
			function: testConnSetAndDiscardResponsesPipeline,
		},
		{
			scenario: "sending an invalid command sequentially keeps the connection in a stable state",
			function: testConnSetInvalidAndSetSequential,
		},
		{
			scenario: "sending an invalid command in a pipeline keeps the connection in a stable state",
			function: testConnSetInvalidAndSetPipeline,
		},
		{
			scenario: "sending an empty transaction returns no results and no errors",
			function: testConnMultiExecEmpty,
		},
		{
			scenario: "sending a transaction with one command returns the result and no errors",
			function: testConnMultiExecSingle,
		},
		{
			scenario: "sending a transaction with many commands returns the results and no errors",
			function: testConnMultiExecMany,
		},
		{
			scenario: "discarding an empty transaction returns an EXECABORT error",
			function: testConnMultiDiscardError,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			deadline := time.Now().Add(4 * time.Second)

			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()

			c, err := redis.DialContext(ctx, "tcp", "localhost:6379")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			c.SetDeadline(deadline)
			testFunc(t, c)
		})
	}
}

func testConnCloseHasNoError(t *testing.T, c *redis.Conn) {
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

func testConnReadAfterClose(t *testing.T, c *redis.Conn) {
	c.Close()

	n, err := c.Read(make([]byte, 1024))

	if n != 0 {
		t.Error("too many bytes were able to be read from a closed connection:", n)
	}

	if err == nil {
		t.Errorf("bad error returned from the closed connection: %v", err)
	}
}

func testConnReadArgsAfterClose(t *testing.T, c *redis.Conn) {
	c.Close()

	var value interface{}
	var args = c.ReadArgs()

	if args.Next(&value) {
		t.Error("too many arguments read from a closed connection:", value)
	}

	if err := args.Close(); err == nil {
		t.Errorf("bad error returned from the closed connection: %v", err)
	}
}

func testConnWriteAfterClose(t *testing.T, c *redis.Conn) {
	c.Close()

	n, err := c.Write(make([]byte, 1<<16))

	if n != 0 {
		t.Error("too many bytes were able to be writen to a closed connection:", n)
	}

	if err == nil {
		t.Errorf("bad error returned from the closed connection: %v", err)
	}
}

func testConnWriteCommandsAfterClose(t *testing.T, c *redis.Conn) {
	c.Close()

	err := c.WriteCommands(
		redis.Command{"SET", redis.List("key-A", "value-1")},
		redis.Command{"SET", redis.List("key-B", "value-2")},
		redis.Command{"SET", redis.List("key-C", "value-3")},
	)

	if err == nil {
		t.Errorf("bad error returned from the closed connection: %v", err)
	}
}

func testConnEmptyWriteCommandsAfterClose(t *testing.T, c *redis.Conn) {
	c.Close()

	if err := c.WriteCommands(); err != nil {
		t.Error(err)
	}
}

func testConnEmptyWriteCommandsBeforeClose(t *testing.T, c *redis.Conn) {
	if err := c.WriteCommands(); err != nil {
		t.Error(err)
	}
}

func testConnWriteArgsPingPong(t *testing.T, c *redis.Conn) {
	if err := c.WriteArgs(redis.List([]byte("PING"))); err != nil {
		t.Fatal(err)
	}

	var args = c.ReadArgs()
	var pong string
	var value interface{}

	if n := args.Len(); n != 1 {
		t.Error("bad number of arguments returned in the response to a PING command:", n)
	}

	if !args.Next(&pong) {
		t.Error("no arguments returned as a response to a PING command")
	}

	if pong != "PONG" {
		t.Error("bad argument returned as a response to a PING command:", pong)
	}

	for args.Next(&value) {
		t.Error("too many arguments returned as a response to a PING command:", value)
	}

	if err := args.Close(); err != nil {
		t.Error("error reading a response to a PING command:", err)
	}
}

func testConnWriteCommandsPingPong(t *testing.T, c *redis.Conn) {
	if err := c.WriteCommands(redis.Command{Cmd: "PING"}); err != nil {
		t.Fatal(err)
	}

	var args = c.ReadArgs()
	var pong string
	var value interface{}

	if n := args.Len(); n != 1 {
		t.Error("bad number of arguments returned in the response to a PING command:", n)
	}

	if !args.Next(&pong) {
		t.Error("no arguments returned as a response to a PING command")
	}

	if pong != "PONG" {
		t.Error("bad argument returned as a response to a PING command:", pong)
	}

	for args.Next(&value) {
		t.Error("too many arguments returned as a response to a PING command:", value)
	}

	if err := args.Close(); err != nil {
		t.Error("error reading a response to a PING command:", err)
	}
}

func testConnSetAndGetSequential(t *testing.T, c *redis.Conn) {
	var args redis.Args
	var key = generateKey()
	var val string

	if err := c.WriteCommands(redis.Command{Cmd: "SET", Args: redis.List(key, "A")}); err != nil {
		t.Error(err)
	}
	if args = c.ReadArgs(); !args.Next(&val) {
		t.Error("no values read as response to SET")
	}
	if val != "OK" {
		t.Error("bad value read as response to SET:", val)
	}
	if err := args.Close(); err != nil {
		t.Error("error closing the arguments list:", err)
	}

	if err := c.WriteCommands(redis.Command{Cmd: "GET", Args: redis.List(key)}); err != nil {
		t.Error(err)
	}
	if args = c.ReadArgs(); !args.Next(&val) {
		t.Error("no values read as response to GET")
	}
	if val != "A" {
		t.Error("bad value read as response to GET:", val)
	}
	if err := args.Close(); err != nil {
		t.Error("error closing the arguments list:", err)
	}
}

func testConnSetAndGetPipeline(t *testing.T, c *redis.Conn) {
	var args redis.Args
	var key = generateKey()
	var val string

	if err := c.WriteCommands(
		redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
		redis.Command{Cmd: "GET", Args: redis.List(key)},
	); err != nil {
		t.Error("error writing SET commands:", err)
	}

	if args = c.ReadArgs(); !args.Next(&val) {
		t.Error("no values read as response to SET")
	}
	if val != "OK" {
		t.Error("bad value read as response to SET:", val)
	}
	if err := args.Close(); err != nil {
		t.Error("error closing the arguments list:", err)
	}

	if args = c.ReadArgs(); !args.Next(&val) {
		t.Error("no values read as response to GET")
	}
	if val != "A" {
		t.Error("bad value read as response to GET:", val)
	}
	if err := args.Close(); err != nil {
		t.Error("error closing the arguments list:", err)
	}
}

func testConnMultiSetAndMGetSequential(t *testing.T, c *redis.Conn) {
	pairs := []struct {
		key string
		val string
	}{
		{generateKey(), "A"},
		{generateKey(), "B"},
		{generateKey(), "C"},
	}

	var args redis.Args
	var val string

	for _, kv := range pairs {
		if err := c.WriteCommands(redis.Command{Cmd: "SET", Args: redis.List(kv.key, kv.val)}); err != nil {
			t.Error(err)
		}
		if args = c.ReadArgs(); !args.Next(&val) {
			t.Error("no values read as response to SET")
		}
		if val != "OK" {
			t.Error("bad value read as response to SET:", val)
		}
		if err := args.Close(); err != nil {
			t.Error("error closing the arguments list:", err)
		}
	}

	if err := c.WriteCommands(redis.Command{
		Cmd:  "MGET",
		Args: redis.List(pairs[0].key, pairs[1].key, pairs[2].key),
	}); err != nil {
		t.Error(err)
	}

	args = c.ReadArgs()

	for _, kv := range pairs {
		if !args.Next(&val) {
			t.Error("no values read as response to MGET")
		}
		if val != kv.val {
			t.Error("bad value read as response to MGET:", val, "!=", kv.val)
		}
	}

	if err := args.Close(); err != nil {
		t.Error("error closing the arguments list:", err)
	}
}

func testConnMultiSetAndMGetPipeline(t *testing.T, c *redis.Conn) {
	pairs := []struct {
		key string
		val string
	}{
		{generateKey(), "A"},
		{generateKey(), "B"},
		{generateKey(), "C"},
	}

	var args redis.Args
	var val string

	if err := c.WriteCommands(
		redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key, pairs[1].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)},
		redis.Command{Cmd: "MGET", Args: redis.List(pairs[0].key, pairs[1].key, pairs[2].key)},
	); err != nil {
		t.Error(err)
	}

	for range pairs {
		if args = c.ReadArgs(); !args.Next(&val) {
			t.Error("no values read as response to SET")
		}
		if val != "OK" {
			t.Error("bad value read as response to SET:", val)
		}
		if err := args.Close(); err != nil {
			t.Error("error closing the arguments list:", err)
		}
	}

	args = c.ReadArgs()

	for _, kv := range pairs {
		if !args.Next(&val) {
			t.Error("no values read as response to MGET")
		}
		if val != kv.val {
			t.Error("bad value read as response to MGET:", val, "!=", kv.val)
		}
	}

	if err := args.Close(); err != nil {
		t.Error("error closing the arguments list:", err)
	}
}

func testConnSetAndDiscardResponsesSequential(t *testing.T, c *redis.Conn) {
	pairs := []struct {
		key string
		val string
	}{
		{generateKey(), "A"},
		{generateKey(), "B"},
		{generateKey(), "C"},
	}

	for _, kv := range pairs {
		if err := c.WriteCommands(redis.Command{Cmd: "SET", Args: redis.List(kv.key, kv.val)}); err != nil {
			t.Error(err)
		}
		if err := c.ReadArgs().Close(); err != nil {
			t.Error(err)
		}
	}
}

func testConnSetAndDiscardResponsesPipeline(t *testing.T, c *redis.Conn) {
	pairs := []struct {
		key string
		val string
	}{
		{generateKey(), "A"},
		{generateKey(), "B"},
		{generateKey(), "C"},
	}

	if err := c.WriteCommands(
		redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key, pairs[1].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)},
	); err != nil {
		t.Error(err)
	}

	for range pairs {
		if err := c.ReadArgs().Close(); err != nil {
			t.Error(err)
		}
	}
}

func testConnSetInvalidAndSetSequential(t *testing.T, c *redis.Conn) {
	pairs := []struct {
		key string
		val string
	}{
		{generateKey(), "A"},
		{generateKey(), "B"},
		{generateKey(), "C"},
	}

	if err := c.WriteCommands(redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)}); err != nil {
		t.Error(err)
	}
	if err := c.ReadArgs().Close(); err != nil {
		t.Error(err)
	}

	if err := c.WriteCommands(redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key)}); err != nil { // missing value
		t.Error(err)
	}
	err := c.ReadArgs().Close()
	switch e := err.(type) {
	case *resp.Error:
		t.Log(e)
	default:
		t.Error(err)
	}

	if err := c.WriteCommands(redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)}); err != nil {
		t.Error(err)
	}
	if err := c.ReadArgs().Close(); err != nil {
		t.Error(err)
	}
}

func testConnSetInvalidAndSetPipeline(t *testing.T, c *redis.Conn) {
	pairs := []struct {
		key string
		val string
	}{
		{generateKey(), "A"},
		{generateKey(), "B"},
		{generateKey(), "C"},
	}

	if err := c.WriteCommands(
		redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key)}, // missing value
		redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)},
	); err != nil {
		t.Error(err)
	}

	// first SET
	if err := c.ReadArgs().Close(); err != nil {
		t.Error(err)
	}

	// second SET (invalid)
	err := c.ReadArgs().Close()
	switch e := err.(type) {
	case *resp.Error:
		t.Log(e)
	default:
		t.Error(err)
	}

	// third set
	if err := c.ReadArgs().Close(); err != nil {
		t.Error(err)
	}
}

func testConnMultiExecEmpty(t *testing.T, c *redis.Conn) {
	for i := 0; i != 10; i++ {
		if err := c.WriteCommands(
			redis.Command{Cmd: "MULTI"},
			redis.Command{Cmd: "EXEC"},
		); err != nil {
			t.Fatal(err)
		}

		tx, err := c.ReadTxArgs(0)
		if err != nil {
			t.Fatal(err)
		}

		if args := tx.Next(); args != nil {
			t.Error("too many argument lists returned by an empty transaction")
		}

		if err := tx.Close(); err != nil {
			t.Error("error returned by an empty transaction:", err)
		}
	}
}

func testConnMultiExecSingle(t *testing.T, c *redis.Conn) {
	key := generateKey()

	for i := 0; i != 10; i++ {
		if err := c.WriteCommands(
			redis.Command{Cmd: "MULTI"},
			redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
			redis.Command{Cmd: "EXEC"},
		); err != nil {
			t.Fatal(err)
		}

		tx, err := c.ReadTxArgs(1)
		if err != nil {
			t.Fatal(err)
		}

		var status interface{}
		if args := tx.Next(); args == nil {
			t.Error("no result list returned for the transaction:", err)
		} else {
			if !args.Next(&status) {
				t.Error("no response returned by the argument list of the transcation")
			}
			if status != "OK" {
				t.Error("bad status returned by the argument list of the transaction:", status)
			}
			if err := args.Close(); err != nil {
				t.Error("error returned by the argument list returned by the transcation:", err)
			}
		}

		if err := tx.Close(); err != nil {
			t.Error("error returned by an empty transaction:", err)
		}
	}
}

func testConnMultiExecMany(t *testing.T, c *redis.Conn) {
	key1 := generateKey()
	key2 := generateKey()
	key3 := generateKey()
	key4 := generateKey()
	key5 := generateKey()

	for i := 0; i != 10; i++ {
		if err := c.WriteCommands(
			redis.Command{Cmd: "MULTI"},
			redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
			redis.Command{Cmd: "SET", Args: redis.List(key2, "B")},
			redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
			redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
			redis.Command{Cmd: "SET", Args: redis.List(key5, "E")},
			redis.Command{Cmd: "EXEC"},
		); err != nil {
			t.Fatal(err)
		}

		tx, err := c.ReadTxArgs(5)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i != 5; i++ {
			var status interface{}
			if args := tx.Next(); args == nil {
				t.Error("no result list returned for the transaction:", err)
			} else {
				if !args.Next(&status) {
					t.Error("no response returned by the argument list of the transcation")
				}
				if status != "OK" {
					t.Error("bad status returned by the argument list of the transaction:", status)
				}
				if err := args.Close(); err != nil {
					t.Error("error returned by the argument list returned by the transcation:", err)
				}
			}
		}

		if err := tx.Close(); err != nil {
			t.Error("error returned by an empty transaction:", err)
		}
	}
}

func testConnMultiDiscardError(t *testing.T, c *redis.Conn) {
	for i := 0; i != 10; i++ {
		if err := c.WriteCommands(
			redis.Command{Cmd: "MULTI"},
			redis.Command{Cmd: "DISCARD"},
		); err != nil {
			t.Fatal(err)
		}

		tx, err := c.ReadTxArgs(0)
		if err != nil {
			t.Fatal(err)
		}

		if args := tx.Next(); args != nil {
			t.Error("too many argument lists returned by an empty transaction")
		}

		switch err := tx.Close().(type) {
		case *resp.Error:
			if err.Type() != "EXECABORT" {
				t.Error("bad error type:", err)
			}

		default:
			t.Error("bad error returned by the discarded transaction:", err)
		}
	}
}

var connKeyTS = time.Now().Format(time.RFC3339)
var connKeyID uint64

func generateKey() string {
	return fmt.Sprintf("test-conn.%s.test-conn.%00d", connKeyTS, atomic.AddUint64(&connKeyID, 1))
}
