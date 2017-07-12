package redis_test

import (
	"context"
	"fmt"
	"reflect"
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
		close    bool
	}{
		{
			scenario: "cleanly closing a connection doesn't return any error",
			function: testConnCloseHasNoError,
			close:    true,
		},
		{
			scenario: "reading after closing a connection returns an error",
			function: testConnReadAfterClose,
			close:    true,
		},
		{
			scenario: "reading arguments after closing a connection returns an error",
			function: testConnReadArgsAfterClose,
			close:    true,
		},
		{
			scenario: "writing after closing a connection returns an error",
			function: testConnWriteAfterClose,
			close:    true,
		},
		{
			scenario: "writing commands after closing a connection returns an error",
			function: testConnWriteCommandsAfterClose,
			close:    true,
		},
		{
			scenario: "writing an empty list of commands after closing a connection returns no errors",
			function: testConnEmptyWriteCommandsAfterClose,
			close:    true,
		},
		{
			scenario: "writing an empty list of commands returns no errors",
			function: testConnEmptyWriteCommandsBeforeClose,
			close:    true,
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
			scenario: "sending list commands properly returns sequence of values",
			function: testConnListComands,
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
			scenario: "discarding an empty transaction returns an ErrDiscard error",
			function: testConnMultiDiscardError,
		},
		{
			scenario: "discarding a transaction with one command returns ErrDiscard errors",
			function: testConnMultiDiscardSingle,
		},
		{
			scenario: "discarding a transaction with many commands returns ErrDiscard errors",
			function: testConnMultiDiscardMany,
		},
		{
			scenario: "transactions with an invalid command are aborted but the connection remains open",
			function: testConnMultiExecAbortBadCommand,
		},
		{
			scenario: "transactions with multiple MULTI comands report errors but are valid",
			function: testConnMultiExecManyMulti,
		},
		{
			scenario: "transactions on list datastructures properly return sequences of values",
			function: testConnMultiExecSubListArguments,
		},
	}

	t.Run("short-lived", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("long-lived", func(t *testing.T) {
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

		for _, test := range tests {
			// In this test suite we're reusing the same connection to excercise
			// long-lived scenarios, so we cannot run tests that close it or the
			// next ones will fail.
			if !test.close {
				testFunc := test.function
				t.Run(test.scenario, func(t *testing.T) {
					testFunc(t, c)
				})
			}
		}
	})
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
	readArgsEqual(t, c.ReadArgs(), nil, "PONG")
}

func testConnWriteCommandsPingPong(t *testing.T, c *redis.Conn) {
	if err := c.WriteCommands(redis.Command{Cmd: "PING"}); err != nil {
		t.Fatal(err)
	}
	readArgsEqual(t, c.ReadArgs(), nil, "PONG")
}

func testConnSetAndGetSequential(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c, redis.Command{Cmd: "SET", Args: redis.List(key, "A")})
	readArgsEqual(t, c.ReadArgs(), nil, "OK")

	writeCommands(t, c, redis.Command{Cmd: "GET", Args: redis.List(key)})
	readArgsEqual(t, c.ReadArgs(), nil, "A")
}

func testConnSetAndGetPipeline(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
		redis.Command{Cmd: "GET", Args: redis.List(key)},
	)

	readArgsEqual(t, c.ReadArgs(), nil, "OK")
	readArgsEqual(t, c.ReadArgs(), nil, "A")
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

	for _, kv := range pairs {
		writeCommands(t, c, redis.Command{Cmd: "SET", Args: redis.List(kv.key, kv.val)})
		readArgsEqual(t, c.ReadArgs(), nil, "OK")
	}

	writeCommands(t, c, redis.Command{
		Cmd:  "MGET",
		Args: redis.List(pairs[0].key, pairs[1].key, pairs[2].key),
	})

	readArgsEqual(t, c.ReadArgs(), nil, "A", "B", "C")
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

	writeCommands(t, c,
		redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key, pairs[1].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)},
		redis.Command{Cmd: "MGET", Args: redis.List(pairs[0].key, pairs[1].key, pairs[2].key)},
	)

	for range pairs {
		readArgsEqual(t, c.ReadArgs(), nil, "OK")
	}

	readArgsEqual(t, c.ReadArgs(), nil, "A", "B", "C")
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
		writeCommands(t, c, redis.Command{Cmd: "SET", Args: redis.List(kv.key, kv.val)})
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

	writeCommands(t, c,
		redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key, pairs[1].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)},
	)

	for range pairs {
		discardArgs(t, c.ReadArgs())
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

	writeCommands(t, c, redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)})
	discardArgs(t, c.ReadArgs())

	writeCommands(t, c, redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key)}) // missing value
	readArgsEqual(t, c.ReadArgs(), resp.NewError("ERR wrong number of arguments for 'set' command"))

	writeCommands(t, c, redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)})
	discardArgs(t, c.ReadArgs())
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

	writeCommands(t, c,
		redis.Command{Cmd: "SET", Args: redis.List(pairs[0].key, pairs[0].val)},
		redis.Command{Cmd: "SET", Args: redis.List(pairs[1].key)}, // missing value
		redis.Command{Cmd: "SET", Args: redis.List(pairs[2].key, pairs[2].val)},
	)

	discardArgs(t, c.ReadArgs())
	readArgsEqual(t, c.ReadArgs(), resp.NewError("ERR wrong number of arguments for 'set' command"))
	discardArgs(t, c.ReadArgs())
}

func testConnListComands(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "RPUSH", Args: redis.List(key, "A", "B", "C")},
		redis.Command{Cmd: "RPUSH", Args: redis.List(key, "D", "E", "F")},
		redis.Command{Cmd: "LRANGE", Args: redis.List(key, 2, 5)},
		redis.Command{Cmd: "RPOP", Args: redis.List(key)},
		redis.Command{Cmd: "LRANGE", Args: redis.List(key, 2, 5)},
	)

	readArgsEqual(t, c.ReadArgs(), nil, "3")
	readArgsEqual(t, c.ReadArgs(), nil, "6")
	readArgsEqual(t, c.ReadArgs(), nil, "C", "D", "E", "F")
	readArgsEqual(t, c.ReadArgs(), nil, "F")
	readArgsEqual(t, c.ReadArgs(), nil, "C", "D", "E")
}

func testConnMultiExecEmpty(t *testing.T, c *redis.Conn) {
	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 0, nil, func(tx *redis.TxArgs) {})
}

func testConnMultiExecSingle(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 1, nil, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), nil, "OK")
	})
}

func testConnMultiExecMany(t *testing.T, c *redis.Conn) {
	key1 := generateKey()
	key2 := generateKey()
	key3 := generateKey()
	key4 := generateKey()
	key5 := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
		redis.Command{Cmd: "SET", Args: redis.List(key2, "B")},
		redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
		redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
		redis.Command{Cmd: "SET", Args: redis.List(key5, "E")},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 5, nil, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), nil, "OK")
	})
}

func testConnMultiDiscardError(t *testing.T, c *redis.Conn) {
	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "DISCARD"},
	)

	withTxArgs(t, c, 0, redis.ErrDiscard, func(tx *redis.TxArgs) {})
}

func testConnMultiDiscardSingle(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
		redis.Command{Cmd: "DISCARD"},
	)

	withTxArgs(t, c, 1, redis.ErrDiscard, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
	})
}

func testConnMultiDiscardMany(t *testing.T, c *redis.Conn) {
	key1 := generateKey()
	key2 := generateKey()
	key3 := generateKey()
	key4 := generateKey()
	key5 := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
		redis.Command{Cmd: "SET", Args: redis.List(key2, "B")},
		redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
		redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
		redis.Command{Cmd: "SET", Args: redis.List(key5, "E")},
		redis.Command{Cmd: "DISCARD"},
	)

	withTxArgs(t, c, 5, redis.ErrDiscard, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
	})
}

func testConnMultiExecAbortBadCommand(t *testing.T, c *redis.Conn) {
	key1 := generateKey()
	key2 := generateKey()
	key3 := generateKey()
	key4 := generateKey()
	key5 := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
		redis.Command{Cmd: "SET", Args: redis.List(key2)}, // missing value
		redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
		redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
		redis.Command{Cmd: "SET", Args: redis.List(key5, "E")},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 5, redis.ErrDiscard, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), resp.NewError("ERR wrong number of arguments for 'set' command"))
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
		readArgsEqual(t, tx.Next(), redis.ErrDiscard)
	})
}

func testConnMultiExecManyMulti(t *testing.T, c *redis.Conn) {
	key1 := generateKey()
	key2 := generateKey()
	key3 := generateKey()
	key4 := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
		redis.Command{Cmd: "MULTI"}, // invalid but doesn't cause a transaction failure
		redis.Command{Cmd: "SET", Args: redis.List(key2, "B")},
		redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
		redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 5, nil, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), resp.NewError("ERR MULTI calls can not be nested"))
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), nil, "OK")
		readArgsEqual(t, tx.Next(), nil, "OK")
	})
}

func testConnMultiExecSubListArguments(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "RPUSH", Args: redis.List(key, "A", "B", "C")},
		redis.Command{Cmd: "RPUSH", Args: redis.List(key, "D", "E", "F")},
		redis.Command{Cmd: "LRANGE", Args: redis.List(key, 2, 5)},
		redis.Command{Cmd: "RPOP", Args: redis.List(key)},
		redis.Command{Cmd: "LRANGE", Args: redis.List(key, 2, 5)},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 5, nil, func(tx *redis.TxArgs) {
		readArgsEqual(t, tx.Next(), nil, "3")
		readArgsEqual(t, tx.Next(), nil, "6")
		readArgsEqual(t, tx.Next(), nil, "C", "D", "E", "F")
		readArgsEqual(t, tx.Next(), nil, "F")
		readArgsEqual(t, tx.Next(), nil, "C", "D", "E")
	})
}

var connKeyTS = time.Now().Format(time.RFC3339)
var connKeyID uint64

func generateKey() string {
	return fmt.Sprintf("test-conn.%s.test-conn.%00d", connKeyTS, atomic.AddUint64(&connKeyID, 1))
}

func writeCommands(t *testing.T, conn *redis.Conn, cmds ...redis.Command) {
	if err := conn.WriteCommands(cmds...); err != nil {
		t.Fatal(err)
	}
}

func discardArgs(t *testing.T, args redis.Args) {
	if err := args.Close(); err != nil {
		t.Error(err)
	}
}

func readArgsEqual(t *testing.T, args redis.Args, error error, values ...string) {
	if args == nil {
		t.Error("invalid nil argument list")
		return
	}

	n := args.Len()

	for i, v := range values {
		var x string

		if !args.Next(&x) {
			t.Error("not enough values returned by the list of arguments:")
			t.Logf("expected: %v", len(values))
			t.Logf("found:    %v", i)
			break
		}

		if x != v {
			t.Error("bad value returned by the list of argument at index", i)
			t.Logf("expected: %v", v)
			t.Logf("found:    %v", x)
		}
	}

	var v interface{}
	for args.Next(&v) {
		t.Errorf("unexpected extra value returned by the list of arguments: %#v", v)
		v = nil
	}

	err := args.Close()

	if !reflect.DeepEqual(err, error) {
		t.Error("bad error returned when closing the arguments:")
		t.Logf("expected: %v", error)
		t.Logf("found:    %v", err)
	}

	if n > 0 {
		if _, ok := err.(*resp.Error); ok {
			n-- // an error occurred, we couldn't read the value
		}
	}

	if n != len(values) {
		t.Error("bad arguments length:")
		t.Log("expected:", len(values))
		t.Log("found:   ", n)
	}
}

func withTxArgs(t *testing.T, c *redis.Conn, n int, error error, do func(*redis.TxArgs)) {
	tx, err := c.ReadTxArgs(n)
	if err != nil {
		t.Fatal(err)
	}

	do(tx)

	if err := tx.Close(); !reflect.DeepEqual(err, error) {
		t.Error("bad error returned when closing the arguments:")
		t.Logf("expected: %v", error)
		t.Logf("found:    %v", err)
	}
}
