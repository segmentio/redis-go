package redis_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/objconv/resp"
	redis "github.com/JoseFeng/redis-go"
)

func TestConn(t *testing.T) {
	t.Run("client", func(t *testing.T) {
		t.Parallel()

		clientTests := []struct {
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

			for _, test := range clientTests {
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

			for _, test := range clientTests {
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
	})

	t.Run("server", func(t *testing.T) {
		t.Parallel()

		serverTests := []struct {
			scenario string
			function func(*testing.T, *redis.Conn, *redis.Conn)
		}{
			{
				scenario: "a single command written to a client connection is received by the server side",
				function: testConnReadSingleCommand,
			},
			{
				scenario: "multiple commands written to a client connection are received by the server side",
				function: testConnReadManyCommands,
			},
			{
				scenario: "a single empty transaction written to a client connection is received by the server side",
				function: testConnReadSingleEmptyTransaction,
			},
			{
				scenario: "a single non-empty transaction written to a client connection is received by the server side",
				function: testConnReadSingleNonEmptyTransaction,
			},
			{
				scenario: "multiple transactions written to a client connection are received by the server side",
				function: testConnReadManyTransactions,
			},
		}

		for _, test := range serverTests {
			testFunc := test.function
			t.Run(test.scenario, func(t *testing.T) {
				c1, c2 := newTestConnPair()

				client := redis.NewClientConn(c1)
				server := redis.NewServerConn(c2)

				deadline := time.Now().Add(4 * time.Second)
				client.SetDeadline(deadline)
				server.SetDeadline(deadline)

				defer client.Close()
				defer server.Close()

				testFunc(t, client, server)
			})
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

	withTxArgs(t, c, 0, nil, func(tx redis.TxArgs) {})
}

func testConnMultiExecSingle(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
		redis.Command{Cmd: "EXEC"},
	)

	withTxArgs(t, c, 1, nil, func(tx redis.TxArgs) {
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

	withTxArgs(t, c, 5, nil, func(tx redis.TxArgs) {
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

	withTxArgs(t, c, 0, redis.ErrDiscard, func(tx redis.TxArgs) {})
}

func testConnMultiDiscardSingle(t *testing.T, c *redis.Conn) {
	key := generateKey()

	writeCommands(t, c,
		redis.Command{Cmd: "MULTI"},
		redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
		redis.Command{Cmd: "DISCARD"},
	)

	withTxArgs(t, c, 1, redis.ErrDiscard, func(tx redis.TxArgs) {
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

	withTxArgs(t, c, 5, redis.ErrDiscard, func(tx redis.TxArgs) {
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

	withTxArgs(t, c, 5, redis.ErrDiscard, func(tx redis.TxArgs) {
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

	withTxArgs(t, c, 5, nil, func(tx redis.TxArgs) {
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

	withTxArgs(t, c, 5, nil, func(tx redis.TxArgs) {
		readArgsEqual(t, tx.Next(), nil, "3")
		readArgsEqual(t, tx.Next(), nil, "6")
		readArgsEqual(t, tx.Next(), nil, "C", "D", "E", "F")
		readArgsEqual(t, tx.Next(), nil, "F")
		readArgsEqual(t, tx.Next(), nil, "C", "D", "E")
	})
}

func testConnReadSingleCommand(t *testing.T, c *redis.Conn, s *redis.Conn) {
	key := generateKey()

	parallelConnPair(c, s,
		func(c *redis.Conn) {
			writeCommands(t, c,
				redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
			)
		},
		func(s *redis.Conn) {
			readCommands(t, s, nil,
				redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
			)
		},
	)
}

func testConnReadManyCommands(t *testing.T, c *redis.Conn, s *redis.Conn) {
	key1 := generateKey()
	key2 := generateKey()
	key3 := generateKey()
	key4 := generateKey()
	key5 := generateKey()

	parallelConnPair(c, s,
		func(c *redis.Conn) {
			writeCommands(t, c,
				redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
				redis.Command{Cmd: "SET", Args: redis.List(key2, "B")},
				redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
				redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
				redis.Command{Cmd: "SET", Args: redis.List(key5, "E")},
			)
		},
		func(s *redis.Conn) {
			readCommands(t, s, nil,
				redis.Command{Cmd: "SET", Args: redis.List(key1, "A")},
			)
			readCommands(t, s, nil,
				redis.Command{Cmd: "SET", Args: redis.List(key2, "B")},
			)
			readCommands(t, s, nil,
				redis.Command{Cmd: "SET", Args: redis.List(key3, "C")},
			)
			readCommands(t, s, nil,
				redis.Command{Cmd: "SET", Args: redis.List(key4, "D")},
			)
			readCommands(t, s, nil,
				redis.Command{Cmd: "SET", Args: redis.List(key5, "E")},
			)
		},
	)
}

func testConnReadSingleEmptyTransaction(t *testing.T, c *redis.Conn, s *redis.Conn) {
	parallelConnPair(c, s,
		func(c *redis.Conn) {
			writeCommands(t, c,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "EXEC"},
			)
		},
		func(s *redis.Conn) {
			readCommands(t, s, nil,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "EXEC"},
			)
		},
	)
}

func testConnReadSingleNonEmptyTransaction(t *testing.T, c *redis.Conn, s *redis.Conn) {
	key := generateKey()

	parallelConnPair(c, s,
		func(c *redis.Conn) {
			writeCommands(t, c,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
				redis.Command{Cmd: "GET", Args: redis.List(key)},
				redis.Command{Cmd: "EXEC"},
			)
		},
		func(s *redis.Conn) {
			readCommands(t, s, nil,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
				redis.Command{Cmd: "GET", Args: redis.List(key)},
				redis.Command{Cmd: "EXEC"},
			)
		},
	)
}

func testConnReadManyTransactions(t *testing.T, c *redis.Conn, s *redis.Conn) {
	key := generateKey()

	parallelConnPair(c, s,
		func(c *redis.Conn) {
			writeCommands(t, c,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
				redis.Command{Cmd: "GET", Args: redis.List(key)},
				redis.Command{Cmd: "EXEC"},
			)

			writeCommands(t, c,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "EXEC"},
			)

			writeCommands(t, c,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "LRANGE", Args: redis.List(key, "0", "1")},
				redis.Command{Cmd: "EXEC"},
			)
		},
		func(s *redis.Conn) {
			readCommands(t, s, nil,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "SET", Args: redis.List(key, "A")},
				redis.Command{Cmd: "GET", Args: redis.List(key)},
				redis.Command{Cmd: "EXEC"},
			)

			readCommands(t, s, nil,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "EXEC"},
			)

			readCommands(t, s, nil,
				redis.Command{Cmd: "MULTI"},
				redis.Command{Cmd: "LRANGE", Args: redis.List(key, "0", "1")},
				redis.Command{Cmd: "EXEC"},
			)
		},
	)
}

var connKeyTS = time.Now().Format(time.RFC3339)
var connKeyID uint64

func generateKey() string {
	return fmt.Sprintf("redis-go.%s.%00d", connKeyTS, atomic.AddUint64(&connKeyID, 1))
}

func writeCommands(t *testing.T, conn *redis.Conn, cmds ...redis.Command) {
	if err := conn.WriteCommands(cmds...); err != nil {
		panic(err)
	}
}

func readCommands(t *testing.T, conn *redis.Conn, expectErr error, cmds ...redis.Command) {
	r := conn.ReadCommands()
	c := redis.Command{}
	i := 0

	for r.Read(&c) {
		if i > len(cmds) {
			t.Error("too many commands read from the connection:", c.Cmd)
			t.Log("expected:", len(cmds))
			t.Log("found:   ", i)
			i++
			c.Args.Close()
			continue
		}

		if c.Cmd != cmds[i].Cmd {
			t.Error("bad command found at index", i)
			t.Logf("expected: %s", cmds[i].Cmd)
			t.Logf("found:    %s", c.Cmd)
		}

		a1 := c.Args
		a2 := cmds[i].Args

		for {
			var v1 interface{}
			var v2 interface{}
			var ok1 bool
			var ok2 bool

			if a2 != nil {
				ok2 = a2.Next(&v2)
			}

			if v2 != nil {
				v1 = reflect.New(reflect.TypeOf(v2)).Interface()
			} else {
				var x interface{}
				v1 = &x
			}

			ok1 = a1.Next(v1)
			if v1 != nil {
				v1 = reflect.ValueOf(v1).Elem().Interface()
			}

			if ok1 != ok2 {
				t.Errorf("number of arguments mismatch in %s command at index %d", c.Cmd, i)
				t.Log("expected:", ok2)
				t.Log("found:   ", ok1)
			}

			if !ok1 || !ok2 {
				break
			}

			if !reflect.DeepEqual(v1, v2) {
				t.Errorf("bad value reader from the %s command arguments at index %d", c.Cmd, i)
				t.Logf("expected: %#v", v2)
				t.Logf("found:    %#v", v1)
			}
		}

		var err1 error
		var err2 error

		if a1 != nil {
			err1 = a1.Close()
		}
		if a2 != nil {
			err2 = a2.Close()
		}

		if !reflect.DeepEqual(err1, err2) {
			t.Errorf("bad error returned when closing the arugment list of %s at index %d", c.Cmd, i)
			t.Logf("expected: %v", err2)
			t.Logf("found:    %v", err1)
		}

		i++
	}

	if i < len(cmds) {
		t.Error("not enough commands read from the connection")
		t.Log("expected:", len(cmds))
		t.Log("found:   ", i)
	}

	if err := r.Close(); !reflect.DeepEqual(err, expectErr) {
		t.Error("bad error returned when closing the command reader:")
		t.Logf("expected: %v", expectErr)
		t.Logf("found:    %v", err)
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

func withTxArgs(t *testing.T, c *redis.Conn, n int, error error, do func(redis.TxArgs)) {
	tx := c.ReadTxArgs(n)
	do(tx)

	if err := tx.Close(); !reflect.DeepEqual(err, error) {
		t.Error("bad error returned when closing the arguments:")
		t.Logf("expected: %v", error)
		t.Logf("found:    %v", err)
	}
}

func parallelConnPair(c *redis.Conn, s *redis.Conn, cf func(*redis.Conn), sf func(*redis.Conn)) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	do := func(conn *redis.Conn, connFunc func(*redis.Conn)) {
		defer wg.Done()
		defer conn.Close()
		connFunc(conn)
	}

	go do(c, cf)
	go do(s, sf)

	wg.Wait()
}

type testConn struct {
	*io.PipeReader
	*io.PipeWriter
}

func newTestConnPair() (*testConn, *testConn) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	c1 := &testConn{
		PipeReader: r1,
		PipeWriter: w2,
	}

	c2 := &testConn{
		PipeReader: r2,
		PipeWriter: w1,
	}

	return c1, c2
}

func (c *testConn) Close() error {
	c.PipeWriter.Close()
	return nil
}

func (c *testConn) SetDeadline(t time.Time) error      { return nil }
func (c *testConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *testConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *testConn) LocalAddr() net.Addr                { return nil }
func (c *testConn) RemoteAddr() net.Addr               { return nil }
