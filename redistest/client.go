package redistest

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

// Client is an interface that must be implemented by types that represent redis
// clients and wish to be tested using the TestClient test suite.
type Client interface {
	Exec(context.Context, string, ...interface{}) error
	Query(context.Context, string, ...interface{}) redis.Args
	MultiExec(context.Context, ...redis.Command) error
	MultiQuery(context.Context, ...redis.Command) redis.TxArgs
}

// MakeClient is the type of factory functions that the TestClient test suite
// uses to create Clients to run the tests against.
type MakeClient func() (client Client, close func(), err error)

// TestClient is a test suite which verifies the behavior of redis clients.
func TestClient(t *testing.T, makeClient MakeClient) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, Client)
	}{
		{
			scenario: "sending an unknown command returns an error",
			function: testClientUnknownCommand,
		},
		{
			scenario: "sending APPEND commands returns the expected results",
			function: testClientAppend,
		},
		{
			scenario: "sending BITCOUNT commands returns the expected results",
			function: testClientBitcount,
		},
	}

	for _, test := range tests {
		testFunc := test.function

		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			client, close, err := makeClient()
			if err != nil {
				t.Fatal(err)
			}
			defer close()
			testFunc(t, ctx, client)
		})
	}
}

func testClientUnknownCommand(t *testing.T, ctx context.Context, client Client) {
	if err := client.Exec(ctx, "WHATEVER"); !reflect.DeepEqual(err, resp.NewError("ERR unknown command 'WHATEVER'")) {
		t.Errorf("bad error: %#v", err)
	}
}

func testClientAppend(t *testing.T, ctx context.Context, client Client) {
	key := generateKey()

	query(t, ctx, client,
		rt(cmd("APPEND", key, ""), int64(0)),
		rt(cmd("APPEND", key, "Hello World"), int64(11)),
		rt(cmd("APPEND", key, "!"), int64(12)),
	)
}

func testClientBitcount(t *testing.T, ctx context.Context, client Client) {
	key1 := generateKey()
	key2 := generateKey()

	exec(t, ctx, client,
		cmd("SET", key1, "*"),
	)

	query(t, ctx, client,
		rt(cmd("BITCOUNT", key1), int64(3)),
		rt(cmd("BITCOUNT", key2), int64(0)),
	)
}

func exec(t *testing.T, ctx context.Context, client Client, cmds ...command) {
	for _, cmd := range cmds {
		if err := client.Exec(ctx, cmd.cmd, cmd.args...); err != nil {
			t.Error(err)
		}
	}
}

func query(t *testing.T, ctx context.Context, client Client, rts ...roundTrip) {
	for _, rt := range rts {
		args := client.Query(ctx, rt.cmd.cmd, rt.cmd.args...)
		values := make([]interface{}, args.Len()+1)

		for i := 0; args.Next(&values[i]); i++ {
		}
		values = values[:len(values)-1]

		if err := args.Close(); err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(rt.res, values) {
			t.Error("bad values returned:")
			t.Logf("expected: %#v", rt.res)
			t.Logf("found:    %#v", values)
		}
	}
}

type command struct {
	cmd  string
	args []interface{}
}

func cmd(cmd string, args ...interface{}) command {
	return command{cmd: cmd, args: args}
}

type roundTrip struct {
	cmd command
	res []interface{}
}

func rt(cmd command, res ...interface{}) roundTrip {
	return roundTrip{cmd: cmd, res: res}
}

var connKeyTS = time.Now().Format(time.RFC3339)
var connKeyID uint64

func generateKey() string {
	return fmt.Sprintf("redis-go.test.%s.%00d", connKeyTS, atomic.AddUint64(&connKeyID, 1))
}
