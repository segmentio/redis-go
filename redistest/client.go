package redistest

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	redis "github.com/segmentio/redis-go"
)

// Client is an interface that must be implemented by types that represent redis
// clients and wish to be tested using the TestClient test suite.
type Client interface {
	Exec(context.Context, string, ...interface{}) error
	Query(context.Context, string, ...interface{}) redis.Args
	Subscribe(context.Context, ...string) (*redis.SubConn, error)
	PSubscribe(context.Context, ...string) (*redis.SubConn, error)
}

// MakeClient is the type of factory functions that the TestClient test suite
// uses to create Clients to run the tests against.
type MakeClient func() (client Client, close func(), err error)

// TestClient is a test suite that verify the behavior of redis clients.
func TestClient(t *testing.T, makeClient MakeClient) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, Client, string)
	}{
		{
			scenario: "sends SET and GET commands and verify that the correct values are read",
			function: testClientSetAndGet,
		},

		{
			scenario: "subscribe to channels, publish messages to some of those channels, and verify that they are read by the subscriber",
			function: testClientSubscribe,
		},
	}

	for i, test := range tests {
		testFunc := test.function
		namespace := fmt.Sprintf("redis-go.test.client.%d", i)

		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			client, close, err := makeClient()
			if err != nil {
				t.Fatal(err)
			}
			defer close()
			testFunc(t, ctx, client, namespace)
		})
	}
}

func testClientSetAndGet(t *testing.T, ctx context.Context, client Client, namespace string) {
	values := []interface{}{
		"Hello World!",
	}

	for _, value := range values {
		typ := fmt.Sprintf("%T", value)
		val := value
		key := namespace + ":" + typ

		t.Run(typ, func(t *testing.T) {
			if err := client.Exec(ctx, "SET", key, val); err != nil {
				t.Error(err)
				return
			}

			get := reflect.New(reflect.TypeOf(val))
			args := client.Query(ctx, "GET", key)

			if !args.Next(get.Interface()) {
				t.Error(args.Close())
				return
			}

			if err := args.Close(); err != nil {
				t.Error(err)
				return
			}

			if !reflect.DeepEqual(get.Elem().Interface(), val) {
				t.Error("values don't match:")
				t.Logf("expected: %#v", val)
				t.Logf("found:    %#v", get.Elem().Interface())
			}
		})
	}
}

func testClientSubscribe(t *testing.T, ctx context.Context, client Client, namespace string) {
	channelA := namespace + ":A"
	channelB := namespace + ":B"
	channelC := namespace + ":C"

	sub, err := client.Subscribe(ctx, channelA, channelB, channelC)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	deadline, _ := ctx.Deadline()
	sub.SetDeadline(deadline)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		publish := []struct {
			channel string
			message string
		}{
			{channelA, "1"},
			{channelA, "2"},
			{channelA, "3"},
			{channelC, "Hello World!"},
		}

		for _, p := range publish {
			if err := client.Exec(ctx, "PUBLISH", p.channel, p.message); err != nil {
				t.Error(err)
			}
		}
	}()

	received := map[string][]string{}
	expected := map[string][]string{channelA: {"1", "2", "3"}, channelC: {"Hello World!"}}

	for i := 0; i != 4; i++ {
		channel, message, err := sub.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		received[channel] = append(received[channel], string(message))
	}

	if !reflect.DeepEqual(expected, received) {
		t.Error("bad messages received:")
		t.Logf("expected: %#v", expected)
		t.Logf("received: %#v", received)
	}

	wg.Wait()
}
