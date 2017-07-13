package redistest

import (
	"context"
	"fmt"
	"math/rand"
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

// TestClient is a test suite which verifies the behavior of redis clients.
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
	t.Skip("skipping this test for now because the proxy doesn't have PUB/SUB support yet, will add back later")

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

type multiError []error

func (err multiError) Error() string {
	if len(err) > 0 {
		return fmt.Sprintf("%d errors. First one: '%s'.", len(err), err[0])
	} else {
		return fmt.Sprintf("No errors.")
	}
}

// WriteTestPattern writes a test pattern to a Redis client. The objective is to read the test pattern back
// at a later stage using ReadTestPattern.
func WriteTestPattern(client *redis.Client, n int, keyTempl string, sleep time.Duration, timeout time.Duration) (numSuccess int, numFailure int, err error) {
	writeErrs := make(chan error, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(keyTempl, i)
		val := "1"
		waiter.Add(1)
		go func(key, val string) {
			defer waiter.Done()
			time.Sleep(time.Duration(rand.Intn(int(sleep.Seconds()))) * time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := client.Exec(ctx, "SET", key, val); err != nil {
				writeErrs <- err
			}
		}(key, val)
	}
	waiter.Wait()
	close(writeErrs)
	numFailure = len(writeErrs)
	numSuccess = n - numFailure
	if len(writeErrs) > 0 {
		var merr multiError
		for writeErr := range writeErrs {
			merr = append(merr, writeErr)
		}
		return numSuccess, numFailure, merr
	}
	return numSuccess, numFailure, nil
}

// ReadTestPattern reads a test pattern (previously writen using WriteTestPattern) from a Redis client and returns hit statistics.
func ReadTestPattern(client *redis.Client, n int, keyTempl string, sleep time.Duration, timeout time.Duration) (numHits int, numMisses int, numErrors int, err error) {
	type tresult struct {
		hit bool
		err error
	}
	results := make(chan tresult, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < cap(results); i++ {
		key := fmt.Sprintf(keyTempl, i)
		waiter.Add(1)
		go func(key string, rq chan<- tresult) {
			defer waiter.Done()
			time.Sleep(time.Duration(rand.Intn(int(sleep.Seconds()))) * time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			args := client.Query(ctx, "GET", key)
			if args.Len() != 1 {
				rq <- tresult{err: fmt.Errorf("Unexpected response: 1 arg expected; contains %d. ", args.Len())}
				return
			}
			var v string
			args.Next(&v)
			if err := args.Close(); err != nil {
				rq <- tresult{err: err}
			} else {
				rq <- tresult{hit: v == "1"}
			}
		}(key, results)
	}
	waiter.Wait()
	close(results)
	var merr multiError
	for result := range results {
		if result.err != nil {
			numErrors++
			merr = append(merr, result.err)
		} else {
			if result.hit {
				numHits++
			} else {
				numMisses++
			}
		}
	}
	if merr != nil {
		return numHits, numMisses, numErrors, merr
	}
	return numHits, numMisses, numErrors, nil
}
