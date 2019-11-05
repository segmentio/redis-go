package redis_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	redis "github.com/JoseFeng/redis-go"
)

func TestTransport(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "close idle connections",
			function: testTransportCloseIdleConnections,
		},
		{
			scenario: "sending requests in parallel produces valid responses an no errors",
			function: testTransportRoundTrip,
		},
		{
			scenario: "sending requests and waiting causes pings to be sent to the server",
			function: testTransportKeepAlive,
		},
		{
			scenario: "cancelling an inflight request returns an net.OpError with context.Canceled as reason",
			function: testTransportCancelRoundTrip,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			testFunc(t)
		})
	}
}

func testTransportCloseIdleConnections(t *testing.T) {
	tr := redis.Transport{}

	key := generateKey()
	req := redis.NewRequest("localhost:6379", "SET", redis.List(key, "0123456789"))

	if res, err := tr.RoundTrip(req); err != nil {
		t.Error(err)
	} else {
		res.Args.Close()
	}

	tr.CloseIdleConnections()
}

func testTransportRoundTrip(t *testing.T) {
	var tr = redis.Transport{MaxIdleConns: 3}
	var wg sync.WaitGroup

	for i := 0; i != 10; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var res *redis.Response
			var err error
			var key = generateKey()
			var val = fmt.Sprintf("%02d", i)

			if res, err = tr.RoundTrip(redis.NewRequest("localhost:6379", "SET", redis.List(key, val))); err != nil {
				t.Error(err)
				return
			}

			if err = res.Args.Close(); err != nil {
				t.Error(err)
				return
			}

			if res, err = tr.RoundTrip(redis.NewRequest("localhost:6379", "GET", redis.List(key))); err != nil {
				t.Error(err)
				return
			}

			var arg string
			for res.Args.Next(&arg) {
				if arg != val {
					t.Errorf("bad value: %s != %s", arg, val)
				}
			}

			if err = res.Args.Close(); err != nil {
				t.Error(err)
				return
			}
		}(i)
	}

	wg.Wait()
	tr.CloseIdleConnections()
}

func testTransportKeepAlive(t *testing.T) {
	tr := redis.Transport{
		PingInterval: 10 * time.Millisecond,
		PingTimeout:  1 * time.Second,
	}

	key := generateKey()
	req := redis.NewRequest("localhost:6379", "SET", redis.List(key, "0123456789"))

	if res, err := tr.RoundTrip(req); err != nil {
		t.Error(err)
	} else {
		res.Args.Close()
	}

	// Wait a bunch so the transport's internal goroutines will cleanup the
	// open connection.
	time.Sleep(100 * time.Millisecond)
	tr.CloseIdleConnections()
}

func testTransportCancelRoundTrip(t *testing.T) {
	tr := redis.Transport{
		PingInterval: 10 * time.Millisecond,
		PingTimeout:  1 * time.Second,
	}

	key := generateKey()
	req := redis.NewRequest("localhost:6379", "SET", redis.List(key, "0123456789"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req.Context = ctx

	if res, err := tr.RoundTrip(req); err == nil {
		res.Args.Close()

	} else if e, ok := err.(*net.OpError); !ok {
		t.Error("bad error type:", err)

	} else if e.Err != context.Canceled {
		t.Errorf("bad root cause of the error: %#v", e.Err)
	}
}
