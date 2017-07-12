package redis

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTransport(t *testing.T) {
	t.Run("CloseIdleConnections", func(t *testing.T) {
		t.Parallel()

		var transport Transport

		if res, err := transport.RoundTrip(
			NewRequest("localhost:6379", "SET", List("redis-go.test.transport.CloseIdleConnections", "0123456789")),
		); err != nil {
			t.Error(err)
		} else {
			res.Args.Close()
		}

		transport.CloseIdleConnections()
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		var transport = Transport{MaxIdleConns: 3}
		var wg sync.WaitGroup

		for i := 0; i != 10; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				var res *Response
				var err error
				var key = fmt.Sprintf("redis-go.test.transport.RoundTrip-%02d", i)
				var val = fmt.Sprintf("%02d", i)

				if res, err = transport.RoundTrip(NewRequest("localhost:6379", "SET", List(key, val))); err != nil {
					t.Error(err)
					return
				}

				if err = res.Args.Close(); err != nil {
					t.Error(err)
					return
				}

				if res, err = transport.RoundTrip(NewRequest("localhost:6379", "GET", List(key))); err != nil {
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

		transport.CloseIdleConnections()
	})

	t.Run("KeepAlive", func(t *testing.T) {
		t.Parallel()

		var transport = Transport{
			PingInterval: 10 * time.Millisecond,
			PingTimeout:  1 * time.Second,
		}

		if res, err := transport.RoundTrip(
			NewRequest("localhost:6379", "SET", List("redis-go.test.transport.KeepAlive", "0123456789")),
		); err != nil {
			t.Error(err)
		} else {
			res.Args.Close()
		}

		// Wait a bunch so the transport's internal goroutines will cleanup the
		// open connection.
		time.Sleep(100 * time.Millisecond)

		transport.CloseIdleConnections()
	})
}
