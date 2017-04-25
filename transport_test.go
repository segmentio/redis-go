package redis

import (
	"testing"
	"time"
)

func TestTransport(t *testing.T) {
	t.Run("CloseIdleConnections", func(t *testing.T) {
		var transport Transport

		if res, err := transport.RoundTrip(NewRequest("localhost:6379", "SET", "redis-go.test.transport.CloseIdleConnections", "0123456789")); err != nil {
			t.Error(err)
		} else {
			res.Args.Close()
		}

		transport.CloseIdleConnections()
	})

	t.Run("RoundTrip", func(t *testing.T) {
		var transport Transport
		var res *Response
		var err error
		var val string

		if res, err = transport.RoundTrip(NewRequest("localhost:6379", "SET", "redis-go.test.transport.RoundTrip", "0123456789")); err != nil {
			t.Error(err)
			return
		}

		if err = res.Args.Close(); err != nil {
			t.Error(err)
			return
		}

		if res, err = transport.RoundTrip(NewRequest("localhost:6379", "GET", "redis-go.test.transport.RoundTrip")); err != nil {
			t.Error(err)
			return
		}

		for res.Args.Next(&val) {
			if val != "0123456789" {
				t.Error("bad value:", val)
			}
		}

		if err = res.Args.Close(); err != nil {
			t.Error(err)
			return
		}

		transport.CloseIdleConnections()
	})

	t.Run("KeepAlive", func(t *testing.T) {
		var transport = Transport{
			PingInterval: 10 * time.Millisecond,
			PingTimeout:  1 * time.Second,
		}

		if res, err := transport.RoundTrip(NewRequest("localhost:6379", "SET", "redis-go.test.transport.KeepAlive", "0123456789")); err != nil {
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
