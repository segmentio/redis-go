package redis_test

import (
	"log"
	"net/url"
	"os"
	"testing"

	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/redis-go/redistest"
)

func TestReverseProxy(t *testing.T) {
	redistest.TestClient(t, func() (redistest.Client, func(), error) {
		transport := &redis.Transport{}

		_, serverURL := newServer(&redis.ReverseProxy{
			Transport: transport,
			Registry: redis.ServerList{
				{Name: "backend", Addr: "localhost:6379"},
				{Name: "backend", Addr: "localhost:6380"},
				{Name: "backend", Addr: "localhost:6381"},
				{Name: "backend", Addr: "localhost:6382"},
			},
			ErrorLog: log.New(os.Stderr, "proxy test ==> ", 0),
		})

		teardown := func() {
			transport.CloseIdleConnections()
		}

		u, _ := url.Parse(serverURL)
		return &testClient{Client: redis.Client{Addr: u.Host, Transport: transport}}, teardown, nil
	})
}
