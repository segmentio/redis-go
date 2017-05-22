package redis_test

import (
	"net"
	"testing"

	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/redis-go/redistest"
)

func TestServerEndpoint(t *testing.T) {
	redistest.TestServerRegistry(t, func() (redis.ServerRegistry, []redis.ServerEndpoint, func(), error) {
		endpoint := redis.ServerEndpoint{Name: "A", Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4242}}
		return endpoint, []redis.ServerEndpoint{endpoint}, func() {}, nil
	})
}

func TestServerList(t *testing.T) {
	redistest.TestServerRegistry(t, func() (redis.ServerRegistry, []redis.ServerEndpoint, func(), error) {
		endpoints := []redis.ServerEndpoint{
			{Name: "A", Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4242}},
			{Name: "B", Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4243}},
			{Name: "C", Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4244}},
		}
		return redis.ServerList(endpoints), endpoints, func() {}, nil
	})
}
