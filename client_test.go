package redis_test

import (
	"context"
	"testing"

	redis "github.com/JoseFeng/redis-go"
	"github.com/JoseFeng/redis-go/redistest"
)

func TestClient(t *testing.T) {
	redistest.TestClient(t, func() (redistest.Client, func(), error) {
		transport := &redis.Transport{}
		return &testClient{Client: redis.Client{Addr: "localhost:6379", Transport: transport}}, transport.CloseIdleConnections, nil
	})
}

type testClient struct {
	redis.Client
}

func (tc *testClient) Subscribe(ctx context.Context, channels ...string) (*redis.SubConn, error) {
	return tc.Transport.(*redis.Transport).Subscribe(ctx, "tcp", tc.Addr, channels...)
}

func (tc *testClient) PSubscribe(ctx context.Context, patterns ...string) (*redis.SubConn, error) {
	return tc.Transport.(*redis.Transport).PSubscribe(ctx, "tcp", tc.Addr, patterns...)
}
