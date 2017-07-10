package redistest

import (
	"context"
	"reflect"
	"testing"
	"time"

	redis "github.com/segmentio/redis-go"
)

// MakeServerRegistry is the type of factory functions that the
// TestServerRegistry test suite uses to create Clients to run the
// tests against.
type MakeServerRegistry func() (redis.ServerRegistry, []redis.ServerEndpoint, func(), error)

// TestServerRegistry is a test suite which verifies the behavior of
// ServerRegistry implementations.
func TestServerRegistry(t *testing.T, makeServerRegistry MakeServerRegistry) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, redis.ServerRegistry, []redis.ServerEndpoint)
	}{
		{
			scenario: "calling LookupServers with a canceled context returns an error",
			function: testServerRegistryCancel,
		},
		{
			scenario: "calling LookupServers returns the expected list of servers",
			function: testServerRegistryLookupServers,
		},
	}

	for _, test := range tests {
		testFunc := test.function

		t.Run(test.scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			registry, endpoints, close, err := makeServerRegistry()
			if err != nil {
				t.Fatal(err)
			}
			defer close()
			testFunc(t, ctx, registry, endpoints)
		})
	}
}

func testServerRegistryCancel(t *testing.T, ctx context.Context, registry redis.ServerRegistry, endpoints []redis.ServerEndpoint) {
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := registry.LookupServers(ctx); err == nil {
		t.Error("expected a non-nil error but got", err)
	}
}

func testServerRegistryLookupServers(t *testing.T, ctx context.Context, registry redis.ServerRegistry, endpoints []redis.ServerEndpoint) {
	servers, err := registry.LookupServers(ctx)

	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(servers, endpoints) {
		t.Error("server endpoints don't match:")
		t.Logf("expected: %#v", endpoints)
		t.Logf("found:    %#v", servers)
	}
}
