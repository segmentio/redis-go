package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

func TestClient(t *testing.T) {
	n := int32(0)

	getKey := func() string {
		i := atomic.AddInt32(&n, 1)
		return fmt.Sprintf("redis-go.test.client.%d", i)
	}

	t.Run("set a string key", func(t *testing.T) {
		t.Parallel()

		key := getKey()

		if err := Exec(context.Background(), "SET", key, "0123456789"); err != nil {
			t.Error(err)
		}
	})

	t.Run("set and get a string key", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		key := getKey()

		if err := Exec(ctx, "SET", key, "0123456789"); err != nil {
			t.Error(err)
		}

		if s, err := String(Query(ctx, "GET", key)); err != nil {
			t.Error(err)
		} else if s != "0123456789" {
			t.Error("bad value:", s)
		}
	})
}
