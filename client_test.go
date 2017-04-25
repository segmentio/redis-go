package redis

import (
	"context"
	"fmt"
	"testing"
)

func TestClient(t *testing.T) {
	n := 0

	getKey := func() string {
		n++
		return fmt.Sprintf("redis-go.test.client.%d", n)
	}

	t.Run("set a string key", func(t *testing.T) {
		key := getKey()

		if err := Exec(context.Background(), "SET", key, "0123456789"); err != nil {
			t.Error(err)
		}
	})

	t.Run("set and get a string key", func(t *testing.T) {
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
