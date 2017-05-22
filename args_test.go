package redis_test

import (
	"testing"

	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/redis-go/redistest"
)

func TestList(t *testing.T) {
	redistest.TestArgs(t, func(values ...interface{}) (redis.Args, func(), error) {
		return redis.List(values...), func() {}, nil
	})
}

func TestMultiArgs(t *testing.T) {
	redistest.TestArgs(t, func(values ...interface{}) (redis.Args, func(), error) {
		args := make([]redis.Args, len(values))

		for i, v := range values {
			args[i] = redis.List(v)
		}

		return redis.MultiArgs(args...), func() {}, nil
	})
}
