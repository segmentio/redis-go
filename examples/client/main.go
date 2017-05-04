package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/redis-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the default client which is configured to connect to the redis server
	// running at localhost:6379.
	if err := redis.Exec(ctx, "SET", "hello", "world"); err != nil {
		fmt.Println(err)
	}

	// Passing request or response arguments is done by consuming the stream of
	// values.
	var args = redis.Query(ctx, "GET", "hello")
	var value string

	if args.Next(&value) {
		fmt.Println(value)
	}

	if err := args.Close(); err != nil {
		fmt.Println(err)
	}
}
