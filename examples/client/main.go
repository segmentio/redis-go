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

	// MultiQuery
	cmds := []redis.Command{
		redis.Command{Cmd: "SET", Args: redis.List("foo", "bar")},
		redis.Command{Cmd: "SET", Args: redis.List("bob", "alice")},
	}

	if err := redis.MultiExec(ctx, cmds...); err != nil {
		fmt.Println(err)
	}

	args = redis.MultiQuery(ctx,
		redis.Command{Cmd: "GET", Args: redis.List("foo")},
		redis.Command{Cmd: "GET", Args: redis.List("bob")},
	)

	fmt.Printf("%v\n", args)
	var v1, v2 string
	fmt.Println(args.Len())
	if err := redis.ParseArgs(args, &v1); err != nil {
		fmt.Println(err)
	}
	if err := redis.ParseArgs(args, &v2); err != nil {
		fmt.Println(err)
	}

	fmt.Printf("v1 :%s", v1)
	fmt.Printf("v2 :%s", v2)

	if err := args.Close(); err != nil {
		fmt.Println(err)
	}
}
