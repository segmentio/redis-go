# redis-go [![CircleCI](https://circleci.com/gh/segmentio/redis-go.svg?style=shield)](https://circleci.com/gh/segmentio/redis-go) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/redis-go)](https://goreportcard.com/report/github.com/segmentio/redis-go) [![GoDoc](https://godoc.org/github.com/segmentio/redis-go?status.svg)](https://godoc.org/github.com/segmentio/redis-go)

Go package providing tools for building redis clients, servers and middleware.

## Motivation

While there's already good client support for Redis in Go, when it comes to
building middleware (which require server components) the landscape of options
shrinks dramatically. The existing client libraries also have limitations when
it comes to supporting newer Go features (like `context.Context`) and each of
them adopts a different design, making it harder to integrate with other
components of a system.  
On the other hand, the standard `net/http` package has proven to have a simple,
and still extensible design, making it possible to leverage composition to build
software that is easier to develop, maintain and evolve.  
This is where the `redis-go` package comes into play, it follows the same design
than the standard `net/http` package while offering both client and server-side
abstractions to build Redis-compatible software.

## Client

```go
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
```

## Server

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/segmentio/redis-go"
)

func main() {
    // Starts a new server speaking the redis protocol, the server automatically
    // handle asynchronusly pipelining the requests and responses.
    redis.ListenAndServe(":6380", redis.HandlerFunc(func(res redis.ResponseWriter, req *redis.Request) {
        // Put the response in streaming mode, will send 3 values.
        res.WriteStream(3)

        // The response writer automatically encodes Go values into their RESP
        // representation.
        res.Write(1)
        res.Write(2)
        res.Write(3)
    }))
}
```
