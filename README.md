redis-go [![CircleCI](https://circleci.com/gh/segmentio/redis-go.svg?style=shield)](https://circleci.com/gh/segmentio/redis-go) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/redis-go)](https://goreportcard.com/report/github.com/segmentio/redis-go) [![GoDoc](https://godoc.org/github.com/segmentio/redis-go?status.svg)](https://godoc.org/github.com/segmentio/redis-go)
========

Go package providing tools for building redis clients, servers and middleware.

Installation
------------

```shell
go get github.com/segmentio/redis-go
```

Usage
-----

redis-go barrows concepts from the standard net/http package, using the package
should be very similar.

### Connections

Clients connections can be opened via the `redis.Dial` function. It returns a
value of type `redis.Conn` which implements `net.Conn` and extends it with
redis-sepcific oeprations.

Connections support pipelining, a program can send multiple requests and then
start reading responses. Reading and Writing operations can be done concurrently
from multiple goroutines, however multiple goroutines should not be performing
read operations at the same time, the same applies to write operations.

```go
package main

import (
    "log"

    "github.com/segmentio/redis-go"
)

func main() {
    var conn redis.Conn
    var res *redis.Response
    var err error

    if conn, err = redis.Dial("tcp", "localhost:6739"); err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Write pipelined requests.
    if err = conn.WriteRequest(&redis.Request{
        Cmd:  "SET",
        Args: redis.Args("hello", "world"),
    }); err != nil {
        log.Fatal(err)
    }

    if err = conn.WriteRequest(&redis.Request{
        Cmd:  "SET",
        Args: redis.Args("question", "answer"),
    }); err != nil {
        log.Fatal(err)
    }

    // Read pipelined responses.
    if res, err = conn.ReadResponse(nil); err != nil {
        log.Fatal(err)
    }

    // ...
    res.Args.Close() // will the entire response stream is consumed

    // ...
}
```

### Server

Starting a redis server is similar to using the standard net/http package as
well, here's a quick example:

```go
package main

import (
    "log"

    "github.com/segmentio/redis-go"
)

func main() {
    if err := redis.ListenAndServe(":6379", redis.HandlerFunc(serve)); err != nil {
        log.Fatal(err)
    }
}

func serve(res redis.ResponseWriter, req *redis.Request) {
    // Read the request from req and writes the response to res
    // ...
}
```
