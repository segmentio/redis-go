// This program is a micro implementation of a subset of the redis commands.
// It's used for testing an benchmarking of the redis package and is not
// intended to be used in production.

package main

import (
	"flag"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/segmentio/redis-go"
)

var logger = log.New(os.Stderr, "redis: ", log.LstdFlags)

func main() {
	var host string
	var port string
	var socket string

	flag.StringVar(&host, "h", "127.0.0.1", "Server hostname")
	flag.StringVar(&port, "p", "6379", "Server port")
	flag.StringVar(&socket, "s", "", "Server socket (overrides host and port)")
	flag.Parse()

	server := &redis.Server{
		Handler:      redis.HandlerFunc(handle),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorLog:     logger,
	}

	if len(socket) != 0 {
		server.Network = "unix"
		server.Address = socket
	} else {
		server.Network = "tcp"
		server.Address = net.JoinHostPort(host, port)
	}

	logger.Printf("starting micro redis server on %s://%s", server.Network, server.Address)

	if err := server.ListenAndServe(); err != nil {
		logger.Fatal(err)
	}
}

func handle(res redis.ResponseWriter, req *redis.Request) {
	switch req.Cmd {
	case "GET":
		handleGET(res, req)

	case "SET":
		handleSET(res, req)

	case "PING":
		handlePING(res, req)
	}
}

func handleGET(res redis.ResponseWriter, req *redis.Request) {
	var key string
	var val string
	var err error

	if key, err = redis.ReadString(req.Args); err != nil {
		logger.Print(err)
		return
	}

	mutex.RLock()
	val = store[key]
	mutex.RUnlock()

	res.Write(val)
}

func handleSET(res redis.ResponseWriter, req *redis.Request) {
	var key string
	var val string
	var err error

	if key, err = redis.ReadString(req.Args); err != nil {
		logger.Print(err)
		return
	}

	mutex.Lock()
	store[key] = val
	mutex.Unlock()
}

func handlePING(res redis.ResponseWriter, req *redis.Request) {
	var msg = "PONG"
	var err error

	if req.Args.Len() != 0 {
		if msg, err = redis.ReadString(req.Args); err != nil {
			logger.Print(err)
			return
		}
	}

	res.Write(msg)
}

var (
	mutex sync.RWMutex
	store = make(map[string]string, 1000000)
)
