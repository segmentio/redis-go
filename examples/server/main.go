package main

import "github.com/JoseFeng/redis-go"

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
