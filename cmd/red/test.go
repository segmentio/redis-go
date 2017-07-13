package main

import (
	"fmt"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	eventslog "github.com/segmentio/events/log"
	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/stats"
	"github.com/segmentio/stats/datadog"
	"math/rand"
	"os"
	"time"
)

type testConfig struct {
	Proxy     string `conf:"proxy" help:"Address on which the RED proxy under test is listening for incoming connections, in ip:port format." validate:"nonzero"`
	Dogstatsd string `conf:"dogstatsd" help:"Address of the dogstatsd agent to send metrics to, in ip:port format."                validate:"nonzero"`
	Runs      int    `conf:"runs" help:"Number of times to cycle the test. Specify zero to run indefinitely."`
}

func test(args []string) (err error) {
	logger := eventslog.NewLogger("", 0, events.DefaultHandler)

	config := testConfig{}

	conf.LoadWith(&config, conf.Loader{
		Name: "red proxy",
		Args: args,
		Sources: []conf.Source{
			conf.NewEnvSource("RED", os.Environ()...),
		},
	})

	if len(config.Dogstatsd) != 0 {
		stats.Register(datadog.NewClient(config.Dogstatsd))
	}
	defer stats.Flush()

	for i := 0; config.Runs == 0 || i < config.Runs; i++ {
		logger.Printf("Starting run #%d.", i)
		transport := &redis.Transport{}
		client := &redis.Client{Addr: "localhost:6379", Transport: transport}
		logger.Printf("Instantiated client.")

		keyNs := fmt.Sprintf("redis-go.red_test.%d.%06d", time.Now().UnixNano(), rand.Int31n(1000000))
		logger.Printf("keyNs: '%s'", keyNs)

		n := 160
		logger.Printf("n: '%s'", keyNs)

		keyTempl := fmt.Sprintf("%s.%%d", keyNs)
		ws, wf, werr := redis.WriteTestPattern(client, n, keyTempl, 30*time.Second)
		if werr != nil {
			logger.Printf("Error during write: %s", werr)
			return werr
		}
		logger.Printf("ws: %d, wf: %d, err: %s", ws, wf, werr)

		rh, rm, rerr := redis.ReadTestPattern(client, n, keyTempl, 30*time.Second)
		if rerr != nil {
			logger.Printf("Error during read: %s", rerr)
			return rerr
		}
		logger.Printf("rh: %d, rm: %d, err: %s", rh, rm, rerr)
		logger.Printf("Completed run #%d.", i)
	}
	logger.Printf("Completed all runs.")
	return nil
}
