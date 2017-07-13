package main

import (
	"fmt"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	eventslog "github.com/segmentio/events/log"
	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/redis-go/redistest"
	"github.com/segmentio/stats"
	"github.com/segmentio/stats/datadog"
	"github.com/segmentio/stats/redisstats"
	"math/rand"
	"os"
	"time"
)

type testConfig struct {
	Proxy     string `conf:"proxy" help:"Address on which the RED proxy under test is listening for incoming connections, in ip:port format." validate:"nonzero"`
	Dogstatsd string `conf:"dogstatsd" help:"Address of the dogstatsd agent to send metrics to, in ip:port format."                validate:"nonzero"`
	Batch     int    `conf:"batch" help:"Test batch size: number of keys to read and write in each test run."`
	Runs      int    `conf:"runs" help:"Number of times to cycle the test. Specify zero to run indefinitely."`
	Sleep     int    `conf:"sleep" help:"Maximum interval, in seconds, that each operation is delayed in order to throttle traffic to the proxy."`
}

func test(args []string) (err error) {
	logger := eventslog.NewLogger("", 0, events.DefaultHandler)
	engine := stats.DefaultEngine

	config := testConfig{
		Batch: 160,
		Sleep: 5,
	}

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

	logger.Printf("Starting test.\n\tProxy: %s\n\tDogstatsd: %s\n\tBatch: %d\n\tRuns: %d\n\tSleep: %d\n",
		config.Proxy, config.Dogstatsd, config.Batch, config.Runs, config.Sleep)
	for i := 0; config.Runs == 0 || i < config.Runs; i++ {
		logger.Printf("Starting run #%d.", i)
		transport := redisstats.NewTransport(&redis.Transport{})
		client := &redis.Client{Addr: config.Proxy, Transport: transport}
		logger.Printf("Instantiated client.")

		keyNs := fmt.Sprintf("redis-go.red_test.%d.%06d", time.Now().UnixNano(), rand.Int31n(1000000))
		logger.Printf("keyNs: '%s'", keyNs)

		logger.Printf("config.Batch: %d", config.Batch)

		keyTempl := fmt.Sprintf("%s.%%d", keyNs)
		ws, wf, werr := redistest.WriteTestPattern(client, config.Batch, keyTempl, time.Duration(config.Sleep)*time.Second, 30*time.Second)
		if werr != nil {
			logger.Printf("Errors during write: %s", werr)
		}
		logger.Printf("ws: %d, wf: %d", ws, wf)
		engine.Add("test.writes", float64(ws), stats.Tag{Name: "result", Value: "success"})
		engine.Add("test.writes", float64(wf), stats.Tag{Name: "result", Value: "failure"})
		rh, rm, re, rerr := redistest.ReadTestPattern(client, config.Batch, keyTempl, time.Duration(config.Sleep)*time.Second, 30*time.Second)
		if rerr != nil {
			logger.Printf("Errors during read: %s", rerr)
		}
		logger.Printf("rh: %d, rm: %d, re: %d", rh, rm, re)
		engine.Add("test.reads", float64(rh), stats.Tag{Name: "result", Value: "hit"})
		engine.Add("test.reads", float64(rm), stats.Tag{Name: "result", Value: "miss"})
		engine.Add("test.reads", float64(re), stats.Tag{Name: "result", Value: "error"})
		logger.Printf("Completed run #%d.", i)
	}
	logger.Printf("Completed all runs.")
	return nil
}
