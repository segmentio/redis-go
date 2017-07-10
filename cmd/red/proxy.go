package main

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	"github.com/segmentio/events/log"
	redis "github.com/segmentio/redis-go"
)

type proxyConfig struct {
	Bind  string `conf:"bind"  help:"Address on which the proxy is listening for incoming connections." validate:"nonzero"`
	Debug bool   `conf:"debug" help:"Enable debug mode."`
}

func proxy(args []string) (err error) {
	config := proxyConfig{
		Bind: ":6479",
	}

	conf.LoadWith(&config, conf.Loader{
		Name: "red proxy",
		Args: args,
		Sources: []conf.Source{
			conf.NewEnvSource("RED", os.Environ()...),
		},
	})

	events.DefaultLogger.EnableDebug = config.Debug
	events.DefaultLogger.EnableSource = config.Debug

	server := &redis.Server{
		Addr:         config.Bind,
		Handler:      &redis.ReverseProxy{},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  90 * time.Second,
		ErrorLog:     log.NewLogger("", 0, events.DefaultHandler),
	}

	sigchan, sigstop := signals(syscall.SIGINT, syscall.SIGTERM)
	defer sigstop()

	go func() {
		<-sigchan
		sigstop()
		server.Shutdown(context.Background())
	}()

	if err = server.ListenAndServe(); err == redis.ErrServerClosed {
		err = nil
	}

	return
}

type consulRegistry struct {
	agent string
}
