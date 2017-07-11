package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/log"
	_ "github.com/segmentio/events/sigevents"
	_ "github.com/segmentio/events/text"
)

var version = ""

func main() {
	var err error
	var ld = conf.Loader{
		Name: "red",
		Args: os.Args[1:],
		Commands: []conf.Command{
			{"proxy", "Run the RED proxy"},
			{"help", "Show the RED help"},
			{"version", "Show the RED version"},
		},
	}

	switch cmd, args := conf.LoadWith(nil, ld); cmd {
	case "proxy":
		err = proxy(args)
	case "help":
		ld.PrintHelp(nil)
	case "version":
		fmt.Println(version)
	default:
		panic("unreachable")
	}

	if err != nil {
		events.Log("%{error}s", err)
		os.Exit(1)
	}
}

// The signals function configures the program to receive the list of signals
// given as argument. It returns a channel from which the program can receive
// notifications that those signals arrived, and a teardown function that
// removes the signal handlers when it is called.
func signals(signals ...os.Signal) (<-chan os.Signal, func()) {
	sigchan := make(chan os.Signal)
	sigrecv := events.Signal(sigchan)
	signal.Notify(sigchan, signals...)
	return sigrecv, func() { signal.Stop(sigchan) }
}
