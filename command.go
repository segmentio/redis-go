package redis

import "github.com/segmentio/objconv"

// Command represents the name of a redis command.
type Command string

const (
	// GET command.
	GET Command = "GET"

	// PING command.
	PING Command = "PING"

	// PONG command.
	PONG Command = "PONG"

	// SET command.
	SET Command = "SET"
)

func makeCommandArray(cmd Command) commandArray { return commandArray(cmd) }

type commandArray string

type commandArrayIter struct {
	c  string
	ok bool
}

func (a commandArray) Len() int { return 1 }

func (a commandArray) Iter() objconv.ArrayIter { return &commandArrayIter{c: string(a), ok: true} }

func (it *commandArrayIter) Next() (v interface{}, ok bool) {
	if ok, it.ok = it.ok, false; ok {
		v = it.c
	}
	return
}
