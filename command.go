package redis

import "github.com/segmentio/objconv"

// Command represents the name of a redis command.
type Command string

func (c Command) Is(s string) bool {
	n1 := len(c)
	n2 := len(s)

	if n1 != n2 {
		return false
	}

	for i := 0; i != n1; i++ {
		b1 := byteToLower(c[i])
		b2 := byteToLower(s[i])

		if b1 != b2 {
			return false
		}
	}

	return true
}

func byteToLower(b byte) byte {
	if (b >= 'A') && (b <= 'Z') {
		return b + ('a' - 'A')
	}
	return b
}

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
