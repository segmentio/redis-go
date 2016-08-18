package redis

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestMessageWrite(t *testing.T) {
	tests := []struct {
		m Message
		s string
	}{
		{
			s: "*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			m: Message{
				Type:    "message",
				Channel: []byte("hello"),
				Payload: []byte("world"),
			},
		},
		{
			s: "*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n",
			m: Message{
				Type:          "subscribe",
				Channel:       []byte("hello"),
				Subscriptions: 1,
			},
		},
	}

	b := &bytes.Buffer{}

	for _, test := range tests {
		b.Reset()

		if err := test.m.Write(b); err != nil {
			t.Errorf("%#v: %s", test.m, err)
		} else if s := b.String(); s != test.s {
			t.Errorf("%#v: %#v != %#v", test.m, test.s, s)
		}
	}
}

func TestReadMessage(t *testing.T) {
	tests := []struct {
		s string
		m Message
	}{
		{
			s: "*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			m: Message{
				Type:    "message",
				Channel: []byte("hello"),
				Payload: []byte("world"),
			},
		},
		{
			// malformed but we should accept it
			s: "*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n+world\r\n",
			m: Message{
				Type:    "message",
				Channel: []byte("hello"),
				Payload: []byte("world"),
			},
		},
		{
			s: "*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n",
			m: Message{
				Type:          "subscribe",
				Channel:       []byte("hello"),
				Subscriptions: 1,
			},
		},
	}

	for _, test := range tests {
		if msg, err := ReadMessage(strings.NewReader(test.s)); err != nil {
			t.Errorf("%#v: %s", test.s, err)
		} else if !reflect.DeepEqual(*msg, test.m) {
			t.Errorf("%#v: %#v != %#v", test.s, test.m, *msg)
		}
	}
}
