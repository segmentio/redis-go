package redis

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestRequestWrite(t *testing.T) {
	tests := []struct {
		r Request
		s string
	}{
		{
			r: Request{},
			s: "*1\r\n$0\r\n\r\n",
		},
		{
			r: Request{
				Cmd: SET,
			},
			s: "*1\r\n$3\r\nSET\r\n",
		},
		{
			r: Request{
				Cmd:  SET,
				Args: Args("hello", "world"),
			},
			s: "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
		},
	}

	b := &bytes.Buffer{}

	for _, test := range tests {
		b.Reset()

		if err := test.r.Write(b); err != nil {
			t.Errorf("%#v: %s", test.r, err)
		} else if s := b.String(); s != test.s {
			t.Errorf("%#v: %#v != %#v", test.r, test.s, s)
		}
	}
}

func TestReadRequest(t *testing.T) {
	tests := []struct {
		s string
		r Request
	}{
		{
			s: "*1\r\n$3\r\nSET\r\n",
			r: Request{
				Cmd:  SET,
				Args: Args(),
			},
		},
		{
			s: "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			r: Request{
				Cmd:  SET,
				Args: Args("hello", "world"),
			},
		},
	}

	for _, test := range tests {
		if req, err := ReadRequest(strings.NewReader(test.s)); err != nil {
			t.Errorf("%#v: %s", test.s, err)
		} else if err := req.Read(); err != nil {
			t.Errorf("%#v: %s", test.s, err)
		} else if !reflect.DeepEqual(*req, test.r) {
			t.Errorf("%#v: %#v != %#v", test.s, test.r, *req)
		}
	}
}

func TestReadRequestError(t *testing.T) {
	tests := []struct {
		s string
	}{
		{
			// missing command
			s: "*0\r\n",
		},
	}

	for _, test := range tests {
		if _, err := ReadRequest(strings.NewReader(test.s)); err == nil {
			t.Errorf("%#v: expected to fail but no error was returned", test.s)
		}
	}
}
