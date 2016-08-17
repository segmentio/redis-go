package redis

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestResponseWrite(t *testing.T) {
	tests := []struct {
		r Response
		s string
	}{
		{
			r: Response{},
			s: "*0\r\n",
		},
		{
			r: Response{
				Args: Args("hello", "world"),
			},
			s: "*2\r\n+hello\r\n+world\r\n",
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

func TestReadResponse(t *testing.T) {
	tests := []struct {
		s string
		r Response
	}{
		{
			s: "*0\r\n",
			r: Response{
				Args: Args(),
			},
		},
		{
			s: "*2\r\n+hello\r\n+world\r\n",
			r: Response{
				Args: Args("hello", "world"),
			},
		},
	}

	for _, test := range tests {
		if req, err := ReadResponse(strings.NewReader(test.s), nil); err != nil {
			t.Errorf("%#v: %s", test.s, err)
		} else if err := req.Read(); err != nil {
			t.Errorf("%#v: %s", test.s, err)
		} else if !reflect.DeepEqual(*req, test.r) {
			t.Errorf("%#v: %#v != %#v", test.s, test.r, *req)
		}
	}
}
