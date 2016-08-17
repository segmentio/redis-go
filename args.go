package redis

import (
	"fmt"
	"io"
	"reflect"

	"github.com/segmentio/objconv"
)

// Arguments is an interface that represents the stream of values in redis
// requests or responses.
type Arguments interface {
	io.Closer

	// Len returns the number of arguments that the application should expect to
	// receive from the stream.
	Len() int

	// Read loads the next argument from the stram into v, returning an error if
	// something went wrong.
	Read(v interface{}) (err error)
}

// Args constructs an Arguments value from v.
func Args(v ...interface{}) Arguments { return &args{s: v, n: len(v)} }

// ReadInt reads the next value from args as a int.
func ReadInt(args Arguments) (i int, err error) {
	err = args.Read(&i)
	return
}

// ReadInt64 reads the next value from args as a int64.
func ReadInt64(args Arguments) (i int64, err error) {
	err = args.Read(&i)
	return
}

// ReadFloat64 reads the next value from args as a float64.
func ReadFloat64(args Arguments) (f float64, err error) {
	err = args.Read(&f)
	return
}

// ReadString reads the next value from args as a string.
func ReadString(args Arguments) (s string, err error) {
	err = args.Read(&s)
	return
}

// ReadValue reads the next value of arbitrary type from args.
func ReadValue(args Arguments) (v interface{}, err error) {
	err = args.Read(&v)
	return
}

// ReadStrings reads all values from args as strings, returning them in list.
func ReadStrings(args Arguments) (list []string, err error) {
	list = make([]string, 0, args.Len())
	for {
		var s string
		switch err = args.Read(&s); err {
		case nil:
			list = append(list, s)
		case io.EOF:
			if len(list) == 0 {
				list = nil
			}
			err = nil
			return
		default:
			return
		}
	}
}

// ReadValues reads all values from args, returing them in list.
func ReadValues(args Arguments) (list []interface{}, err error) {
	list = make([]interface{}, 0, args.Len())
	for {
		var v interface{}
		switch err = args.Read(&v); err {
		case nil:
			list = append(list, v)
		case io.EOF:
			if len(list) == 0 {
				list = nil
			}
			err = nil
			return
		default:
			return
		}
	}
}

type args struct {
	s []interface{}
	n int
	i int
}

func (a *args) Close() (err error) {
	a.i = a.n
	return
}

func (a *args) Len() int { return a.n - a.i }

func (a *args) Read(v interface{}) (err error) {
	if a.i >= a.n {
		return io.EOF
	}
	defer func() {
		if x := recover(); x != nil {
			switch e := x.(type) {
			case error:
				err = e
			default:
				err = fmt.Errorf("redis: %v", e)
			}
		}
	}()
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(a.s[a.i]))
	a.i++
	return
}

type argsDecoder struct{ objconv.StreamDecoder }

func (a argsDecoder) Close() (err error) {
	for a.Error() != io.EOF {
		var v interface{}
		switch err = a.Decode(&v); err {
		case nil, io.EOF:
		default:
			return
		}
	}
	return
}

func (a argsDecoder) Read(v interface{}) error { return a.Decode(v) }

type argsArray struct {
	Arguments
	n int
}

func makeArgsArray(a Arguments) argsArray {
	n := 0
	if a != nil {
		n = a.Len()
	}
	return argsArray{a, n}
}

func (a argsArray) Len() int { return a.n }

func (a argsArray) Iter() objconv.ArrayIter { return &argsArrayIter{a: a.Arguments, n: a.n} }

type argsArrayIter struct {
	a Arguments
	n int
	i int
}

func (it *argsArrayIter) Next() (v interface{}, ok bool) {
	if ok = it.i < it.n; ok {
		if err := it.a.Read(&v); err != nil {
			panic(err)
		}
		if it.i++; it.i == it.n {
			it.a.Close()
		}
	}
	return
}
