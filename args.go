package redis

import (
	"sync"

	"github.com/segmentio/objconv"
)

type Args interface {
	Close() error

	Len() int

	Next(interface{}) bool
}

func List(args ...interface{}) Args {
	list := make([]interface{}, len(args))
	copy(list, args)
	return &argsReader{
		dec: objconv.StreamDecoder{
			Parser: objconv.NewValueParser(list),
		},
	}
}

type argsError struct {
	err error
}

func (args *argsError) Close() error              { return args.err }
func (args *argsError) Len() int                  { return 0 }
func (args *argsError) Next(val interface{}) bool { return false }

type argsReader struct {
	dec  objconv.StreamDecoder
	once sync.Once
	done chan<- error
}

func (args *argsReader) Close() (err error) {
	args.once.Do(func() {
		for args.dec.Decode(nil) == nil {
			// discard all remaining values
		}
		err = args.dec.Err()
		args.done <- err
	})
	return
}

func (args *argsReader) Len() int {
	return args.dec.Len()
}

func (args *argsReader) Next(val interface{}) bool {
	return args.dec.Decode(val) == nil
}

func Int(args Args) (i int, err error) {
	err = Load(args, &i)
	return
}

func Int64(args Args) (i int64, err error) {
	err = Load(args, &i)
	return
}

func String(args Args) (s string, err error) {
	err = Load(args, &s)
	return
}

func Load(args Args, values ...interface{}) error {
	for _, val := range values {
		if !args.Next(val) {
			break
		}
	}
	return args.Close()
}
