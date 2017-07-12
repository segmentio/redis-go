package redis

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/objutil"
	"github.com/segmentio/objconv/resp"
)

// Args represents a list of arguments in Redis requests and responses.
//
// Args is an interface because there are multiple implementations that
// load values from memory, or from network connections. Using an interface
// allows the code consuming the list of arguments to be agnostic of the actual
// source from which the values are read.
type Args interface {
	// Close closes the argument list, returning any error that occurred while
	// reading the values.
	Close() error

	// Len returns the number of values remaining to be read from this argument
	// list.
	Len() int

	// Next reads the next value from the argument list into dst, which must be
	// a pointer.
	Next(dst interface{}) bool
}

// List creates an argument list from a sequence of values.
func List(args ...interface{}) Args {
	list := make([]interface{}, len(args))
	copy(list, args)
	return &argsReader{
		dec: objconv.StreamDecoder{
			Parser: objconv.NewValueParser(list),
		},
	}
}

// Int parses an integer value from the list of arguments and closes it,
// returning an error if no integer could not be read.
func Int(args Args) (i int, err error) {
	err = ParseArgs(args, &i)
	return
}

// Int64 parses a 64 bits integer value from the list of arguments and closes
// it, returning an error if no integer could not be read.
func Int64(args Args) (i int64, err error) {
	err = ParseArgs(args, &i)
	return
}

// String parses a string value from the list of arguments and closes it,
// returning an error if no string could not be read.
func String(args Args) (s string, err error) {
	err = ParseArgs(args, &s)
	return
}

// ParseArgs reads a list of arguments into a sequence of destination pointers
// and closes it, returning any error that occurred while parsing the values.
func ParseArgs(args Args, dsts ...interface{}) error {
	for _, dst := range dsts {
		if !args.Next(dst) {
			break
		}
	}
	return args.Close()
}

// MultiArgs returns an Args value that produces values sequentially from all of
// the given argument lists.
func MultiArgs(args ...Args) Args {
	return &multiArgs{args: args}
}

type multiArgs struct {
	args []Args
	err  error
}

func (m *multiArgs) Close() (err error) {
	for _, a := range m.args {
		if e := a.Close(); e != nil && err == nil {
			err = e
		}
	}

	if m.err != nil {
		err = m.err
	}

	return
}

func (m *multiArgs) Len() (n int) {
	if m.err == nil {
		for _, a := range m.args {
			n += a.Len()
		}
	}
	return
}

func (m *multiArgs) Next(dst interface{}) bool {
	if len(m.args) == 0 || m.err != nil {
		return false
	}

	for !m.args[0].Next(dst) {
		if err := m.args[0].Close(); err != nil {
			m.err = err
			return false
		}
		if m.args = m.args[1:]; len(m.args) == 0 {
			return false
		}
	}

	return true
}

// TxArgs is a type returned by Conn.ReadTxArgs to produce the list of values
// received in response to a transaction.
type TxArgs struct {
	mutex sync.Mutex
	conn  *Conn
	args  []Args
	err   error
}

// Close closes tx, allowing the connection to be used for furtherreturning
func (tx *TxArgs) Close() error {
	tx.mutex.Lock()

	for _, arg := range tx.args {
		if err := arg.Close(); err != nil {
			if tx.err == nil {
				tx.err = err
			}

			if _, stable := err.(*resp.Error); !stable {
				if tx.conn != nil {
					tx.conn.Close()
				}
				// always report fatal error over protocol errors
				tx.err = err
			}
		}
	}

	if tx.conn != nil {
		tx.conn.rmutex.Unlock()
		tx.conn = nil
	}

	err := tx.err
	tx.mutex.Unlock()
	return err
}

// Len returns the number of argument lists remaining to be read from the
// transaction.
func (tx *TxArgs) Len() int {
	tx.mutex.Lock()
	n := len(tx.args)
	tx.mutex.Unlock()
	return n
}

// Next returns the next argument list of the transaction, or nil if they have
// all been consumed.
//
// When the returned value is not nil the program must call its Close method
// before calling any other function of the TxArgs value.
func (tx *TxArgs) Next() Args {
	tx.mutex.Lock()

	if len(tx.args) == 0 {
		tx.mutex.Unlock()
		return nil
	}

	args := tx.args[0]
	tx.args = tx.args[1:]
	return args
}

type argsError struct {
	err error
}

func newArgsError(err error) *argsError {
	return &argsError{
		err: err,
	}
}

func (args *argsError) Close() error              { return args.err }
func (args *argsError) Len() int                  { return 0 }
func (args *argsError) Next(val interface{}) bool { return false }

type argsReader struct {
	dec  objconv.StreamDecoder
	err  error
	once sync.Once
	done chan<- error
}

func newArgsReader(p *resp.Parser, done chan<- error) *argsReader {
	return &argsReader{
		dec:  objconv.StreamDecoder{Parser: p},
		done: done,
	}
}

func (args *argsReader) Close() error {
	args.once.Do(func() {
		for args.dec.Decode(nil) == nil {
			// discard all remaining values
		}

		err := args.dec.Err()

		if args.done != nil {
			args.done <- err
		}

		if args.err == nil {
			args.err = err
		}
	})
	return args.err
}

func (args *argsReader) Len() int {
	if args.err != nil {
		return 0
	}
	return args.dec.Len()
}

func (args *argsReader) Next(val interface{}) bool {
	if args.err != nil {
		return false
	}

	if args.dec.Len() != 0 {
		if t, _ := args.dec.Parser.ParseType(); t == objconv.Error {
			args.dec.Decode(&args.err)
			return false
		}
	}

	return args.dec.Decode(val) == nil
}

type byteArgsReader struct {
	argsReader
	b []byte
	a [128]byte
}

func newByteArgsReader(p *resp.Parser, done chan<- error) *byteArgsReader {
	r := &byteArgsReader{argsReader: *newArgsReader(p, done)}
	r.b = r.a[:0]
	return r
}

func (args *byteArgsReader) Next(val interface{}) (ok bool) {
	args.b = args.b[:0]

	if ok = args.argsReader.Next(&args.b); ok {
		if v := reflect.ValueOf(val); v.IsValid() {
			if err := args.parse(v.Elem()); err != nil {
				args.err, ok = err, false
			}
		}
	}

	return
}

func (args *byteArgsReader) parse(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Bool:
		return args.parseBool(v)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return args.parseInt(v)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return args.parseUint(v)

	case reflect.Float32, reflect.Float64:
		return args.parseFloat(v)

	case reflect.String:
		return args.parseString(v)

	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return args.parseBytes(v)
		}

	case reflect.Interface:
		return args.parseValue(v)
	}

	return fmt.Errorf("unsupported output type for value in argument of a redis command: %s", v.Type())
}

func (args *byteArgsReader) parseBool(v reflect.Value) error {
	i, err := objutil.ParseInt(args.b)
	if err != nil {
		return err
	}
	v.SetBool(i != 0)
	return nil
}

func (args *byteArgsReader) parseInt(v reflect.Value) error {
	i, err := objutil.ParseInt(args.b)
	if err != nil {
		return err
	}
	v.SetInt(i)
	return nil
}

func (args *byteArgsReader) parseUint(v reflect.Value) error {
	u, err := strconv.ParseUint(string(args.b), 10, 64) // this could be optimized
	if err != nil {
		return err
	}
	v.SetUint(u)
	return nil
}

func (args *byteArgsReader) parseFloat(v reflect.Value) error {
	f, err := strconv.ParseFloat(string(args.b), 64)
	if err != nil {
		return err
	}
	v.SetFloat(f)
	return nil
}

func (args *byteArgsReader) parseString(v reflect.Value) error {
	v.SetString(string(args.b))
	return nil
}

func (args *byteArgsReader) parseBytes(v reflect.Value) error {
	v.SetBytes(append(v.Bytes()[:0], args.b...))
	return nil
}

func (args *byteArgsReader) parseValue(v reflect.Value) error {
	v.Set(reflect.ValueOf(append(make([]byte, 0, len(args.b)), args.b...)))
	return nil
}
