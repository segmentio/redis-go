package redis

// A Command represent a Redis command used withing a Request.
type Command struct {
	// Cmd is the Redis command that's being sent with this request.
	Cmd string

	// Args is the list of arguments for the request's command. This field
	// may be nil for client requests if there are no arguments to send with
	// the request.
	//
	// For server request, Args is never nil, even if there are no values in
	// the argument list.
	Args Args
}

/*
type CommandReader struct {
	mutex sync.Mutex
	conn  *Conn
	cmd   Command
	multi bool
	done  bool
}

func (r *CommandReader) Close() error {
	r.mutex.Lock()

	r.mutex.Unlock()
	return
}

func (r *ComandReader) Next(cmd *Command) bool {
	ok := false
	r.mutex.Lock()

	if !r.done {

	}

	r.mutex.Unlock()
	return ok
}

func newCmdArgsReader(d objconv.StreamDecoder, r *CommandReader) *cmdArgsReader {
	args := &cmdArgsReader{dec: d, r: r}
	args.b = args.a[:0]
	return args
}


type cmdArgsReader struct {
	once sync.Once
	err  error
	dec  objconv.StreamDecoder
	r    *CommandReader
	b    []byte
	a    [128]byte
}

func (args *cmdArgsReader) Close() error {
	args.once.Do(func() {
		for args.dec.Decode(nil) == nil {
			// discard all remaining values
		}

		err := args.dec.Err()

		if args.err == nil {
			args.err = err
		}

		// Unlocking the parent command reader allows it to make progress and
		// read the next command.
		if args.r != nil {
			args.r.err = err
			args.r.mu.Unlock()
		}
	})
	return args.err
}

func (args *cmdArgsReader) Len() int {
	if args.err != nil {
		return 0
	}
	return args.dec.Len()
}

func (args *cmdArgsReader) Next(val interface{}) bool {
	args.b = args.b[:0]

	if args.err != nil {
		return false
	}

	if args.err = args.dec.Decode(val); args.err != nil {
		return false
	}

	if v := reflect.ValueOf(val); v.IsValid() {
		if err := args.parse(v.Elem()); err != nil {
			args.err = err
			return false
		}
	}

	return true
}

func (args *cmdArgsReader) parse(v reflect.Value) error {
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

func (args *cmdArgsReader) parseBool(v reflect.Value) error {
	i, err := objutil.ParseInt(args.b)
	if err != nil {
		return err
	}
	v.SetBool(i != 0)
	return nil
}

func (args *cmdArgsReader) parseInt(v reflect.Value) error {
	i, err := objutil.ParseInt(args.b)
	if err != nil {
		return err
	}
	v.SetInt(i)
	return nil
}

func (args *cmdArgsReader) parseUint(v reflect.Value) error {
	u, err := strconv.ParseUint(string(args.b), 10, 64) // this could be optimized
	if err != nil {
		return err
	}
	v.SetUint(u)
	return nil
}

func (args *cmdArgsReader) parseFloat(v reflect.Value) error {
	f, err := strconv.ParseFloat(string(args.b), 64)
	if err != nil {
		return err
	}
	v.SetFloat(f)
	return nil
}

func (args *cmdArgsReader) parseString(v reflect.Value) error {
	v.SetString(string(args.b))
	return nil
}

func (args *cmdArgsReader) parseBytes(v reflect.Value) error {
	v.SetBytes(append(v.Bytes()[:0], args.b...))
	return nil
}

func (args *cmdArgsReader) parseValue(v reflect.Value) error {
	v.Set(reflect.ValueOf(append(make([]byte, 0, len(args.b)), args.b...)))
	return nil
}
*/
