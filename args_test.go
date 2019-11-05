package redis_test

import (
	"reflect"
	"testing"

	redis "github.com/JoseFeng/redis-go"
)

func TestList(t *testing.T) {
	testArgs(t, func(values ...interface{}) redis.Args {
		return redis.List(values...)
	})
}

func TestMultiArgs(t *testing.T) {
	testArgs(t, func(values ...interface{}) redis.Args {
		args := make([]redis.Args, len(values))

		for i, v := range values {
			args[i] = redis.List(v)
		}

		return redis.MultiArgs(args...)
	})
}

type makeArgsFunc func(...interface{}) redis.Args

func testArgs(t *testing.T, makeArgs makeArgsFunc) {
	tests := []struct {
		scenario string
		function func(*testing.T, makeArgsFunc)
	}{
		{
			scenario: "read a list of values",
			function: testArgsReadValues,
		},
	}

	for _, test := range tests {
		testFunc := test.function

		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			testFunc(t, makeArgs)
		})
	}
}

func testArgsReadValues(t *testing.T, makeArgs makeArgsFunc) {
	expected := []interface{}{
		42,
		"Hello World!",
	}

	args := makeArgs(expected...)
	values := make([]interface{}, len(expected))

	for i, expect := range expected {
		v := reflect.New(reflect.TypeOf(expect))

		if n := args.Len(); n != (len(expected) - i) {
			t.Error("bad args lenght:", n)
			return
		}

		if !args.Next(v.Interface()) {
			t.Error(args.Close())
			return
		}

		values[i] = v.Elem().Interface()
	}

	if err := args.Close(); err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(expected, values) {
		t.Error("values don't match:")
		t.Logf("expected: %#v", expected)
		t.Logf("found:    %#v", values)
	}
}
