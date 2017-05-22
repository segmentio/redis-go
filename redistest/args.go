package redistest

import (
	"reflect"
	"testing"

	redis "github.com/segmentio/redis-go"
)

// MakeArgs is the type of factory functions that the TestArgs test suite uses
// to create Args to run the tests against.
type MakeArgs func(...interface{}) (redis.Args, func(), error)

// TestArgs is a test suite which verifies the behavior of Args implementations.
func TestArgs(t *testing.T, makeArgs MakeArgs) {
	tests := []struct {
		scenario string
		function func(*testing.T, setupArgs)
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
			testFunc(t, func(values ...interface{}) (redis.Args, func()) {
				args, close, err := makeArgs(values...)
				if err != nil {
					t.Fatal(err)
				}
				return args, close
			})
		})
	}
}

type setupArgs func(...interface{}) (redis.Args, func())

func testArgsReadValues(t *testing.T, setup setupArgs) {
	expected := []interface{}{
		42,
		"Hello World!",
	}

	args, teardown := setup(expected...)
	defer teardown()

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
