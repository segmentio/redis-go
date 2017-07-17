package redistest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	redis "github.com/segmentio/redis-go"
)

type multiError []error

func (err multiError) Error() string {
	if len(err) > 0 {
		return fmt.Sprintf("%d errors. First one: '%s'.", len(err), err[0])
	} else {
		return fmt.Sprintf("No errors.")
	}
}

// WriteTestPattern writes a test pattern to a Redis client. The objective is to read the test pattern back
// at a later stage using ReadTestPattern.
func WriteTestPattern(client *redis.Client, n int, keyTempl string, sleep time.Duration, timeout time.Duration) (numSuccess int, numFailure int, err error) {
	writeErrs := make(chan error, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(keyTempl, i)
		val := "1"
		waiter.Add(1)
		go func(key, val string) {
			defer waiter.Done()
			time.Sleep(time.Duration(rand.Intn(int(timeout))))
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := client.Exec(ctx, "SET", key, val); err != nil {
				writeErrs <- err
			}
		}(key, val)
	}
	waiter.Wait()
	close(writeErrs)
	numFailure = len(writeErrs)
	numSuccess = n - numFailure
	if len(writeErrs) > 0 {
		var merr multiError
		for writeErr := range writeErrs {
			merr = append(merr, writeErr)
		}
		return numSuccess, numFailure, merr
	}
	return numSuccess, numFailure, nil
}

// ReadTestPattern reads a test pattern (previously writen using WriteTestPattern) from a Redis client and returns hit statistics.
func ReadTestPattern(client *redis.Client, n int, keyTempl string, sleep time.Duration, timeout time.Duration) (numHits int, numMisses int, numErrors int, err error) {
	type tresult struct {
		hit bool
		err error
	}
	results := make(chan tresult, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < cap(results); i++ {
		key := fmt.Sprintf(keyTempl, i)
		waiter.Add(1)
		go func(key string, rq chan<- tresult) {
			defer waiter.Done()
			time.Sleep(time.Duration(rand.Intn(int(timeout))))
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			args := client.Query(ctx, "GET", key)
			if args.Len() != 1 {
				rq <- tresult{err: fmt.Errorf("Unexpected response: 1 arg expected; contains %d. ", args.Len())}
				return
			}
			var v string
			args.Next(&v)
			if err := args.Close(); err != nil {
				rq <- tresult{err: err}
			} else {
				rq <- tresult{hit: v == "1"}
			}
		}(key, results)
	}
	waiter.Wait()
	close(results)
	var merr multiError
	for result := range results {
		if result.err != nil {
			numErrors++
			merr = append(merr, result.err)
		} else {
			if result.hit {
				numHits++
			} else {
				numMisses++
			}
		}
	}
	if merr != nil {
		return numHits, numMisses, numErrors, merr
	}
	return numHits, numMisses, numErrors, nil
}
