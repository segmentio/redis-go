package redis_test

import (
	"context"
	"fmt"
	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/redis-go/redistest"
	"github.com/stretchr/testify/assert"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

func TestReverseProxy(t *testing.T) {
	redistest.TestClient(t, func() (redistest.Client, func(), error) {
		transport := &redis.Transport{}

		serverList, _, _ := makeServerList()

		_, serverURL := newServer(&redis.ReverseProxy{
			Transport: transport,
			Registry:  serverList,
			ErrorLog:  log.New(os.Stderr, "proxy test ==> ", 0),
		})

		teardown := func() {
			transport.CloseIdleConnections()
		}

		u, _ := url.Parse(serverURL)
		return &testClient{Client: redis.Client{Addr: u.Host, Transport: transport}}, teardown, nil
	})
}

func TestReverseProxyHash(t *testing.T) {
	transport := &redis.Transport{}

	full, broken, onedowns := makeServerList()

	proxy := &redis.ReverseProxy{
		Transport: transport,
		Registry:  full,
		ErrorLog:  log.New(os.Stderr, "proxy hash test ==> ", 0),
	}

	_, serverURL := newServerTimeout(proxy, 10000*time.Millisecond)
	u, _ := url.Parse(serverURL)
	client := &redis.Client{Addr: u.Host, Transport: transport}

	// full backend - write n keys
	n := 160
	keyTempl := "redis-go.test.rphash.%d"

	numSuccess, numFailure, errs := writeBlock(client, n, keyTempl)
	assert.Equal(t, n, numSuccess, "All writes succeeded")
	assert.Equal(t, 0, numFailure, "No writes failed")
	if errs != nil {
		t.Error(errs)
		return
	}

	timeout := 30 * time.Second

	// full backend - read n back
	numHits, numMisses, err := measHitMiss(client, n, keyTempl, timeout)
	t.Logf("full backend n read: numHits = %d, numMisses = %d, err = %v", numHits, numMisses, err)
	assert.Nil(t, err, "Full backend - no errors")
	assert.Equal(t, n, numHits, "Full backend - all hit")
	assert.Equal(t, 0, numMisses, "Full backend - none missed")

	// full backend - read n+1 back
	numHits, numMisses, err = measHitMiss(client, n+1, keyTempl, timeout)
	t.Logf("full backend n+1 read: numHits = %d, numMisses = %d, err = %v", numHits, numMisses, err)
	assert.Nil(t, err, "Extra read - no errors")
	assert.Equal(t, n, numHits, "Extra read - all hit")
	assert.Equal(t, 1, numMisses, "Extra read - one (additional) missed")

	// malfunctioning backend - read fails
	proxy.Registry = broken
	numHits, numMisses, err = measHitMiss(client, n+1, keyTempl, 2*time.Second)
	t.Logf("broken backend n read: numHits = %d, numMisses = %d, err = %v", numHits, numMisses, err)
	assert.NotNil(t, err, "Broken backend - errors")
	assert.Equal(t, 0, numHits, "Broken backend - none hit")
	assert.Equal(t, 0, numMisses, "Broken backend - none missed")

	// single backend dropped (all combinations) - read n back
	accHits, accMisses := 0, 0
	for i := 0; i < len(onedowns); i++ {
		proxy.Registry = onedowns[i]
		numHits, numMisses, err = measHitMiss(client, n, keyTempl, timeout)
		t.Logf("single backend dropped (%d): numHits = %d, numMisses = %d, %d%% miss rate, err = %v", i, numHits, numMisses, 100*numMisses/n, err)
		assert.Nil(t, err, "One down - no errors")
		assert.True(t, numHits < n, "One down - not all hit")
		assert.True(t, numHits > 0, "One down - some hit")
		assert.True(t, numMisses < n, "One down - not all missed")
		assert.True(t, numMisses > 0, "One down - some missed")
		assert.Equal(t, n, numHits+numMisses, "One down - hits and misses adds up")
		accHits += numHits
		accMisses += numMisses
	}
	assert.Equal(t, n, accMisses, "Misses add up")
}

func makeServerList() (full redis.ServerList, broken redis.ServerList, onedowns []redis.ServerList) {
	full = redis.ServerList{
		{Name: "backend", Addr: "localhost:6379"},
		{Name: "backend", Addr: "localhost:6380"},
		{Name: "backend", Addr: "localhost:6381"},
		{Name: "backend", Addr: "localhost:6382"},
	}

	broken = append(full, redis.ServerEndpoint{Name: "backend", Addr: "localhost:0"})

	onedowns = []redis.ServerList{}
	for i := 0; i < len(full); i++ {
		// list containing all but the i'th element of full
		notith := make(redis.ServerList, 0, len(full)-1)
		for j := 0; j < len(full); j++ {
			if j != i {
				notith = append(notith, full[j])
			}
		}
		onedowns = append(onedowns, notith)
	}
	return
}

type MultiError []error

func (err MultiError) Error() string {
	if len(err) > 0 {
		return fmt.Sprintf("%d errors. First one: '%s'.", len(err), err[0])
	} else {
		return fmt.Sprintf("No errors (weird).")
	}
}

func writeBlock(client *redis.Client, n int, keyTempl string) (numSuccess int, numFailure int, err MultiError) {
	writeErrs := make(chan error, n)
	waiter := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(keyTempl, i)
		val := "1"
		waiter.Add(1)
		go func(key, val string) {
			defer waiter.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		err = make(MultiError, 0)
		for writeErr := range writeErrs {
			err = append(err, writeErr)
		}
	}
	return
}

func measHitMiss(client *redis.Client, n int, keyTempl string, timeout time.Duration) (numHits, numMisses int, err error) {
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
	for result := range results {
		if result.err != nil {
			return 0, 0, result.err
		}
		if result.hit {
			numHits++
		} else {
			numMisses++
		}
	}
	return
}
