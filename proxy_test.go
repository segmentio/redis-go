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
	"testing"
	"time"
)

func TestReverseProxy(t *testing.T) {
	redistest.TestClient(t, func() (redistest.Client, func(), error) {
		transport := &redis.Transport{}

		serverList, _ := makeServerList()

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

	full, onedowns := makeServerList()
	t.Logf("makeServerList(): full='%v', onedowns = '%v'", full, onedowns)

	proxy := &redis.ReverseProxy{
		Transport: transport,
		Registry:  full,
		ErrorLog:  log.New(os.Stderr, "proxy hash test ==> ", 0),
	}

	_, serverURL := newServer(proxy)
	u, _ := url.Parse(serverURL)
	client := &redis.Client{Addr: u.Host, Transport: transport}

	// full backend - write n keys
	n := 1600
	keyTempl := "redis-go.test.rphash.%d"
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(keyTempl, i)
		val := "1"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Exec(ctx, "SET", key, val); err != nil {
			t.Error(err)
			return
		}
	}

	// full backend - read n back
	numHits, numMisses, err := measHitMiss(t, client, n, keyTempl)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("full backend n read: numHits = %d, numMisses = %d", numHits, numMisses)
	assert.Equal(t, n, numHits, "Full backend - all hit")
	assert.Equal(t, 0, numMisses, "Full backend - none missed")

	// full backend - read n+1 back
	numHits, numMisses, err = measHitMiss(t, client, n+1, keyTempl)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("full backend n+1 read: numHits = %d, numMisses = %d", numHits, numMisses)
	assert.Equal(t, n, numHits, "Extra read - all hit")
	assert.Equal(t, 1, numMisses, "Extra read - one (additional) missed")

	// single backend dropped (all combinations) - read n back
	accHits, accMisses := 0, 0
	for i := 0; i < len(onedowns); i++ {
		proxy.Registry = onedowns[i]
		numHits, numMisses, err = measHitMiss(t, client, n, keyTempl)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("single backend dropped (%d): numHits = %d, numMisses = %d, %d%% miss rate", i, numHits, numMisses, 100*numMisses/n)
		assert.True(t, numHits < n, "One down - not all hit")
		assert.True(t, numHits > 0, "One down - some hit")
		assert.True(t, numMisses < n, "One down - not all missed")
		assert.True(t, numMisses > 0, "One down - some missed")
		accHits += numHits
		accMisses += numMisses
	}
	assert.Equal(t, n, accMisses, "Misses add up")
}

func measHitMiss(t *testing.T, client *redis.Client, n int, keyTempl string) (numHits, numMisses int, err error) {
	for i := 0; i < n; i++ {
		key := fmt.Sprintf(keyTempl, i)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		args := client.Query(ctx, "GET", key)
		for j := 0; j < args.Len(); j++ {
			var v string
			args.Next(&v)
			if v == "1" {
				numHits++
			} else {
				numMisses++
			}
		}
		if err := args.Close(); err != nil {
			return 0, 0, err
		}
	}
	return
}

func makeServerList() (full redis.ServerList, onedowns []redis.ServerList) {
	full = redis.ServerList{
		{Name: "backend", Addr: "localhost:6379"},
		{Name: "backend", Addr: "localhost:6380"},
		{Name: "backend", Addr: "localhost:6381"},
		{Name: "backend", Addr: "localhost:6382"},
	}

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
