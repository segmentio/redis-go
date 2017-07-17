package redis_test

import (
	"log"
	"net/url"
	"os"
	"testing"
	"time"

	redis "github.com/segmentio/redis-go"
	"github.com/segmentio/redis-go/redistest"
	"github.com/stretchr/testify/assert"
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

	_, serverURL := newServerTimeout(proxy, 10*time.Second)
	u, _ := url.Parse(serverURL)
	client := &redis.Client{Addr: u.Host, Transport: transport}

	// full backend - write n keys
	n := 160
	keyTempl := "redis-go.test.rphash.%d"

	sleep := time.Millisecond
	timeout := time.Second

	numSuccess, numFailure, err := redistest.WriteTestPattern(client, n, keyTempl, sleep, timeout)
	assert.Equal(t, n, numSuccess, "All writes succeeded")
	assert.Equal(t, 0, numFailure, "No writes failed")
	if err != nil {
		t.Error(err)
		return
	}

	// full backend - read n back
	numHits, numMisses, numErrs, err := redistest.ReadTestPattern(client, n, keyTempl, sleep, timeout)
	t.Logf("full backend n read: numHits = %d, numMisses = %d, numErrs = %d, err = %v", numHits, numMisses, numErrs, err)
	assert.Nil(t, err, "Full backend - no errors")
	assert.Equal(t, n, numHits, "Full backend - all hit")
	assert.Equal(t, 0, numMisses, "Full backend - none missed")
	assert.Equal(t, 0, numErrs, "Full backend - no errors")

	// full backend - read n+1 back
	numHits, numMisses, numErrs, err = redistest.ReadTestPattern(client, n+1, keyTempl, sleep, timeout)
	t.Logf("full backend n+1 read: numHits = %d, numMisses = %d, numErrs = %d, err = %v", numHits, numMisses, numErrs, err)
	assert.Nil(t, err, "Extra read - no errors")
	assert.Equal(t, n, numHits, "Extra read - all hit")
	assert.Equal(t, 1, numMisses, "Extra read - one (additional) missed")
	assert.Equal(t, 0, numErrs, "Extra read - no errors")

	// malfunctioning backend - read fails
	proxy.Registry = broken
	numHits, numMisses, numErrs, err = redistest.ReadTestPattern(client, 1, keyTempl, sleep, 2*time.Second)
	t.Logf("broken backend n read: numHits = %d, numMisses = %d, numErrs = %d, err = %v", numHits, numMisses, numErrs, err)
	assert.NotNil(t, err, "Broken backend - errors")
	assert.Equal(t, 0, numHits, "Broken backend - none hit")
	assert.Equal(t, 0, numMisses, "Broken backend - none missed")
	assert.Equal(t, 1, numErrs, "Broken backend - all errors")

	// single backend dropped (all combinations) - read n back
	accHits, accMisses := 0, 0
	for i := 0; i < len(onedowns); i++ {
		proxy.Registry = onedowns[i]
		numHits, numMisses, numErrs, err := redistest.ReadTestPattern(client, n, keyTempl, sleep, timeout)
		t.Logf("single backend dropped (%d): numHits = %d, numMisses = %d, numErrs = %d, %d%% miss rate, err = %v", i, numHits, numMisses, numErrs, 100*numMisses/n, err)
		assert.Nil(t, err, "One down - no errors")
		assert.True(t, numHits < n, "One down - not all hit")
		assert.True(t, numHits > 0, "One down - some hit")
		assert.True(t, numMisses < n, "One down - not all missed")
		assert.True(t, numMisses > 0, "One down - some missed")
		assert.Equal(t, n, numHits+numMisses, "One down - hits and misses adds up")
		assert.Equal(t, 0, numErrs, "One down - no errors")
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
