package redis

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestHashRing(t *testing.T) {
	ring2 := makeHashRing(
		ServerEndpoint{Addr: "127.0.0.1:1000"},
		ServerEndpoint{Addr: "127.0.0.1:1001"},
	)

	ring3 := makeHashRing(
		ServerEndpoint{Addr: "127.0.0.1:1000"},
		ServerEndpoint{Addr: "127.0.0.1:1001"},
		ServerEndpoint{Addr: "127.0.0.1:1002"},
	)

	ring4 := makeHashRing(
		ServerEndpoint{Addr: "127.0.0.1:1000"},
		ServerEndpoint{Addr: "127.0.0.1:1001"},
		ServerEndpoint{Addr: "127.0.0.1:1002"},
		ServerEndpoint{Addr: "127.0.0.1:1003"},
	)

	ring5 := makeHashRing(
		ServerEndpoint{Addr: "127.0.0.1:1000"},
		ServerEndpoint{Addr: "127.0.0.1:1001"},
		ServerEndpoint{Addr: "127.0.0.1:1002"},
		ServerEndpoint{Addr: "127.0.0.1:1003"},
		ServerEndpoint{Addr: "127.0.0.1:1005"},
	)

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = strconv.Itoa(rand.Int())
	}

	for _, dist := range []struct {
		ringA hashRing
		ringB hashRing
	}{{ring2, ring3}, {ring3, ring4}, {ring4, ring5}, {ring3, ring5}} {
		countA := len(dist.ringA) / hashRingReplication
		countB := len(dist.ringB) / hashRingReplication
		distA := distribute(dist.ringA, keys...)
		distB := distribute(dist.ringB, keys...)
		diff := difference(distA, distB)

		switch n := (100 * len(diff)) / len(keys); {
		case n == 0:
			t.Errorf("going from %d to %d servers should have redistributed keys", countA, countB)
		case n == 100:
			t.Errorf("going from %d to %d servers should not have redistributed all the keys", countA, countB)
		default:
			t.Logf("going from %d to %d servers redistributed ~%d%% of the keys (%d/%d)", countA, countB, n, len(diff), len(keys))
		}
	}
}

func distribute(ring hashRing, keys ...string) map[string]string {
	dist := make(map[string]string)

	for _, k := range keys {
		dist[k] = ring.lookup(k)
	}

	return dist
}

func difference(dist1, dist2 map[string]string) map[string]struct{} {
	diff := make(map[string]struct{})

	for key1, addr1 := range dist1 {
		if addr2 := dist2[key1]; addr1 != addr2 {
			diff[key1] = struct{}{}
		}
	}

	for key2, addr2 := range dist2 {
		if addr1 := dist1[key2]; addr1 != addr2 {
			diff[key2] = struct{}{}
		}
	}

	return diff
}

func BenchmarkHashRing(b *testing.B) {
	ring := makeHashRing(
		ServerEndpoint{Addr: "127.0.0.1:1000"},
		ServerEndpoint{Addr: "127.0.0.1:1001"},
		ServerEndpoint{Addr: "127.0.0.1:1002"},
		ServerEndpoint{Addr: "127.0.0.1:1003"},
		ServerEndpoint{Addr: "127.0.0.1:1004"},
		ServerEndpoint{Addr: "127.0.0.1:1005"},
		ServerEndpoint{Addr: "127.0.0.1:1006"},
		ServerEndpoint{Addr: "127.0.0.1:1007"},
		ServerEndpoint{Addr: "127.0.0.1:1008"},
		ServerEndpoint{Addr: "127.0.0.1:1009"},
	)

	for i := 0; i != b.N; i++ {
		ring.lookup("DAB45194-42CC-4106-AB9F-2447FA4D35C2")
	}
}
