package redis

import (
	"fmt"
	"sort"
	"strings"
)

// hashRing is the implementation of a consistent hashing distribution of string
// keys to server addresses.
type hashRing []hashNode

func (r hashRing) Len() int {
	return len(r)
}

func (r hashRing) Less(i int, j int) bool {
	return r[i].hash < r[j].hash
}

func (r hashRing) Swap(i int, j int) {
	r[i], r[j] = r[j], r[i]
}

type hashNode struct {
	addr string
	hash uint64
}

const (
	hashRingReplication = 100
)

func makeHashRing(endpoints ...ServerEndpoint) hashRing {
	if len(endpoints) == 0 {
		return nil
	}

	ring := make(hashRing, 0, hashRingReplication*len(endpoints))

	for _, endpoint := range endpoints {
		h := hash(endpoint.Addr)

		for i := 0; i != hashRingReplication; i++ {
			ring = append(ring, hashNode{
				addr: endpoint.Addr,
				hash: consistentHash(hashN(h, uint64(i))),
			})
		}
	}

	sort.Sort(ring)
	return ring
}

func (ring hashRing) lookup(key string) (addr string) {
	n := len(ring)
	h := consistentHash(hash(key))
	i := sort.Search(n, func(i int) bool { return h < ring[i].hash })

	if i == n {
		i = 0
	}

	addr = ring[i].addr
	return
}

func (ring hashRing) String() string {
	parts := make([]string, 0, len(ring))
	for _, node := range ring {
		parts = append(parts, fmt.Sprintf("- %s: %d", node.addr, node.hash))
	}
	return strings.Join(parts, "\n")
}

// much better throughput because it doesn't force memory allocation to mask
// the hash sum into an interface and convert the string to a byte slice.
const (
	// FNV-1a
	offset64 = uint64(14695981039346656037)
	prime64  = uint64(1099511628211)
)

func consistentHash(h uint64) uint64 {
	const radix = 1e9
	return h % radix
}

func hash(s string) uint64 {
	return hashS(offset64, s)
}

func hashS(h uint64, s string) uint64 {
	for i := range s {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

func hashN(h uint64, n uint64) uint64 {
	for i := 0; i != 8; i++ {
		h ^= (n >> uint64(i*8)) & 0xFF
		h *= prime64
	}
	return h
}
