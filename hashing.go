package redis

import (
	"sort"

	"github.com/segmentio/fasthash/fnv1a"
)

// hashRing is the implementation of a consistent hashing distribution of string
// keys to server addresses.
type hashRing []hashNode

type hashNode struct {
	addr string
	hash uint64
}

const (
	hashRingReplication = 40
)

func makeHashRing(endpoints ...ServerEndpoint) hashRing {
	if len(endpoints) == 0 {
		return nil
	}

	ring := make(hashRing, 0, hashRingReplication*len(endpoints))

	for _, endpoint := range endpoints {
		h := fnv1a.HashString64(endpoint.Addr)

		for i := 0; i != hashRingReplication; i++ {
			ring = append(ring, hashNode{
				addr: endpoint.Addr,
				hash: consistentHash(fnv1a.AddUint64(h, uint64(i))),
			})
		}
	}

	sort.Sort(ring)
	return ring
}

func (ring hashRing) lookup(key string) (addr string) {
	n := len(ring)
	h := consistentHash(fnv1a.HashString64(key))
	i := sort.Search(n, func(i int) bool { return h < ring[i].hash })

	if i == n {
		i = 0
	}

	addr = ring[i].addr
	return
}

func (r hashRing) Len() int {
	return len(r)
}

func (r hashRing) Less(i int, j int) bool {
	return r[i].hash < r[j].hash
}

func (r hashRing) Swap(i int, j int) {
	r[i], r[j] = r[j], r[i]
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
