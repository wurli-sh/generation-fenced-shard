// Package placement provides deterministic candidate placement.
package placement

import (
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// RingSpec identifies the virtual-node key format and hash function.
const RingSpec = "xxhash64/vnode-shard-index/v2"

type ringPoint struct {
	hash  uint64
	shard string
	index int
}

// Baseline is a fixed-membership ring, published only after construction.
type Baseline struct {
	points []ringPoint
}

func NewBaseline(shards []string, virtualNodes int) (*Baseline, error) {
	if virtualNodes <= 0 {
		return nil, fmt.Errorf("virtual nodes must be positive")
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("at least one shard is required")
	}
	ids := slices.Clone(shards)
	slices.Sort(ids)
	points := make([]ringPoint, 0, len(ids)*virtualNodes)
	for i, id := range ids {
		if id == "" || (i > 0 && id == ids[i-1]) {
			return nil, fmt.Errorf("shard IDs must be nonempty and unique")
		}
		for replica := 0; replica < virtualNodes; replica++ {
			key := id + "\x00" + strconv.Itoa(replica)
			points = append(points, ringPoint{hash: xxhash.Sum64String(key), shard: id, index: replica})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		if points[i].shard != points[j].shard {
			return points[i].shard < points[j].shard
		}
		return points[i].index < points[j].index
	})
	return &Baseline{points: points}, nil
}

// Lookup returns proposed placement only; it grants no permission to write.
func (b *Baseline) Lookup(key string) string {
	hash := xxhash.Sum64String(key)
	i := sort.Search(len(b.points), func(i int) bool { return b.points[i].hash >= hash })
	if i == len(b.points) {
		i = 0
	}
	return b.points[i].shard
}
