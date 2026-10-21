package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// Config fixes the placement algorithm for one deployment. PartitionCount must
// not change while entities exist; changing it needs a separate migration.
type Config struct {
	PartitionCount uint32
	VirtualNodes   int
	Shards         []string
}

type Assignment struct {
	Partition uint32
	Shard     string
}

type Topology struct {
	config Config
	owners []string
	digest string
}

func PartitionFor(entityID string, count uint32) (uint32, error) {
	if count == 0 || entityID == "" {
		return 0, fmt.Errorf("partition count and entity ID must be nonzero")
	}
	return uint32(xxhash.Sum64String(entityID) % uint64(count)), nil
}

// PartitionKey is independent of platform endianness and integer formatting.
func PartitionKey(partition uint32) string {
	return "partition/" + strconv.FormatUint(uint64(partition), 10)
}

func NewTopology(c Config) (*Topology, error) {
	if c.PartitionCount == 0 || c.PartitionCount > math.MaxInt32 {
		return nil, fmt.Errorf("partition count must fit PostgreSQL integer")
	}
	if c.VirtualNodes <= 0 || uint64(c.VirtualNodes) > math.MaxUint32 {
		return nil, fmt.Errorf("virtual node count out of range")
	}
	ids := slices.Clone(c.Shards)
	slices.Sort(ids)
	baseline, err := NewBaseline(ids, c.VirtualNodes)
	if err != nil {
		return nil, err
	}
	c.Shards = ids
	owners := make([]string, c.PartitionCount)
	for p := range owners {
		owners[p] = baseline.Lookup(PartitionKey(uint32(p)))
	}
	h := sha256.New()
	h.Write([]byte(RingSpec + "/partition-decimal\x00"))
	var n [8]byte
	binary.BigEndian.PutUint32(n[:4], c.PartitionCount)
	binary.BigEndian.PutUint32(n[4:], uint32(c.VirtualNodes))
	h.Write(n[:])
	for _, id := range ids {
		binary.BigEndian.PutUint32(n[:4], uint32(len(id)))
		h.Write(n[:4])
		h.Write([]byte(id))
	}
	return &Topology{config: c, owners: owners, digest: hex.EncodeToString(h.Sum(nil))}, nil
}

func (t *Topology) Digest() string { return t.digest }
func (t *Topology) Config() Config {
	c := t.config
	c.Shards = slices.Clone(c.Shards)
	return c
}
func (t *Topology) Owner(partition uint32) (string, error) {
	if partition >= uint32(len(t.owners)) {
		return "", fmt.Errorf("partition %d out of range", partition)
	}
	return t.owners[partition], nil
}

// Diff reports only partitions whose desired placement changes. It never
// changes current authority.
func Diff(before, after *Topology) ([]Assignment, error) {
	if before == nil || after == nil || len(before.owners) != len(after.owners) || before.config.VirtualNodes != after.config.VirtualNodes {
		return nil, fmt.Errorf("incompatible partition topology")
	}
	var changes []Assignment
	for p, owner := range after.owners {
		if owner != before.owners[p] {
			changes = append(changes, Assignment{Partition: uint32(p), Shard: owner})
		}
	}
	return changes, nil
}
