package placement

import (
	"slices"
	"testing"
)

func TestTopologyDeterminismAndDiff(t *testing.T) {
	c := Config{PartitionCount: 4096, VirtualNodes: 100, Shards: []string{"C", "A", "B"}}
	a, err := NewTopology(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Shards = []string{"B", "C", "A"}
	b, err := NewTopology(c)
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest() != b.Digest() {
		t.Fatal("digest depends on input order")
	}
	for p := uint32(0); p < c.PartitionCount; p++ {
		x, _ := a.Owner(p)
		y, _ := b.Owner(p)
		if x != y {
			t.Fatalf("partition %d changed", p)
		}
	}
	c.Shards = append(c.Shards, "D")
	d, err := NewTopology(c)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := Diff(a, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) == 0 || len(changes) == int(c.PartitionCount) {
		t.Fatalf("unexpected movement: %d", len(changes))
	}
	for _, change := range changes {
		if change.Shard != "D" {
			t.Fatalf("addition moved partition %d to existing shard", change.Partition)
		}
	}
	if !slices.Equal(a.Config().Shards, []string{"A", "B", "C"}) {
		t.Fatal("configuration not canonical")
	}
}

func TestPartitionInputValidation(t *testing.T) {
	if _, err := PartitionFor("", 10); err == nil {
		t.Fatal("empty entity accepted")
	}
	if _, err := PartitionFor("x", 0); err == nil {
		t.Fatal("zero partitions accepted")
	}
	if _, err := NewTopology(Config{PartitionCount: 0, VirtualNodes: 1, Shards: []string{"A"}}); err == nil {
		t.Fatal("zero partitions accepted")
	}
	if _, err := NewTopology(Config{PartitionCount: 1, VirtualNodes: 0, Shards: []string{"A"}}); err == nil {
		t.Fatal("zero virtual nodes accepted")
	}
}
