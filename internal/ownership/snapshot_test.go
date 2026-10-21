package ownership

import (
	"fmt"
	"sync"
	"testing"

	"generation-fenced-shard-transfer/internal/placement"
)

func TestCandidateDoesNotChangeAuthority(t *testing.T) {
	base, _ := placement.NewTopology(placement.Config{PartitionCount: 1024, VirtualNodes: 50, Shards: []string{"A", "B"}})
	candidate, _ := placement.NewTopology(placement.Config{PartitionCount: 1024, VirtualNodes: 50, Shards: []string{"A", "B", "C"}})
	s, err := Initial(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := NewPublisher(s)
	changes, err := placement.Diff(base, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) == 0 {
		t.Fatal("no candidate changes")
	}
	first := changes[0]
	old, _ := pub.Load().Route(first.Partition)
	if old.Owner == first.Shard {
		t.Fatal("candidate already owns partition")
	}
	next := Route{Partition: first.Partition, Owner: first.Shard, Generation: old.Generation + 1}
	revised, err := s.WithCommitted(2, old, next)
	if err != nil {
		t.Fatal(err)
	}
	if !pub.Publish(s, revised) {
		t.Fatal("publication failed")
	}
	if pub.Publish(s, revised) {
		t.Fatal("stale publication succeeded")
	}
	if got, _ := s.Route(first.Partition); got != old {
		t.Fatal("old snapshot mutated")
	}
	for p := uint32(0); p < s.Count(); p++ {
		if p == first.Partition {
			continue
		}
		a, _ := s.Route(p)
		b, _ := revised.Route(p)
		if a != b {
			t.Fatalf("unmoved partition %d changed", p)
		}
	}
}

func TestConcurrentReaders(t *testing.T) {
	topology, _ := placement.NewTopology(placement.Config{PartitionCount: 64, VirtualNodes: 10, Shards: []string{"A", "B"}})
	s, _ := Initial(topology, 1)
	pub, _ := NewPublisher(s)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				p := uint32(j % 64)
				if r, err := pub.Load().Route(p); err != nil || r.Owner == "" {
					t.Errorf("bad route: %+v %v", r, err)
					return
				}
			}
		}()
	}
	old, _ := s.Route(0)
	next, _ := s.WithCommitted(2, old, Route{Partition: 0, Owner: "C", Generation: 2})
	pub.Publish(s, next)
	wg.Wait()
}

var routeSink Route

func BenchmarkPartitionRoute(b *testing.B) {
	topology, err := placement.NewTopology(placement.Config{PartitionCount: 4096, VirtualNodes: 100, Shards: []string{"A", "B", "C"}})
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := Initial(topology, 1)
	if err != nil {
		b.Fatal(err)
	}
	pub, _ := NewPublisher(snapshot)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("entity-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		partition, _ := placement.PartitionFor(keys[i%len(keys)], snapshot.Count())
		routeSink, _ = pub.Load().Route(partition)
	}
}
