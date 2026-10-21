package placement

import (
	"fmt"
	"slices"
	"testing"
)

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		shards []string
		vnodes int
	}{
		{nil, 100}, {[]string{"A"}, 0}, {[]string{"A"}, -1},
		{[]string{""}, 100}, {[]string{"A", "A"}, 100},
	} {
		if _, err := NewBaseline(tc.shards, tc.vnodes); err == nil {
			t.Errorf("accepted invalid configuration: %+v", tc)
		}
	}
}

func TestCanonicalConstructionAndAddition(t *testing.T) {
	ids := []string{"C", "A", "B"}
	original := slices.Clone(ids)
	a, err := NewBaseline(ids, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, original) {
		t.Fatal("constructor mutated caller's shard list")
	}
	b, err := NewBaseline([]string{"A", "B", "C"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewBaseline([]string{"D", "C", "B", "A"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	moved := 0
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("entity-%d", i)
		old := a.Lookup(key)
		if old != b.Lookup(key) {
			t.Fatal("placement depends on input ordering")
		}
		if next := c.Lookup(key); next != old {
			moved++
			if next != "D" {
				t.Fatalf("addition moved %q between old shards", key)
			}
		}
	}
	if moved == 0 || moved == 10000 {
		t.Fatalf("unexpected movement count: %d", moved)
	}
}

var lookupSink string

func BenchmarkLookup(b *testing.B) {
	for _, shardCount := range []int{3, 10, 100} {
		b.Run(fmt.Sprintf("%d-shards", shardCount), func(b *testing.B) {
			ids := make([]string, shardCount)
			for i := range ids {
				ids[i] = fmt.Sprintf("shard-%d", i)
			}
			r, err := NewBaseline(ids, 100)
			if err != nil {
				b.Fatal(err)
			}
			keys := make([]string, 4096)
			for i := range keys {
				keys[i] = fmt.Sprintf("entity-%d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lookupSink = r.Lookup(keys[i%len(keys)])
			}
		})
	}
}
