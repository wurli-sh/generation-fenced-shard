package history

import "testing"

func TestCheckerDetectsKnownFaults(t *testing.T) {
	base := []Event{
		{Kind: "write", Partition: 1, Entity: "e", Operation: "one", Shard: "A", Generation: 1, Sequence: 1, Version: 1, Value: "v1"},
		{Kind: "write", Partition: 1, Entity: "e", Operation: "two", Shard: "B", Generation: 2, Sequence: 2, Version: 2, Value: "v2"},
		{Kind: "read", Partition: 1, Entity: "e", Shard: "B", Generation: 2, Version: 2, Minimum: 2, Value: "v2"},
	}
	fences := []Fence{{Partition: 1, Source: "A", Target: "B", OldGeneration: 1, Sequence: 1}}
	final := map[string]State{"e": {Version: 2, Value: "v2"}}
	if got := Check(base, fences, final); len(got) != 0 {
		t.Fatalf("valid history: %+v", got)
	}
	for _, tc := range []struct {
		name string
		edit func([]Event) []Event
		want string
	}{
		{"lost_effect", func(e []Event) []Event { return e }, "lost_acknowledged_effect"},
		{"duplicate", func(e []Event) []Event {
			return append(e, Event{Kind: "write", Partition: 1, Entity: "e", Operation: "one", Shard: "B", Generation: 2, Sequence: 3, Version: 3, Value: "v3"})
		}, "duplicate_effect"},
		{"stale_writer", func(e []Event) []Event { e[1].Shard, e[1].Generation = "A", 1; return e }, "authority"},
		{"session_regression", func(e []Event) []Event { e[2].Version = 1; return e }, "session_version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := append([]Event(nil), base...)
			actualFinal := final
			if tc.name == "lost_effect" {
				actualFinal = map[string]State{"e": {Version: 1, Value: "v1"}}
			}
			var found bool
			for _, f := range Check(tc.edit(events), fences, actualFinal) {
				if f.Code == tc.want {
					found = true
				}
			}
			if !found {
				t.Fatalf("checker missed %s", tc.want)
			}
		})
	}
}

func TestCheckerAcceptsReturnMigration(t *testing.T) {
	events := []Event{
		{Kind: "write", Partition: 1, Entity: "e", Operation: "one", Shard: "A", Generation: 1, Sequence: 1, Version: 1, Value: "v1"},
		{Kind: "write", Partition: 1, Entity: "e", Operation: "two", Shard: "B", Generation: 2, Sequence: 2, Version: 2, Value: "v2"},
		{Kind: "write", Partition: 1, Entity: "e", Operation: "three", Shard: "A", Generation: 3, Sequence: 3, Version: 3, Value: "v3"},
	}
	fences := []Fence{
		{Partition: 1, Source: "A", Target: "B", OldGeneration: 1, Sequence: 1},
		{Partition: 1, Source: "B", Target: "A", OldGeneration: 2, Sequence: 2},
	}
	if got := Check(events, fences, map[string]State{"e": {Version: 3, Value: "v3"}}); len(got) != 0 {
		t.Fatalf("valid return migration: %+v", got)
	}
}
