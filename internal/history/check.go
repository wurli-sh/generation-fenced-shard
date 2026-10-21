// Package history checks completed, sequential experiment histories. It does
// not infer outcomes for timed-out requests or prove concurrent linearizability.
package history

import (
	"fmt"
	"sort"
)

type Event struct {
	Kind       string `json:"kind"` // write or read
	Partition  uint32 `json:"partition"`
	Entity     string `json:"entity"`
	Operation  string `json:"operation,omitempty"`
	Shard      string `json:"shard"`
	Generation uint64 `json:"generation"`
	Sequence   uint64 `json:"sequence,omitempty"`
	Version    uint64 `json:"version"`
	Minimum    uint64 `json:"minimum,omitempty"`
	Value      string `json:"value,omitempty"`
	Deleted    bool   `json:"deleted,omitempty"`
	InvokedNS  int64  `json:"invoked_ns,omitempty"`
	FinishedNS int64  `json:"finished_ns,omitempty"`
}

type Fence struct {
	Partition     uint32 `json:"partition"`
	Source        string `json:"source"`
	Target        string `json:"target"`
	OldGeneration uint64 `json:"old_generation"`
	Sequence      uint64 `json:"sequence"`
}

type State struct {
	Version uint64 `json:"version"`
	Value   string `json:"value,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

type Finding struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// Check examines successful operations and final authoritative state. Events
// must be recorded by a sequential workload; a concurrent run needs a full
// invocation/completion history and a separate linearizability checker.
func Check(events []Event, fences []Fence, final map[string]State) []Finding {
	var findings []Finding
	add := func(code, detail string) { findings = append(findings, Finding{code, detail}) }
	byPartition := make(map[uint32][]Event)
	fencesByPartition := make(map[uint32][]Fence)
	for _, f := range fences {
		fencesByPartition[f.Partition] = append(fencesByPartition[f.Partition], f)
	}
	for partition := range fencesByPartition {
		fs := fencesByPartition[partition]
		sort.Slice(fs, func(i, j int) bool { return fs[i].OldGeneration < fs[j].OldGeneration })
		for i := 1; i < len(fs); i++ {
			if fs[i].OldGeneration != fs[i-1].OldGeneration+1 || fs[i].Source != fs[i-1].Target || fs[i].Sequence <= fs[i-1].Sequence {
				add("invalid_fence", fmt.Sprintf("partition %d has an inconsistent handoff chain", partition))
			}
		}
	}
	for _, e := range events {
		if e.Kind == "read" {
			if e.Version < e.Minimum {
				add("session_version", fmt.Sprintf("%s returned %d below minimum %d", e.Entity, e.Version, e.Minimum))
			}
			continue
		}
		if e.Kind != "write" || e.Operation == "" || e.Sequence == 0 || e.Version == 0 {
			add("invalid_event", fmt.Sprintf("incomplete operation for %s", e.Entity))
			continue
		}
		byPartition[e.Partition] = append(byPartition[e.Partition], e)
	}
	for partition, writes := range byPartition {
		sort.Slice(writes, func(i, j int) bool { return writes[i].Sequence < writes[j].Sequence })
		seen := make(map[string]Event)
		versions := make(map[string]uint64)
		states := make(map[string]State)
		var lastSeq uint64
		for _, e := range writes {
			if prior, ok := seen[e.Operation]; ok {
				if prior.Sequence != e.Sequence || prior.Version != e.Version || prior.Entity != e.Entity || prior.Value != e.Value || prior.Deleted != e.Deleted {
					add("duplicate_effect", fmt.Sprintf("operation %s changed result", e.Operation))
				}
				continue
			}
			seen[e.Operation] = e
			if e.Sequence != lastSeq+1 {
				add("sequence_gap", fmt.Sprintf("partition %d: expected %d, saw %d", partition, lastSeq+1, e.Sequence))
			}
			lastSeq = e.Sequence
			if e.Version != versions[e.Entity]+1 {
				add("entity_version", fmt.Sprintf("%s: expected %d, saw %d", e.Entity, versions[e.Entity]+1, e.Version))
			}
			versions[e.Entity] = e.Version
			states[e.Entity] = State{Version: e.Version, Value: e.Value, Deleted: e.Deleted}
			if fs := fencesByPartition[partition]; len(fs) > 0 {
				expectedShard, expectedGeneration := fs[0].Source, fs[0].OldGeneration
				for _, f := range fs {
					if e.Sequence <= f.Sequence {
						break
					}
					expectedShard, expectedGeneration = f.Target, f.OldGeneration+1
				}
				if e.Shard != expectedShard || e.Generation != expectedGeneration {
					add("authority", fmt.Sprintf("%s: sequence %d admitted at %s/%d, expected %s/%d", e.Operation, e.Sequence, e.Shard, e.Generation, expectedShard, expectedGeneration))
				}
			}
		}
		for entity, expected := range states {
			actual, ok := final[entity]
			if !ok || actual != expected {
				add("lost_acknowledged_effect", fmt.Sprintf("%s: expected %+v, got %+v", entity, expected, actual))
			}
		}
	}
	return findings
}
