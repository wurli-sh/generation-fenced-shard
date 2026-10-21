// Package ownership publishes immutable effective routes. Candidate placement
// is kept in placement.Topology and never grants write authority.
package ownership

import (
	"fmt"
	"sync/atomic"

	"generation-fenced-shard-transfer/internal/placement"
)

type Route struct {
	Partition  uint32
	Owner      string
	Generation uint64
}

type Snapshot struct {
	Version uint64
	routes  []Route
}

func Initial(topology *placement.Topology, version uint64) (*Snapshot, error) {
	if topology == nil || version == 0 {
		return nil, fmt.Errorf("topology and positive version required")
	}
	c := topology.Config()
	routes := make([]Route, c.PartitionCount)
	for p := range routes {
		owner, err := topology.Owner(uint32(p))
		if err != nil {
			return nil, err
		}
		routes[p] = Route{Partition: uint32(p), Owner: owner, Generation: 1}
	}
	return &Snapshot{Version: version, routes: routes}, nil
}

func (s *Snapshot) Count() uint32 { return uint32(len(s.routes)) }
func (s *Snapshot) Route(partition uint32) (Route, error) {
	if s == nil || partition >= uint32(len(s.routes)) {
		return Route{}, fmt.Errorf("partition %d out of range", partition)
	}
	return s.routes[partition], nil
}

// WithCommitted applies one durable control decision. Callers must verify the
// decision in the control database before publishing this local copy.
func (s *Snapshot) WithCommitted(version uint64, old Route, next Route) (*Snapshot, error) {
	if s == nil || version <= s.Version || old.Partition != next.Partition || old.Partition >= uint32(len(s.routes)) ||
		s.routes[old.Partition] != old || next.Owner == "" || next.Generation != old.Generation+1 || next.Owner == old.Owner {
		return nil, fmt.Errorf("invalid ownership transition")
	}
	routes := make([]Route, len(s.routes))
	copy(routes, s.routes)
	routes[old.Partition] = next
	return &Snapshot{Version: version, routes: routes}, nil
}

type Publisher struct{ active atomic.Pointer[Snapshot] }

func NewPublisher(initial *Snapshot) (*Publisher, error) {
	if initial == nil {
		return nil, fmt.Errorf("initial snapshot required")
	}
	p := &Publisher{}
	p.active.Store(initial)
	return p, nil
}
func (p *Publisher) Load() *Snapshot { return p.active.Load() }
func (p *Publisher) Publish(old, next *Snapshot) bool {
	if old == nil || next == nil || next.Version <= old.Version {
		return false
	}
	return p.active.CompareAndSwap(old, next)
}
