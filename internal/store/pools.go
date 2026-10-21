package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolRegistry owns shard connections. A lease prevents a pool from closing
// while a request uses it. Call Retire only after its migrations finish.
type PoolRegistry struct {
	mu      sync.Mutex
	entries map[string]*poolEntry
}

type poolEntry struct {
	pool     *pgxpool.Pool
	users    int
	retiring bool
}

type PoolLease struct {
	DB       *pgxpool.Pool
	registry *PoolRegistry
	entry    *poolEntry
	once     sync.Once
}

func NewPoolRegistry() *PoolRegistry { return &PoolRegistry{entries: make(map[string]*poolEntry)} }

func (r *PoolRegistry) Add(ctx context.Context, id, dsn string) error {
	if r == nil || id == "" || dsn == "" {
		return fmt.Errorf("invalid pool registration")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return err
	}
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[string]*poolEntry)
	}
	if _, exists := r.entries[id]; exists {
		r.mu.Unlock()
		pool.Close()
		return fmt.Errorf("shard %q already registered", id)
	}
	r.entries[id] = &poolEntry{pool: pool}
	r.mu.Unlock()
	return nil
}

func (r *PoolRegistry) Acquire(id string) (*PoolLease, error) {
	if r == nil {
		return nil, fmt.Errorf("pool registry is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	if !ok || e.retiring {
		return nil, fmt.Errorf("shard %q is not available", id)
	}
	e.users++
	return &PoolLease{DB: e.pool, registry: r, entry: e}, nil
}

func (l *PoolLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		r := l.registry
		r.mu.Lock()
		l.entry.users--
		closeNow := l.entry.retiring && l.entry.users == 0
		r.mu.Unlock()
		if closeNow {
			l.entry.pool.Close()
		}
	})
}

// Retire prevents new acquisitions immediately. It may return while existing
// users still hold leases; the final release closes the pool.
func (r *PoolRegistry) Retire(id string) error {
	if r == nil {
		return fmt.Errorf("pool registry is nil")
	}
	r.mu.Lock()
	e, ok := r.entries[id]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("shard %q is not registered", id)
	}
	delete(r.entries, id)
	e.retiring = true
	closeNow := e.users == 0
	r.mu.Unlock()
	if closeNow {
		e.pool.Close()
	}
	return nil
}
