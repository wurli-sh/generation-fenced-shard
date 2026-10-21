package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPoolRetirementDrainsLeases(t *testing.T) {
	dsn := os.Getenv("WHM_TEST_B_DSN")
	if dsn == "" {
		t.Skip("WHM_TEST_B_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := NewPoolRegistry()
	if err := r.Add(ctx, "B", dsn); err != nil {
		t.Fatal(err)
	}
	lease, err := r.Acquire("B")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Retire("B"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire("B"); err == nil {
		t.Fatal("acquired retiring pool")
	}
	if err := lease.DB.Ping(ctx); err != nil {
		t.Fatalf("in-flight lease closed early: %v", err)
	}
	lease.Release()
	lease.Release()
	if err := lease.DB.Ping(ctx); err == nil {
		t.Fatal("retired pool stayed open")
	}
}

func TestPoolConcurrentAcquireAndRetire(t *testing.T) {
	dsn := os.Getenv("WHM_TEST_B_DSN")
	if dsn == "" {
		t.Skip("WHM_TEST_B_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := NewPoolRegistry()
	if err := r.Add(ctx, "B", dsn); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lease, err := r.Acquire("B")
				if err != nil {
					return
				}
				if err := lease.DB.Ping(ctx); err != nil {
					t.Errorf("acquired closed pool: %v", err)
				}
				lease.Release()
			}
		}()
	}
	if err := r.Retire("B"); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
