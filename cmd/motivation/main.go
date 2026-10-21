// motivation reproduces the immediate-reroute missing-row failure against two
// isolated PostgreSQL connections. It only creates connection-local temp tables.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"generation-fenced-shard-transfer/internal/placement"
	"github.com/jackc/pgx/v5"
)

func main() {
	aDSN, bDSN := os.Getenv("WHM_TEST_A_DSN"), os.Getenv("WHM_TEST_B_DSN")
	if aDSN == "" || bDSN == "" {
		log.Fatal("set WHM_TEST_A_DSN and WHM_TEST_B_DSN to isolated PostgreSQL databases")
	}
	ctx := context.Background()
	a, err := pgx.Connect(ctx, aDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer a.Close(ctx)
	b, err := pgx.Connect(ctx, bDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close(ctx)
	for _, db := range []*pgx.Conn{a, b} {
		if _, err := db.Exec(ctx, `CREATE TEMP TABLE motivation_rows(entity_id text PRIMARY KEY,payload text NOT NULL)`); err != nil {
			log.Fatal(err)
		}
	}
	old, err := placement.NewTopology(placement.Config{PartitionCount: 256, VirtualNodes: 100, Shards: []string{"A"}})
	if err != nil {
		log.Fatal(err)
	}
	next, err := placement.NewTopology(placement.Config{PartitionCount: 256, VirtualNodes: 100, Shards: []string{"A", "B"}})
	if err != nil {
		log.Fatal(err)
	}
	changes, err := placement.Diff(old, next)
	if err != nil || len(changes) == 0 {
		log.Fatalf("topology diff: %v", err)
	}
	moved := changes[0].Partition
	var entity string
	for i := 0; i < 100000; i++ {
		candidate := fmt.Sprintf("entity-%d", i)
		p, _ := placement.PartitionFor(candidate, 256)
		if p == moved {
			entity = candidate
			break
		}
	}
	if entity == "" {
		log.Fatal("could not find entity for moved partition")
	}
	if _, err := a.Exec(ctx, `INSERT INTO motivation_rows(entity_id,payload) VALUES($1,'acknowledged-value')`, entity); err != nil {
		log.Fatal(err)
	}
	var before string
	if err := a.QueryRow(ctx, `SELECT payload FROM motivation_rows WHERE entity_id=$1`, entity).Scan(&before); err != nil {
		log.Fatal(err)
	}
	var after string
	err = b.QueryRow(ctx, `SELECT payload FROM motivation_rows WHERE entity_id=$1`, entity).Scan(&after)
	if err != pgx.ErrNoRows {
		log.Fatalf("expected missing row at B, got %q: %v", after, err)
	}
	fmt.Printf("entity=%s partition=%d before=A value=%s candidate=B immediate_read=missing\n", entity, moved, before)
}
