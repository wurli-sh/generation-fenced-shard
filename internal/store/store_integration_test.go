package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"generation-fenced-shard-transfer/internal/ownership"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAdmissionRollbackAndPermissions(t *testing.T) {
	dsn := os.Getenv("WHM_TEST_A_DSN")
	if dsn == "" {
		t.Skip("WHM_TEST_A_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const partition = 778
	_, err = db.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, partition)
	if err != nil {
		t.Fatal(err)
	}
	route := ownership.Route{Partition: partition, Owner: "A", Generation: 1}
	digest := Digest("x", []byte("rolled back"), false)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var version, seq int64
	var deleted bool
	err = tx.QueryRow(ctx, `SELECT * FROM whm_write($1,$2,$3,$4,$5,$6,$7)`, partition, 1, "x", "00000000-0000-4000-8000-000000000201", digest[:], []byte("rolled back"), false).Scan(&version, &seq, &deleted)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var committed int64
	if err := db.QueryRow(ctx, `SELECT committed_seq FROM partition_guards WHERE partition_id=$1`, partition).Scan(&committed); err != nil || committed != 0 {
		t.Fatalf("rollback advanced sequence: %d %v", committed, err)
	}
	s := &Store{DB: db}
	if _, err := s.Read(ctx, route, "x", 1); !errors.Is(err, ErrVersionNotReady) {
		t.Fatalf("rolled back read: %v", err)
	}
	result, err := s.Write(ctx, route, "00000000-0000-4000-8000-000000000201", "x", []byte("rolled back"), false)
	if err != nil || result.PartitionSeq != 1 {
		t.Fatalf("write after rollback: %+v %v", result, err)
	}
	if _, err := s.Read(ctx, route, "x", 2); !errors.Is(err, ErrVersionNotReady) {
		t.Fatalf("newer session token: %v", err)
	}
	if _, err := s.Write(ctx, ownership.Route{Partition: partition, Owner: "A", Generation: 2}, "00000000-0000-4000-8000-000000000202", "x", []byte("stale"), false); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("stale generation: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SET ROLE whm_app`); err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, `UPDATE entities SET payload=decode('00','hex') WHERE partition_id=$1`, partition)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("direct DML was not denied: %v", err)
	}
	var observed int64
	if err := conn.QueryRow(ctx, `SELECT entity_version FROM whm_read($1,$2,$3,$4)`, partition, 1, "x", 1).Scan(&observed); err != nil || observed != 1 {
		t.Fatalf("restricted role cannot read through function: %d %v", observed, err)
	}
	if _, err := conn.Exec(ctx, `SELECT whm_fence($1,$2,$3)`, partition, 1, "00000000-0000-4000-8000-000000000203"); !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("application role can fence: %v", err)
	}
}
