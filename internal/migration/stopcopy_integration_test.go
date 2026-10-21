package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"generation-fenced-shard-transfer/internal/ownership"
	"generation-fenced-shard-transfer/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Run against three isolated databases with db/001-control.sql on control and
// db/002-shard.sql plus db/003-admission.sql on each shard.
func TestStopCopyWithRestartAndReturn(t *testing.T) {
	controlDSN, aDSN, bDSN := os.Getenv("WHM_TEST_CONTROL_DSN"), os.Getenv("WHM_TEST_A_DSN"), os.Getenv("WHM_TEST_B_DSN")
	if controlDSN == "" || aDSN == "" || bDSN == "" {
		t.Skip("three PostgreSQL test DSNs required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	open := func(dsn string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	control, a, b := open(controlDSN), open(aDSN), open(bDSN)
	const partition = 777
	_, err := control.Exec(ctx, `INSERT INTO configurations(version,partition_count,virtual_nodes,hash_spec,membership,digest)
		VALUES(1,1024,10,'test','["A","B"]','test-1'),(2,1024,10,'test','["A","B"]','test-2')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = control.Exec(ctx, `INSERT INTO owners(partition_id,owner_shard,generation,config_version) VALUES($1,'A',1,1)`, partition)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, partition)
	if err != nil {
		t.Fatal(err)
	}
	coord := &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
	oldRoute := ownership.Route{Partition: partition, Owner: "A", Generation: 1}
	aStore := &store.Store{DB: a}
	bStore := &store.Store{DB: b}
	const op1 = "00000000-0000-4000-8000-000000000001"
	first, err := aStore.Write(ctx, oldRoute, op1, "entity-1", []byte("hello"), false)
	if err != nil {
		t.Fatal(err)
	}
	if first.Token.Version != 1 || first.PartitionSeq != 1 {
		t.Fatalf("bad first result: %+v", first)
	}
	if again, err := aStore.Write(ctx, oldRoute, op1, "entity-1", []byte("hello"), false); err != nil || again.PartitionSeq != 1 {
		t.Fatalf("retry: %+v %v", again, err)
	}
	if _, err := aStore.Write(ctx, oldRoute, op1, "entity-1", []byte("different"), false); !errors.Is(err, store.ErrOperationConflict) {
		t.Fatalf("conflict: %v", err)
	}
	const move1 = "00000000-0000-4000-8000-000000000101"
	if err := coord.Begin(ctx, move1, partition, "A", "B", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := coord.Begin(ctx, move1, partition, "A", "B", 1, 1); err != nil {
		t.Fatalf("idempotent begin: %v", err)
	}
	if err := coord.Begin(ctx, move1, partition, "A", "B", 1, 2); err == nil {
		t.Fatal("accepted changed migration request")
	}
	for _, phase := range []string{"FENCED", "READY", "COMMITTED", "ACTIVE", "RETIRED"} {
		// Recreate the coordinator to model loss of all process-local state.
		coord = &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
		got, err := coord.Step(ctx, move1)
		if err != nil || got != phase {
			t.Fatalf("phase %s: %s %v", phase, got, err)
		}
		if phase == "FENCED" {
			if _, err := aStore.Write(ctx, oldRoute, "00000000-0000-4000-8000-000000000002", "entity-1", []byte("late"), false); !errors.Is(err, store.ErrStaleRoute) {
				t.Fatalf("old owner admitted write: %v", err)
			}
			j, err := coord.Load(ctx, move1)
			if err != nil {
				t.Fatal(err)
			}
			if err := copyPartition(ctx, a, b, j); err != nil {
				t.Fatalf("first copy: %v", err)
			}
			if err := copyPartition(ctx, a, b, j); err != nil {
				t.Fatalf("repeated copy: %v", err)
			}
		}
	}
	if err := coord.Begin(ctx, move1, partition, "A", "B", 1, 1); err != nil {
		t.Fatalf("begin retry after retirement: %v", err)
	}
	newRoute := ownership.Route{Partition: partition, Owner: "B", Generation: 2}
	if got, err := bStore.Read(ctx, newRoute, "entity-1", first.Token.Version); err != nil || string(got.Payload) != "hello" {
		t.Fatalf("read after move: %+v %v", got, err)
	}
	if got, err := bStore.Write(ctx, newRoute, op1, "entity-1", []byte("hello"), false); err != nil || got.PartitionSeq != 1 {
		t.Fatalf("dedup after move: %+v %v", got, err)
	}
	const op2 = "00000000-0000-4000-8000-000000000003"
	second, err := bStore.Write(ctx, newRoute, op2, "entity-1", nil, true)
	if err != nil || second.Token.Version != 2 {
		t.Fatalf("delete: %+v %v", second, err)
	}
	const move2 = "00000000-0000-4000-8000-000000000102"
	if err := coord.Begin(ctx, move2, partition, "B", "A", 2, 2); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"FENCED", "READY", "COMMITTED", "ACTIVE", "RETIRED"} {
		got, err := coord.Step(ctx, move2)
		if err != nil || got != phase {
			t.Fatalf("return phase %s: %s %v", phase, got, err)
		}
	}
	returnRoute := ownership.Route{Partition: partition, Owner: "A", Generation: 3}
	got, err := aStore.Read(ctx, returnRoute, "entity-1", second.Token.Version)
	if err != nil || !got.Deleted || got.Token.Version != 2 {
		t.Fatalf("returned tombstone: %+v %v", got, err)
	}
	if _, err := aStore.Read(ctx, returnRoute, "entity-1", 3); !errors.Is(err, store.ErrVersionNotReady) {
		t.Fatalf("minimum version: %v", err)
	}
	if got, err := aStore.Write(ctx, returnRoute, op2, "entity-1", nil, true); err != nil || got.Token.Version != 2 {
		t.Fatalf("dedup after return: %+v %v", got, err)
	}
}

// The parent test verifies recovery after an actual coordinator process exit.
// The helper exits only after the named database operation has committed.
func TestCoordinatorCrashHelper(t *testing.T) {
	id, boundary := os.Getenv("WHM_CRASH_MIGRATION"), os.Getenv("WHM_CRASH_BOUNDARY")
	if id == "" || boundary == "" {
		t.Skip("crash helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	open := func(name string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, os.Getenv(name))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	coord := &Coordinator{Control: open("WHM_TEST_CONTROL_DSN"), Shards: map[string]*pgxpool.Pool{"A": open("WHM_TEST_A_DSN"), "B": open("WHM_TEST_B_DSN")}}
	coord.Barrier = func(_ context.Context, name string) error {
		if name == boundary {
			os.Exit(73)
		}
		return nil
	}
	for step := 0; step < 5; step++ {
		if _, err := coord.Step(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("crash boundary was not reached")
}

func TestRecoveryAfterCoordinatorProcessExit(t *testing.T) {
	controlDSN, aDSN, bDSN := os.Getenv("WHM_TEST_CONTROL_DSN"), os.Getenv("WHM_TEST_A_DSN"), os.Getenv("WHM_TEST_B_DSN")
	if controlDSN == "" || aDSN == "" || bDSN == "" {
		t.Skip("three PostgreSQL test DSNs required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	open := func(dsn string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	control, a, b := open(controlDSN), open(aDSN), open(bDSN)
	for i, boundary := range []string{"after_source_fence", "after_owner_commit"} {
		t.Run(boundary, func(t *testing.T) {
			partition := int32(900 + i)
			version := int64(1100 + i)
			migrationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 3000+i)
			operationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 4000+i)
			entityID := fmt.Sprintf("process-crash-%d", i)
			_, err := control.Exec(ctx, `INSERT INTO configurations(version,partition_count,virtual_nodes,hash_spec,membership,digest) VALUES($1,1024,10,'test','["A","B"]',$2)`, version, entityID)
			if err != nil {
				t.Fatal(err)
			}
			_, err = control.Exec(ctx, `INSERT INTO owners(partition_id,owner_shard,generation,config_version) VALUES($1,'A',1,$2)`, partition, version)
			if err != nil {
				t.Fatal(err)
			}
			_, err = a.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, partition)
			if err != nil {
				t.Fatal(err)
			}
			oldRoute := ownership.Route{Partition: uint32(partition), Owner: "A", Generation: 1}
			newRoute := ownership.Route{Partition: uint32(partition), Owner: "B", Generation: 2}
			_, err = (&store.Store{DB: a}).Write(ctx, oldRoute, operationID, entityID, []byte("survives exit"), false)
			if err != nil {
				t.Fatal(err)
			}
			coord := &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
			if err := coord.Begin(ctx, migrationID, partition, "A", "B", 1, version); err != nil {
				t.Fatal(err)
			}
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCoordinatorCrashHelper$")
			child.Env = append(os.Environ(), "WHM_CRASH_MIGRATION="+migrationID, "WHM_CRASH_BOUNDARY="+boundary)
			err = child.Run()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 73 {
				t.Fatalf("helper did not crash at %s: %v", boundary, err)
			}
			for step := 0; step < 6; step++ {
				phase, err := coord.Step(ctx, migrationID)
				if err != nil {
					t.Fatalf("recovery step %d: %v", step, err)
				}
				if phase == "RETIRED" {
					break
				}
			}
			j, err := coord.Load(ctx, migrationID)
			if err != nil || j.Phase != "RETIRED" {
				t.Fatalf("incomplete recovery: %+v %v", j, err)
			}
			r, err := (&store.Store{DB: b}).Read(ctx, newRoute, entityID, 1)
			if err != nil || string(r.Payload) != "survives exit" {
				t.Fatalf("acknowledged write lost: %+v %v", r, err)
			}
		})
	}
}

// The test harness owns a disposable PostgreSQL cluster. An immediate stop
// drops every connection and models a database process crash with durable WAL.
// This test is run separately from the package suite to avoid interrupting
// unrelated integration tests that use the same local cluster.
func TestRecoveryAfterDatabaseRestart(t *testing.T) {
	dataDir := os.Getenv("WHM_TEST_PGDATA")
	controlDSN, aDSN, bDSN := os.Getenv("WHM_TEST_CONTROL_DSN"), os.Getenv("WHM_TEST_A_DSN"), os.Getenv("WHM_TEST_B_DSN")
	if dataDir == "" || controlDSN == "" || aDSN == "" || bDSN == "" {
		t.Skip("disposable PostgreSQL cluster and three test DSNs required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	open := func(dsn string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	control, a, b := open(controlDSN), open(aDSN), open(bDSN)
	for i, boundary := range []string{"FENCED", "COMMITTED"} {
		t.Run(boundary, func(t *testing.T) {
			partition := int32(950 + i)
			version := int64(1200 + i)
			migrationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 5000+i)
			operationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 6000+i)
			entityID := fmt.Sprintf("database-restart-%d", i)
			if _, err := control.Exec(ctx, `INSERT INTO configurations(version,partition_count,virtual_nodes,hash_spec,membership,digest) VALUES($1,1024,10,'test','["A","B"]',$2)`, version, entityID); err != nil {
				t.Fatal(err)
			}
			if _, err := control.Exec(ctx, `INSERT INTO owners(partition_id,owner_shard,generation,config_version) VALUES($1,'A',1,$2)`, partition, version); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, partition); err != nil {
				t.Fatal(err)
			}
			oldRoute := ownership.Route{Partition: uint32(partition), Owner: "A", Generation: 1}
			newRoute := ownership.Route{Partition: uint32(partition), Owner: "B", Generation: 2}
			written, err := (&store.Store{DB: a}).Write(ctx, oldRoute, operationID, entityID, []byte("survives database restart"), false)
			if err != nil {
				t.Fatal(err)
			}
			coord := &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
			if err := coord.Begin(ctx, migrationID, partition, "A", "B", 1, version); err != nil {
				t.Fatal(err)
			}
			for {
				phase, err := coord.Step(ctx, migrationID)
				if err != nil {
					t.Fatal(err)
				}
				if phase == boundary {
					break
				}
			}
			command := exec.CommandContext(ctx, "pg_ctl", "-D", dataDir, "-m", "immediate", "-w", "restart")
			// PostgreSQL's restarted postmaster inherits stdout/stderr. A
			// CombinedOutput pipe would stay open after pg_ctl exits.
			if err := command.Run(); err != nil {
				t.Fatalf("restart PostgreSQL: %v", err)
			}
			// A restarted coordinator creates fresh pools; old pooled sockets
			// can still point at the terminated database process.
			control, a, b = open(controlDSN), open(aDSN), open(bDSN)
			coord = &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
			for step := 0; step < 6; step++ {
				phase, err := coord.Step(ctx, migrationID)
				if err != nil {
					t.Fatalf("recovery step %d: %v", step, err)
				}
				if phase == "RETIRED" {
					break
				}
			}
			if job, err := coord.Load(ctx, migrationID); err != nil || job.Phase != "RETIRED" {
				t.Fatalf("incomplete recovery: %+v %v", job, err)
			}
			if got, err := (&store.Store{DB: b}).Read(ctx, newRoute, entityID, written.Token.Version); err != nil || string(got.Payload) != "survives database restart" {
				t.Fatalf("acknowledged value lost: %+v %v", got, err)
			}
			if retry, err := (&store.Store{DB: b}).Write(ctx, newRoute, operationID, entityID, []byte("survives database restart"), false); err != nil || retry.PartitionSeq != written.PartitionSeq {
				t.Fatalf("retry result lost: %+v %v", retry, err)
			}
			if _, err := (&store.Store{DB: a}).Read(ctx, oldRoute, entityID, 0); !errors.Is(err, store.ErrStaleRoute) {
				t.Fatalf("stale source served read: %v", err)
			}
		})
	}
}

func TestRecoveryAfterEveryDurableBoundary(t *testing.T) {
	controlDSN, aDSN, bDSN := os.Getenv("WHM_TEST_CONTROL_DSN"), os.Getenv("WHM_TEST_A_DSN"), os.Getenv("WHM_TEST_B_DSN")
	if controlDSN == "" || aDSN == "" || bDSN == "" {
		t.Skip("three PostgreSQL test DSNs required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	open := func(dsn string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	control, a, b := open(controlDSN), open(aDSN), open(bDSN)
	barriers := []string{
		"after_target_prepare", "after_source_mark", "after_source_fence",
		"after_record_fence", "after_copy", "after_record_ready",
		"after_owner_commit", "after_target_activate", "after_record_active",
		"after_source_retire",
	}
	for i, barrier := range barriers {
		t.Run(barrier, func(t *testing.T) {
			partition := int32(800 + i)
			version := int64(1000 + i)
			migrationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 1000+i)
			operationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 2000+i)
			entityID := fmt.Sprintf("boundary-%d", i)
			if _, err := control.Exec(ctx, `INSERT INTO configurations(version,partition_count,virtual_nodes,hash_spec,membership,digest)
				VALUES($1,1024,10,'test','["A","B"]',$2)`, version, fmt.Sprintf("boundary-%d", i)); err != nil {
				t.Fatal(err)
			}
			if _, err := control.Exec(ctx, `INSERT INTO owners(partition_id,owner_shard,generation,config_version) VALUES($1,'A',1,$2)`, partition, version); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, partition); err != nil {
				t.Fatal(err)
			}
			oldRoute := ownership.Route{Partition: uint32(partition), Owner: "A", Generation: 1}
			newRoute := ownership.Route{Partition: uint32(partition), Owner: "B", Generation: 2}
			written, err := (&store.Store{DB: a}).Write(ctx, oldRoute, operationID, entityID, []byte("retained"), false)
			if err != nil {
				t.Fatal(err)
			}
			coord := &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
			if err := coord.Begin(ctx, migrationID, partition, "A", "B", 1, version); err != nil {
				t.Fatal(err)
			}
			coord.Barrier = func(_ context.Context, name string) error {
				if name == barrier {
					return errors.New("injected lost response")
				}
				return nil
			}
			for {
				phase, err := coord.Step(ctx, migrationID)
				if err != nil {
					if err.Error() != "injected lost response" {
						t.Fatalf("unexpected failure in %s: %v", phase, err)
					}
					break
				}
				if phase == "RETIRED" {
					t.Fatal("barrier was not reached")
				}
			}
			// Drop all coordinator-local state and reconcile from the databases.
			coord = &Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
			for step := 0; step < 6; step++ {
				phase, err := coord.Step(ctx, migrationID)
				if err != nil {
					t.Fatalf("resume step %d: %v", step, err)
				}
				if phase == "RETIRED" {
					break
				}
			}
			j, err := coord.Load(ctx, migrationID)
			if err != nil || j.Phase != "RETIRED" {
				t.Fatalf("incomplete recovery: %+v %v", j, err)
			}
			got, err := (&store.Store{DB: b}).Read(ctx, newRoute, entityID, written.Token.Version)
			if err != nil || string(got.Payload) != "retained" {
				t.Fatalf("acknowledged value lost: %+v %v", got, err)
			}
			if retry, err := (&store.Store{DB: b}).Write(ctx, newRoute, operationID, entityID, []byte("retained"), false); err != nil || retry.PartitionSeq != written.PartitionSeq {
				t.Fatalf("retry result lost: %+v %v", retry, err)
			}
			if _, err := (&store.Store{DB: a}).Read(ctx, oldRoute, entityID, 0); !errors.Is(err, store.ErrStaleRoute) {
				t.Fatalf("stale source served read: %v", err)
			}
		})
	}
}
