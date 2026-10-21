// Package migration implements the initial, intentionally disruptive,
// per-partition stop-and-copy protocol. The coordinator uses privileged DB
// credentials; application credentials can only execute admission functions.
package migration

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Coordinator struct {
	Control *pgxpool.Pool
	Shards  map[string]*pgxpool.Pool
	// Barrier is an optional test hook. Returning an error simulates a lost
	// coordinator response at a named durable boundary.
	Barrier func(context.Context, string) error
}

func (c *Coordinator) barrier(ctx context.Context, name string) error {
	if c.Barrier != nil {
		return c.Barrier(ctx, name)
	}
	return nil
}

type Job struct {
	ID                           string
	Partition                    int32
	Source, Target               string
	OldGeneration, NewGeneration int64
	Phase                        string
	FenceSeq                     int64
}

// Begin records intent only. It checks the current owner under a row lock;
// desired placement is supplied by the caller and carries no authority.
func (c *Coordinator) Begin(ctx context.Context, id string, partition int32, source, target string, generation, configVersion int64) error {
	if c == nil || c.Control == nil || c.Shards[source] == nil || c.Shards[target] == nil || source == target || id == "" || generation <= 0 {
		return fmt.Errorf("invalid migration request")
	}
	tx, err := c.Control.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var existingPartition int32
	var existingSource, existingTarget string
	var existingGeneration, existingConfig int64
	err = tx.QueryRow(ctx, `SELECT partition_id,source_shard,target_shard,old_generation,config_version
		FROM migrations WHERE migration_id=$1`, id).Scan(&existingPartition, &existingSource, &existingTarget, &existingGeneration, &existingConfig)
	if err == nil {
		if existingPartition != partition || existingSource != source || existingTarget != target || existingGeneration != generation || existingConfig != configVersion {
			return fmt.Errorf("migration ID reused with different request")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var owner string
	var current int64
	err = tx.QueryRow(ctx, `SELECT owner_shard,generation FROM owners WHERE partition_id=$1 FOR UPDATE`, partition).Scan(&owner, &current)
	if err != nil {
		return err
	}
	if owner != source || current != generation {
		return fmt.Errorf("owner changed before migration")
	}
	_, err = tx.Exec(ctx, `INSERT INTO migrations(migration_id,partition_id,config_version,source_shard,target_shard,old_generation,new_generation,phase)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'PREPARED')`, id, partition, configVersion, source, target, generation, generation+1)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Coordinator) Load(ctx context.Context, id string) (Job, error) {
	var j Job
	err := c.Control.QueryRow(ctx, `SELECT migration_id::text,partition_id,source_shard,target_shard,old_generation,new_generation,phase,COALESCE(fence_seq,0)
		FROM migrations WHERE migration_id=$1`, id).Scan(&j.ID, &j.Partition, &j.Source, &j.Target, &j.OldGeneration, &j.NewGeneration, &j.Phase, &j.FenceSeq)
	return j, err
}

// Step completes one durable phase. Calling it again after an uncertain
// response resumes from database evidence rather than in-memory state.
func (c *Coordinator) Step(ctx context.Context, id string) (string, error) {
	j, err := c.Load(ctx, id)
	if err != nil {
		return "", err
	}
	source, target := c.Shards[j.Source], c.Shards[j.Target]
	if source == nil || target == nil {
		return j.Phase, fmt.Errorf("missing shard connection")
	}
	switch j.Phase {
	case "PREPARED":
		if err := prepareTarget(ctx, target, j); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_target_prepare"); err != nil {
			return j.Phase, err
		}
		if err := markSource(ctx, source, j); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_source_mark"); err != nil {
			return j.Phase, err
		}
		var q int64
		err := source.QueryRow(ctx, `SELECT public.whm_fence($1,$2,$3)`, j.Partition, j.OldGeneration, j.ID).Scan(&q)
		if err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_source_fence"); err != nil {
			return j.Phase, err
		}
		if err := c.advance(ctx, j, "FENCED", q); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_record_fence"); err != nil {
			return "FENCED", err
		}
		return "FENCED", nil
	case "FENCED":
		if err := copyPartition(ctx, source, target, j); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_copy"); err != nil {
			return j.Phase, err
		}
		if err := c.advance(ctx, j, "READY", j.FenceSeq); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_record_ready"); err != nil {
			return "READY", err
		}
		return "READY", nil
	case "READY":
		if err := c.commit(ctx, j, target); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_owner_commit"); err != nil {
			return "COMMITTED", err
		}
		return "COMMITTED", nil
	case "COMMITTED":
		if err := c.verifyDecision(ctx, j); err != nil {
			return j.Phase, err
		}
		if err := activate(ctx, target, j); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_target_activate"); err != nil {
			return j.Phase, err
		}
		if err := c.advance(ctx, j, "ACTIVE", j.FenceSeq); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_record_active"); err != nil {
			return "ACTIVE", err
		}
		return "ACTIVE", nil
	case "ACTIVE":
		if err := retire(ctx, source, j); err != nil {
			return j.Phase, err
		}
		if err := c.barrier(ctx, "after_source_retire"); err != nil {
			return j.Phase, err
		}
		if err := c.advance(ctx, j, "RETIRED", j.FenceSeq); err != nil {
			return j.Phase, err
		}
		return "RETIRED", nil
	case "RETIRED":
		return j.Phase, nil
	default:
		return j.Phase, fmt.Errorf("unsupported phase %q", j.Phase)
	}
}

func prepareTarget(ctx context.Context, db *pgxpool.Pool, j Job) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var generation int64
	var state, migration string
	err = tx.QueryRow(ctx, `SELECT generation,state,COALESCE(migration_id::text,'') FROM partition_guards WHERE partition_id=$1 FOR UPDATE`, j.Partition).Scan(&generation, &state, &migration)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil {
		if generation == j.NewGeneration && migration == j.ID && state == "STAGING" {
			return tx.Commit(ctx)
		}
		if state != "FENCED" && state != "RETIRED" {
			return fmt.Errorf("target partition is still serving")
		}
		if generation >= j.NewGeneration {
			return fmt.Errorf("target generation is newer than migration")
		}
		// A former owner may retain old rows. Clear them before this generation
		// becomes staging; the rejection fence stays in the guard row.
		for _, table := range []string{"operation_log", "operation_results", "entities", "import_progress"} {
			_, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE partition_id=$1", j.Partition)
			if err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `UPDATE partition_guards SET generation=$2,state='STAGING',migration_id=$3,committed_seq=0,fence_seq=NULL WHERE partition_id=$1`, j.Partition, j.NewGeneration, j.ID)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state,migration_id) VALUES($1,$2,'STAGING',$3)`, j.Partition, j.NewGeneration, j.ID)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func markSource(ctx context.Context, db *pgxpool.Pool, j Job) error {
	tag, err := db.Exec(ctx, `UPDATE partition_guards SET migration_id=$3
		WHERE partition_id=$1 AND generation=$2 AND
		(state='ACTIVE' OR (state='FENCED' AND migration_id=$3))`, j.Partition, j.OldGeneration, j.ID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("source guard does not match migration")
	}
	return nil
}

// copyPartition runs after the durable source fence. One target transaction
// replaces the staging image, so a crash or retry never leaves a mixed copy.
func copyPartition(ctx context.Context, source, target *pgxpool.Pool, j Job) error {
	var q int64
	var state, migration string
	err := source.QueryRow(ctx, `SELECT state,COALESCE(migration_id::text,''),fence_seq FROM partition_guards WHERE partition_id=$1`, j.Partition).Scan(&state, &migration, &q)
	if err != nil {
		return err
	}
	if state != "FENCED" || migration != j.ID || q != j.FenceSeq {
		return fmt.Errorf("source fence evidence does not match")
	}
	tx, err := target.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var targetGeneration int64
	err = tx.QueryRow(ctx, `SELECT generation,state,COALESCE(migration_id::text,'') FROM partition_guards WHERE partition_id=$1 FOR UPDATE`, j.Partition).Scan(&targetGeneration, &state, &migration)
	if err != nil {
		return err
	}
	if targetGeneration != j.NewGeneration || state != "STAGING" || migration != j.ID {
		return fmt.Errorf("target staging guard does not match")
	}
	for _, table := range []string{"operation_log", "operation_results", "entities", "import_progress"} {
		if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE partition_id=$1", j.Partition); err != nil {
			return err
		}
	}
	rows, err := source.Query(ctx, `SELECT entity_id,version,payload,deleted FROM entities WHERE partition_id=$1 ORDER BY entity_id`, j.Partition)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var v int64
		var payload []byte
		var deleted bool
		if err = rows.Scan(&id, &v, &payload, &deleted); err != nil {
			break
		}
		_, err = tx.Exec(ctx, `INSERT INTO entities(partition_id,entity_id,version,payload,deleted) VALUES($1,$2,$3,$4,$5)`, j.Partition, id, v, payload, deleted)
		if err != nil {
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = source.Query(ctx, `SELECT operation_id,request_digest,entity_id,entity_version,partition_seq,result FROM operation_results WHERE partition_id=$1 ORDER BY partition_seq`, j.Partition)
	if err != nil {
		return err
	}
	var copied int64
	for rows.Next() {
		var op, entity string
		var digest []byte
		var version, seq int64
		var result []byte
		if err = rows.Scan(&op, &digest, &entity, &version, &seq, &result); err != nil {
			break
		}
		_, err = tx.Exec(ctx, `INSERT INTO operation_results(partition_id,operation_id,request_digest,entity_id,entity_version,partition_seq,result) VALUES($1,$2,$3,$4,$5,$6,$7)`, j.Partition, op, digest, entity, version, seq, result)
		if err != nil {
			break
		}
		copied++
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if copied != q {
		return fmt.Errorf("operation history incomplete: copied %d of %d", copied, q)
	}
	_, err = tx.Exec(ctx, `UPDATE partition_guards SET committed_seq=$2 WHERE partition_id=$1`, j.Partition, q)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO import_progress(migration_id,partition_id,generation,snapshot_seq,snapshot_complete,applied_seq)
		VALUES($1,$2,$3,$4,true,$4)`, j.ID, j.Partition, j.NewGeneration, q)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Coordinator) advance(ctx context.Context, j Job, next string, q int64) error {
	var tag pgconnCommandTag
	var err error
	switch next {
	case "FENCED":
		tag, err = c.Control.Exec(ctx, `UPDATE migrations SET phase='FENCED',fence_seq=$2,phase_version=phase_version+1,updated_at=now()
			WHERE migration_id=$1 AND phase='PREPARED'`, j.ID, q)
	case "READY":
		tag, err = c.Control.Exec(ctx, `UPDATE migrations SET phase='READY',target_ready_seq=$2,phase_version=phase_version+1,updated_at=now()
			WHERE migration_id=$1 AND phase='FENCED' AND fence_seq=$2`, j.ID, q)
	case "ACTIVE":
		tag, err = c.Control.Exec(ctx, `UPDATE migrations SET phase='ACTIVE',phase_version=phase_version+1,updated_at=now()
			WHERE migration_id=$1 AND phase='COMMITTED'`, j.ID)
	case "RETIRED":
		tag, err = c.Control.Exec(ctx, `UPDATE migrations SET phase='RETIRED',phase_version=phase_version+1,updated_at=now()
			WHERE migration_id=$1 AND phase='ACTIVE'`, j.ID)
	default:
		return fmt.Errorf("invalid transition")
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("migration phase changed concurrently")
	}
	return nil
}

// Small interface avoids exposing a driver-specific command tag in callers.
type pgconnCommandTag interface{ RowsAffected() int64 }

func (c *Coordinator) commit(ctx context.Context, j Job, target *pgxpool.Pool) error {
	var generation, applied int64
	var complete bool
	err := target.QueryRow(ctx, `SELECT generation,snapshot_complete,applied_seq FROM import_progress WHERE migration_id=$1 AND partition_id=$2`, j.ID, j.Partition).Scan(&generation, &complete, &applied)
	if err != nil {
		return err
	}
	if generation != j.NewGeneration || !complete || applied != j.FenceSeq {
		return fmt.Errorf("target is not ready through fence")
	}
	tx, err := c.Control.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var phase string
	var fence, ready int64
	err = tx.QueryRow(ctx, `SELECT phase,fence_seq,target_ready_seq FROM migrations WHERE migration_id=$1 FOR UPDATE`, j.ID).Scan(&phase, &fence, &ready)
	if err != nil {
		return err
	}
	if phase != "READY" || fence != j.FenceSeq || ready != fence {
		return fmt.Errorf("migration not ready for commit")
	}
	tag, err := tx.Exec(ctx, `UPDATE owners SET owner_shard=$3,generation=$4,config_version=(SELECT config_version FROM migrations WHERE migration_id=$5)
		WHERE partition_id=$1 AND owner_shard=$2 AND generation=$6`, j.Partition, j.Source, j.Target, j.NewGeneration, j.ID, j.OldGeneration)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("owner changed before commit")
	}
	_, err = tx.Exec(ctx, `INSERT INTO ownership_decisions(migration_id,partition_id,owner_shard,generation,fence_seq)
		VALUES($1,$2,$3,$4,$5)`, j.ID, j.Partition, j.Target, j.NewGeneration, j.FenceSeq)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE migrations SET phase='COMMITTED',phase_version=phase_version+1,updated_at=now() WHERE migration_id=$1`, j.ID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Coordinator) verifyDecision(ctx context.Context, j Job) error {
	var partition int32
	var owner string
	var generation, fence int64
	err := c.Control.QueryRow(ctx, `SELECT d.partition_id,d.owner_shard,d.generation,d.fence_seq
		FROM ownership_decisions d JOIN owners o ON o.partition_id=d.partition_id
		WHERE d.migration_id=$1 AND o.owner_shard=d.owner_shard AND o.generation=d.generation`, j.ID).
		Scan(&partition, &owner, &generation, &fence)
	if err != nil {
		return err
	}
	if partition != j.Partition || owner != j.Target || generation != j.NewGeneration || fence != j.FenceSeq {
		return fmt.Errorf("committed ownership decision does not match migration")
	}
	return nil
}

func activate(ctx context.Context, target *pgxpool.Pool, j Job) error {
	tag, err := target.Exec(ctx, `UPDATE partition_guards SET state='ACTIVE'
		WHERE partition_id=$1 AND generation=$2 AND migration_id=$3 AND state IN ('STAGING','ACTIVE')
		AND committed_seq=$4 AND EXISTS
		(SELECT 1 FROM import_progress WHERE migration_id=$3 AND partition_id=$1 AND generation=$2 AND snapshot_complete AND applied_seq=$4)`,
		j.Partition, j.NewGeneration, j.ID, j.FenceSeq)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("target activation evidence missing")
	}
	return nil
}

func retire(ctx context.Context, source *pgxpool.Pool, j Job) error {
	tx, err := source.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var generation, fence int64
	var state, migration string
	err = tx.QueryRow(ctx, `SELECT generation,state,COALESCE(migration_id::text,''),fence_seq
		FROM partition_guards WHERE partition_id=$1 FOR UPDATE`, j.Partition).Scan(&generation, &state, &migration, &fence)
	if err != nil {
		return err
	}
	if generation != j.OldGeneration || migration != j.ID || fence != j.FenceSeq {
		return fmt.Errorf("source retirement evidence does not match")
	}
	if state == "RETIRED" {
		return tx.Commit(ctx)
	}
	if state != "FENCED" {
		return fmt.Errorf("source is not fenced")
	}
	for _, table := range []string{"operation_log", "operation_results", "entities", "import_progress"} {
		if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE partition_id=$1", j.Partition); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE partition_guards SET state='RETIRED' WHERE partition_id=$1`, j.Partition)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
