// Package store calls storage-enforced admission functions. The caller must
// supply a route from a committed ownership snapshot, never candidate placement.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"generation-fenced-shard-transfer/internal/ownership"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrStaleRoute        = errors.New("stale or unavailable partition")
	ErrVersionNotReady   = errors.New("version not ready")
	ErrOperationConflict = errors.New("operation ID conflict")
	ErrNotFound          = errors.New("entity not found")
)

type SessionToken struct {
	EntityID string
	Version  uint64
}

type Result struct {
	Token        SessionToken
	PartitionSeq uint64
	Deleted      bool
	Payload      []byte
}

type Store struct{ DB *pgxpool.Pool }

// Digest binds the operation ID to the complete logical mutation. A retry with
// the same ID and different contents is rejected even after migration.
func Digest(entityID string, payload []byte, deleted bool) [32]byte {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(entityID)))
	h.Write(n[:])
	h.Write([]byte(entityID))
	if deleted {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	binary.BigEndian.PutUint64(n[:], uint64(len(payload)))
	h.Write(n[:])
	h.Write(payload)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func (s *Store) Write(ctx context.Context, route ownership.Route, operationID, entityID string, payload []byte, deleted bool) (Result, error) {
	if s == nil || s.DB == nil || route.Owner == "" || route.Generation == 0 || route.Generation > math.MaxInt64 || route.Partition > math.MaxInt32 || entityID == "" || operationID == "" || (!deleted && payload == nil) || (deleted && payload != nil) {
		return Result{}, fmt.Errorf("invalid write parameters")
	}
	digest := Digest(entityID, payload, deleted)
	var version, seq int64
	var wasDeleted bool
	err := s.DB.QueryRow(ctx, `SELECT entity_version, partition_seq, deleted FROM public.whm_write($1,$2,$3,$4,$5,$6,$7)`,
		int32(route.Partition), int64(route.Generation), entityID, operationID, digest[:], payload, deleted).Scan(&version, &seq, &wasDeleted)
	if err != nil {
		return Result{}, classify(err)
	}
	return Result{Token: SessionToken{EntityID: entityID, Version: uint64(version)}, PartitionSeq: uint64(seq), Deleted: wasDeleted}, nil
}

func (s *Store) Read(ctx context.Context, route ownership.Route, entityID string, minimumVersion uint64) (Result, error) {
	if s == nil || s.DB == nil || route.Owner == "" || route.Generation == 0 || route.Generation > math.MaxInt64 || route.Partition > math.MaxInt32 || entityID == "" || minimumVersion > math.MaxInt64 {
		return Result{}, fmt.Errorf("invalid read parameters")
	}
	var version int64
	var payload []byte
	var deleted bool
	err := s.DB.QueryRow(ctx, `SELECT entity_version, payload, deleted FROM public.whm_read($1,$2,$3,$4)`,
		int32(route.Partition), int64(route.Generation), entityID, int64(minimumVersion)).Scan(&version, &payload, &deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, ErrNotFound
	}
	if err != nil {
		return Result{}, classify(err)
	}
	return Result{Token: SessionToken{EntityID: entityID, Version: uint64(version)}, Payload: payload, Deleted: deleted}, nil
}

func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "P0001":
			return fmt.Errorf("%w: %s", ErrStaleRoute, pgErr.Message)
		case "P0002":
			return fmt.Errorf("%w: %s", ErrVersionNotReady, pgErr.Message)
		case "23505":
			return fmt.Errorf("%w: %s", ErrOperationConflict, pgErr.Message)
		}
	}
	return err
}
