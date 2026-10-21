-- Control-plane schema, applied by the PostgreSQL integration and pilot harnesses.
-- Production deployment still needs migrations, access provisioning, backups,
-- and high-availability procedures appropriate to its environment.
CREATE TABLE configurations (
    version bigint PRIMARY KEY CHECK (version > 0),
    partition_count integer NOT NULL CHECK (partition_count > 0),
    virtual_nodes integer NOT NULL CHECK (virtual_nodes > 0),
    hash_spec text NOT NULL,
    membership jsonb NOT NULL,
    digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE owners (
    partition_id integer PRIMARY KEY CHECK (partition_id >= 0),
    owner_shard text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    config_version bigint NOT NULL REFERENCES configurations(version)
);

CREATE TABLE migrations (
    migration_id uuid PRIMARY KEY,
    partition_id integer NOT NULL REFERENCES owners(partition_id),
    config_version bigint NOT NULL REFERENCES configurations(version),
    source_shard text NOT NULL,
    target_shard text NOT NULL,
    old_generation bigint NOT NULL CHECK (old_generation > 0),
    new_generation bigint NOT NULL CHECK (new_generation > old_generation),
    phase text NOT NULL CHECK (phase IN
        ('PREPARED', 'COPYING', 'FENCED', 'READY', 'COMMITTED', 'ACTIVE', 'RETIRED')),
    phase_version bigint NOT NULL DEFAULT 1 CHECK (phase_version > 0),
    snapshot_seq bigint CHECK (snapshot_seq >= 0),
    fence_seq bigint CHECK (fence_seq >= 0),
    target_ready_seq bigint CHECK (target_ready_seq >= 0),
    last_error text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (source_shard <> target_shard),
    CHECK (phase NOT IN ('READY', 'COMMITTED', 'ACTIVE', 'RETIRED') OR
        (fence_seq IS NOT NULL AND target_ready_seq IS NOT NULL
         AND target_ready_seq = fence_seq))
);

CREATE UNIQUE INDEX one_live_migration_per_partition ON migrations(partition_id)
WHERE phase <> 'RETIRED';

-- Insert in the same transaction that CASes owners and marks COMMITTED.
-- Recovery and target activation verify this immutable decision.
CREATE TABLE ownership_decisions (
    migration_id uuid PRIMARY KEY REFERENCES migrations(migration_id),
    partition_id integer NOT NULL REFERENCES owners(partition_id),
    owner_shard text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    fence_seq bigint NOT NULL CHECK (fence_seq >= 0),
    committed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (partition_id, generation)
);
