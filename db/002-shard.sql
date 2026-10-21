-- Shard tables, applied with 003-admission.sql by the PostgreSQL integration
-- and pilot harnesses. The tables require the admission functions for storage
-- enforcement; production deployment still needs operational provisioning.
CREATE TABLE partition_guards (
    partition_id integer PRIMARY KEY CHECK (partition_id >= 0),
    generation bigint NOT NULL CHECK (generation > 0),
    state text NOT NULL CHECK (state IN ('STAGING', 'ACTIVE', 'COPYING', 'FENCED', 'RETIRED')),
    migration_id uuid,
    committed_seq bigint NOT NULL DEFAULT 0 CHECK (committed_seq >= 0),
    fence_seq bigint CHECK (fence_seq >= 0),
    CHECK (state NOT IN ('FENCED', 'RETIRED') OR fence_seq IS NOT NULL)
);

CREATE TABLE entities (
    partition_id integer NOT NULL REFERENCES partition_guards(partition_id),
    entity_id text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    payload bytea,
    deleted boolean NOT NULL DEFAULT false,
    PRIMARY KEY (partition_id, entity_id),
    CHECK ((deleted AND payload IS NULL) OR (NOT deleted AND payload IS NOT NULL))
);

CREATE TABLE operation_results (
    partition_id integer NOT NULL REFERENCES partition_guards(partition_id),
    operation_id uuid NOT NULL,
    request_digest bytea NOT NULL,
    entity_id text NOT NULL,
    entity_version bigint NOT NULL CHECK (entity_version > 0),
    partition_seq bigint NOT NULL CHECK (partition_seq > 0),
    result jsonb NOT NULL,
    PRIMARY KEY (partition_id, operation_id)
);

-- Mutation, result, and this log entry commit together under the guard lock.
CREATE TABLE operation_log (
    partition_id integer NOT NULL REFERENCES partition_guards(partition_id),
    partition_seq bigint NOT NULL CHECK (partition_seq > 0),
    generation bigint NOT NULL CHECK (generation > 0),
    operation_id uuid NOT NULL,
    entity_id text NOT NULL,
    entity_version bigint NOT NULL CHECK (entity_version > 0),
    request_digest bytea NOT NULL,
    mutation jsonb NOT NULL,
    result jsonb NOT NULL,
    PRIMARY KEY (partition_id, partition_seq),
    UNIQUE (partition_id, operation_id)
);

CREATE TABLE import_progress (
    migration_id uuid PRIMARY KEY,
    partition_id integer NOT NULL REFERENCES partition_guards(partition_id),
    generation bigint NOT NULL CHECK (generation > 0),
    snapshot_seq bigint NOT NULL CHECK (snapshot_seq >= 0),
    snapshot_complete boolean NOT NULL DEFAULT false,
    cursor_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    manifest_digest text,
    applied_seq bigint NOT NULL CHECK (applied_seq >= 0),
    CHECK (applied_seq >= snapshot_seq)
);
