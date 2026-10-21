-- Apply after 002-shard.sql as a privileged schema owner. Grant the NOLOGIN
-- role to the actual application login after deployment. That login must not
-- own the tables or inherit a second role with direct DML privileges.
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'whm_app') THEN
        CREATE ROLE whm_app NOLOGIN;
    END IF;
END $$;

REVOKE ALL ON public.partition_guards, public.entities,
    public.operation_results, public.operation_log, public.import_progress
    FROM PUBLIC, whm_app;

-- This function is the only initial application mutation entry point. The
-- guard row serializes writes and fences, making committed_seq a commit-order
-- prefix rather than a PostgreSQL sequence allocation.
CREATE OR REPLACE FUNCTION public.whm_write(
    p_partition integer, p_generation bigint, p_entity text,
    p_operation uuid, p_digest bytea, p_payload bytea, p_delete boolean
) RETURNS TABLE(entity_version bigint, partition_seq bigint, deleted boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    g public.partition_guards%ROWTYPE;
    prior public.operation_results%ROWTYPE;
    next_version bigint;
    next_seq bigint;
    stored_result jsonb;
BEGIN
    IF p_partition IS NULL OR p_partition < 0 OR p_generation IS NULL OR p_generation < 1 OR
       p_entity IS NULL OR p_entity = '' OR p_operation IS NULL OR p_delete IS NULL OR
       p_digest IS NULL OR length(p_digest) <> 32 OR
       (NOT p_delete AND p_payload IS NULL) OR (p_delete AND p_payload IS NOT NULL) THEN
        RAISE EXCEPTION 'invalid write request' USING ERRCODE = '22023';
    END IF;
    IF p_digest <> public.digest(
        int8send(octet_length(convert_to(p_entity,'UTF8'))::bigint) ||
        convert_to(p_entity,'UTF8') ||
        CASE WHEN p_delete THEN decode('01','hex') ELSE decode('00','hex') END ||
        int8send(octet_length(COALESCE(p_payload,''::bytea))::bigint) ||
        COALESCE(p_payload,''::bytea), 'sha256') THEN
        RAISE EXCEPTION 'request digest mismatch' USING ERRCODE = '22023';
    END IF;
    SELECT * INTO g FROM public.partition_guards
      WHERE partition_id = p_partition FOR UPDATE;
    IF NOT FOUND OR g.generation <> p_generation OR g.state NOT IN ('ACTIVE', 'COPYING') THEN
        RAISE EXCEPTION 'stale or unavailable partition' USING ERRCODE = 'P0001';
    END IF;
    SELECT * INTO prior FROM public.operation_results
      WHERE operation_results.partition_id = p_partition AND operation_id = p_operation;
    IF FOUND THEN
        IF prior.request_digest <> p_digest OR prior.entity_id <> p_entity THEN
            RAISE EXCEPTION 'operation ID conflict' USING ERRCODE = '23505';
        END IF;
        entity_version := prior.entity_version;
        partition_seq := prior.partition_seq;
        deleted := (prior.result->>'deleted')::boolean;
        RETURN NEXT;
        RETURN;
    END IF;
    SELECT e.version + 1 INTO next_version FROM public.entities e
      WHERE e.partition_id = p_partition AND e.entity_id = p_entity;
    next_version := COALESCE(next_version, 1);
    next_seq := g.committed_seq + 1;
    INSERT INTO public.entities(partition_id, entity_id, version, payload, deleted)
      VALUES (p_partition, p_entity, next_version,
              CASE WHEN p_delete THEN NULL ELSE p_payload END, p_delete)
      ON CONFLICT (partition_id, entity_id) DO UPDATE
      SET version = EXCLUDED.version, payload = EXCLUDED.payload, deleted = EXCLUDED.deleted;
    stored_result := jsonb_build_object('deleted', p_delete, 'version', next_version);
    INSERT INTO public.operation_results(partition_id, operation_id, request_digest,
        entity_id, entity_version, partition_seq, result)
      VALUES (p_partition, p_operation, p_digest, p_entity, next_version, next_seq, stored_result);
    INSERT INTO public.operation_log(partition_id, partition_seq, generation, operation_id,
        entity_id, entity_version, request_digest, mutation, result)
      VALUES (p_partition, next_seq, p_generation, p_operation, p_entity, next_version,
              p_digest, jsonb_build_object('deleted', p_delete, 'payload_hex',
                  CASE WHEN p_delete THEN NULL ELSE encode(p_payload, 'hex') END), stored_result);
    UPDATE public.partition_guards SET committed_seq = next_seq WHERE partition_id = p_partition;
    entity_version := next_version;
    partition_seq := next_seq;
    deleted := p_delete;
    RETURN NEXT;
END $$;

CREATE OR REPLACE FUNCTION public.whm_read(
    p_partition integer, p_generation bigint, p_entity text, p_minimum_version bigint
) RETURNS TABLE(entity_version bigint, payload bytea, deleted boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE g public.partition_guards%ROWTYPE;
BEGIN
    IF p_partition IS NULL OR p_partition < 0 OR p_generation IS NULL OR p_generation < 1 OR
       p_entity IS NULL OR p_entity = '' OR p_minimum_version IS NULL OR p_minimum_version < 0 THEN
        RAISE EXCEPTION 'invalid read request' USING ERRCODE = '22023';
    END IF;
    SELECT * INTO g FROM public.partition_guards
      WHERE partition_id = p_partition FOR SHARE;
    IF NOT FOUND OR g.generation <> p_generation OR g.state NOT IN ('ACTIVE', 'COPYING') THEN
        RAISE EXCEPTION 'stale or unavailable partition' USING ERRCODE = 'P0001';
    END IF;
    SELECT e.version, e.payload, e.deleted
      INTO entity_version, payload, deleted FROM public.entities e
      WHERE e.partition_id = p_partition AND e.entity_id = p_entity;
    IF NOT FOUND THEN
        IF p_minimum_version > 0 THEN
            RAISE EXCEPTION 'version not ready' USING ERRCODE = 'P0002';
        END IF;
        RETURN;
    END IF;
    IF entity_version < p_minimum_version THEN
        RAISE EXCEPTION 'version not ready' USING ERRCODE = 'P0002';
    END IF;
    RETURN NEXT;
END $$;

-- Coordinator entry point. It persists a rejection fence even if the caller
-- loses the response. Repeated calls return the same committed boundary.
CREATE OR REPLACE FUNCTION public.whm_fence(
    p_partition integer, p_generation bigint, p_migration uuid
) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE g public.partition_guards%ROWTYPE;
BEGIN
    IF p_partition IS NULL OR p_partition < 0 OR p_generation IS NULL OR p_generation < 1 OR p_migration IS NULL THEN
        RAISE EXCEPTION 'migration ID required' USING ERRCODE = '22023';
    END IF;
    SELECT * INTO g FROM public.partition_guards
      WHERE partition_id = p_partition FOR UPDATE;
    IF NOT FOUND OR g.generation <> p_generation OR g.migration_id IS DISTINCT FROM p_migration THEN
        RAISE EXCEPTION 'stale fence request' USING ERRCODE = 'P0001';
    END IF;
    IF g.state = 'FENCED' THEN RETURN g.fence_seq; END IF;
    IF g.state NOT IN ('ACTIVE', 'COPYING') THEN
        RAISE EXCEPTION 'partition cannot be fenced' USING ERRCODE = 'P0001';
    END IF;
    UPDATE public.partition_guards SET state = 'FENCED', fence_seq = g.committed_seq
      WHERE partition_id = p_partition;
    RETURN g.committed_seq;
END $$;

REVOKE ALL ON FUNCTION public.whm_write(integer,bigint,text,uuid,bytea,bytea,boolean),
    public.whm_read(integer,bigint,text,bigint), public.whm_fence(integer,bigint,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.whm_write(integer,bigint,text,uuid,bytea,bytea,boolean),
    public.whm_read(integer,bigint,text,bigint) TO whm_app;
