# Storage contracts and initial functions

`001-control.sql` defines control records. `002-shard.sql` defines shard state, and `003-admission.sql` adds guarded write/read/fence functions plus a restricted `whm_app` role. The scripts are exercised by `make test-db` against disposable PostgreSQL databases. They are still prototype migrations: there is no schema version runner or deployment packaging.

The control file belongs in one control database; both shard files belong in each data shard. The coordinator currently uses privileged credentials; the application login must be granted only `whm_app`, must not own the tables, and must have no second role granting direct DML. Required next work:

- Database-side prepare/import/activation functions and restricted coordinator credentials.
- A migration runner and version checks.
- Online snapshot/replay, bounded log retention, chunk progress, checksums, and retry horizon.
- Broader crash, concurrency, network partition, and history tests.

All counters use positive/nonnegative signed PostgreSQL `bigint`. The Go layer must reject overflow instead of wrapping its unsigned counters. `entity_id` hashes deterministically to `partition_id`; this must be validated in the eventual service API. Guard states and phases are distinct: source serving state need not equal coordinator phase.
