# Generation-Fenced Shard Transfer

This repository contains a Go/PostgreSQL prototype for recoverable,
generation-fenced shard handoffs, along with integration tests and selected raw
experiment archives.

## Reproduce

Requirements: Go, PostgreSQL tools (`initdb`, `pg_ctl`, `createdb`, and
`psql`), and a Linux or compatible Unix-like environment.

```sh
make test
make test-db
make motivation
make baseline
make bench
```

`make test-db` creates disposable local PostgreSQL databases, applies the
control and shard schemas, exercises recovery and restart paths, and removes
the temporary cluster when it completes. The versioned consistent-hash ring is
included in this repository; no sibling checkout is required.

To repeat the local studies:

```sh
make evaluate
make controlled
```

`evaluate` runs a local five-trial admission and interruption pilot. Its
selected archive is `results/runs/release-20261020-v2/`. `controlled` runs a
single-host size and concurrency sweep; its selected archive is
`results/runs/controlled-20261021-v2/`. Both are bounded local studies, not
multi-host scalability or concurrent-linearizability experiments.

## Layout

- `cmd/`: runnable baseline, motivation, pilot, and controlled-study tools.
- `internal/`: placement, ownership publication, storage, handoff, and history checking.
- `db/`: control/shard schemas and guarded admission functions.
- `scripts/`: disposable PostgreSQL setup and test support.
- `results/`: selected raw traces, metrics, manifests, and provenance.
- `experiments/`: proposed larger scenario configuration.

The source code is licensed under [Apache-2.0](LICENSE).
