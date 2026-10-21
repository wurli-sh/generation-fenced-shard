# Results

No completed full-scale research experiments are included. The earlier [development pilot](pilots/2026-10-17-local.md) records a deterministic missing-row example and lookup samples; it used an older external ring and is retained as historical context. The selected [five-trial local pilot](runs/release-20261020-v2/manifest.json) is committed with PostgreSQL admission and interruption measurements, successful operation histories, offered attempts, phase events, final-state checks, and per-trial metrics. Its `metrics.csv` separates rejected requests for the moving and unaffected partitions.

The selected [single-host concurrent sweep](runs/controlled-20261021-v2/manifest.json) has 27 fresh partition-pair runs across three sizes, three client counts, and three trials per setting. Its `metrics.csv` records fence-to-activation intervals and offered-write outcomes; each case includes raw attempts, successful write events, configuration, and final-state checks. The checks establish sequence and final-state consistency for these runs, not concurrent linearizability. Both selected studies place control and shard databases in one local PostgreSQL instance.

Future layout:

```text
runs/<run-id>/
  manifest.json       machine/tool/source provenance and exit status
  config.json         exact configuration and seed
  history.jsonl       operation invocations, completions, retries, session versions
  phases.jsonl        migration decisions, shard evidence, failure injection events
  metrics.csv         latency/throughput/resources/backlog time series
  checks.json         oracle findings and unresolved operations
  logs/              process logs
```

Keep raw runs immutable. Derive plots or tables using versioned analysis scripts and record input run IDs. Only `release-20261020-v2/` and `controlled-20261021-v2/` are tracked within `runs/`; subsequent local runs are ignored until explicitly selected. No synthetic chart should be presented as a measurement.
