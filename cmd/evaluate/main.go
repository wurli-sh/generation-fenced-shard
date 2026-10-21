// evaluate runs bounded, sequential PostgreSQL experiments and archives raw
// operations. It is a local pilot harness, not a distributed fault study.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"generation-fenced-shard-transfer/internal/history"
	"generation-fenced-shard-transfer/internal/migration"
	"generation-fenced-shard-transfer/internal/ownership"
	"generation-fenced-shard-transfer/internal/placement"
	"generation-fenced-shard-transfer/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

type settings struct {
	Trials   int `json:"trials"`
	Entities int `json:"entities_per_moving_partition"`
	Bytes    int `json:"value_bytes"`
	Ops      int `json:"admission_operations_per_trial"`
	Warmup   int `json:"admission_warmup_operations"`
	TickMS   int `json:"offered_interval_ms"`
}

type attempt struct {
	InvokedNS  int64  `json:"invoked_ns"`
	FinishedNS int64  `json:"finished_ns"`
	Partition  uint32 `json:"partition"`
	Shard      string `json:"shard"`
	Generation uint64 `json:"generation"`
	Result     string `json:"result"`
	LatencyNS  int64  `json:"latency_ns"`
}

type phase struct {
	Name string `json:"name"`
	AtNS int64  `json:"at_ns"`
}

type metric struct {
	Scenario          string  `json:"scenario"`
	Trial             int     `json:"trial"`
	Operations        int     `json:"operations"`
	Successes         int     `json:"successes"`
	Unavailable       int     `json:"unavailable"`
	PauseMS           float64 `json:"pause_ms,omitempty"`
	TransferMS        float64 `json:"transfer_ms,omitempty"`
	P50US             float64 `json:"p50_us,omitempty"`
	P95US             float64 `json:"p95_us,omitempty"`
	ChecksPassed      bool    `json:"checks_passed"`
	RunDirectory      string  `json:"run_directory"`
	MovingUnavailable int     `json:"moving_unavailable"`
	OtherUnavailable  int     `json:"other_unavailable"`
}

func main() {
	cfg := settings{}
	out := flag.String("out", "", "new artifact directory; default results/runs/<UTC timestamp>")
	flag.IntVar(&cfg.Trials, "trials", 5, "independent trial pairs")
	flag.IntVar(&cfg.Entities, "entities", 200, "entities in the moving partition")
	flag.IntVar(&cfg.Bytes, "bytes", 1024, "payload bytes")
	flag.IntVar(&cfg.Ops, "admission-ops", 300, "operations per admission variant and trial")
	flag.IntVar(&cfg.Warmup, "admission-warmup", 100, "untimed operations before each admission variant")
	flag.IntVar(&cfg.TickMS, "tick-ms", 2, "offered workload interval in milliseconds")
	flag.Parse()
	if cfg.Trials < 1 || cfg.Trials > 100 || cfg.Entities < 1 || cfg.Entities > 100000 || cfg.Bytes < 1 || cfg.Bytes > 1<<20 || cfg.Ops < 10 || cfg.Warmup < 0 || cfg.TickMS < 1 {
		fatal("invalid experiment settings")
	}
	if *out == "" {
		*out = filepath.Join("results", "runs", time.Now().UTC().Format("20060102T150405Z"))
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0755); err != nil {
		fatal("create artifact parent: %v", err)
	}
	if err := os.Mkdir(*out, 0755); err != nil {
		fatal("artifact directory must be new: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	control := open(ctx, "WHM_TEST_CONTROL_DSN")
	a := open(ctx, "WHM_TEST_A_DSN")
	b := open(ctx, "WHM_TEST_B_DSN")
	defer control.Close()
	defer a.Close()
	defer b.Close()
	var postgresVersion, fsync, synchronousCommit string
	check(a.QueryRow(ctx, `SHOW server_version`).Scan(&postgresVersion))
	check(a.QueryRow(ctx, `SHOW fsync`).Scan(&fsync))
	check(a.QueryRow(ctx, `SHOW synchronous_commit`).Scan(&synchronousCommit))
	manifest := map[string]any{"status": "in_progress", "created_utc": time.Now().UTC().Format(time.RFC3339), "go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "postgres_version": postgresVersion, "fsync": fsync, "synchronous_commit": synchronousCommit, "settings": cfg, "source_sha256": sourceHashes()}
	writeJSON(filepath.Join(*out, "manifest.json"), manifest)
	var metrics []metric
	for trial := 0; trial < cfg.Trials; trial++ {
		metrics = append(metrics, admission(ctx, a, cfg, trial, *out)...)
		for _, mode := range []string{"global_pause", "partition_stop_copy"} {
			metrics = append(metrics, migrationTrial(ctx, control, a, b, cfg, trial, mode, *out))
		}
	}
	writeJSON(filepath.Join(*out, "summary.json"), metrics)
	writeMetrics(filepath.Join(*out, "metrics.csv"), metrics)
	manifest["status"] = "complete_local_pilot"
	manifest["finished_utc"] = time.Now().UTC().Format(time.RFC3339)
	writeJSON(filepath.Join(*out, "manifest.json"), manifest)
	fmt.Printf("artifacts=%s\n", *out)
	for _, m := range metrics {
		fmt.Printf("%s trial=%d successes=%d unavailable=%d pause_ms=%.2f p50_us=%.1f checks=%t\n", m.Scenario, m.Trial, m.Successes, m.Unavailable, m.PauseMS, m.P50US, m.ChecksPassed)
	}
}

func open(ctx context.Context, env string) *pgxpool.Pool {
	dsn := os.Getenv(env)
	if dsn == "" {
		fatal("%s is required", env)
	}
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fatal("connect %s: %v", env, err)
	}
	if err := p.Ping(ctx); err != nil {
		fatal("ping %s: %v", env, err)
	}
	return p
}

func admission(ctx context.Context, db *pgxpool.Pool, cfg settings, trial int, root string) []metric {
	_, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS eval_plain (partition_id integer NOT NULL, entity_id text NOT NULL, version bigint NOT NULL, payload bytea NOT NULL, PRIMARY KEY(partition_id,entity_id))`)
	check(err)
	_, err = db.Exec(ctx, `CREATE OR REPLACE FUNCTION eval_plain_write(p_partition integer,p_entity text,p_payload bytea) RETURNS bigint LANGUAGE plpgsql AS $$
	DECLARE next_version bigint;
	BEGIN
		INSERT INTO eval_plain(partition_id,entity_id,version,payload) VALUES(p_partition,p_entity,1,p_payload)
		ON CONFLICT(partition_id,entity_id) DO UPDATE SET version=eval_plain.version+1,payload=EXCLUDED.payload
		RETURNING version INTO next_version;
		RETURN next_version;
	END $$`)
	check(err)
	_, err = db.Exec(ctx, `CREATE OR REPLACE FUNCTION eval_generation_write(p_partition integer,p_generation bigint,p_entity text,p_payload bytea) RETURNS bigint LANGUAGE plpgsql AS $$
	DECLARE g partition_guards%ROWTYPE; next_version bigint;
	BEGIN
		SELECT * INTO g FROM partition_guards WHERE partition_id=p_partition FOR UPDATE;
		IF NOT FOUND OR g.generation<>p_generation OR g.state<>'ACTIVE' THEN
			RAISE EXCEPTION 'stale partition';
		END IF;
		INSERT INTO eval_plain(partition_id,entity_id,version,payload) VALUES(p_partition,p_entity,1,p_payload)
		ON CONFLICT(partition_id,entity_id) DO UPDATE SET version=eval_plain.version+1,payload=EXCLUDED.payload
		RETURNING version INTO next_version;
		RETURN next_version;
	END $$`)
	check(err)
	order := []string{"plain_upsert", "generation_guard", "guarded_write"}
	for rotate := 0; rotate < trial%len(order); rotate++ {
		order[0], order[1], order[2] = order[1], order[2], order[0]
	}
	var result []metric
	for variantIndex, variant := range order {
		partition := int32(2000 + trial*3 + variantIndex)
		if variant != "plain_upsert" {
			_, err = db.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, partition)
			check(err)
		}
		var durations []int64
		for i := -cfg.Warmup; i < cfg.Ops; i++ {
			entity := fmt.Sprintf("admission-%d", (i+cfg.Warmup)%32)
			payload := value(cfg.Bytes, fmt.Sprintf("trial-%d-op-%d", trial, i))
			start := time.Now()
			if variant == "plain_upsert" {
				var version int64
				err = db.QueryRow(ctx, `SELECT eval_plain_write($1,$2,$3)`, partition, entity, payload).Scan(&version)
			} else if variant == "generation_guard" {
				var version int64
				err = db.QueryRow(ctx, `SELECT eval_generation_write($1,$2,$3,$4)`, partition, 1, entity, payload).Scan(&version)
			} else {
				op := uuid(uint64(1000000 + trial*100000 + variantIndex*(cfg.Ops+cfg.Warmup) + i + cfg.Warmup))
				_, err = (&store.Store{DB: db}).Write(ctx, ownership.Route{Partition: uint32(partition), Owner: "A", Generation: 1}, op, entity, payload, false)
			}
			check(err)
			if i >= 0 {
				durations = append(durations, time.Since(start).Nanoseconds())
			}
		}
		dir := filepath.Join(root, fmt.Sprintf("admission-%s-%02d", variant, trial))
		check(os.Mkdir(dir, 0755))
		writeJSON(filepath.Join(dir, "latencies_ns.json"), durations)
		writeJSON(filepath.Join(dir, "config.json"), map[string]any{"variant": variant, "trial": trial, "operations": cfg.Ops, "warmup_operations": cfg.Warmup, "value_bytes": cfg.Bytes, "entities": 32, "sequential": true})
		result = append(result, metric{Scenario: variant, Trial: trial, Operations: cfg.Ops, Successes: cfg.Ops, P50US: percentile(durations, 0.5) / 1000, P95US: percentile(durations, 0.95) / 1000, ChecksPassed: true, RunDirectory: dir})
	}
	return result
}

func migrationTrial(ctx context.Context, control, a, b *pgxpool.Pool, cfg settings, trial int, mode, root string) metric {
	modeIndex := 0
	if mode == "partition_stop_copy" {
		modeIndex = 1
	}
	p := uint32(100 + trial*4 + modeIndex*2)
	q := p + 1
	configVersion := int64(3000 + trial*2 + modeIndex)
	_, err := control.Exec(ctx, `INSERT INTO configurations(version,partition_count,virtual_nodes,hash_spec,membership,digest) VALUES($1,1024,100,$2,'["A","B"]',$3)`, configVersion, placement.RingSpec+"/partition-decimal", fmt.Sprintf("pilot-%d-%s", trial, mode))
	check(err)
	for _, part := range []uint32{p, q} {
		_, err = control.Exec(ctx, `INSERT INTO owners(partition_id,owner_shard,generation,config_version) VALUES($1,'A',1,$2)`, part, configVersion)
		check(err)
		_, err = a.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, part)
		check(err)
	}
	start := time.Now()
	var events []history.Event
	ids := keysForPartition(p, cfg.Entities, 1024, fmt.Sprintf("moving-%d-%s", trial, mode))
	other := keysForPartition(q, 1, 1024, fmt.Sprintf("stable-%d-%s", trial, mode))[0]
	allIDs := append(append([]string(nil), ids...), other)
	serial := uint64(3000000 + trial*1000000 + modeIndex*500000)
	write := func(part uint32, route ownership.Route, entity string, payload []byte) (store.Result, error) {
		serial++
		db := a
		if route.Owner == "B" {
			db = b
		}
		begin := time.Since(start).Nanoseconds()
		r, err := (&store.Store{DB: db}).Write(ctx, route, uuid(serial), entity, payload, false)
		end := time.Since(start).Nanoseconds()
		if err == nil {
			events = append(events, history.Event{Kind: "write", Partition: part, Entity: entity, Operation: uuid(serial), Shard: route.Owner, Generation: route.Generation, Sequence: r.PartitionSeq, Version: r.Token.Version, Value: digest(payload), InvokedNS: begin, FinishedNS: end})
		}
		return r, err
	}
	for i, id := range ids {
		_, err = write(p, ownership.Route{Partition: p, Owner: "A", Generation: 1}, id, value(cfg.Bytes, fmt.Sprintf("seed-%d", i)))
		check(err)
	}
	_, err = write(q, ownership.Route{Partition: q, Owner: "A", Generation: 1}, other, value(cfg.Bytes, "stable-seed"))
	check(err)
	preloadEvents := events
	events = nil
	migrationID := uuid(uint64(9000000 + trial*2 + modeIndex))
	coord := &migration.Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
	check(coord.Begin(ctx, migrationID, int32(p), "A", "B", 1, configVersion))
	var switched, paused atomic.Bool
	stop := make(chan struct{})
	type workloadResult struct {
		events   []history.Event
		attempts []attempt
	}
	done := make(chan workloadResult, 1)
	go func() {
		var result workloadResult
		ticker := time.NewTicker(time.Duration(cfg.TickMS) * time.Millisecond)
		defer ticker.Stop()
		count := 0
		for {
			select {
			case <-stop:
				done <- result
				return
			case <-ticker.C:
			}
			part, entity := p, ids[count%len(ids)]
			if count%2 == 1 {
				part, entity = q, other
			}
			count++
			route := ownership.Route{Partition: part, Owner: "A", Generation: 1}
			if part == p && switched.Load() {
				route.Owner, route.Generation = "B", 2
			}
			invoked := time.Since(start).Nanoseconds()
			at := attempt{InvokedNS: invoked, Partition: part, Shard: route.Owner, Generation: route.Generation}
			if paused.Load() {
				at.Result = "global_pause"
			} else {
				serial++
				payload := value(cfg.Bytes, fmt.Sprintf("work-%d", count))
				db := a
				if route.Owner == "B" {
					db = b
				}
				r, e := (&store.Store{DB: db}).Write(ctx, route, uuid(serial), entity, payload, false)
				if e == nil {
					at.Result = "ok"
					result.events = append(result.events, history.Event{Kind: "write", Partition: part, Entity: entity, Operation: uuid(serial), Shard: route.Owner, Generation: route.Generation, Sequence: r.PartitionSeq, Version: r.Token.Version, Value: digest(payload), InvokedNS: invoked, FinishedNS: time.Since(start).Nanoseconds()})
				} else if errors.Is(e, store.ErrStaleRoute) {
					at.Result = "unavailable"
				} else {
					at.Result = "error: " + e.Error()
				}
			}
			at.FinishedNS = time.Since(start).Nanoseconds()
			at.LatencyNS = at.FinishedNS - invoked
			result.attempts = append(result.attempts, at)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	if mode == "global_pause" {
		paused.Store(true)
	}
	var phases []phase
	transferStart := time.Now()
	for step := 0; step < 5; step++ {
		name, e := coord.Step(ctx, migrationID)
		check(e)
		phases = append(phases, phase{Name: name, AtNS: time.Since(start).Nanoseconds()})
		if name == "COMMITTED" {
			switched.Store(true)
		}
	}
	transferMS := float64(time.Since(transferStart).Nanoseconds()) / 1e6
	if mode == "global_pause" {
		paused.Store(false)
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	work := <-done
	events = append(preloadEvents, work.events...)
	var fenceSequence uint64
	j, err := coord.Load(ctx, migrationID)
	check(err)
	fenceSequence = uint64(j.FenceSeq)
	final := make(map[string]history.State)
	for _, id := range allIDs {
		part, db, route := p, b, ownership.Route{Partition: p, Owner: "B", Generation: 2}
		if id == other {
			part, db, route = q, a, ownership.Route{Partition: q, Owner: "A", Generation: 1}
		}
		_ = part
		r, e := (&store.Store{DB: db}).Read(ctx, route, id, 0)
		check(e)
		final[id] = history.State{Version: r.Token.Version, Value: digest(r.Payload), Deleted: r.Deleted}
	}
	findings := history.Check(events, []history.Fence{{Partition: p, Source: "A", Target: "B", OldGeneration: 1, Sequence: fenceSequence}}, final)
	dir := filepath.Join(root, fmt.Sprintf("migration-%s-%02d", mode, trial))
	check(os.Mkdir(dir, 0755))
	writeJSON(filepath.Join(dir, "config.json"), map[string]any{"mode": mode, "trial": trial, "partition": p, "unaffected_partition": q, "entities": cfg.Entities, "value_bytes": cfg.Bytes, "tick_ms": cfg.TickMS, "sequential_workload": true})
	writeJSONLines(filepath.Join(dir, "history.jsonl"), events)
	writeJSONLines(filepath.Join(dir, "attempts.jsonl"), work.attempts)
	writeJSONLines(filepath.Join(dir, "phases.jsonl"), phases)
	writeJSON(filepath.Join(dir, "checks.json"), map[string]any{"findings": findings, "final_state": final, "fence_sequence": fenceSequence})
	if len(findings) != 0 {
		fatal("history checker found %d problems in %s", len(findings), dir)
	}
	var lastSource, firstTarget int64
	var latencies []int64
	var unavailable, successes, movingUnavailable, otherUnavailable int
	for _, at := range work.attempts {
		if at.Result == "ok" {
			successes++
			latencies = append(latencies, at.LatencyNS)
			if at.Partition == p && at.Shard == "A" {
				lastSource = at.FinishedNS
			}
			if at.Partition == p && at.Shard == "B" && firstTarget == 0 {
				firstTarget = at.FinishedNS
			}
		} else {
			if strings.HasPrefix(at.Result, "error:") {
				fatal("unexpected workload error in %s: %s", dir, at.Result)
			}
			unavailable++
			if at.Partition == p {
				movingUnavailable++
			} else {
				otherUnavailable++
			}
		}
	}
	if lastSource == 0 || firstTarget == 0 {
		fatal("moving partition did not have observed writes before and after transfer")
	}
	return metric{Scenario: mode, Trial: trial, Operations: len(work.attempts), Successes: successes, Unavailable: unavailable, MovingUnavailable: movingUnavailable, OtherUnavailable: otherUnavailable, PauseMS: float64(firstTarget-lastSource) / 1e6, TransferMS: transferMS, P50US: percentile(latencies, 0.5) / 1000, P95US: percentile(latencies, 0.95) / 1000, ChecksPassed: true, RunDirectory: dir}
}

func keysForPartition(part uint32, count int, total uint32, prefix string) []string {
	keys := make([]string, 0, count)
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		p, err := placement.PartitionFor(key, total)
		check(err)
		if p == part {
			keys = append(keys, key)
		}
	}
	return keys
}

func value(size int, tag string) []byte {
	b := make([]byte, size)
	copy(b, []byte(tag))
	for i := len(tag); i < size; i++ {
		b[i] = byte('a' + i%26)
	}
	return b
}
func digest(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }
func uuid(n uint64) string   { return fmt.Sprintf("00000000-0000-4000-8000-%012x", n) }
func percentile(raw []int64, p float64) float64 {
	if len(raw) == 0 {
		return 0
	}
	x := append([]int64(nil), raw...)
	sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
	index := int(float64(len(x)-1)*p + 0.5)
	return float64(x[index])
}
func writeJSON(path string, v any) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	e := json.NewEncoder(f)
	e.SetIndent("", "  ")
	check(e.Encode(v))
}
func writeJSONLines[T any](path string, v []T) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	e := json.NewEncoder(f)
	for _, item := range v {
		check(e.Encode(item))
	}
}
func writeMetrics(path string, values []metric) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	w := csv.NewWriter(f)
	check(w.Write([]string{"scenario", "trial", "operations", "successes", "unavailable", "pause_ms", "transfer_ms", "p50_us", "p95_us", "checks_passed", "run_directory", "moving_unavailable", "other_unavailable"}))
	for _, m := range values {
		check(w.Write([]string{m.Scenario, strconv.Itoa(m.Trial), strconv.Itoa(m.Operations), strconv.Itoa(m.Successes), strconv.Itoa(m.Unavailable), fmt.Sprintf("%.6f", m.PauseMS), fmt.Sprintf("%.6f", m.TransferMS), fmt.Sprintf("%.6f", m.P50US), fmt.Sprintf("%.6f", m.P95US), strconv.FormatBool(m.ChecksPassed), m.RunDirectory, strconv.Itoa(m.MovingUnavailable), strconv.Itoa(m.OtherUnavailable)}))
	}
	w.Flush()
	check(w.Error())
}
func sourceHashes() map[string]string {
	paths := []string{"internal/placement/baseline.go", "internal/placement/partitions.go", "internal/migration/stopcopy.go", "internal/store/store.go", "internal/history/check.go", "db/001-control.sql", "db/002-shard.sql", "db/003-admission.sql", "cmd/evaluate/main.go", "cmd/plot/main.go", "scripts/run-evaluation.sh"}
	out := make(map[string]string)
	for _, path := range paths {
		b, err := os.ReadFile(path)
		check(err)
		out[path] = digest(b)
	}
	return out
}
func check(err error) {
	if err != nil {
		fatal("%v", err)
	}
}
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "evaluation: "+strings.TrimSuffix(format, "\n")+"\n", args...)
	os.Exit(1)
}
