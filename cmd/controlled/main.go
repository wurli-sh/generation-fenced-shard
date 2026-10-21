// controlled measures the implemented handoff under a small matrix of
// partition sizes and concurrent clients. It records every offered write and
// verifies successful writes against the final database image. This is a
// single-host load study, not a linearizability or multi-host fault study.
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
	"sync"
	"sync/atomic"
	"time"

	"generation-fenced-shard-transfer/internal/migration"
	"generation-fenced-shard-transfer/internal/ownership"
	"generation-fenced-shard-transfer/internal/placement"
	"generation-fenced-shard-transfer/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

type settings struct {
	Sizes      []int `json:"partition_entities"`
	Clients    []int `json:"clients"`
	Trials     int   `json:"trials"`
	ValueBytes int   `json:"value_bytes"`
	BeforeMS   int   `json:"before_ms"`
	AfterMS    int   `json:"after_ms"`
	ThinkMS    int   `json:"think_ms"`
}

type event struct {
	Partition  uint32 `json:"partition"`
	Entity     string `json:"entity"`
	Operation  string `json:"operation"`
	Shard      string `json:"shard"`
	Generation uint64 `json:"generation"`
	Sequence   uint64 `json:"sequence"`
	Version    uint64 `json:"version"`
	ValueSHA   string `json:"value_sha256"`
}

type attempt struct {
	StartNS    int64  `json:"start_ns"`
	EndNS      int64  `json:"end_ns"`
	Partition  uint32 `json:"partition"`
	Shard      string `json:"shard"`
	Generation uint64 `json:"generation"`
	Result     string `json:"result"`
}

type row struct {
	Size                int     `json:"entities"`
	Clients             int     `json:"clients"`
	Trial               int     `json:"trial"`
	OfferedMoving       int     `json:"offered_moving"`
	SucceededMoving     int     `json:"succeeded_moving"`
	RejectedMoving      int     `json:"rejected_moving"`
	OfferedUnaffected   int     `json:"offered_unaffected"`
	SucceededUnaffected int     `json:"succeeded_unaffected"`
	RejectedUnaffected  int     `json:"rejected_unaffected"`
	FenceToActiveMS     float64 `json:"fence_to_active_ms"`
	ObservedGapMS       float64 `json:"observed_gap_ms"`
	TransferMS          float64 `json:"transfer_ms"`
	MovingP95US         float64 `json:"moving_p95_us"`
	UnaffectedP95US     float64 `json:"unaffected_p95_us"`
	ChecksPassed        bool    `json:"checks_passed"`
	RunDirectory        string  `json:"run_directory"`
}

type workerResult struct {
	attempts []attempt
	events   []event
	err      error
}

func main() {
	sizesFlag := flag.String("sizes", "50,200,1000", "comma-separated moving partition entity counts")
	clientsFlag := flag.String("clients", "1,4,8", "comma-separated concurrent client counts")
	out := flag.String("out", "", "new artifact directory")
	cfg := settings{}
	flag.IntVar(&cfg.Trials, "trials", 3, "independent trials per setting")
	flag.IntVar(&cfg.ValueBytes, "bytes", 1024, "payload bytes")
	flag.IntVar(&cfg.BeforeMS, "before-ms", 200, "load time before migration")
	flag.IntVar(&cfg.AfterMS, "after-ms", 200, "load time after migration")
	flag.IntVar(&cfg.ThinkMS, "think-ms", 1, "per-client delay between writes")
	flag.Parse()
	cfg.Sizes = parseList(*sizesFlag)
	cfg.Clients = parseList(*clientsFlag)
	if cfg.Trials < 1 || cfg.Trials > 20 || cfg.ValueBytes < 1 || cfg.ValueBytes > 1<<20 || cfg.BeforeMS < 50 || cfg.AfterMS < 50 || cfg.ThinkMS < 0 || len(cfg.Sizes)*len(cfg.Clients)*cfg.Trials > 200 {
		fatal("invalid experiment settings")
	}
	for _, size := range cfg.Sizes {
		if size < 1 || size > 10000 {
			fatal("size must be between 1 and 10000")
		}
	}
	for _, clients := range cfg.Clients {
		if clients < 1 || clients > 64 {
			fatal("clients must be between 1 and 64")
		}
	}
	if *out == "" {
		*out = filepath.Join("results", "runs", "controlled-"+time.Now().UTC().Format("20060102T150405Z"))
	}
	check(os.MkdirAll(filepath.Dir(*out), 0755))
	check(os.Mkdir(*out, 0755))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	control := open(ctx, "WHM_TEST_CONTROL_DSN")
	a := open(ctx, "WHM_TEST_A_DSN")
	b := open(ctx, "WHM_TEST_B_DSN")
	defer control.Close()
	defer a.Close()
	defer b.Close()
	var pgVersion, fsync, syncCommit string
	check(a.QueryRow(ctx, `SHOW server_version`).Scan(&pgVersion))
	check(a.QueryRow(ctx, `SHOW fsync`).Scan(&fsync))
	check(a.QueryRow(ctx, `SHOW synchronous_commit`).Scan(&syncCommit))
	manifest := map[string]any{
		"status": "in_progress", "created_utc": time.Now().UTC().Format(time.RFC3339),
		"go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"logical_cpus": runtime.NumCPU(), "postgres_version": pgVersion,
		"fsync": fsync, "synchronous_commit": syncCommit, "settings": cfg,
		"source_sha256": sourceHashes(),
	}
	writeJSON(filepath.Join(*out, "manifest.json"), manifest)
	var rows []row
	index := 0
	for _, size := range cfg.Sizes {
		for _, clients := range cfg.Clients {
			for trial := 0; trial < cfg.Trials; trial++ {
				rows = append(rows, run(ctx, control, a, b, cfg, size, clients, trial, index, *out))
				index++
			}
		}
	}
	writeCSV(filepath.Join(*out, "metrics.csv"), rows)
	writeJSON(filepath.Join(*out, "summary.json"), rows)
	manifest["status"] = "complete_single_host_concurrent_study"
	manifest["finished_utc"] = time.Now().UTC().Format(time.RFC3339)
	writeJSON(filepath.Join(*out, "manifest.json"), manifest)
	fmt.Printf("artifacts=%s\n", *out)
	for _, r := range rows {
		fmt.Printf("entities=%d clients=%d trial=%d fence_to_active_ms=%.1f moving_rejected=%d unaffected_rejected=%d checks=%t\n", r.Size, r.Clients, r.Trial, r.FenceToActiveMS, r.RejectedMoving, r.RejectedUnaffected, r.ChecksPassed)
	}
}

func run(ctx context.Context, control, a, b *pgxpool.Pool, cfg settings, size, clients, trial, index int, root string) row {
	p := uint32(100 + 2*index)
	q := p + 1
	version := int64(5000 + index)
	dir := filepath.Join(root, fmt.Sprintf("entities-%05d_clients-%02d_trial-%02d", size, clients, trial))
	check(os.Mkdir(dir, 0755))
	checkExec(control.Exec(ctx, `INSERT INTO configurations(version,partition_count,virtual_nodes,hash_spec,membership,digest) VALUES($1,1024,100,$2,'["A","B"]',$3)`, version, placement.RingSpec+"/partition-decimal", fmt.Sprintf("controlled-%d", index)))
	for _, part := range []uint32{p, q} {
		checkExec(control.Exec(ctx, `INSERT INTO owners(partition_id,owner_shard,generation,config_version) VALUES($1,'A',1,$2)`, part, version))
		checkExec(a.Exec(ctx, `INSERT INTO partition_guards(partition_id,generation,state) VALUES($1,1,'ACTIVE')`, part))
	}
	ids := keysForPartition(p, size, fmt.Sprintf("controlled-%d", index))
	stable := keysForPartition(q, 1, fmt.Sprintf("stable-%d", index))[0]
	aStore, bStore := &store.Store{DB: a}, &store.Store{DB: b}
	old := ownership.Route{Partition: p, Owner: "A", Generation: 1}
	newRoute := ownership.Route{Partition: p, Owner: "B", Generation: 2}
	other := ownership.Route{Partition: q, Owner: "A", Generation: 1}
	serial := uint64(10000000 + index*1000000)
	var preload []event
	for i, id := range ids {
		op := uuid(atomic.AddUint64(&serial, 1))
		payload := value(cfg.ValueBytes, fmt.Sprintf("seed-%d", i))
		r, err := aStore.Write(ctx, old, op, id, payload, false)
		check(err)
		preload = append(preload, makeEvent(old, op, id, payload, r))
	}
	op := uuid(atomic.AddUint64(&serial, 1))
	stablePayload := value(cfg.ValueBytes, "stable-seed")
	r, err := aStore.Write(ctx, other, op, stable, stablePayload, false)
	check(err)
	preload = append(preload, makeEvent(other, op, stable, stablePayload, r))
	coord := &migration.Coordinator{Control: control, Shards: map[string]*pgxpool.Pool{"A": a, "B": b}}
	migrationID := uuid(uint64(90000000 + index))
	check(coord.Begin(ctx, migrationID, int32(p), "A", "B", 1, version))
	start := time.Now()
	var switched atomic.Bool
	stop := make(chan struct{})
	results := make(chan workerResult, clients)
	var wg sync.WaitGroup
	for worker := 0; worker < clients; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var out workerResult
			for count := 0; ; count++ {
				select {
				case <-stop:
					results <- out
					return
				default:
				}
				part := p
				route := old
				entity := ids[(worker+count*clients)%len(ids)]
				if (worker+count)%2 == 1 {
					part, route, entity = q, other, stable
				} else if switched.Load() {
					route = newRoute
				}
				payload := value(cfg.ValueBytes, fmt.Sprintf("work-%d-%d", worker, count))
				opID := uuid(atomic.AddUint64(&serial, 1))
				at := attempt{StartNS: time.Since(start).Nanoseconds(), Partition: part, Shard: route.Owner, Generation: route.Generation}
				selected := aStore
				if route.Owner == "B" {
					selected = bStore
				}
				written, e := selected.Write(ctx, route, opID, entity, payload, false)
				at.EndNS = time.Since(start).Nanoseconds()
				switch {
				case e == nil:
					at.Result = "ok"
					out.events = append(out.events, makeEvent(route, opID, entity, payload, written))
				case errors.Is(e, store.ErrStaleRoute):
					at.Result = "stale_route"
				default:
					at.Result = "error"
					out.err = e
				}
				out.attempts = append(out.attempts, at)
				if out.err != nil {
					results <- out
					return
				}
				if cfg.ThinkMS > 0 {
					timer := time.NewTimer(time.Duration(cfg.ThinkMS) * time.Millisecond)
					select {
					case <-stop:
						timer.Stop()
						results <- out
						return
					case <-timer.C:
					}
				}
			}
		}(worker)
	}
	time.Sleep(time.Duration(cfg.BeforeMS) * time.Millisecond)
	transferStart := time.Since(start).Nanoseconds()
	var fenceAt, activeAt int64
	for step := 0; step < 5; step++ {
		phase, err := coord.Step(ctx, migrationID)
		check(err)
		if phase == "FENCED" {
			fenceAt = time.Since(start).Nanoseconds()
		}
		if phase == "COMMITTED" {
			switched.Store(true)
		}
		if phase == "ACTIVE" {
			activeAt = time.Since(start).Nanoseconds()
		}
	}
	transferEnd := time.Since(start).Nanoseconds()
	time.Sleep(time.Duration(cfg.AfterMS) * time.Millisecond)
	close(stop)
	wg.Wait()
	close(results)
	events := append([]event(nil), preload...)
	var attempts []attempt
	for result := range results {
		check(result.err)
		events = append(events, result.events...)
		attempts = append(attempts, result.attempts...)
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].StartNS < attempts[j].StartNS })
	job, err := coord.Load(ctx, migrationID)
	check(err)
	if job.Phase != "RETIRED" || fenceAt == 0 || activeAt <= fenceAt {
		fatal("incomplete migration in %s", dir)
	}
	findings := verify(ctx, aStore, bStore, p, q, ids, stable, events, uint64(job.FenceSeq))
	writeJSONLines(filepath.Join(dir, "events.jsonl"), events)
	writeJSONLines(filepath.Join(dir, "attempts.jsonl"), attempts)
	writeJSON(filepath.Join(dir, "checks.json"), map[string]any{"findings": findings, "fence_sequence": job.FenceSeq, "phase": job.Phase})
	writeJSON(filepath.Join(dir, "config.json"), map[string]any{"entities": size, "clients": clients, "trial": trial, "value_bytes": cfg.ValueBytes, "before_ms": cfg.BeforeMS, "after_ms": cfg.AfterMS, "think_ms": cfg.ThinkMS, "moving_partition": p, "unaffected_partition": q})
	if len(findings) != 0 {
		fatal("%d consistency findings in %s: %s", len(findings), dir, strings.Join(findings, "; "))
	}
	result := row{Size: size, Clients: clients, Trial: trial, FenceToActiveMS: float64(activeAt-fenceAt) / 1e6, TransferMS: float64(transferEnd-transferStart) / 1e6, ChecksPassed: true, RunDirectory: dir}
	var lastSource, firstTarget int64
	var movingLatencies, unaffectedLatencies []int64
	for _, at := range attempts {
		if at.Partition == p {
			result.OfferedMoving++
			if at.Result == "ok" {
				result.SucceededMoving++
				movingLatencies = append(movingLatencies, at.EndNS-at.StartNS)
				if at.Shard == "A" && at.EndNS > lastSource {
					lastSource = at.EndNS
				}
				if at.Shard == "B" && (firstTarget == 0 || at.EndNS < firstTarget) {
					firstTarget = at.EndNS
				}
			} else {
				result.RejectedMoving++
			}
		} else {
			result.OfferedUnaffected++
			if at.Result == "ok" {
				result.SucceededUnaffected++
				unaffectedLatencies = append(unaffectedLatencies, at.EndNS-at.StartNS)
			} else {
				result.RejectedUnaffected++
			}
		}
	}
	if lastSource == 0 || firstTarget == 0 {
		fatal("missing successful writes on both sides of handoff in %s", dir)
	}
	result.ObservedGapMS = float64(firstTarget-lastSource) / 1e6
	result.MovingP95US = percentile(movingLatencies, 0.95) / 1000
	result.UnaffectedP95US = percentile(unaffectedLatencies, 0.95) / 1000
	return result
}

func verify(ctx context.Context, aStore, bStore *store.Store, p, q uint32, ids []string, stable string, events []event, fence uint64) []string {
	var findings []string
	for _, part := range []uint32{p, q} {
		var writes []event
		for _, e := range events {
			if e.Partition == part {
				writes = append(writes, e)
			}
		}
		sort.Slice(writes, func(i, j int) bool { return writes[i].Sequence < writes[j].Sequence })
		versions := make(map[string]uint64)
		latest := make(map[string]event)
		for i, e := range writes {
			if e.Sequence != uint64(i+1) {
				findings = append(findings, fmt.Sprintf("partition %d sequence gap at %d", part, i+1))
			}
			if e.Version != versions[e.Entity]+1 {
				findings = append(findings, fmt.Sprintf("entity %s version gap at %d", e.Entity, e.Version))
			}
			versions[e.Entity] = e.Version
			latest[e.Entity] = e
			if part == p && ((e.Sequence <= fence && (e.Shard != "A" || e.Generation != 1)) || (e.Sequence > fence && (e.Shard != "B" || e.Generation != 2))) {
				findings = append(findings, fmt.Sprintf("wrong owner for partition %d sequence %d", part, e.Sequence))
			}
		}
		all := ids
		selected := bStore
		route := ownership.Route{Partition: p, Owner: "B", Generation: 2}
		if part == q {
			all = []string{stable}
			selected = aStore
			route = ownership.Route{Partition: q, Owner: "A", Generation: 1}
		}
		for _, id := range all {
			got, err := selected.Read(ctx, route, id, 0)
			if err != nil {
				findings = append(findings, fmt.Sprintf("read %s: %v", id, err))
				continue
			}
			want := latest[id]
			if got.Token.Version != want.Version || digest(got.Payload) != want.ValueSHA {
				findings = append(findings, fmt.Sprintf("final state mismatch for %s", id))
			}
		}
	}
	return findings
}

func makeEvent(route ownership.Route, operation, entity string, payload []byte, r store.Result) event {
	return event{Partition: route.Partition, Entity: entity, Operation: operation, Shard: route.Owner, Generation: route.Generation, Sequence: r.PartitionSeq, Version: r.Token.Version, ValueSHA: digest(payload)}
}

func keysForPartition(part uint32, count int, prefix string) []string {
	keys := make([]string, 0, count)
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		assigned, err := placement.PartitionFor(key, 1024)
		check(err)
		if assigned == part {
			keys = append(keys, key)
		}
	}
	return keys
}

func parseList(raw string) []int {
	var out []int
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		check(err)
		out = append(out, n)
	}
	return out
}

func open(ctx context.Context, env string) *pgxpool.Pool {
	dsn := os.Getenv(env)
	if dsn == "" {
		fatal("%s is required", env)
	}
	p, err := pgxpool.New(ctx, dsn)
	check(err)
	check(p.Ping(ctx))
	return p
}

func value(size int, tag string) []byte {
	b := make([]byte, size)
	copy(b, tag)
	for i := len(tag); i < size; i++ {
		b[i] = byte('a' + i%26)
	}
	return b
}

func uuid(n uint64) string   { return fmt.Sprintf("00000000-0000-4000-8000-%012x", n) }
func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func percentile(raw []int64, p float64) float64 {
	if len(raw) == 0 {
		return 0
	}
	x := append([]int64(nil), raw...)
	sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
	return float64(x[int(float64(len(x)-1)*p+0.5)])
}
func checkExec(_ any, err error) { check(err) }
func check(err error) {
	if err != nil {
		fatal("%v", err)
	}
}
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "controlled: "+format+"\n", args...)
	os.Exit(1)
}
func writeJSON(path string, v any) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	e := json.NewEncoder(f)
	e.SetIndent("", "  ")
	check(e.Encode(v))
}
func writeJSONLines[T any](path string, values []T) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	e := json.NewEncoder(f)
	for _, v := range values {
		check(e.Encode(v))
	}
}
func writeCSV(path string, rows []row) {
	f, err := os.Create(path)
	check(err)
	defer f.Close()
	w := csv.NewWriter(f)
	check(w.Write([]string{"entities", "clients", "trial", "offered_moving", "succeeded_moving", "rejected_moving", "offered_unaffected", "succeeded_unaffected", "rejected_unaffected", "fence_to_active_ms", "observed_gap_ms", "transfer_ms", "moving_p95_us", "unaffected_p95_us", "checks_passed", "run_directory"}))
	for _, r := range rows {
		check(w.Write([]string{strconv.Itoa(r.Size), strconv.Itoa(r.Clients), strconv.Itoa(r.Trial), strconv.Itoa(r.OfferedMoving), strconv.Itoa(r.SucceededMoving), strconv.Itoa(r.RejectedMoving), strconv.Itoa(r.OfferedUnaffected), strconv.Itoa(r.SucceededUnaffected), strconv.Itoa(r.RejectedUnaffected), fmt.Sprintf("%.6f", r.FenceToActiveMS), fmt.Sprintf("%.6f", r.ObservedGapMS), fmt.Sprintf("%.6f", r.TransferMS), fmt.Sprintf("%.6f", r.MovingP95US), fmt.Sprintf("%.6f", r.UnaffectedP95US), strconv.FormatBool(r.ChecksPassed), r.RunDirectory}))
	}
	w.Flush()
	check(w.Error())
}
func sourceHashes() map[string]string {
	paths := []string{"cmd/controlled/main.go", "internal/migration/stopcopy.go", "internal/store/store.go", "db/001-control.sql", "db/002-shard.sql", "db/003-admission.sql", "scripts/run-controlled.sh"}
	out := make(map[string]string)
	for _, path := range paths {
		b, err := os.ReadFile(path)
		check(err)
		out[path] = digest(b)
	}
	return out
}
