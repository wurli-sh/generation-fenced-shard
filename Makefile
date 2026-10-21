.PHONY: test test-db evaluate controlled baseline motivation bench

RUN ?= results/runs/release-20261020-v2

test:
	go test -race ./...

test-db:
	bash scripts/test-postgres.sh

evaluate:
	bash scripts/run-evaluation.sh

controlled:
	bash scripts/run-controlled.sh

baseline:
	go run ./cmd/baseline

motivation:
	go run ./cmd/motivation

bench:
	go test ./internal/placement -run '^$$' -bench BenchmarkLookup -benchmem -count=5
