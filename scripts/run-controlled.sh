#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
test_root="$(mktemp -d /tmp/whm-controlled.XXXXXX)"
cleanup() {
    pg_ctl -D "$test_root/data" -m immediate stop >/dev/null 2>&1 || true
    find "$test_root" -depth -delete
}
trap cleanup EXIT

initdb -D "$test_root/data" -A trust --no-instructions >/dev/null
pg_ctl -D "$test_root/data" -l "$test_root/server.log" \
    -o "-k $test_root -c listen_addresses='' -c fsync=on -c synchronous_commit=on" start >/dev/null

test_user="$(id -un)"
for db_name in whm_control whm_a whm_b; do
    createdb -h "$test_root" -U "$test_user" "$db_name"
done
psql -h "$test_root" -U "$test_user" -d whm_control -v ON_ERROR_STOP=1 -q -f db/001-control.sql
for db_name in whm_a whm_b; do
    psql -h "$test_root" -U "$test_user" -d "$db_name" -v ON_ERROR_STOP=1 -q \
        -f db/002-shard.sql -f db/003-admission.sql
done

export WHM_TEST_CONTROL_DSN="host=$test_root user=$test_user dbname=whm_control"
export WHM_TEST_A_DSN="host=$test_root user=$test_user dbname=whm_a"
export WHM_TEST_B_DSN="host=$test_root user=$test_user dbname=whm_b"
go run ./cmd/controlled "$@"
