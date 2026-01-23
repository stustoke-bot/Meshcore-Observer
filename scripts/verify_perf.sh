#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-5199}"
BASE="http://127.0.0.1:${PORT}"
LOG="/tmp/observer_demo_perf.log"

fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
: > "${LOG}"

DEBUG_PERF=1 DEBUG_SQL=1 node "${ROOT_DIR}/tools/observer_demo/server.js" > "${LOG}" 2>&1 &
PID=$!

cleanup() {
  kill "${PID}" >/dev/null 2>&1 || true
  fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 1

hit() {
  local url="$1"
  echo "== ${url}"
  curl -s -o /tmp/resp -w "time_total=%{time_total} size=%{size_download}\n" "${BASE}${url}"
}

hit "/api/dashboard?channel=%23public&limit=10"
hit "/api/channels"
hit "/api/messages?limit=10"
hit "/api/meshscore"
hit "/api/repeater-rank-summary"
hit "/api/node-rank-summary"
hit "/api/observer-rank-summary"

if command -v sqlite3 >/dev/null 2>&1; then
  DB_PATH="${ROOT_DIR}/data/meshrank.db"
  if [ -f "${DB_PATH}" ]; then
    echo "== EXPLAIN QUERY PLAN"
    sqlite3 "${DB_PATH}" "EXPLAIN QUERY PLAN SELECT message_hash FROM messages WHERE channel_name = '#public' ORDER BY ts DESC LIMIT 10;"
    sqlite3 "${DB_PATH}" "EXPLAIN QUERY PLAN SELECT message_hash FROM messages ORDER BY ts DESC LIMIT 10;"
    sqlite3 "${DB_PATH}" "EXPLAIN QUERY PLAN SELECT message_hash, observer_id FROM message_observers WHERE message_hash IN ('AA', 'BB');"
    sqlite3 "${DB_PATH}" "EXPLAIN QUERY PLAN SELECT channel_name, messages FROM stats_5m WHERE bucket_start = (SELECT MAX(bucket_start) FROM stats_5m);"
  else
    echo "DB not found at ${DB_PATH}; skipping EXPLAIN QUERY PLAN."
  fi
else
  echo "sqlite3 not found; skipping EXPLAIN QUERY PLAN."
fi

echo "Perf log written to ${LOG}"
