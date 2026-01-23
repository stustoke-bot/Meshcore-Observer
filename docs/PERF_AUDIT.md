# Performance Audit

## Baseline run
- Timestamp (UTC): 2026-01-23T10:56:01Z
- Host: 127.0.0.1:5199
- Server: tools/observer_demo/server.js
- Data state: fresh data directory (empty), new SQLite DB created
- Debug flags: DEBUG_PERF=1, DEBUG_SQL=1

## Endpoint timings (curl)
Format: time_total (s), size_download (bytes)

- GET /api/auth/me: 0.0016s, 23b
- GET /api/admin/status: 0.0014s, 28b
- GET /api/channels: 0.0015s, 15b
- GET /api/messages: 0.0021s, 29b
- GET /api/rotm: 0.0034s, 76b
- GET /api/meshscore: 0.0073s, 262b
- GET /api/repeater-rank-summary: 0.0039s, 95b
- GET /api/node-rank-summary: 0.0041s, 88b
- GET /api/observer-rank-summary: 0.0027s, 89b
- GET /api/observers: 0.0014s, 11b
- GET /api/rf-latest?limit=25: 0.0034s, 51b

## Sample debug logs
Request timing log format:
- [perf] METHOD URL STATUS DURATION_MS BYTES

SQLite timing log format:
- [sql] DURATION_MS METHOD SQL

Notes:
- This baseline uses an empty dataset; payload sizes and query costs will grow with real data.
- Subsequent phases will include production-like data via scripts/seed_synthetic.js.

## Updated instrumentation
- Use `DEBUG_PERF=1` to log request timing + payload sizes.
- Use `DEBUG_SQL=1` to log SQLite query timings.
- `scripts/verify_perf.sh` runs a repeatable curl baseline plus EXPLAIN QUERY PLAN checks.

## Seeded synthetic run
- Timestamp (UTC): 2026-01-23T11:17:21Z
- Seed: `node scripts/seed_synthetic.js 200 4 20`
- Perf script: `scripts/verify_perf.sh`

Format: time_total (s), size_download (bytes)
- GET /api/dashboard?channel=#public&limit=10: 0.0139s, 8208b
- GET /api/channels: 0.0022s, 346b
- GET /api/messages?limit=10: 0.0066s, 31119b
- GET /api/meshscore: 0.0017s, 262b
- GET /api/repeater-rank-summary: 0.0031s, 95b
- GET /api/node-rank-summary: 0.0046s, 88b
- GET /api/observer-rank-summary: 0.0027s, 89b

## Seeded synthetic run (with EXPLAIN)
- Timestamp (UTC): 2026-01-23T11:19:40Z
- Seed: `node scripts/seed_synthetic.js 200 4 20` (existing)
- Perf script: `scripts/verify_perf.sh`

Format: time_total (s), size_download (bytes)
- GET /api/dashboard?channel=#public&limit=10: 0.0112s, 8208b
- GET /api/channels: 0.0071s, 346b
- GET /api/messages?limit=10: 0.0066s, 31119b
- GET /api/meshscore: 0.0012s, 262b
- GET /api/repeater-rank-summary: 0.0011s, 95b
- GET /api/node-rank-summary: 0.0049s, 88b
- GET /api/observer-rank-summary: 0.0038s, 89b

EXPLAIN QUERY PLAN highlights:
- `messages` channel filter uses `idx_messages_channel_ts`.
- `messages` latest scan uses `idx_messages_ts`.
- `message_observers` lookup uses PK autoindex.
- `stats_5m` uses `idx_stats_5m_bucket`.

## Live ingest run (60s MQTT + EXPLAIN)
- Timestamp (UTC): 2026-01-23T11:24:19Z
- Ingest: `node tools/observer_demo/mqtt_ingest.js` (60s)
- Perf script: `scripts/verify_perf.sh`

Format: time_total (s), size_download (bytes)
- GET /api/dashboard?channel=#public&limit=10: 0.0118s, 9361b
- GET /api/channels: 0.0221s, 332b
- GET /api/messages?limit=10: 0.0082s, 32194b
- GET /api/meshscore: 0.0613s, 262b
- GET /api/repeater-rank-summary: 0.0019s, 95b
- GET /api/node-rank-summary: 0.0189s, 88b
- GET /api/observer-rank-summary: 0.0089s, 92b

EXPLAIN QUERY PLAN highlights:
- `messages` channel filter uses `idx_messages_channel_ts`.
- `messages` latest scan uses `idx_messages_ts`.
- `message_observers` lookup uses PK autoindex.
- `stats_5m` uses `idx_stats_5m_bucket`.
