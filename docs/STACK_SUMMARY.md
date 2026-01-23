# Stack Summary

- Runtime: Node.js (HTTP + SSE) serving the demo frontend.
- Frontend: `tools/observer_demo/index.html` (static HTML/JS/CSS).
- Backend: `tools/observer_demo/server.js` (API + SSE + SQLite).
- Ingest: `tools/observer_demo/mqtt_ingest.js` (MQTT -> SQLite + NDJSON).
- Storage: `data/meshrank.db` (SQLite), plus NDJSON files in `data/`.
- Bootstrap: `GET /api/dashboard` consolidates initial auth, channels/messages, rollup stats, meshscore, and ROTM payload.
- Streaming: `GET /api/message-stream` (SSE) emits `packet`, `counters`, `ranks`, and `health` events for live updates.
- Rollups: `stats_5m` materialized aggregates used for counter payloads.
