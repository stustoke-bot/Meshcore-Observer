#!/usr/bin/env node
"use strict";

/**
 * End-to-end check for Observer Rank "Packets (24h)".
 * Run from repo root (same cwd as meshrank.service): node tools/observer_demo/check_observer_rank_e2e.js
 *
 * Flow traced:
 * 1. UI: Observer Rank page shows "Packets (24h)" from summary.totals.packets24h
 * 2. API: GET /api/observer-rank-summary returns totals.packets24h = sum of items[].packetsToday
 * 3. Cache: observerRankCache.items[].packetsToday (and .packets24h) come from buildObserverRank()
 * 4. Build: buildObserverRank() sets s.packetsToday from rf_packets SQL (24h) or file fallback
 */

const fs = require("fs");
const path = require("path");
const http = require("http");

// Use same path resolution as server (db_path.js is in tools/observer_demo)
const scriptDir = __dirname;
const projectRoot = path.resolve(scriptDir, "..", "..");
const dataDir = path.join(projectRoot, "data");
const defaultDbPath = path.join(dataDir, "meshrank.db");

function getDbPath() {
  const envPath = process.env.MESHRANK_DB_PATH;
  const candidate = envPath && String(envPath).trim() ? envPath : defaultDbPath;
  return path.resolve(candidate);
}

const dbPath = getDbPath();
const dbPathResolved = fs.existsSync(dbPath) ? fs.realpathSync(dbPath) : dbPath;

console.log("=== 1. DB path (same as server) ===");
console.log("  config:", dbPath);
console.log("  resolved:", dbPathResolved);
console.log("  cwd:", process.cwd());

let Database;
try {
  Database = require("better-sqlite3");
} catch (e) {
  console.log("  (better-sqlite3 not loaded, skip DB checks)");
}

if (Database) {
  const db = new Database(dbPath, { readonly: true });
  const packetsTodayWindowMs = 24 * 60 * 60 * 1000;
  const packetsTodayCutoff = new Date(Date.now() - packetsTodayWindowMs).toISOString();

  console.log("\n=== 2. rf_packets table (source for packets 24h) ===");
  const hasRf = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='rf_packets'").get();
  if (!hasRf) {
    console.log("  rf_packets table: NOT FOUND (server will use observer.ndjson fallback)");
  } else {
    const total = db.prepare("SELECT COUNT(*) AS n FROM rf_packets WHERE ts >= ?").get(packetsTodayCutoff);
    const rows = db.prepare(`
      SELECT observer_id, COUNT(*) AS packet_count
      FROM rf_packets WHERE ts >= ? AND observer_id IS NOT NULL
      GROUP BY observer_id
    `).all(packetsTodayCutoff);
    const sumPackets = rows.reduce((s, r) => s + (r.packet_count || 0), 0);
    console.log("  rf_packets: exists");
    console.log("  packets in last 24h (ts >= " + packetsTodayCutoff + "):", total?.n ?? 0);
    console.log("  per-observer rows:", rows.length);
    console.log("  sum packet_count:", sumPackets);
  }

  console.log("\n=== 3. observer_rank_cache (persisted payload) ===");
  const cacheRow = db.prepare("SELECT updated_at, length(payload) AS len FROM observer_rank_cache WHERE id = 1").get();
  if (!cacheRow) {
    console.log("  No row in observer_rank_cache");
  } else {
    const payloadRow = db.prepare("SELECT payload FROM observer_rank_cache WHERE id = 1").get();
    let parsed;
    try {
      parsed = JSON.parse(payloadRow.payload);
    } catch (e) {
      parsed = null;
    }
    console.log("  updated_at:", cacheRow.updated_at);
    console.log("  payload length:", cacheRow.len);
    if (parsed && Array.isArray(parsed.items)) {
      const packetsTotal = parsed.items.reduce((s, o) => s + (o.packetsToday || o.packets24h || 0), 0);
      const sample = parsed.items.slice(0, 3).map((o) => ({ id: o.id, packetsToday: o.packetsToday, packets24h: o.packets24h }));
      console.log("  items.length:", parsed.items.length);
      console.log("  sum packetsToday/packets24h in payload:", packetsTotal);
      console.log("  sample items:", JSON.stringify(sample, null, 2));
    }
  }
  db.close();
}

console.log("\n=== 4. Live API (observer-rank-summary with refresh+wait) ===");
const port = Number(process.env.PORT) || 5199;
const url = `http://127.0.0.1:${port}/api/observer-rank-summary?refresh=1&wait=1`;
const req = http.get(url, { timeout: 120000 }, (res) => {
  let buf = "";
  res.on("data", (c) => { buf += c; });
  res.on("end", () => {
    if (res.statusCode !== 200) {
      console.log("  status:", res.statusCode);
      console.log("  body:", buf.slice(0, 500));
      return;
    }
    let j;
    try {
      j = JSON.parse(buf);
    } catch (e) {
      console.log("  parse error:", e.message);
      console.log("  body slice:", buf.slice(0, 300));
      return;
    }
    console.log("  count:", j.count);
    console.log("  totals.active:", j.totals?.active);
    console.log("  totals.packets24h:", j.totals?.packets24h);
    console.log("  totals.packetsToday:", j.totals?.packetsToday);
    console.log("  updatedAt:", j.updatedAt);
    if ((j.totals?.packets24h === 0 || j.totals?.packets24h === undefined) && (j.totals?.packetsToday ?? 0) > 0) {
      console.log("\n  >>> FIX: API has packetsToday but UI reads packets24h. Use totals.packets24h ?? totals.packetsToday in UI (done). Restart server so API sends packets24h.");
    }
    if (j.totals?.packets24h === 0 && j.totals?.packetsToday === 0 && j.count > 0) {
      console.log("\n  >>> ISSUE: count > 0 but packets 24h is 0. buildObserverRank may not be filling packetsToday.");
    }
  });
});
req.on("error", (err) => {
  console.log("  request error:", err.message);
  console.log("  (is the server running on port " + port + "?)");
});
req.on("timeout", () => {
  req.destroy();
  console.log("  timeout (build may be slow)");
});
