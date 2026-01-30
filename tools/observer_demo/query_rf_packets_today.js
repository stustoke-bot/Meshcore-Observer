#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");
const { getDbPath } = require("./db_path");

// Resolve symlinks so we see the real file (e.g. data -> /root/meshrank/data)
const dbPath = getDbPath();
const dbPathResolved = fs.existsSync(dbPath) ? fs.realpathSync(dbPath) : dbPath;
console.log("DB path (config):", dbPath);
console.log("DB path (resolved):", dbPathResolved);

const db = new Database(dbPath, { readonly: true });

const packetsTodayWindowMs = 24 * 60 * 60 * 1000;
const now = Date.now();
const packetsTodayCutoff = new Date(now - packetsTodayWindowMs).toISOString();

const hasTable = db.prepare(
  "SELECT name FROM sqlite_master WHERE type='table' AND name='rf_packets'"
).get();

if (!hasTable) {
  console.log("Table rf_packets does not exist.");
  process.exit(0);
}

// Total rows in last 24h
const totalStmt = db.prepare(`
  SELECT COUNT(*) as n FROM rf_packets WHERE ts >= ?
`);
const total = totalStmt.get(packetsTodayCutoff);
console.log("Packets in last 24h (ts >= " + packetsTodayCutoff + "):", total.n);

// Per-observer counts (same query as buildObserverRank)
const perObserver = db.prepare(`
  SELECT observer_id, observer_name, COUNT(*) as packet_count,
         MIN(ts) as first_ts, MAX(ts) as last_ts
  FROM rf_packets
  WHERE ts >= ? AND observer_id IS NOT NULL
  GROUP BY observer_id
  ORDER BY packet_count DESC
  LIMIT 20
`).all(packetsTodayCutoff);
console.log("\nTop 20 observers by packet count (last 24h):");
console.log(perObserver);

// Sample of ts values (to see format)
const sampleTs = db.prepare(`
  SELECT ts FROM rf_packets ORDER BY id DESC LIMIT 5
`).all();
console.log("\nSample ts (latest 5 rows):", sampleTs.map((r) => r.ts));

db.close();
