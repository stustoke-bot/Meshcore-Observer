#!/usr/bin/env node
"use strict";

const Database = require("better-sqlite3");
const { getDbPath } = require("./observer_demo/db_path");

const dbPath = getDbPath();
const db = new Database(dbPath, { readonly: true });

console.log("DB Path:", dbPath);
console.log("Database List:", JSON.stringify(db.pragma("database_list")));

const now = Date.now();
const last24h = now - 24 * 60 * 60 * 1000;
const last3d = now - 3 * 24 * 60 * 60 * 1000;
const last10m = now - 10 * 60 * 1000;

function safeCount(sql, params = []) {
  try {
    const stmt = db.prepare(sql);
    const row = Array.isArray(params) ? stmt.get(...params) : stmt.get(params);
    return row ? Number(row.count || 0) : 0;
  } catch (err) {
    console.error("count error", err.message || err);
    return 0;
  }
}

const totalRepeaters = safeCount("SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1");
const repeaters24h = safeCount("SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1 AND last_advert_heard_ms >= ?", [last24h]);
const repeaters3d = safeCount("SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1 AND last_advert_heard_ms >= ?", [last3d]);
const stale1dTo3d = safeCount(
  "SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1 AND last_advert_heard_ms >= ? AND last_advert_heard_ms < ?",
  [last3d, last24h]
);
const rejectedAdvertsLast10m = safeCount("SELECT COUNT(*) AS count FROM rejected_adverts WHERE heard_ms >= ?", [last10m]);

const maxRow = db.prepare("SELECT MAX(last_advert_heard_ms) AS max_ms FROM devices").get();
const maxIso = maxRow && Number.isFinite(maxRow.max_ms)
  ? new Date(maxRow.max_ms).toISOString()
  : null;
console.log("Repeaters (total / 24h / 3d / stale1d-3d):", totalRepeaters, repeaters24h, repeaters3d, stale1dTo3d);
console.log("Rejected adverts last 10m:", rejectedAdvertsLast10m);
console.log("Max last_advert_heard_ms:", maxIso);

const sampleRepeaters = db.prepare(`
  SELECT pub, name, last_advert_heard_ms
  FROM devices
  WHERE is_repeater = 1 AND last_advert_heard_ms IS NOT NULL
  ORDER BY last_advert_heard_ms DESC
  LIMIT 5
`).all();

console.log("Sample repeaters:");
sampleRepeaters.forEach((row, idx) => {
  const pub = String(row.pub || "").toUpperCase();
  const shortPub = pub ? `${pub.slice(0, 8)}...` : "(no pub)";
  const iso = Number.isFinite(row.last_advert_heard_ms)
    ? new Date(row.last_advert_heard_ms).toISOString()
    : "N/A";
  console.log(`${idx + 1}. ${shortPub} name=${row.name || "<no name>"} heard=${iso}`);
});

if (repeaters3d === 0) {
  console.error("ERROR: no repeaters seen in the last 3 days");
  process.exit(1);
}
