#!/usr/bin/env node
"use strict";

const path = require("path");
const Database = require("better-sqlite3");

const dbPath = path.join(__dirname, "..", "data", "meshrank.db");

function tableExists(db, name) {
  const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(name);
  return !!row;
}

function main() {
  const db = new Database(path.resolve(dbPath), { readonly: true });
  const tables = [
    "geoscore_model_versions",
    "geoscore_observer_profiles",
    "geoscore_edges",
    "geoscore_routes",
    "geoscore_metrics_daily"
  ];
  console.log("GeoScore eval:", path.resolve(dbPath));
  tables.forEach((tbl) => {
    console.log(`  table ${tbl}: ${tableExists(db, tbl) ? "present" : "MISSING"}`);
  });
  const counts = db.prepare(`
    SELECT
      (SELECT COUNT(1) FROM geoscore_observer_profiles) AS observers,
      (SELECT COUNT(1) FROM geoscore_routes) AS routes,
      (SELECT COUNT(1) FROM geoscore_routes WHERE unresolved=1) AS unresolved
  `).get();
  const resolvedRoutes = db.prepare("SELECT COUNT(1) AS c FROM geoscore_routes WHERE unresolved=0").get();
  const tenMinutesAgoMs = Date.now() - 10 * 60 * 1000;
  const last10mStats = db.prepare(`
    SELECT
      COUNT(1) AS total,
      SUM(CASE WHEN unresolved=0 THEN 1 ELSE 0 END) AS resolved,
      AVG(route_confidence) AS avg_conf,
      SUM(CASE WHEN teleport_max_km > 330 THEN 1 ELSE 0 END) AS teleports
    FROM geoscore_routes
    WHERE ts_ms >= ?
  `).get(tenMinutesAgoMs);
  const resolvedPct = last10mStats.total
    ? ((last10mStats.resolved || 0) / last10mStats.total * 100).toFixed(1)
    : "0.0";
  const avgConfLast10m = last10mStats.avg_conf ? Number(last10mStats.avg_conf).toFixed(3) : "0.000";
  console.log(`  observer profiles: ${counts.observers}`);
  console.log(`  routes scored: ${counts.routes}`);
  console.log(`  resolved routes: ${resolvedRoutes?.c ?? 0}`);
  console.log(`  unresolved routes: ${counts.unresolved} (${counts.routes ? ((counts.unresolved / counts.routes) * 100).toFixed(1) : "0"}%)`);
  console.log(
    `  last10m: total ${last10mStats.total || 0}, resolved ${last10mStats.resolved || 0} (${resolvedPct}%), avg conf ${avgConfLast10m}, teleports ${last10mStats.teleports || 0}`
  );
  const sample = db.prepare(`
    SELECT msg_key, unresolved, route_confidence, teleport_max_km, path_tokens, ts, ts_ms
    FROM geoscore_routes
    ORDER BY ts_ms DESC
    LIMIT 3
  `).all();
  console.log("  sample routes:");
  sample.forEach((row) => {
    let tokens = [];
    try {
      tokens = JSON.parse(row.path_tokens || "[]");
    } catch {
      tokens = [];
    }
    const tokenCount = Array.isArray(tokens) ? tokens.length : 0;
    const conf = Number.isFinite(row.route_confidence) ? Number(row.route_confidence).toFixed(3) : "null";
    const teleport = Number.isFinite(row.teleport_max_km) ? row.teleport_max_km.toFixed(2) : "null";
    console.log(
      `    ${row.msg_key} tokens=${tokenCount} unresolved=${row.unresolved} conf=${conf} teleport=${teleport} ts=${row.ts || "n/a"} ts_ms=${row.ts_ms || "n/a"}`
    );
  });
  db.close();
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error("geoscore_eval failed", err);
    process.exitCode = 1;
  }
}
