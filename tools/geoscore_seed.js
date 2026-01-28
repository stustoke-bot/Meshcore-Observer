#!/usr/bin/env node
"use strict";

const path = require("path");

// Require server module (will not start listener due to require.main guard).
const server = require("./observer_demo/server.js");

function seedRoutes() {
  const db = server.getDb();
  server.refreshGeoscoreObserverProfiles();
  const stmt = server.ensureGeoscoreRouteStmt(db);
  const rows = db
    .prepare(`
      SELECT mo.message_hash, m.frame_hash, mo.observer_id, mo.path_json, mo.ts
      FROM message_observers mo
      LEFT JOIN messages m ON m.message_hash = mo.message_hash
      WHERE mo.path_json IS NOT NULL
      ORDER BY mo.ts DESC
      LIMIT 10
    `)
    .all();
  let inserted = 0;
  rows.forEach((row) => {
    const tokens = server.parsePathJsonTokens(row.path_json);
    if (!tokens.length) return;
    const msgKey = String(row.message_hash || row.frame_hash || "").toUpperCase();
    if (!msgKey) return;
    const payload = server.buildGeoscoreRoutePayload({
      msgKey,
      ts: row.ts ? Date.parse(row.ts) : Date.now(),
      observerId: String(row.observer_id || "").toUpperCase(),
      pathTokens: tokens
    });
    stmt.run(
      payload.msgKey,
      payload.ts,
      payload.tsMs,
      payload.observerId,
      JSON.stringify(payload.pathTokens),
      JSON.stringify(payload.inferredPubs),
      JSON.stringify(payload.hopConfidences),
      payload.routeConfidence,
      payload.unresolved,
      payload.candidatesJson,
      payload.teleportMaxKm
    );
    inserted += 1;
  });
  console.log(`Seeded ${inserted} geoscore routes`);
}

if (require.main === module) {
  try {
    seedRoutes();
  } catch (err) {
    console.error("geoscore_seed failed", err);
    process.exitCode = 1;
  }
}
