#!/usr/bin/env node
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const Database = require("better-sqlite3");
const bcrypt = require("bcryptjs");

const projectRoot = path.resolve(__dirname, "..", "..");
function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const raw = fs.readFileSync(filePath, "utf8");
  raw.split(/\r?\n/).forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) return;
    const idx = trimmed.indexOf("=");
    if (idx <= 0) return;
    const key = trimmed.slice(0, idx).trim();
    let value = trimmed.slice(idx + 1).trim();
    if (!key) return;
    if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) process.env[key] = value;
  });
}
loadEnvFile(path.join(projectRoot, ".env"));
const dataDir = path.join(projectRoot, "data");
const devicesPath = path.join(dataDir, "devices.json");
const decodedPath = path.join(dataDir, "decoded.ndjson");
const rfPath = path.join(dataDir, "rf.ndjson");
const observerPath = path.join(dataDir, "observer.ndjson");
const observersPath = path.join(dataDir, "observers.json");
const devicesPathAlt = path.join(projectRoot, "devices.json");
const observersPathAlt = path.join(projectRoot, "observers.json");
const ingestLogPath = path.join(dataDir, "ingest.log");
const routeSuggestionsPath = path.join(dataDir, "route_suggestions.json");
const rotmConfigPath = path.join(dataDir, "rotm_config.json");
const rotmOverridesPath = path.join(dataDir, "rotm_overrides.json");
const zeroHopOverridesPath = path.join(dataDir, "zero_hop_overrides.json");
const indexPath = path.join(__dirname, "index.html");
const staticDir = __dirname;
const keysPath = path.join(projectRoot, "tools", "meshcore_keys.json");
const crypto = require("crypto");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");
const { getDbPath, logDbInfo } = require("./db_path");
const dbPath = getDbPath();
const DEBUG_PERF = process.env.DEBUG_PERF === "1";
const DEBUG_SQL = process.env.DEBUG_SQL === "1";
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || "";

const CHANNEL_TAIL_LINES = 6000;
const CHANNEL_HISTORY_LIMIT = 10;
const CHANNEL_HISTORY_LIMITS = {
  "#hashtags": 30
};
const OBSERVER_DEBUG_TAIL_LINES = 5000;
const MESSAGE_DEBUG_TAIL_LINES = 6000;
const NODE_RANK_TAIL_LINES = 6000;

let observerHitsCache = { mtimeMs: null, size: null, map: new Map(), offset: 0, lastReadAt: 0 };
let rfAggCache = { builtAt: 0, map: new Map(), limit: 0 };
let channelMessagesCache = { mtimeMs: null, size: null, payload: null, builtAt: 0 };
let channelMessagesInFlight = null;
let rotmCache = { builtAt: 0, payload: null };
const DEVICES_CACHE_MS = 1000;
const OBSERVERS_CACHE_MS = 1000;
let devicesCache = { readAt: 0, data: null };
let observersCache = { readAt: 0, data: null };
const CHANNEL_CACHE_MIN_MS = 500;
const CHANNEL_CACHE_STALE_MS = 1500;
const MESSAGE_ROUTE_CACHE_MS = 60 * 1000;
const CONFIDENCE_HISTORY_MAX_ROWS = 400;
const CONFIDENCE_DEAD_END_LIMIT = 20;
const messageRouteCache = new Map();

const packetTimeline = [];
let meshTrendCache = { updatedAt: 0, payload: null };

const OBSERVER_HITS_MAX_BYTES = 2 * 1024 * 1024;
const OBSERVER_HITS_COOLDOWN_MS = 30 * 1000;
const OBSERVER_HITS_TAIL_INTERVAL_MS = 2000;
const MESSAGE_OBSERVER_STREAM_POLL_MS = 1000;
const MESSAGE_OBSERVER_STREAM_PING_MS = 15000;
const MESSAGE_OBSERVER_STREAM_MAX_ROWS = 200;
const MESSAGE_STREAM_HEALTH_MS = 12000;
const MESSAGE_STREAM_COUNTERS_MS = 10000;
const MESSAGE_STREAM_RANKS_MS = 30000;
const RF_AGG_COOLDOWN_MS = 1500;
const RF_AGG_LIMIT = 800;
const REPEATER_FLAG_REVIEW_HOURS = 24;
const REPEATER_ESTIMATE_MIN_NEIGHBORS = 3;
const REPEATER_ESTIMATE_SINGLE_OFFSET_KM = 4.8; // ~3 miles
const ROTM_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const ROTM_QSO_WINDOW_MS = 5 * 60 * 1000;
const ROTM_MIN_OBSERVER_HITS = 1;
const ROTM_FEED_LIMIT = 200;
const ROTM_DB_LIMIT = 2000;
const ROTM_CACHE_MS = 2000;
const STATS_ROLLUP_WINDOW_MS = 5 * 60 * 1000;
const STATS_ROLLUP_SEED_BUCKETS = 12;
const STATS_ROLLUP_INTERVAL_MS = 60 * 1000;
const STATS_ROLLUP_MIN_INTERVAL_MS = 10000;
const PACKET_TREND_WINDOW_MS = 2 * 60 * 1000;
const PACKET_RATE_DURATION_MS = 60 * 1000;
const MESH_TREND_CACHE_MS = 60 * 1000;
const MESHFLOW_POLL_MS = 1000;
const MESHFLOW_HISTORY_MAX_SEC = 120;
const MESHFLOW_DEFAULT_WINDOW_SEC = 30;
const MESHFLOW_MAX_FLOWS = 300;
const MESHFLOW_MAX_NODES = 500;
const MESHFLOW_READ_LIMIT = 400;
const MESHFLOW_JITTER_ENABLED = process.env.MESHFLOW_JITTER === "1";
const MESHFLOW_JITTER_DEGREES = 0.004;

const { inferRouteViterbi } = require("./geoscore_infer");

const GEO_SCORE_RETENTION_DAYS = Number(process.env.GEOSCORE_RETENTION_DAYS || 7);
const GEO_SCORE_CANDIDATE_K = 5;
const GEO_SCORE_BATCH_SIZE = 20;
const GEO_SCORE_STATUS_WINDOWS = ["1h", "24h", "14d"];
const GEO_SCORE_OBSERVER_SOURCE = "observers.json:bestRepeaterPub";
const GEO_SCORE_DEBUG = process.env.GEOSCORE_DEBUG === "1";
const GEO_SCORE_LAST_HEARD_REFRESH_MS = 60 * 1000;
const GEO_SCORE_LAST_HEARD_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

const meshFlowHistory = [];
let meshFlowLastRowId = 0;
let meshFlowTimer = null;
const meshFlowCache = {
  nodeMap: { updatedAt: 0, map: null },
  repeaterHash: { updatedAt: 0, map: null }
};

const geoscoreQueue = [];
let geoscoreProcessing = false;
let geoscoreObserverProfilesCache = new Map();
let geoscoreRouteUpsertStmt = null;
let geoscoreObserverProfileUpsertStmt = null;
let lastHeardByHash = new Map();
let lastHeardByPub = new Map();
let lastGeoscoreDiagLogMs = 0;
let dbInfoLogged = false;
let dbInfoCache = null;
const INGEST_METRICS_KEYS = ["countAdvertsSeenLast10m", "lastAdvertSeenAtIso"];

function recordObserverHit(rec, map, keyStore) {
  if (!rec) return;
  let observerKey = rec.observerId || "";
  if (!observerKey && rec.topic) {
    const m = String(rec.topic).match(/observers\/([^/]+)\//i);
    if (m) observerKey = m[1];
  }
  if (!observerKey) observerKey = rec.observerName || "";
  observerKey = String(observerKey).trim();
  if (!observerKey) return;

  const keys = [];
  const frameKey = rec.frameHash ? String(rec.frameHash).toUpperCase() : null;
  const hashKey = rec.hash ? String(rec.hash).toUpperCase() : null;
  const msgKey = rec.messageHash ? String(rec.messageHash).toUpperCase() : null;
  if (frameKey) keys.push(frameKey);
  if (hashKey) keys.push(hashKey);
  if (msgKey) keys.push(msgKey);
  if (!msgKey && rec.payloadHex && keyStore) {
    try {
      const decoded = MeshCoreDecoder.decode(String(rec.payloadHex).toUpperCase(), { keyStore });
      const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
      if (payloadType === "GroupText" && decoded.messageHash) {
        keys.push(String(decoded.messageHash).toUpperCase());
      }
    } catch {}
  }
  if (!keys.length) return;

  keys.forEach((key) => {
    if (!map.has(key)) map.set(key, new Set());
    map.get(key).add(observerKey);
  });
}

async function getObserverHitsMap() {
  if (!fs.existsSync(observerPath)) return new Map();
  if (observerHitsCache.map && observerHitsCache.map.size) return observerHitsCache.map;
  const keyStore = buildKeyStore(loadKeys());
  let stat = null;
  try {
    stat = fs.statSync(observerPath);
  } catch {
    return new Map();
  }
  if (observerHitsCache.mtimeMs === stat.mtimeMs && observerHitsCache.size === stat.size) {
    return observerHitsCache.map;
  }
  const now = Date.now();
  if (observerHitsCache.lastReadAt && (now - observerHitsCache.lastReadAt) < OBSERVER_HITS_COOLDOWN_MS) {
    if (observerHitsCache.map && observerHitsCache.map.size) return observerHitsCache.map;
  }
  let map = observerHitsCache.map || new Map();
  let offset = observerHitsCache.offset || 0;
  if (stat.size < offset) {
    map = new Map();
    offset = 0;
  }
  if (offset === 0 && stat.size > OBSERVER_HITS_MAX_BYTES) {
    map = new Map();
    offset = Math.max(0, stat.size - OBSERVER_HITS_MAX_BYTES);
  }
  const stream = fs.createReadStream(observerPath, { encoding: "utf8", start: offset });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let isFirst = true;
  for await (const line of rl) {
    if (isFirst && offset > 0) {
      isFirst = false;
      continue; // skip partial line
    }
    const t = line.trim();
    if (!t) continue;
    let rec;
    try { rec = JSON.parse(t); } catch { continue; }
    let observerKey = rec.observerId || "";
    if (!observerKey && rec.topic) {
      const m = String(rec.topic).match(/observers\/([^/]+)\//i);
      if (m) observerKey = m[1];
    }
    recordObserverHit(rec, map, keyStore);
  }
  observerHitsCache = {
    mtimeMs: stat.mtimeMs,
    size: stat.size,
    map,
    offset: stat.size,
    lastReadAt: now
  };
  return map;
}

function startObserverHitsTailer() {
  if (startObserverHitsTailer.started) return;
  startObserverHitsTailer.started = true;
  if (!fs.existsSync(observerPath)) return;
  const keyStore = buildKeyStore(loadKeys());
  let busy = false;
  const tick = async () => {
    if (busy) return;
    busy = true;
    try {
      let stat = null;
      try {
        stat = fs.statSync(observerPath);
      } catch {
        return;
      }
      let map = observerHitsCache.map || new Map();
      let offset = observerHitsCache.offset || 0;
      if (stat.size < offset) {
        map = new Map();
        offset = 0;
      }
      if (offset === 0 && stat.size > OBSERVER_HITS_MAX_BYTES) {
        map = new Map();
        offset = Math.max(0, stat.size - OBSERVER_HITS_MAX_BYTES);
      }
      if (stat.size === offset) return;
      const stream = fs.createReadStream(observerPath, { encoding: "utf8", start: offset });
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      let isFirst = true;
      for await (const line of rl) {
        if (isFirst && offset > 0) {
          isFirst = false;
          continue;
        }
        const t = line.trim();
        if (!t) continue;
        let rec;
        try { rec = JSON.parse(t); } catch { continue; }
        recordObserverHit(rec, map, keyStore);
      }
      observerHitsCache = {
        mtimeMs: stat.mtimeMs,
        size: stat.size,
        map,
        offset: stat.size,
        lastReadAt: Date.now()
      };
    } finally {
      busy = false;
    }
  };
  tick().catch(() => {});
  setInterval(() => { tick().catch(() => {}); }, OBSERVER_HITS_TAIL_INTERVAL_MS);
}
const RANK_REFRESH_MS = 15 * 60 * 1000;
const NODE_RANK_REFRESH_MS = 5 * 60 * 1000;
const OBSERVER_RANK_REFRESH_MS = 5 * 60 * 1000;
const MESH_REFRESH_MS = 15 * 60 * 1000;
const OBSERVER_OFFLINE_MINUTES = 24 * 60;
const OBSERVER_OFFLINE_HOURS = OBSERVER_OFFLINE_MINUTES / 60;
const OBSERVER_LOW_PACKET_MINUTES = 15;
const OBSERVER_MAX_REPEATER_KM = 300;
const REPEAT_EVIDENCE_WINDOW_MS = 24 * 60 * 60 * 1000;
const REPEAT_EVIDENCE_MIN_MIDDLE = 5;
const REPEAT_EVIDENCE_MIN_NEIGHBORS = 2;

let rankCache = { updatedAt: null, count: 0, items: [], excluded: [], cachedAt: null };
let rankRefreshInFlight = null;
let rankSummaryCache = {
  updatedAt: null,
  totals: { total: 0, active: 0, total24h: 0 },
  lastPersistAt: 0
};
const RANK_HISTORY_PERSIST_INTERVAL = 10 * 60 * 1000;

let observerRankCache = { updatedAt: null, items: [] };
let observerRankRefresh = null;

let nodeRankCache = { updatedAt: null, count: 0, items: [], cachedAt: null };
let nodeRankRefreshInFlight = null;
let meshScoreCache = { updatedAt: null, payload: null };
let meshScoreRefresh = null;
let db = null;
let statsRollupLastAt = 0;

let ChannelCrypto;
try {
  ({ ChannelCrypto } = require("@michaelhart/meshcore-decoder/dist/crypto/channel-crypto"));
} catch {
  ChannelCrypto = null;
}

const host = "0.0.0.0";
const port = Number(process.env.PORT || 5199);
const AUTO_REFRESH_MS = 60 * 1000;

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function channelHistoryLimit(channelName) {
  if (!channelName) return CHANNEL_HISTORY_LIMIT;
  const key = String(channelName).trim().toLowerCase();
  const mapped = CHANNEL_HISTORY_LIMITS[key];
  if (Number.isFinite(mapped) && mapped > 0) return mapped;
  return CHANNEL_HISTORY_LIMIT;
}

function resolveRecordTimestamp(rec, baseNow, maxNumericTs) {
  if (!rec) return null;
  if (typeof rec.archivedAt === "string" && parseIso(rec.archivedAt)) return rec.archivedAt;
  if (typeof rec.ts === "string" && parseIso(rec.ts)) return rec.ts;
  if (Number.isFinite(rec.ts) && maxNumericTs !== null) {
    const approx = baseNow - (maxNumericTs - rec.ts);
    return new Date(approx).toISOString();
  }
  return null;
}

function hasMessagesDb(db) {
  try {
    const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='messages'").get();
    return !!row;
  } catch {
    return false;
  }
}

function hasMessageObserversDb(db) {
  try {
    const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='message_observers'").get();
    return !!row;
  } catch {
    return false;
  }
}

function mapMessageRow(row, nodeMap, observerHitsMap, observerAggMap, observerPathsMap) {
  let path = [];
  if (row.path_text) {
    path = String(row.path_text)
      .split("|")
      .map(normalizePathHash)
      .filter(Boolean);
  } else if (row.path_json) {
    try {
      const parsed = JSON.parse(row.path_json);
      if (Array.isArray(parsed)) path = parsed;
    } catch {}
  }
  const observerSet = new Set();
  let hasAggHits = false;
  if (observerAggMap) {
    const agg = observerAggMap.get(String(row.message_hash || "").toUpperCase());
    if (agg?.observerHits) {
      agg.observerHits.forEach((o) => observerSet.add(o));
      hasAggHits = agg.observerHits.size > 0;
    }
    if (agg?.hopCodes && agg.hopCodes.size) {
      path = Array.from(agg.hopCodes);
    }
  }
  if (!hasAggHits && observerHitsMap) {
    const keys = [];
    if (row.message_hash) keys.push(String(row.message_hash).toUpperCase());
    if (row.frame_hash) keys.push(String(row.frame_hash).toUpperCase());
    keys.forEach((key) => {
      const hits = observerHitsMap.get(key);
      if (hits) hits.forEach((o) => observerSet.add(o));
    });
  }
  const observerHits = Array.from(observerSet);
  const observerPathsRaw = observerPathsMap ? (observerPathsMap.get(String(row.message_hash || "").toUpperCase()) || []) : [];
  const observerPaths = observerPathsRaw.map((entry) => {
    const pathPoints = (entry.path || []).map((h) => {
      const hit = nodeMap.get(h);
      return {
        hash: h,
        name: hit ? hit.name : h,
        gps: hit?.gps || null
      };
    });
    return {
      observerId: entry.observerId || null,
      observerName: entry.observerName || entry.observerId || null,
      path: entry.path || [],
      pathPoints
    };
  });
  const observerCount = observerHits.length;
  const baseRepeats = Number.isFinite(row.repeats) ? row.repeats : (path.length || 0);
  const repeats = Math.max(baseRepeats, observerCount);
  const pathPoints = path.map((h) => {
    const hit = nodeMap.get(h);
    return {
      hash: h,
      name: hit ? hit.name : h,
      gps: hit?.gps || null
    };
  });
  // TODO M3: replace placeholder inference with full Viterbi + edge aggregation.
  return {
    id: row.message_hash,
    frameHash: row.frame_hash,
    messageHash: row.message_hash,
    channelName: row.channel_name,
    sender: row.sender || "unknown",
    body: row.body || "",
    ts: row.ts,
    repeats,
    path,
    pathNames: pathPoints.map((p) => p.name),
    pathPoints,
    pathLength: Number.isFinite(row.path_length) ? row.path_length : path.length,
    observerHits,
    observerCount
    ,
    observerPaths
  };
}

function readMessagesFromDb(channelName, limit, beforeTs) {
  const db = getDb();
  if (!hasMessagesDb(db)) return null;
  const before = typeof beforeTs === "string" ? beforeTs : (beforeTs ? new Date(beforeTs).toISOString() : null);
  const max = Number.isFinite(limit) ? limit : CHANNEL_HISTORY_LIMIT;
  let rows = [];
  if (channelName) {
    if (before) {
      rows = db.prepare(`
        SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_text, path_length, repeats
        FROM messages
        WHERE channel_name = ? AND ts < ?
        ORDER BY ts DESC
        LIMIT ?
      `).all(channelName, before, max);
    } else {
      rows = db.prepare(`
        SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_text, path_length, repeats
        FROM messages
        WHERE channel_name = ?
        ORDER BY ts DESC
        LIMIT ?
      `).all(channelName, max);
    }
  } else {
    rows = db.prepare(`
      SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_text, path_length, repeats
      FROM messages
      ORDER BY ts DESC
      LIMIT ?
    `).all(max);
  }
  return rows;
}

function readMessageObserverAgg(db, hashes) {
  if (!hasMessageObserversDb(db)) return new Map();
  const list = Array.isArray(hashes) ? hashes.filter(Boolean) : [];
  if (!list.length) return new Map();
  const placeholders = list.map(() => "?").join(",");
  const rows = db.prepare(`
    SELECT message_hash, observer_id, observer_name, path_json, path_text
    FROM message_observers
    WHERE message_hash IN (${placeholders})
  `).all(...list);
  const map = new Map();
  rows.forEach((row) => {
    const key = String(row.message_hash || "").toUpperCase();
    if (!key) return;
    let entry = map.get(key);
    if (!entry) {
      entry = { observerHits: new Set(), hopCodes: new Set() };
      map.set(key, entry);
    }
    const label = row.observer_name || row.observer_id;
    if (label) entry.observerHits.add(label);
    if (row.path_text) {
      String(row.path_text)
        .split("|")
        .map(normalizePathHash)
        .filter(Boolean)
        .forEach((code) => entry.hopCodes.add(code));
    } else if (row.path_json) {
      try {
        const parsed = JSON.parse(row.path_json);
        if (Array.isArray(parsed)) {
          parsed.forEach((code) => {
            const norm = normalizePathHash(code);
            if (norm) entry.hopCodes.add(norm);
          });
        }
      } catch {}
    }
  });
  return map;
}

function readMessageObserverPaths(db, hashes) {
  if (!hasMessageObserversDb(db)) return new Map();
  const list = Array.isArray(hashes) ? hashes.filter(Boolean) : [];
  if (!list.length) return new Map();
  const placeholders = list.map(() => "?").join(",");
  const rows = db.prepare(`
    SELECT message_hash, observer_id, observer_name, path_json, path_text
    FROM message_observers
    WHERE message_hash IN (${placeholders})
  `).all(...list);
  const map = new Map();
  rows.forEach((row) => {
    const key = String(row.message_hash || "").toUpperCase();
    if (!key) return;
    let parsed;
    if (row.path_text) {
      parsed = String(row.path_text)
        .split("|")
        .map(normalizePathHash)
        .filter(Boolean);
    } else if (row.path_json) {
      try { parsed = JSON.parse(row.path_json); } catch { parsed = null; }
    }
    if (!Array.isArray(parsed) || !parsed.length) return;
    const path = parsed.map(normalizePathHash).filter(Boolean);
    if (!path.length) return;
    const listEntry = map.get(key) || [];
    listEntry.push({
      observerId: row.observer_id || null,
      observerName: row.observer_name || null,
      path
    });
    map.set(key, listEntry);
  });
  return map;
}

function fetchGeoscorePathRows(db, hashes) {
  if (!hasMessageObserversDb(db)) return [];
  const list = Array.isArray(hashes) ? hashes.filter(Boolean) : [];
  if (!list.length) return [];
  const placeholders = list.map(() => "?").join(",");
  const rows = db.prepare(`
    SELECT
      mo.message_hash,
      mo.observer_id,
      mo.path_text,
      mo.path_json,
      mo.ts,
      mo.ts_ms,
      m.frame_hash
    FROM message_observers mo
    LEFT JOIN messages m ON m.message_hash = mo.message_hash
    WHERE mo.message_hash IN (${placeholders})
      AND (mo.path_text IS NOT NULL OR mo.path_json IS NOT NULL)
  `).all(...list);
  return rows;
}

function readMessageObserverUpdatesSince(db, lastRowId, limit) {
  if (!hasMessageObserversDb(db)) return { updates: [], lastRowId };
  const max = Number.isFinite(limit) ? limit : MESSAGE_OBSERVER_STREAM_MAX_ROWS;
  const rows = db.prepare(`
    SELECT mo.rowid,
           mo.message_hash,
           m.frame_hash,
           mo.observer_id,
           mo.observer_name,
           COALESCE(m.path_length, mo.path_length) AS path_length,
           m.repeats AS repeats
    FROM message_observers mo
    LEFT JOIN messages m ON m.message_hash = mo.message_hash
    WHERE mo.rowid > ?
    ORDER BY mo.rowid ASC
    LIMIT ?
  `).all(lastRowId || 0, max);
  let nextRowId = lastRowId || 0;
  const map = new Map();
  rows.forEach((row) => {
    if (row.rowid > nextRowId) nextRowId = row.rowid;
    const key = String(row.message_hash || "").toUpperCase();
    if (!key) return;
    let entry = map.get(key);
    if (!entry) {
      entry = { messageHash: key, frameHash: row.frame_hash || null, observerHits: new Set(), pathLength: 0 };
      map.set(key, entry);
    }
    if (!entry.frameHash && row.frame_hash) entry.frameHash = row.frame_hash;
    const label = row.observer_name || row.observer_id;
    if (label) entry.observerHits.add(label);
    if (Number.isFinite(row.path_length)) {
      entry.pathLength = Math.max(entry.pathLength, row.path_length || 0);
    }
    if (Number.isFinite(row.repeats)) {
      entry.repeats = Math.max(entry.repeats || 0, row.repeats || 0);
    }
  });
  const updates = Array.from(map.values()).map((entry) => ({
    messageHash: entry.messageHash,
    frameHash: entry.frameHash || null,
    observerHits: Array.from(entry.observerHits),
    pathLength: entry.pathLength || null,
    repeats: Number.isFinite(entry.repeats) ? entry.repeats : null
  }));
  return { updates, lastRowId: nextRowId };
}

function ensureColumn(db, tableName, columnName, columnType) {
  const cols = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (cols.some((c) => c.name === columnName)) return;
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`);
}

function recordDbInfo(db) {
  if (dbInfoLogged || !db) return;
  try {
    logDbInfo(db);
    const dbList = db.pragma("database_list") || [];
    dbInfoCache = {
      resolvedPath: getDbPath(),
      cwd: process.cwd(),
      databaseList: dbList,
      mainFile: dbList && dbList.length ? dbList[0].file : null
    };
  } catch (err) {
    console.log("(observer-demo) db info log failed", err?.message || err);
  } finally {
    dbInfoLogged = true;
  }
}

function getDb() {
  if (db) return db;
  db = new Database(dbPath);
  recordDbInfo(db);
  if (DEBUG_SQL) {
    const origPrepare = db.prepare.bind(db);
    db.prepare = (sql) => {
      const stmt = origPrepare(sql);
      ["run", "get", "all", "iterate"].forEach((method) => {
        if (typeof stmt[method] !== "function") return;
        const orig = stmt[method].bind(stmt);
        stmt[method] = (...args) => {
          const start = process.hrtime.bigint();
          const result = orig(...args);
          const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
          console.log(`[sql] ${elapsedMs.toFixed(2)}ms ${method} ${String(sql).trim()}`);
          return result;
        };
      });
      return stmt;
    };
  }
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("temp_store = MEMORY");
  db.pragma("cache_size = -64000");
  db.pragma("foreign_keys = ON");
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      display_name TEXT,
      is_admin INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      last_login TEXT
    );
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id INTEGER NOT NULL,
      expires_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS user_channels (
      user_id INTEGER NOT NULL,
      channel_name TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (user_id, channel_name)
    );
    CREATE INDEX IF NOT EXISTS idx_user_channels_user ON user_channels(user_id);
    CREATE TABLE IF NOT EXISTS user_nodes (
      user_id INTEGER NOT NULL,
      public_id TEXT NOT NULL,
      nickname TEXT,
      created_at TEXT NOT NULL,
      PRIMARY KEY (user_id, public_id)
    );
    CREATE INDEX IF NOT EXISTS idx_user_nodes_user ON user_nodes(user_id);
    CREATE TABLE IF NOT EXISTS channels_catalog (
      name TEXT PRIMARY KEY,
      code TEXT NOT NULL,
      emoji TEXT NOT NULL,
      group_name TEXT NOT NULL,
      allow_popular INTEGER NOT NULL DEFAULT 1,
      created_by INTEGER,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_channels_catalog_group ON channels_catalog(group_name);
    CREATE TABLE IF NOT EXISTS channel_blocks (
      name TEXT PRIMARY KEY,
      blocked_by INTEGER,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS node_profiles (
      pub TEXT PRIMARY KEY,
      name TEXT,
      bio TEXT,
      photo_url TEXT,
      owner_user_id INTEGER,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS node_claims (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pub TEXT NOT NULL,
      nonce TEXT NOT NULL,
      status TEXT NOT NULL,
      user_id INTEGER,
      created_at TEXT NOT NULL,
      verified_at TEXT
    );
    CREATE TABLE IF NOT EXISTS messages (
      message_hash TEXT PRIMARY KEY,
      frame_hash TEXT,
      channel_name TEXT,
      channel_hash TEXT,
      sender TEXT,
      sender_pub TEXT,
      body TEXT,
      ts TEXT,
      path_json TEXT,
      path_text TEXT,
      path_length INTEGER,
      repeats INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_name, ts);
    CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
    CREATE INDEX IF NOT EXISTS idx_messages_sender_channel_ts ON messages(sender, channel_name, ts);
    CREATE TABLE IF NOT EXISTS message_observers (
      message_hash TEXT NOT NULL,
      observer_id TEXT NOT NULL,
      observer_name TEXT,
      ts TEXT,
      ts_ms INTEGER,
      path_json TEXT,
      path_text TEXT,
      path_length INTEGER,
      PRIMARY KEY (message_hash, observer_id)
    );
    CREATE INDEX IF NOT EXISTS idx_message_observers_hash ON message_observers(message_hash);
    CREATE TABLE IF NOT EXISTS geoscore_model_versions (
      model_version TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      window_days INTEGER NOT NULL,
      weights_json TEXT,
      notes TEXT
    );
    CREATE TABLE IF NOT EXISTS geoscore_observer_profiles (
      observer_id TEXT PRIMARY KEY,
      linked_repeater_pub TEXT,
      home_lat REAL,
      home_lon REAL,
      source TEXT,
      updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS geoscore_edges (
      from_pub TEXT NOT NULL,
      to_pub TEXT NOT NULL,
      window TEXT NOT NULL,
      count INTEGER NOT NULL,
      confidence_sum REAL NOT NULL,
      last_seen TEXT,
      PRIMARY KEY(from_pub, to_pub, window)
    );
    CREATE INDEX IF NOT EXISTS idx_geoscore_edges_window ON geoscore_edges(window);
    CREATE TABLE IF NOT EXISTS geoscore_routes (
      msg_key TEXT PRIMARY KEY,
      ts TEXT,
      ts_ms INTEGER,
      observer_id TEXT,
      path_tokens TEXT,
      inferred_pubs TEXT,
      hop_confidences TEXT,
      route_confidence REAL,
      unresolved INTEGER,
      candidates_json TEXT,
      teleport_max_km REAL
    );
    CREATE TABLE IF NOT EXISTS geoscore_metrics_daily (
      day_yyyymmdd TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      routes_scored INTEGER NOT NULL,
      hops_resolved_pct REAL,
      mean_confidence REAL,
      teleport_outliers INTEGER,
      notes TEXT
    );
    CREATE TABLE IF NOT EXISTS site_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS meshscore_daily (
      day TEXT PRIMARY KEY,
      score INTEGER NOT NULL,
      messages INTEGER NOT NULL,
      avg_repeats REAL NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS meshscore_cache (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      updated_at TEXT NOT NULL,
      payload TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS repeater_rank_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      recorded_at TEXT NOT NULL,
      total INTEGER NOT NULL,
      active INTEGER NOT NULL,
      total24h INTEGER NOT NULL,
      cached_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_repeater_rank_history_recorded_at ON repeater_rank_history(recorded_at);
    CREATE TABLE IF NOT EXISTS repeater_rank_cache (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      updated_at TEXT NOT NULL,
      payload TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS observer_rank_cache (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      updated_at TEXT NOT NULL,
      payload TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS stats_5m (
      bucket_start INTEGER NOT NULL,
      channel_name TEXT NOT NULL,
      messages INTEGER NOT NULL,
      unique_senders INTEGER NOT NULL,
      unique_messages INTEGER NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (bucket_start, channel_name)
    );
    CREATE INDEX IF NOT EXISTS idx_stats_5m_bucket ON stats_5m(bucket_start);
    CREATE TABLE IF NOT EXISTS devices (
      pub TEXT PRIMARY KEY,
      name TEXT,
      is_repeater INTEGER,
      is_observer INTEGER,
      last_seen TEXT,
      observer_last_seen TEXT,
      gps_lat REAL,
      gps_lon REAL,
      raw_json TEXT,
      hidden_on_map INTEGER,
      updated_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_devices_last_seen ON devices(last_seen);
    CREATE TABLE IF NOT EXISTS observers (
      observer_id TEXT PRIMARY KEY,
      name TEXT,
      first_seen TEXT,
      last_seen TEXT,
      count INTEGER,
      gps_lat REAL,
      gps_lon REAL,
      updated_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_observers_last_seen ON observers(last_seen);
    CREATE TABLE IF NOT EXISTS rejected_adverts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pub TEXT,
      observer_id TEXT,
      heard_ms INTEGER,
      reason TEXT,
      sample_json TEXT
    );
    CREATE TABLE IF NOT EXISTS ingest_metrics (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TEXT
    );
  `);
  ensureColumn(db, "users", "display_name", "TEXT");
  ensureColumn(db, "messages", "path_text", "TEXT");
  ensureColumn(db, "message_observers", "path_text", "TEXT");
  ensureColumn(db, "message_observers", "ts_ms", "INTEGER");
  ensureColumn(db, "geoscore_routes", "ts_ms", "INTEGER");
  ensureColumn(db, "geoscore_routes", "teleport_max_km", "REAL");
  ensureColumn(db, "channels_catalog", "allow_popular", "INTEGER NOT NULL DEFAULT 1");
  ensureColumn(db, "devices", "last_advert_heard_ms", "INTEGER");
  resetUserChannelsToFixed(db);
  return db;
}

function buildRankTotals(items) {
  const total = items.length;
  const active = items.filter((r) => !r.stale).length;
  const total24h = items.reduce((sum, r) => sum + (Number.isFinite(r.total24h) ? r.total24h : 0), 0);
  return { total, active, total24h };
}

function updateRankSummary(items, cachedAt) {
  const totals = buildRankTotals(items);
  rankSummaryCache = {
    updatedAt: cachedAt || new Date().toISOString(),
    totals: { ...totals },
    lastPersistAt: rankSummaryCache.lastPersistAt
  };
  const now = Date.now();
  if (!rankSummaryCache.lastPersistAt || (now - rankSummaryCache.lastPersistAt) >= RANK_HISTORY_PERSIST_INTERVAL) {
    try {
      const db = getDb();
      db.prepare(
        `INSERT INTO repeater_rank_history (recorded_at, total, active, total24h, cached_at)
         VALUES (?, ?, ?, ?, ?)`
      ).run(new Date(rankSummaryCache.updatedAt).toISOString(), totals.total, totals.active, totals.total24h, rankSummaryCache.updatedAt);
      rankSummaryCache.lastPersistAt = now;
    } catch {
      // ignore persistence errors (history is best-effort)
    }
  }
}

function hydrateRepeaterRankCache() {
  try {
    const row = getDb()
      .prepare("SELECT updated_at, payload FROM repeater_rank_cache WHERE id = 1")
      .get();
    if (!row?.payload) return;
    const parsed = JSON.parse(row.payload);
    if (!parsed || !Array.isArray(parsed.items)) return;
    const updatedAt = row.updated_at || parsed.updatedAt || new Date().toISOString();
    rankCache = {
      ...parsed,
      excluded: Array.isArray(parsed.excluded) ? parsed.excluded : [],
      cachedAt: updatedAt
    };
    updateRankSummary(parsed.items, updatedAt);
  } catch {}
}

function persistRepeaterRankCache(payload) {
  try {
    if (!payload) return;
    const updatedAt = payload.updatedAt || new Date().toISOString();
    const normalized = {
      ...payload,
      excluded: Array.isArray(payload.excluded) ? payload.excluded : []
    };
    getDb()
      .prepare(`
        INSERT INTO repeater_rank_cache (id, updated_at, payload)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, payload=excluded.payload
      `)
      .run(updatedAt, JSON.stringify(normalized));
  } catch {}
}

function hydrateObserverRankCache() {
  try {
    const row = getDb()
      .prepare("SELECT updated_at, payload FROM observer_rank_cache WHERE id = 1")
      .get();
    if (!row?.payload) return;
    const parsed = JSON.parse(row.payload);
    if (!parsed || !Array.isArray(parsed.items)) return;
    const updatedAt = row.updated_at || parsed.updatedAt || new Date().toISOString();
    observerRankCache = { ...parsed, updatedAt };
  } catch {}
}

function persistObserverRankCache(payload) {
  try {
    if (!payload) return;
    const updatedAt = payload.updatedAt || new Date().toISOString();
    getDb()
      .prepare(`
        INSERT INTO observer_rank_cache (id, updated_at, payload)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, payload=excluded.payload
      `)
      .run(updatedAt, JSON.stringify(payload));
  } catch {}
}

function hydrateMeshScoreCache() {
  try {
    const row = getDb()
      .prepare("SELECT updated_at, payload FROM meshscore_cache WHERE id = 1")
      .get();
    if (!row?.payload) return;
    const parsed = JSON.parse(row.payload);
    if (!parsed) return;
    const updatedAt = row.updated_at || parsed.updatedAt || new Date().toISOString();
    meshScoreCache = { updatedAt, payload: parsed };
  } catch {}
}

function persistMeshScoreCache(payload) {
  try {
    if (!payload) return;
    const updatedAt = payload.updatedAt || new Date().toISOString();
    getDb()
      .prepare(`
        INSERT INTO meshscore_cache (id, updated_at, payload)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, payload=excluded.payload
      `)
      .run(updatedAt, JSON.stringify(payload));
  } catch {}
}

function getAuthToken(req) {
  const auth = req.headers["authorization"] || "";
  if (auth.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  const cookie = req.headers["cookie"] || "";
  const m = cookie.match(/(?:^|;\s*)mesh_session=([^;]+)/i);
  return m ? m[1] : null;
}

function getSessionUser(req) {
  const token = getAuthToken(req);
  if (!token) return null;
  const db = getDb();
  const row = db.prepare(`
    SELECT u.id, u.username, u.display_name, u.is_admin, s.expires_at
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.token = ?
  `).get(token);
  if (!row) return null;
  const expiresAt = new Date(row.expires_at);
  if (!Number.isFinite(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    db.prepare("DELETE FROM sessions WHERE token = ?").run(token);
    return null;
  }
  return { id: row.id, username: row.username, displayName: row.display_name || null, isAdmin: !!row.is_admin };
}

function createSession(db, userId) {
  const token = crypto.randomBytes(24).toString("hex");
  const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
  db.prepare("INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)").run(token, userId, expiresAt);
  return { token, expiresAt };
}

function buildSessionCookie(req, token) {
  const proto = String(req.headers["x-forwarded-proto"] || "").toLowerCase();
  const secure = proto.includes("https");
  return `mesh_session=${token}; Path=/; Max-Age=2592000; SameSite=Lax${secure ? "; Secure" : ""}`;
}

const oauthStates = new Map();
function createOauthState(payload = {}) {
  const state = crypto.randomBytes(16).toString("hex");
  oauthStates.set(state, { ...payload, expiresAt: Date.now() + 10 * 60 * 1000 });
  return state;
}
function consumeOauthState(state) {
  const entry = oauthStates.get(state);
  if (!entry) return null;
  oauthStates.delete(state);
  if (entry.expiresAt < Date.now()) return null;
  return entry;
}

function readJsonSafe(p, def) {
  try {
    if (!fs.existsSync(p)) return def;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return def;
  }
}

function writeJsonSafe(p, data) {
  const tmp = p + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2));
  fs.renameSync(tmp, p);
}

async function verifyGoogleIdToken(idToken) {
  if (!idToken) throw new Error("missing id token");
  if (!GOOGLE_CLIENT_ID) throw new Error("google oauth not configured");
  const verifyRes = await fetch(`https://oauth2.googleapis.com/tokeninfo?id_token=${encodeURIComponent(idToken)}`);
  const tokenInfo = await verifyRes.json().catch(() => ({}));
  if (!verifyRes.ok || String(tokenInfo.aud || "") !== GOOGLE_CLIENT_ID) {
    throw new Error("google token validation failed");
  }
  return tokenInfo;
}

function ensureGoogleUser(db, tokenInfo) {
  const email = String(tokenInfo?.email || "").trim();
  if (!email) throw new Error("google account missing email");
  const displayName = String(tokenInfo?.name || "").trim() || null;
  const subKey = String(tokenInfo?.sub || "").trim() || crypto.randomBytes(6).toString("hex");
  const user = db.prepare(`
    SELECT id, username, display_name, is_admin
    FROM users
    WHERE lower(username) = lower(?)
  `).get(email);
  if (!user) {
    const hash = `oauth:google:${subKey}`;
    const now = new Date().toISOString();
    const info = db.prepare("INSERT INTO users (username, password_hash, display_name, created_at) VALUES (?, ?, ?, ?)")
      .run(email, hash, displayName || null, now);
    return { id: info.lastInsertRowid, username: email, display_name: displayName, is_admin: 0 };
  }
  if (displayName && displayName !== user.display_name) {
    db.prepare("UPDATE users SET display_name = ? WHERE id = ?").run(displayName, user.id);
    user.display_name = displayName;
  }
  return user;
}

function createGoogleSession(db, tokenInfo, req, res) {
  const user = ensureGoogleUser(db, tokenInfo);
  const session = createSession(db, user.id);
  db.prepare("UPDATE users SET last_login = ? WHERE id = ?").run(new Date().toISOString(), user.id);
  res.setHeader("Set-Cookie", buildSessionCookie(req, session.token));
  return { session, user };
}

function readDevicesJson() {
  return readJsonSafe(devicesPath, { byPub: {} });
}

function readObserversJson() {
  return readJsonSafe(observersPath, { byId: {} });
}

function readRotmConfig() {
  return readJsonSafe(rotmConfigPath, { channel: "#rotm" });
}

function normalizeRotmChannel(value) {
  const raw = String(value || "").trim();
  if (!raw) return "#rotm";
  return raw.startsWith("#") ? raw : `#${raw}`;
}

function isValidGps(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (lat === 0 && lon === 0) return false;
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return false;
  return true;
}

function normalizeGpsValue(value) {
  if (!value || typeof value !== "object") return null;
  const lat = Number(value.lat ?? value.latitude);
  const lon = Number(value.lon ?? value.lng ?? value.longitude);
  return isValidGps(lat, lon) ? { lat, lon } : null;
}

function normalizeRotmHandle(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function extractRotmCallsign(value) {
  const text = String(value || "").toUpperCase();
  const match = text.match(/[A-Z0-9]{1,3}\d[A-Z0-9/]{1,6}/);
  return match ? match[0] : "";
}

function buildRotmKeys(value) {
  const keys = new Set();
  const direct = normalizeRotmHandle(value);
  if (direct) keys.add(direct);
  const call = extractRotmCallsign(value);
  const callNorm = normalizeRotmHandle(call);
  if (callNorm) keys.add(callNorm);
  return Array.from(keys);
}

function extractRotmMentions(body) {
  const text = String(body || "");
  const out = new Set();
  const re = /@\[?([a-z0-9][a-z0-9_\/-]{1,31})\]?/gi;
  let match;
  while ((match = re.exec(text)) !== null) {
    const norm = normalizeRotmHandle(match[1]);
    if (norm) out.add(norm);
  }
  return Array.from(out);
}

function buildRotmCandidatesByHash(devices) {
  const byPub = devices.byPub || {};
  const map = new Map(); // hash -> [{pub, name, gps}]
  for (const d of Object.values(byPub)) {
    if (!d?.isRepeater) continue;
    if (isCompanionDevice(d)) continue;
    if (d.hiddenOnMap || d.gpsImplausible || d.gpsFlagged) continue;
    if (!d.gps || !isValidGps(d.gps.lat, d.gps.lon)) continue;
    const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || null;
    const hash = nodeHashFromPub(pub);
    if (!hash) continue;
    if (!map.has(hash)) map.set(hash, []);
    map.get(hash).push({
      hash,
      pub: String(pub).toUpperCase(),
      name: d.name || d.raw?.lastAdvert?.appData?.name || hash,
      gps: d.gps
    });
  }
  return map;
}

function readRotmOverrides() {
  const raw = readJsonSafe(rotmOverridesPath, {});
  if (!raw || typeof raw !== "object") return {};
  return raw;
}

function readZeroHopOverrides() {
  const raw = readJsonSafe(zeroHopOverridesPath, {});
  if (!raw || typeof raw !== "object") return {};
  return raw;
}

function resolveRotmRepeaterFromPath(path, candidatesByHash) {
  if (!Array.isArray(path) || path.length === 0) return null;
  const SUPPORT_RADIUS_KM = 80;
  const firstHash = path[0];
  const firstCandidates = candidatesByHash.get(firstHash) || [];
  if (firstCandidates.length === 1) {
    const cand = firstCandidates[0];
    return {
      hash: firstHash,
      name: cand.name || firstHash,
      gps: cand.gps,
      pub: cand.pub
    };
  }
  if (firstCandidates.length > 1) {
    const nextCandidates = path.length > 1 ? (candidatesByHash.get(path[1]) || []) : [];
    const next2Candidates = path.length > 2 ? (candidatesByHash.get(path[2]) || []) : [];
    if (!nextCandidates.length && !next2Candidates.length) {
      const cand = firstCandidates[0];
      return {
        hash: firstHash,
        name: cand.name || firstHash,
        gps: cand.gps,
        pub: cand.pub
      };
    }
    let bestFirst = null;
    let bestScore = Infinity;
    const minDist = (cand, others) => {
      if (!others.length) return Infinity;
      let bestKm = Infinity;
      others.forEach((o) => {
        const km = haversineKm(cand.gps.lat, cand.gps.lon, o.gps.lat, o.gps.lon);
        if (Number.isFinite(km) && km < bestKm) bestKm = km;
      });
      return bestKm;
    };
    firstCandidates.forEach((cand) => {
      const dist1 = minDist(cand, nextCandidates);
      const dist2 = minDist(cand, next2Candidates);
      let score = 0;
      if (Number.isFinite(dist1) && dist1 !== Infinity) {
        score += dist1;
        if (dist1 > SUPPORT_RADIUS_KM) score += 5000;
      }
      if (Number.isFinite(dist2) && dist2 !== Infinity) {
        score += dist2 * 0.7;
        if (dist2 > SUPPORT_RADIUS_KM) score += 2500;
      }
      if (!Number.isFinite(score) || score === 0) score = 9999;
      if (score < bestScore) {
        bestScore = score;
        bestFirst = cand;
      }
    });
    if (bestFirst) {
      return {
        hash: firstHash,
        name: bestFirst.name || firstHash,
        gps: bestFirst.gps,
        pub: bestFirst.pub
      };
    }
  }
  const neighbors = path.map((hash, idx) => ({
    hash,
    idx,
    candidates: candidatesByHash.get(hash) || []
  })).filter((entry) => entry.candidates.length);
  if (!neighbors.length) return null;

  let best = null;
  let bestScore = -Infinity;
  for (const entry of neighbors) {
    const prev = path[entry.idx - 1];
    const next = path[entry.idx + 1];
    const prevCandidates = prev ? (candidatesByHash.get(prev) || []) : [];
    const nextCandidates = next ? (candidatesByHash.get(next) || []) : [];
    for (const cand of entry.candidates) {
      let support = 0;
      let distSum = 0;
      let distCount = 0;
      const measure = (others) => {
        if (!others.length) return false;
        let bestKm = Infinity;
        others.forEach((o) => {
          const km = haversineKm(cand.gps.lat, cand.gps.lon, o.gps.lat, o.gps.lon);
          if (Number.isFinite(km) && km < bestKm) bestKm = km;
        });
        if (Number.isFinite(bestKm)) {
          distSum += bestKm;
          distCount += 1;
        }
        return bestKm <= SUPPORT_RADIUS_KM;
      };
      if (measure(prevCandidates)) support += 1;
      if (measure(nextCandidates)) support += 1;
      const hasChain = path.length > 1;
      if (hasChain && support === 0) continue;
      const avgDist = distCount ? distSum / distCount : 9999;
      const score = support * 1000 - avgDist;
      if (score > bestScore) {
        bestScore = score;
        best = {
          hash: entry.hash,
          name: cand.name || entry.hash,
          gps: cand.gps,
          pub: cand.pub
        };
      }
    }
  }
  return best;
}

function isCompanionDevice(d) {
  if (!d) return false;
  const role = String(d.role || "").toLowerCase();
  if (role === "companion") return true;
  const roleName = String(d.appFlags?.roleName || "").toLowerCase();
  if (roleName === "companion") return true;
  const roleCode = Number.isFinite(d.appFlags?.roleCode) ? d.appFlags.roleCode : null;
  if (roleCode === 0) return true;
  const geoRole = Number.isFinite(d.raw?.lastAdvert?.appData?.deviceRole)
    ? Number(d.raw.lastAdvert.appData.deviceRole)
    : null;
  if (geoRole === 4) return true; // sensors & non-repeater companions
  return false;
}

function readDevices() {
  const now = Date.now();
  if (devicesCache.data && (now - devicesCache.readAt) < DEVICES_CACHE_MS) {
    return devicesCache.data;
  }
  const fallback = readDevicesJson();
  try {
    const db = getDb();
    const has = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='devices'").get();
    if (!has) return fallback;
    const rows = db.prepare(`
      SELECT pub, name, is_repeater, is_observer, last_seen, observer_last_seen,
             last_advert_heard_ms, gps_lat, gps_lon, raw_json, hidden_on_map, updated_at
      FROM devices
    `).all();
    if (!rows.length) return fallback;
    const byPub = {};
    let updatedAt = null;
      rows.forEach((row) => {
        const pub = String(row.pub || "").toUpperCase();
        if (!pub) return;
        let raw = null;
        if (row.raw_json) {
          try { raw = JSON.parse(row.raw_json); } catch {}
        }
        const entryRaw = raw
          ? (raw.lastAdvert ? raw : { lastAdvert: raw })
          : null;
        const gps = isValidGps(row.gps_lat, row.gps_lon)
          ? { lat: row.gps_lat, lon: row.gps_lon }
          : null;
        const entryMeta = entryRaw?.meta || {};
        const nameValid = typeof entryMeta.nameValid === "boolean" ? entryMeta.nameValid : null;
        const entry = {
          pub,
          name: row.name || null,
          isRepeater: !!row.is_repeater,
          isObserver: !!row.is_observer,
          lastSeen: row.last_seen || null,
          observerLastSeen: row.observer_last_seen || null,
          lastAdvertHeardMs: Number.isFinite(row.last_advert_heard_ms) ? row.last_advert_heard_ms : null,
          verifiedAdvert: !!entryMeta.verifiedAdvert,
          nameValid: nameValid === true,
          nameInvalidReason: entryMeta.nameInvalidReason || null,
          gpsInvalidReason: entryMeta.gpsInvalidReason || null,
          gps,
          raw: entryRaw,
          hiddenOnMap: row.hidden_on_map ? true : false
        };
        entry.nameInvalid = entry.nameValid === false;
        if (entryRaw?.lastAdvert) {
          entry.gpsReported = normalizeGpsValue(
            entryRaw.lastAdvert.gps ||
            entryRaw.lastAdvert.appData?.location ||
            null
          );
        }
        const manual = fallback.byPub?.[pub];
        if (manual) {
        if (manual.gpsReported && !entry.gpsReported) entry.gpsReported = manual.gpsReported;
        if (typeof manual.gpsImplausible === "boolean") entry.gpsImplausible = manual.gpsImplausible;
        if (typeof manual.gpsFlagged === "boolean") entry.gpsFlagged = manual.gpsFlagged;
        if (manual.gpsFlaggedAt) entry.gpsFlaggedAt = manual.gpsFlaggedAt;
        if (typeof manual.gpsEstimated === "boolean") entry.gpsEstimated = manual.gpsEstimated;
        if (manual.gpsEstimateAt) entry.gpsEstimateAt = manual.gpsEstimateAt;
        if (manual.gpsEstimateNeighbors) entry.gpsEstimateNeighbors = manual.gpsEstimateNeighbors;
        if (manual.gpsEstimateReason) entry.gpsEstimateReason = manual.gpsEstimateReason;
        if (typeof manual.manualLocation === "boolean") entry.manualLocation = manual.manualLocation;
        if (manual.locSource && !entry.locSource) entry.locSource = manual.locSource;
        if (typeof manual.hiddenOnMap === "boolean") entry.hiddenOnMap = manual.hiddenOnMap;
        if (!entry.raw && manual.raw) entry.raw = manual.raw;
      }
        byPub[pub] = entry;
        const ts = row.updated_at ? new Date(row.updated_at).getTime() : 0;
        if (!updatedAt || ts > new Date(updatedAt).getTime()) updatedAt = row.updated_at;
      });
      const payload = { byPub, updatedAt: updatedAt || new Date().toISOString() };
    devicesCache = { readAt: now, data: payload };
    return payload;
  } catch {
    return fallback;
  }
}

function readObservers() {
  const now = Date.now();
  if (observersCache.data && (now - observersCache.readAt) < OBSERVERS_CACHE_MS) {
    return observersCache.data;
  }
  const fallback = readObserversJson();
  try {
    const db = getDb();
    const has = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='observers'").get();
    if (!has) return fallback;
    const rows = db.prepare(`
      SELECT observer_id, name, first_seen, last_seen, count,
             gps_lat, gps_lon, updated_at
      FROM observers
    `).all();
    if (!rows.length) return fallback;
    const byId = {};
    let updatedAt = null;
    rows.forEach((row) => {
      const id = String(row.observer_id || "").trim();
      if (!id) return;
      const gps = (Number.isFinite(row.gps_lat) && Number.isFinite(row.gps_lon))
        ? { lat: row.gps_lat, lon: row.gps_lon }
        : null;
      const entry = {
        id,
        name: row.name || null,
        firstSeen: row.first_seen || null,
        lastSeen: row.last_seen || null,
        count: row.count || 0,
        gps
      };
      const manual = fallback.byId?.[id];
      if (manual) Object.assign(entry, manual);
      byId[id] = entry;
      const ts = row.updated_at ? new Date(row.updated_at).getTime() : 0;
      if (!updatedAt || ts > new Date(updatedAt).getTime()) updatedAt = row.updated_at;
    });
    const payload = { byId, updatedAt: updatedAt || fallback.updatedAt || new Date().toISOString() };
    observersCache = { readAt: now, data: payload };
    return payload;
  } catch {
    return fallback;
  }
}

function safeCount(db, sql, params = []) {
  try {
    const stmt = db.prepare(sql);
    const row = Array.isArray(params) ? stmt.get(...params) : stmt.get(params);
    return row ? Number(row.count || 0) : 0;
  } catch {
    return 0;
  }
}

function buildHealthPayload() {
  const db = getDb();
  const now = Date.now();
  const last24h = now - 24 * 60 * 60 * 1000;
  const last3d = now - 3 * 24 * 60 * 60 * 1000;
  const last10m = now - 10 * 60 * 1000;
  const totalRepeaters = safeCount(db, "SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1");
  const repeatersLast24h = safeCount(db, "SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1 AND last_advert_heard_ms >= ?", [last24h]);
  const repeatersLast3d = safeCount(db, "SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1 AND last_advert_heard_ms >= ?", [last3d]);
  const stale1dTo3d = safeCount(
    db,
    "SELECT COUNT(*) AS count FROM devices WHERE is_repeater = 1 AND last_advert_heard_ms >= ? AND last_advert_heard_ms < ?",
    [last3d, last24h]
  );
  const ingestRows = db.prepare(
    "SELECT key, value FROM ingest_metrics WHERE key IN (?, ?)"
  ).all("countAdvertsSeenLast10m", "lastAdvertSeenAtIso");
  const ingestMetrics = {};
  ingestRows.forEach((row) => {
    if (row?.key) ingestMetrics[row.key] = row.value;
  });
  const rejectedAdvertsLast10m = safeCount(
    db,
    "SELECT COUNT(*) AS count FROM rejected_adverts WHERE heard_ms >= ?",
    [last10m]
  );
  const rankItems = Array.isArray(rankCache.items) ? rankCache.items : [];
  const excludedItems = Array.isArray(rankCache.excluded) ? rankCache.excluded : [];
  const observersJson = readObserversJson();
  const observerIds = Object.keys(observersJson.byId || {});
  const sampleObserverIds = observerIds.slice(0, 3);
  return {
    nowIso: new Date().toISOString(),
    dbPath: getDbPath(),
    dbDatabaseList: (db.pragma("database_list") || []).map((entry) => entry || {}),
    ingest: {
      advertsLast10m: Number.parseInt(ingestMetrics.countAdvertsSeenLast10m || "0", 10) || 0,
      lastAdvertHeardIso: ingestMetrics.lastAdvertSeenAtIso || null,
      rejectedAdvertsLast10m
    },
    repeaters: {
      totalRepeaters,
      repeatersLast24h,
      repeatersLast3d,
      stale1dTo3d
    },
    rankCache: {
      updatedAtIso: rankCache.updatedAt || rankCache.cachedAt || null,
      includedCount: rankItems.length,
      excludedCount: excludedItems.length
    },
    observerAnchoring: {
      observersJsonLoaded: observerIds.length > 0,
      observersCount: observerIds.length,
      sampleObserverIds
    }
  };
}

function getObserverLink(observerId) {
  const rawId = String(observerId || "").trim();
  if (!rawId) return { linkedPub: null, linkedHash: null, source: "missing" };
  const normalizedId = rawId.toUpperCase();
  const observersJson = readObserversJson();
  const entry = (observersJson.byId?.[normalizedId] || observersJson.byId?.[rawId]) || null;
  if (!entry) return { linkedPub: null, linkedHash: null, source: "missing" };
  const pub = sanitizeRepeaterPub(entry.bestRepeaterPub || entry.bestRepeaterPubKey || entry.bestRepeater || "");
  if (!pub) return { linkedPub: null, linkedHash: null, source: "observers.json:no_pub" };
  return { linkedPub: pub, linkedHash: nodeHashFromPub(pub), source: "observers.json" };
}

function parseIso(ts) {
  const d = new Date(ts);
  return Number.isFinite(d.getTime()) ? d : null;
}

function buildRepeatersWithinWindow(byPub, windowMs) {
  const now = Date.now();
  const repeaters = new Map();
  for (const [pub, heardMs] of getRepeatersLastAdvertHeardMsMap(byPub)) {
    if (!Number.isFinite(heardMs)) continue;
    if (heardMs < now - windowMs || heardMs > now) continue;
    const entry = byPub[pub];
    if (!entry) continue;
    repeaters.set(pub, {
      pub,
      entry,
      lastAdvertHeardMs: heardMs,
      lastAdvertHeardIso: new Date(heardMs).toISOString()
    });
  }
  return repeaters;
}

function getRepeaterLastAdvertHeardMs(device) {
  if (!device) return null;
  return Number.isFinite(device.lastAdvertHeardMs) ? device.lastAdvertHeardMs : null;
}

function getRepeatersLastAdvertHeardMsMap(byPub) {
  const map = new Map();
  for (const entry of Object.values(byPub || {})) {
    if (!entry?.isRepeater) continue;
    const pubKey = String(
      entry.pub ||
      entry.publicKey ||
      entry.pubKey ||
      entry.raw?.lastAdvert?.publicKey ||
      ""
    ).toUpperCase();
    if (!pubKey) continue;
    const heardMs = getRepeaterLastAdvertHeardMs(entry);
    if (!Number.isFinite(heardMs)) continue;
    map.set(pubKey, heardMs);
  }
  return map;
}

function buildRepeatEvidenceMap(windowMs) {
  const db = getDb();
  const map = new Map();
  if (!hasMessageObserversDb(db)) return map;
  const sinceIso = new Date(Date.now() - windowMs).toISOString();
  try {
    const rows = db.prepare(`
      SELECT path_json, path_text
      FROM message_observers
      WHERE ts >= ?
        AND (path_json IS NOT NULL OR path_text IS NOT NULL)
    `).all(sinceIso);
    rows.forEach((row) => {
      const tokens = row.path_json
        ? parsePathJsonTokens(row.path_json)
        : parsePathTokens(row.path_text);
      if (!Array.isArray(tokens) || !tokens.length) return;
      tokens.forEach((token, idx) => {
        if (!token) return;
        let entry = map.get(token);
        if (!entry) {
          entry = { middleCount: 0, upstream: new Set(), downstream: new Set() };
          map.set(token, entry);
        }
        if (idx > 0) entry.upstream.add(tokens[idx - 1]);
        if (idx < tokens.length - 1) entry.downstream.add(tokens[idx + 1]);
        if (idx > 0 && idx < tokens.length - 1) {
          entry.middleCount += 1;
        }
      });
    });
  } catch {
    // best-effort; swallow DB errors
  }
  return map;
}

function summarizeRepeatEvidence(entry) {
  if (!entry) {
    return {
      middleCount: 0,
      upstreamCount: 0,
      downstreamCount: 0,
      isTrueRepeater: false,
      repeatEvidenceReason: "insufficient_repeat_evidence"
    };
  }
  const middleCount = Number.isFinite(entry.middleCount) ? entry.middleCount : 0;
  const upstreamCount = entry.upstream ? entry.upstream.size : 0;
  const downstreamCount = entry.downstream ? entry.downstream.size : 0;
  const meetsMiddleThreshold = middleCount >= REPEAT_EVIDENCE_MIN_MIDDLE;
  const meetsNeighborThreshold = upstreamCount >= REPEAT_EVIDENCE_MIN_NEIGHBORS && downstreamCount >= REPEAT_EVIDENCE_MIN_NEIGHBORS;
  const isTrue = meetsMiddleThreshold || meetsNeighborThreshold;
  return {
    middleCount,
    upstreamCount,
    downstreamCount,
    isTrueRepeater: isTrue,
    repeatEvidenceReason: isTrue ? null : "insufficient_repeat_evidence"
  };
}

function classifyRepeaterQuality(entry, stats, lastAdvertHeardMs) {
  const reasons = [];
  if (!entry?.verifiedAdvert) {
    reasons.push("unverified_advert");
    return { quality: "phantom", reasons };
  }
  if (!Number.isFinite(lastAdvertHeardMs)) {
    reasons.push("missing_heard");
    return { quality: "phantom", reasons };
  }
  const statsTotal = stats?.total24h || 0;
  const hasGps = isValidGps(entry.gps?.lat, entry.gps?.lon);
  const nameValid = entry.nameValid === true;
  const nameInvalid = !nameValid;
  if (!hasGps && !nameValid && statsTotal === 0) {
    reasons.push("name_invalid_no_gps_no_activity");
    return { quality: "phantom", reasons };
  }
  if (!nameValid) {
    const nameReason = entry.nameInvalidReason || "missing";
    reasons.push(`name_invalid_${nameReason}`);
  }
  if (!hasGps) reasons.push("missing_gps");
  if (entry.hiddenOnMap) reasons.push("hidden_on_map");
  if (entry.gpsImplausible) reasons.push("gps_implausible");
  if (entry.gpsFlagged) reasons.push("gps_flagged");
  if (entry.gpsInvalidReason) reasons.push(`invalid_gps_${entry.gpsInvalidReason}`);
  if (reasons.length) return { quality: "low_quality", reasons };
  return { quality: "valid", reasons };
}

function chooseBetterRepeater(a, b) {
  if (!a) return b;
  if (!b) return a;
  const aTs = Number.isFinite(a.lastAdvertIngestMs) ? a.lastAdvertIngestMs : 0;
  const bTs = Number.isFinite(b.lastAdvertIngestMs) ? b.lastAdvertIngestMs : 0;
  if (aTs !== bTs) return aTs > bTs ? a : b;
  const aTotal = Number.isFinite(a.total24h) ? a.total24h : 0;
  const bTotal = Number.isFinite(b.total24h) ? b.total24h : 0;
  if (aTotal !== bTotal) return aTotal > bTotal ? a : b;
  return a.score >= b.score ? a : b;
}

function loadKeys() {
  try {
    if (!fs.existsSync(keysPath)) return { channels: [] };
    const obj = JSON.parse(fs.readFileSync(keysPath, "utf8"));
    if (!obj || typeof obj !== "object") return { channels: [] };
    if (!Array.isArray(obj.channels)) obj.channels = [];
    return obj;
  } catch {
    return { channels: [] };
  }
}

function getRfSourcePath() {
  if (fs.existsSync(rfPath)) return rfPath;
  if (fs.existsSync(observerPath)) return observerPath;
  return rfPath;
}

function getHex(rec) {
  if (rec && typeof rec.hex === "string") return rec.hex;
  if (rec && typeof rec.payloadHex === "string") return rec.payloadHex;
  return null;
}

function extractSenderPublicKey(decoded) {
  if (!decoded) return null;
  const payload = decoded.payload?.decoded || decoded.decoded || decoded;
  return payload?.senderPublicKey || payload?.publicKey || payload?.senderPub || payload?.pub || null;
}

function sha256Hex(hex) {
  try {
    const buf = Buffer.from(hex, "hex");
    return crypto.createHash("sha256").update(buf).digest("hex").toUpperCase();
  } catch {
    return null;
  }
}

function saveKeys(obj) {
  const tmp = keysPath + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, keysPath);
}

function nodeHashFromPub(pubHex) {
  if (!pubHex || typeof pubHex !== "string") return null;
  const clean = pubHex.replace(/\s+/g, "");
  if (!/^[0-9a-fA-F]+$/.test(clean)) return null;
  if (clean.length < 2) return null;
  return clean.slice(0, 2).toUpperCase();
}

function buildNodeHashMap() {
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const map = new Map(); // hash -> {name, lastSeen}

  for (const d of Object.values(byPub)) {
    if (!d) continue;
    const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey;
    const hash = nodeHashFromPub(pub);
    if (!hash) continue;
    const name = d.name || d.raw?.lastAdvert?.appData?.name || "Unknown";
    const lastSeen = d.lastSeen || null;
    const gps = (d.gps && Number.isFinite(d.gps.lat) && Number.isFinite(d.gps.lon)) ? d.gps : null;
    const hiddenOnMap = !!d.hiddenOnMap;
    const gpsImplausible = !!d.gpsImplausible;
    const gpsFlagged = !!d.gpsFlagged;
    const prev = map.get(hash);
    if (!prev) {
      map.set(hash, { name, lastSeen, gps, hiddenOnMap, gpsImplausible, gpsFlagged });
      continue;
    }
    const prevTs = prev.lastSeen ? new Date(prev.lastSeen).getTime() : 0;
    const nextTs = lastSeen ? new Date(lastSeen).getTime() : 0;
    const prefer = (gps && !prev.gps) || (nextTs >= prevTs);
    if (prefer) map.set(hash, { name, lastSeen, gps, hiddenOnMap, gpsImplausible, gpsFlagged });
  }

  return map;
}

function getCachedNodeHashMap() {
  const now = Date.now();
  const cache = meshFlowCache.nodeMap;
  if (!cache.map || (now - cache.updatedAt) > 30000) {
    cache.map = buildNodeHashMap();
    cache.updatedAt = now;
  }
  return cache.map;
}

function buildRepeaterHashMap() {
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const map = new Map();
  for (const d of Object.values(byPub)) {
    if (!d?.isRepeater) continue;
    const pub = String(d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || "").trim().toUpperCase();
    if (!pub) continue;
    const hash = nodeHashFromPub(pub);
    if (!hash) continue;
    const gps = (d.gps && Number.isFinite(d.gps.lat) && Number.isFinite(d.gps.lon)) ? { lat: d.gps.lat, lon: d.gps.lon } : null;
    if (!gps || (gps.lat === 0 && gps.lon === 0)) continue;
    const lastSeen = d.lastSeen || d.last_seen || null;
    const parsedLastSeen = lastSeen ? Date.parse(lastSeen) : NaN;
    const lastSeenMs = Number.isFinite(parsedLastSeen) ? parsedLastSeen : null;
    const candidate = {
      hash,
      pub,
      name: d.name || d.raw?.lastAdvert?.appData?.name || hash,
      gps,
      lastSeen: d.lastSeen || d.last_seen || null,
      gpsFlagged: !!d.gpsFlagged,
      gpsImplausible: !!d.gpsImplausible,
      hiddenOnMap: !!d.hiddenOnMap
    };
    const existing = map.get(hash) || [];
    existing.push(candidate);
    map.set(hash, existing);
  }
  return map;
}

function getCachedRepeaterHashMap() {
  const now = Date.now();
  const cache = meshFlowCache.repeaterHash;
  if (!cache.map || (now - cache.updatedAt) > 30000) {
    cache.map = buildRepeaterHashMap();
    cache.updatedAt = now;
  }
  return cache.map;
}

function normalizeGeoscoreCandidate(entry) {
  if (!entry) return null;
  const pubKey = entry.pub ? String(entry.pub).toUpperCase() : "";
  const lastSeenMs = Number.isFinite(lastHeardByPub.get(pubKey)) ? lastHeardByPub.get(pubKey) : null;
  return {
    hash: entry.hash,
    pub: pubKey,
    name: entry.name,
    gps: entry.gps,
    lastSeenMs,
    gpsFlagged: !!entry.gpsFlagged,
    gpsImplausible: !!entry.gpsImplausible,
    hiddenOnMap: !!entry.hiddenOnMap
  };
}

function buildCandidatesByToken(tokens) {
  const repeaterMap = getCachedRepeaterHashMap();
  const map = new Map();
  if (!tokens || !tokens.length) return map;
  tokens.forEach((token) => {
    if (map.has(token)) return;
    const raw = repeaterMap.get(token) || [];
    const entries = Array.isArray(raw) ? raw : raw ? [raw] : [];
    const normalized = entries.map(normalizeGeoscoreCandidate).filter(Boolean);
    if (!normalized.length) {
      map.set(token, []);
      return;
    }
    const filtered = normalized.filter((cand) => cand.gps && !cand.gpsFlagged && !cand.gpsImplausible && !cand.hiddenOnMap);
    const candidates = (filtered.length ? filtered : normalized).slice(0, GEO_SCORE_CANDIDATE_K);
    map.set(token, candidates);
  });
  return map;
}

function refreshLastHeardCache() {
  try {
    const db = getDb();
    const windowMs = Date.now() - GEO_SCORE_LAST_HEARD_WINDOW_MS;
    const stmt = db.prepare("SELECT ts_ms, path_json, path_text FROM message_observers WHERE ts_ms >= ?");
    const nextHash = new Map();
    for (const row of stmt.iterate(windowMs)) {
      const tsMs = Number.isFinite(row.ts_ms) ? row.ts_ms : NaN;
      if (!Number.isFinite(tsMs)) continue;
      let tokens = [];
      if (row.path_json) {
        tokens = parsePathJsonTokens(row.path_json);
      }
      if (!tokens.length && row.path_text) {
        tokens = parsePathTokens(row.path_text);
      }
      tokens.forEach((token) => {
        const prev = nextHash.get(token) || 0;
        if (tsMs > prev) nextHash.set(token, tsMs);
      });
    }
    lastHeardByHash = nextHash;
    const nextPub = new Map();
    const repeaterMap = getCachedRepeaterHashMap();
    for (const [hash, entries] of repeaterMap.entries()) {
      const heard = nextHash.get(hash);
      if (!Number.isFinite(heard)) continue;
      const candidates = Array.isArray(entries) ? entries : entries ? [entries] : [];
      candidates.forEach((entry) => {
        if (!entry?.pub) return;
        const pubKey = String(entry.pub || "").toUpperCase();
        const prev = nextPub.get(pubKey) || 0;
        if (heard > prev) nextPub.set(pubKey, heard);
      });
    }
    lastHeardByPub = nextPub;
  } catch (err) {
    console.error("(geoscore) last heard refresh failed", err);
  }
}

function jitterGps(gps) {
  if (!gps || !MESHFLOW_JITTER_ENABLED) return gps;
  const delta = MESHFLOW_JITTER_DEGREES;
  return {
    lat: gps.lat + (Math.random() - 0.5) * delta,
    lon: gps.lon + (Math.random() - 0.5) * delta
  };
}

function parseMeshFlowPath(row) {
  let values = null;
  if (row.path_text) {
    values = String(row.path_text).split("|");
  } else if (row.path_json) {
    try {
      values = JSON.parse(row.path_json);
    } catch {
      values = null;
    }
  }
  if (!values || !Array.isArray(values)) return [];
  return values.map(normalizePathHash).filter(Boolean);
}

function readMeshFlowRows(db, lastRowId, limit = MESHFLOW_READ_LIMIT) {
  if (!hasMessageObserversDb(db)) return { rows: [], lastRowId };
  const rows = db.prepare(`
    SELECT mo.rowid, mo.message_hash, mo.path_text, mo.path_json, mo.channel_name,
           m.channel_name AS message_channel, m.path_length
    FROM message_observers mo
    LEFT JOIN messages m ON m.message_hash = mo.message_hash
    WHERE mo.rowid > ?
    ORDER BY mo.rowid ASC
    LIMIT ?
  `).all(lastRowId || 0, limit);
  let next = lastRowId || 0;
  rows.forEach((row) => {
    if (row.rowid > next) next = row.rowid;
  });
  return { rows, lastRowId: next };
}

async function collectMeshFlowTick() {
  try {
    const db = getDb();
    const { rows, lastRowId: nextRow } = readMeshFlowRows(db, meshFlowLastRowId, MESHFLOW_READ_LIMIT);
    if (!rows.length) return;
    meshFlowLastRowId = nextRow;
    const nodeMap = getCachedNodeHashMap();
    const repeaterMap = getCachedRepeaterHashMap();
    const flowCounts = new Map();
    const nodes = new Map();
    rows.forEach((row) => {
      const path = parseMeshFlowPath(row);
      if (path.length < 2) return;
      const channel = String(row.message_channel || row.channel_name || "#public").toLowerCase();
      for (let idx = 0; idx < path.length - 1; idx += 1) {
        const fromHash = path[idx];
        const toHash = path[idx + 1];
        const fromNode = nodeMap.get(fromHash);
        const toNode = nodeMap.get(toHash);
        if (!fromNode?.gps || !toNode?.gps) continue;
        if (!Number.isFinite(fromNode.gps.lat) || !Number.isFinite(fromNode.gps.lon)) continue;
        if (!Number.isFinite(toNode.gps.lat) || !Number.isFinite(toNode.gps.lon)) continue;
        const fromGps = jitterGps(fromNode.gps);
        const toGps = jitterGps(toNode.gps);
        const fromLowScoring = !!fromNode.gpsImplausible || !!fromNode.gpsFlagged;
        const toLowScoring = !!toNode.gpsImplausible || !!toNode.gpsFlagged;
        const fromName = fromNode.name || fromHash;
        const toName = toNode.name || toHash;
        const fromIsRepeater = repeaterMap.has(fromHash);
        const toIsRepeater = repeaterMap.has(toHash);
        const key = `${fromHash}|${toHash}|${channel}`;
        const existing = flowCounts.get(key) || {
          key,
          channel,
          fromHash,
          toHash,
          fromName,
          toName,
          fromLat: fromGps.lat,
          fromLon: fromGps.lon,
          toLat: toGps.lat,
          toLon: toGps.lon,
          fromLowScoring,
          toLowScoring,
          fromIsRepeater,
          toIsRepeater,
          count: 0,
          pathLength: 0
        };
        existing.count += 1;
        const hopLength = Number.isFinite(row.path_length) ? row.path_length : path.length;
        existing.pathLength = Math.max(existing.pathLength || 0, hopLength);
        flowCounts.set(key, existing);
        nodes.set(fromHash, {
          hash: fromHash,
          name: fromName,
          lat: fromGps.lat,
          lon: fromGps.lon,
          lowScoring: fromLowScoring,
          isRepeater: fromIsRepeater
        });
        nodes.set(toHash, {
          hash: toHash,
          name: toName,
          lat: toGps.lat,
          lon: toGps.lon,
          lowScoring: toLowScoring,
          isRepeater: toIsRepeater
        });
      }
    });
    if (!flowCounts.size) return;
    const flows = Array.from(flowCounts.values()).map((item) => ({
      key: item.key,
      channel: item.channel,
      count: item.count,
      fromHash: item.fromHash,
      toHash: item.toHash,
      fromName: item.fromName,
      toName: item.toName,
      fromLat: item.fromLat,
      fromLon: item.fromLon,
      toLat: item.toLat,
      toLon: item.toLon,
      fromLowScoring: item.fromLowScoring,
      toLowScoring: item.toLowScoring,
      fromIsRepeater: item.fromIsRepeater,
      toIsRepeater: item.toIsRepeater,
      pathLength: item.pathLength
    }));
    meshFlowHistory.push({
      ts: Date.now(),
      flows,
      nodes: Array.from(nodes.values()),
      packetCount: rows.length
    });
    const retention = MESHFLOW_HISTORY_MAX_SEC * 1000;
    const cutoff = Date.now() - retention;
    while (meshFlowHistory.length && meshFlowHistory[0].ts < cutoff) {
      meshFlowHistory.shift();
    }
  } catch (err) {
    console.error("MeshFlow tick failed:", err?.message || err);
  }
}

function mapChannelName(rec, keyMap) {
  const decodedHash = typeof rec.decoded?.channelHash === "string" ? rec.decoded.channelHash.toUpperCase() : null;
  const hashByte = typeof rec.channel?.hashByte === "string" ? rec.channel.hashByte.toUpperCase() : null;
  const candidate = decodedHash || hashByte;
  if (candidate && keyMap[candidate]) return keyMap[candidate];
  return null;
}

function buildKeyStore(cfg) {
  if (!cfg || !Array.isArray(cfg.channels)) return null;
  const channelSecrets = [];
  for (const ch of cfg.channels) {
    const sec = String(ch?.secretHex || "").trim();
    if (/^[0-9a-fA-F]{32}$/.test(sec)) channelSecrets.push(sec);
  }
  if (!channelSecrets.length) return null;
  return MeshCoreDecoder.createKeyStore({ channelSecrets });
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (v) => (v * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function normalizePathHash(val) {
  if (typeof val === "number" && Number.isFinite(val)) {
    return val.toString(16).toUpperCase().padStart(2, "0");
  }
  if (typeof val === "string") return val.toUpperCase();
  return "??";
}

function loadJsonFromPaths(paths, defaultValue = {}) {
  for (const p of paths) {
    if (!p) continue;
    const resolved = path.resolve(p);
    if (!fs.existsSync(resolved)) continue;
    const data = readJsonSafe(resolved, defaultValue);
    if (data && typeof data === "object" && Object.keys(data).length) {
      return { data, path: resolved };
    }
  }
  return { data: defaultValue, path: paths[0] ? path.resolve(paths[0]) : null };
}

function parsePathTokens(value) {
  if (!value) return [];
  const raw = String(value).toUpperCase();
  const matches = raw.match(/[0-9A-F]{2}/g) || [];
  return matches.map((token) => normalizePathHash(token)).filter((token) => token && token !== "??");
}

function parsePathJsonTokens(value) {
  if (!value) return [];
  let arr;
  try {
    arr = JSON.parse(value);
  } catch {
    return [];
  }
  if (!Array.isArray(arr)) return [];
  return arr.map((token) => normalizePathHash(token)).filter((token) => token && token !== "??");
}

function safeParseCandidates(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function buildRepeaterPubsByHash(byPub) {
  const map = new Map();
  for (const entry of Object.values(byPub || {})) {
    if (!entry?.isRepeater) continue;
    const pub = String(
      entry.pub ||
      entry.publicKey ||
      entry.pubKey ||
      entry.raw?.lastAdvert?.publicKey ||
      ""
    ).toUpperCase();
    if (!pub) continue;
    const hash = nodeHashFromPub(pub);
    if (!hash) continue;
    const list = map.get(hash) || [];
    if (!list.includes(pub)) list.push(pub);
    map.set(hash, list);
  }
  return map;
}

function deriveLinkedRepeaterForObserver(observerId, db, repeaterHashMap) {
  if (!observerId || !db || !repeaterHashMap || !repeaterHashMap.size || !hasMessageObserversDb(db)) return null;
  const rows = db.prepare(`
    SELECT path_json, path_text
    FROM message_observers
    WHERE observer_id = ?
    ORDER BY rowid DESC
    LIMIT 50
  `).all(observerId);
  for (const row of rows) {
    const tokens = row.path_json
      ? parsePathJsonTokens(row.path_json)
      : parsePathTokens(row.path_text);
    if (!tokens.length) continue;
    for (const token of tokens) {
      const candidates = repeaterHashMap.get(token);
      if (candidates && candidates.length) {
        return { pub: candidates[0], hash: token, source: "derived:message_observers" };
      }
    }
  }
  return null;
}

function distanceKm(a, b) {
  if (!a || !b) return null;
  const toRad = (v) => (v * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;
  const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  return R * c;
}

function getObserverProfile(observerId) {
  if (!observerId) return null;
  return geoscoreObserverProfilesCache.get(String(observerId).toUpperCase()) || null;
}

function ensureGeoscoreObserverStmt(db) {
  if (geoscoreObserverProfileUpsertStmt) return geoscoreObserverProfileUpsertStmt;
  geoscoreObserverProfileUpsertStmt = db.prepare(`
    INSERT INTO geoscore_observer_profiles (
      observer_id, linked_repeater_pub, home_lat, home_lon, source, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(observer_id) DO UPDATE SET
      linked_repeater_pub = excluded.linked_repeater_pub,
      home_lat = excluded.home_lat,
      home_lon = excluded.home_lon,
      source = excluded.source,
      updated_at = excluded.updated_at
  `);
  return geoscoreObserverProfileUpsertStmt;
}

function ensureGeoscoreRouteStmt(db) {
  if (geoscoreRouteUpsertStmt) return geoscoreRouteUpsertStmt;
  geoscoreRouteUpsertStmt = db.prepare(`
    INSERT INTO geoscore_routes (
      msg_key, ts, ts_ms, observer_id, path_tokens, inferred_pubs, hop_confidences,
      route_confidence, unresolved, candidates_json, teleport_max_km
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(msg_key) DO UPDATE SET
      ts = excluded.ts,
      ts_ms = excluded.ts_ms,
      observer_id = excluded.observer_id,
      path_tokens = excluded.path_tokens,
      inferred_pubs = excluded.inferred_pubs,
      hop_confidences = excluded.hop_confidences,
      route_confidence = excluded.route_confidence,
      unresolved = excluded.unresolved,
      candidates_json = excluded.candidates_json,
      teleport_max_km = excluded.teleport_max_km
  `);
  return geoscoreRouteUpsertStmt;
}

function buildGeoscoreRoutePayload(entry) {
  const tokens = Array.isArray(entry.pathTokens) ? entry.pathTokens : [];
  const observerProfile = getObserverProfile(entry.observerId);
  const observerHome = observerProfile?.home_lat && observerProfile?.home_lon
    ? { lat: observerProfile.home_lat, lon: observerProfile.home_lon }
    : null;
  const candidatesByToken = buildCandidatesByToken(tokens);
  const fallbackMetadata = tokens.map((token) => ({ token, count: 0, top: [] }));
  const inference = inferRouteViterbi({
    tokens,
    observerHome,
    ts: entry.ts,
    now: Date.now(),
    candidatesByToken,
    edgePrior: () => 0
  }) || {
    inferredPubs: tokens.map(() => null),
    hopConfidences: tokens.map(() => 0),
    routeConfidence: 0,
    unresolved: 1,
    candidatesJson: JSON.stringify(fallbackMetadata),
    candidateMetadata: fallbackMetadata,
    teleportMaxKm: null,
    diagnostics: {
      candidateCounts: fallbackMetadata.map((m) => m.count),
      zeroCandidateTokens: tokens.slice(),
      bestScore: null,
      runnerUpScore: null,
      routeMargin: null
    }
  };
  const tsMs = Number.isFinite(entry.ts) ? entry.ts : Date.now();
  if (GEO_SCORE_DEBUG && inference.diagnostics && (Date.now() - lastGeoscoreDiagLogMs) >= 60000) {
    lastGeoscoreDiagLogMs = Date.now();
    const diag = inference.diagnostics;
    const counts = (inference.candidateMetadata || []).map((meta) => Number.isFinite(meta.count) ? meta.count : 0);
    const sortedCounts = counts.length ? [...counts].sort((a, b) => a - b) : [];
    const countMin = sortedCounts.length ? sortedCounts[0] : 0;
    const countMax = sortedCounts.length ? sortedCounts[sortedCounts.length - 1] : 0;
    const countMedian = sortedCounts.length
      ? sortedCounts[Math.floor(sortedCounts.length / 2)]
      : 0;
    const zeroCount = counts.filter((value) => value === 0).length;
    const hopConf = inference.hopConfidences || [];
    const hopMin = hopConf.length ? Math.min(...hopConf) : 0;
    const hopAvg = hopConf.length
      ? hopConf.reduce((sum, value) => sum + value, 0) / hopConf.length
      : 0;
    const reason = diag.routeConfidence < ROUTE_CONF_MIN
      ? "routeConfidence"
      : hopMin < HOP_CONF_MIN ? "hopConfidence" : "none";
    const tokenPreview = tokens.slice(0, 10).join(",");
    console.log(
      `(geoscore) diag msg=${entry.msgKey} tokens=${tokens.length}[${tokenPreview}] counts=${countMin}/${countMedian}/${countMax} zero=${zeroCount} best=${diag.bestScore} runnerUp=${diag.runnerUpScore} margin=${diag.routeMargin} conf=${diag.routeConfidence?.toFixed(3) || "null"} hopMin=${hopMin.toFixed(3)} hopAvg=${hopAvg.toFixed(3)} reason=${reason}`
    );
  }
  return {
    msgKey: entry.msgKey,
    ts: new Date(tsMs).toISOString(),
    tsMs,
    observerId: entry.observerId,
    pathTokens: tokens,
    inferredPubs: inference.inferredPubs,
    hopConfidences: inference.hopConfidences,
    routeConfidence: inference.routeConfidence,
    unresolved: inference.unresolved,
    candidatesJson: inference.candidatesJson,
    teleportMaxKm: inference.teleportMaxKm
  };
}

function enqueueGeoscoreRoute(entry) {
  geoscoreQueue.push(entry);
  if (!geoscoreProcessing) processGeoscoreQueue();
}

function processGeoscoreQueue() {
  if (geoscoreProcessing) return;
  geoscoreProcessing = true;
  setImmediate(() => {
    try {
      const batch = geoscoreQueue.splice(0, GEO_SCORE_BATCH_SIZE);
      if (!batch.length) return;
      const db = getDb();
      const stmt = ensureGeoscoreRouteStmt(db);
      for (const entry of batch) {
        const payload = buildGeoscoreRoutePayload(entry);
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
      }
    } catch (err) {
      console.error("(geoscore) queue error", err);
    } finally {
      geoscoreProcessing = false;
      if (geoscoreQueue.length) processGeoscoreQueue();
    }
  });
}

async function refreshGeoscoreObserverProfiles() {
  const observersResult = loadJsonFromPaths([observersPath, observersPathAlt], { byId: {} });
  const devicesResult = loadJsonFromPaths([devicesPath, devicesPathAlt], { byPub: {} });
  const observersData = observersResult.data;
  const devicesData = devicesResult.data;
  const db = getDb();
  const stmt = ensureGeoscoreObserverStmt(db);
  const now = new Date().toISOString();
  const updated = new Map();
  let gpsKnown = 0;
  const repeaterHashMap = buildRepeaterPubsByHash(devicesData.byPub || {});
  for (const [id, info] of Object.entries(observersData.byId || {})) {
    const observerId = String(id || "").toUpperCase();
    if (!observerId) continue;
    const bestPub = String(info?.bestRepeaterPub || "").toUpperCase();
    const derived = bestPub ? null : deriveLinkedRepeaterForObserver(observerId, db, repeaterHashMap);
    const finalPub = bestPub || derived?.pub;
    if (!finalPub) continue;
    let home = null;
    let source = bestPub ? GEO_SCORE_OBSERVER_SOURCE : derived?.source || "derived:message_observers";
    const device = devicesData.byPub ? devicesData.byPub[finalPub] : null;
    if (device?.gps && isValidGps(device.gps.lat, device.gps.lon) && !device.gpsImplausible && !device.gpsFlagged && !device.hiddenOnMap) {
      home = normalizeGpsValue(device.gps);
      source = `${source}->devices`;
    } else if (info?.gps && isValidGps(info.gps.lat, info.gps.lon)) {
      home = normalizeGpsValue(info.gps);
      source = source || "observers.json:gps";
    }
    if (home) gpsKnown += 1;
    stmt.run(
      observerId,
      finalPub,
      home?.lat ?? null,
      home?.lon ?? null,
      source,
      now
    );
    updated.set(observerId, {
      observer_id: observerId,
      linked_repeater_pub: finalPub,
      home_lat: home?.lat ?? null,
      home_lon: home?.lon ?? null,
      source,
      updated_at: now
    });
  }
  geoscoreObserverProfilesCache = updated;
  console.log(`(geoscore) loaded observers from ${observersResult.path}, devices from ${devicesResult.path}`);
  console.log(`(geoscore) profiles: ${updated.size}, gps known: ${gpsKnown}`);
  return { count: updated.size, gpsKnown };
}

function readGeoscoreObserverProfiles() {
  const db = getDb();
  const rows = db.prepare("SELECT observer_id, linked_repeater_pub, home_lat, home_lon, source, updated_at FROM geoscore_observer_profiles").all();
  return rows;
}

function readIngestMetrics(db) {
  if (!db) return {};
  const rows = db.prepare(`
    SELECT key, value
    FROM ingest_metrics
    WHERE key IN (?, ?)
  `).all("countAdvertsSeenLast10m", "lastAdvertSeenAtIso");
  const metrics = {};
  rows.forEach((row) => {
    if (row?.key) metrics[String(row.key)] = row.value;
  });
  return metrics;
}

function readGeoscoreStatus() {
  const db = getDb();
  const counts = db.prepare(`
    SELECT
      (SELECT COUNT(1) FROM geoscore_observer_profiles) AS observer_profiles,
      (SELECT COUNT(1) FROM geoscore_routes) AS routes,
      (SELECT COUNT(1) FROM geoscore_edges) AS edges,
      (SELECT COUNT(1) FROM geoscore_metrics_daily) AS metrics
  `).get();
  const latestModel = db.prepare("SELECT model_version FROM geoscore_model_versions ORDER BY created_at DESC LIMIT 1").get();
  const latestMetric = db.prepare("SELECT day_yyyymmdd FROM geoscore_metrics_daily ORDER BY day_yyyymmdd DESC LIMIT 1").get();
  const lastRoute = db.prepare("SELECT ts, ts_ms FROM geoscore_routes ORDER BY ts_ms DESC LIMIT 1").get();
  const tenMinutesAgoMs = Date.now() - 10 * 60 * 1000;
  const recentCount = db.prepare("SELECT COUNT(1) AS c, SUM(CASE WHEN unresolved=1 THEN 1 ELSE 0 END) AS unresolved FROM geoscore_routes WHERE ts_ms >= ?").get(tenMinutesAgoMs);
  const resolvedRecent = db.prepare("SELECT COUNT(1) AS c FROM geoscore_routes WHERE ts_ms >= ? AND unresolved=0").get(tenMinutesAgoMs);
  const teleportsLast10m = db.prepare("SELECT COUNT(1) AS c FROM geoscore_routes WHERE ts_ms >= ? AND teleport_max_km > 330").get(tenMinutesAgoMs);
  const avgConfRow = db.prepare("SELECT AVG(route_confidence) AS avg_conf FROM geoscore_routes WHERE ts_ms >= ?").get(tenMinutesAgoMs);
  const resolvedTotal = db.prepare("SELECT COUNT(1) AS c FROM geoscore_routes WHERE unresolved=0").get();
  const messageObserversLast10mRow = db.prepare("SELECT COUNT(1) AS c FROM message_observers WHERE ts_ms >= ?").get(tenMinutesAgoMs);
  const repeatersTotalRow = db.prepare("SELECT COUNT(1) AS c FROM devices WHERE is_repeater=1").get();
  const repeaters3dRow = db.prepare("SELECT COUNT(1) AS c FROM devices WHERE is_repeater=1 AND last_advert_heard_ms >= ?").get(Date.now() - (3 * 24 * 60 * 60 * 1000));
  const ingestMetrics = readIngestMetrics(db);
  const dbInfo = dbInfoCache || {
    resolvedPath: path.resolve(dbPath),
    cwd: process.cwd(),
    databaseList: db.pragma("database_list") || [],
    mainFile: db.pragma("database_list")?.[0]?.file || null
  };
  const unresolvedRate = recentCount?.c ? Number(((recentCount.unresolved / recentCount.c) * 100).toFixed(1)) : 0;
  const resolvedRate = recentCount?.c
    ? Number(((resolvedRecent?.c || 0) / recentCount.c * 100).toFixed(1))
    : 0;
  const avgRouteConfidence = avgConfRow?.avg_conf ? Number(avgConfRow.avg_conf.toFixed(3)) : 0;
  const queryResults = {
    messageObserversLast10m: messageObserversLast10mRow?.c || 0,
    repeatersTotal: repeatersTotalRow?.c || 0,
    repeatersWithin3d: repeaters3dRow?.c || 0
  };
  return {
    observerProfiles: counts.observer_profiles,
    routes: counts.routes,
    edges: counts.edges,
    metrics: counts.metrics,
    latestModel: latestModel?.model_version || null,
    latestMetricDay: latestMetric?.day_yyyymmdd || null,
    dbInfo,
    queryResults,
    ingestMetrics,
    lastRouteTs: lastRoute?.ts || null,
    lastRouteMs: lastRoute?.ts_ms || null,
    totalRoutes: counts.routes,
    resolvedRoutes: resolvedTotal?.c || 0,
    routesLast10mCount: recentCount?.c || 0,
    unresolvedRateLast10m: unresolvedRate,
    resolvedRateLast10m: resolvedRate,
    avgRouteConfidenceLast10m: avgRouteConfidence,
    teleportsLast10mCount: teleportsLast10m?.c || 0
  };
}

function readGeoscoreDiagnostics() {
  const db = getDb();
  const rows = db.prepare("SELECT candidates_json FROM geoscore_routes WHERE candidates_json IS NOT NULL ORDER BY ts_ms DESC LIMIT 50").all();
  let totalTokens = 0;
  let totalCandidates = 0;
  let zeroTokens = 0;
  const zeroFreq = new Map();
  rows.forEach((row) => {
    const metadata = safeParseCandidates(row.candidates_json);
    metadata.forEach((entry) => {
      const count = Number.isFinite(entry.count) ? entry.count : 0;
      totalTokens += 1;
      totalCandidates += count;
      if (!count) {
        zeroTokens += 1;
        zeroFreq.set(entry.token, (zeroFreq.get(entry.token) || 0) + 1);
      }
    });
  });
  const avgCandidatesPerToken = totalTokens ? Number((totalCandidates / totalTokens).toFixed(2)) : 0;
  const pctTokensZeroCandidates = totalTokens ? Number(((zeroTokens / totalTokens) * 100).toFixed(1)) : 0;
  const zeroTokenList = Array.from(zeroFreq.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([token, count]) => ({ token, count }));
  const observerStats = db.prepare(`
    SELECT COUNT(1) AS total,
           SUM(CASE WHEN home_lat IS NOT NULL AND home_lon IS NOT NULL THEN 1 ELSE 0 END) AS with_home
    FROM geoscore_observer_profiles
  `).get();
  const pctObserversWithHomeCoords = observerStats.total
    ? Number(((observerStats.with_home || 0) / observerStats.total * 100).toFixed(1))
    : 0;
  return {
    avgCandidatesPerToken,
    pctTokensZeroCandidates,
    pctObserversWithHomeCoords,
    top10MostCommonTokensWithZeroCandidates: zeroTokenList
  };
}

function sanitizeRepeaterPub(val) {
  const pub = String(val || "").trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(pub)) return "";
  return pub;
}

function getRouteSuggestions() {
  const data = readJsonSafe(routeSuggestionsPath, { byCode: {}, updatedAt: null });
  if (!data.byCode || typeof data.byCode !== "object") data.byCode = {};
  return data;
}

function calcSuggestionScores(suggestions, devices) {
  const byPub = (devices && devices.byPub) ? devices.byPub : {};
  const out = {};
  for (const [code, entry] of Object.entries(suggestions.byCode || {})) {
    const candidates = entry?.candidates || {};
    let best = null;
    for (const [pub, cand] of Object.entries(candidates)) {
      const votes = Number(cand.votes || 0);
      const unique = cand.voters ? Object.keys(cand.voters).length : 0;
      const dist = Number.isFinite(cand.minDistanceKm) ? cand.minDistanceKm : null;
      const distanceScore = dist === null ? 0 : Math.max(0, 50 - dist) / 10;
      const score = votes * 1.5 + unique * 2 + distanceScore;
      const confidence = Math.round((score / (score + 6)) * 100);
      const accepted = (votes >= 3 && unique >= 2) || (votes >= 2 && dist !== null && dist <= 50);
      const candOut = {
        pub,
        name: cand.name || byPub[pub]?.name || null,
        votes,
        uniqueVoters: unique,
        minDistanceKm: dist,
        score,
        confidence,
        accepted
      };
      if (!best || candOut.score > best.score) best = candOut;
    }
    if (best) out[code] = best;
  }
  return out;
}

function minDistanceKmFromPath(candidateGps, pathGps) {
  if (!candidateGps || !Number.isFinite(candidateGps.lat) || !Number.isFinite(candidateGps.lon)) return null;
  if (!Array.isArray(pathGps) || !pathGps.length) return null;
  let best = null;
  for (const gps of pathGps) {
    if (!gps || !Number.isFinite(gps.lat) || !Number.isFinite(gps.lon)) continue;
    const d = haversineKm(candidateGps.lat, candidateGps.lon, gps.lat, gps.lon);
    if (!Number.isFinite(d)) continue;
    if (best === null || d < best) best = d;
  }
  return best;
}

async function tailLines(filePath, limit) {
  if (!fs.existsSync(filePath)) return [];
  const buf = [];
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    buf.push(t);
    if (buf.length > limit) buf.shift();
  }
  return buf;
}

async function buildObserverRank() {
  const observers = readObservers();
  const byId = observers.byId || {};
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const repeatersByPub = new Map();
  for (const d of Object.values(byPub)) {
    if (!d?.gps || !Number.isFinite(d.gps.lat) || !Number.isFinite(d.gps.lon)) continue;
    repeatersByPub.set(String(d.pub || d.publicKey || d.pubKey || "").toUpperCase(), {
      gps: d.gps,
      name: d.name || null
    });
  }

  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;
  const windowHours = windowMs / 3600000;
  const rankWindowMs = windowMs * 3;
  const stats = new Map();

  for (const entry of Object.values(byId)) {
    if (!entry?.id) continue;
    stats.set(entry.id, {
      id: entry.id,
      name: entry.name || entry.id,
      firstSeen: entry.firstSeen || null,
      lastSeen: entry.lastSeen || null,
      gps: entry.gps || null,
      locSource: entry.locSource || null,
      bestRepeaterPub: entry.bestRepeaterPub || null,
      packetsToday: 0,
      repeaters: new Set()
    });
  }

  if (fs.existsSync(observerPath)) {
    const rl = readline.createInterface({
      input: fs.createReadStream(observerPath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      const t = line.trim();
      if (!t) continue;
      let rec;
      try { rec = JSON.parse(t); } catch { continue; }
      const id = String(rec.observerId || rec.observerName || "observer").trim();
      if (!stats.has(id)) {
        stats.set(id, {
          id,
          name: rec.observerName || id,
          firstSeen: rec.archivedAt || null,
          lastSeen: rec.archivedAt || null,
          gps: rec.gps || null,
          packetsToday: 0,
          repeaters: new Set(),
          bestRepeaterPub: null,
          bestRepeaterRssi: null
        });
      }
      const s = stats.get(id);
      const ts = parseIso(rec.archivedAt);
      if (ts) {
        if (!s.firstSeen || ts < new Date(s.firstSeen)) s.firstSeen = ts.toISOString();
        if (!s.lastSeen || ts > new Date(s.lastSeen)) s.lastSeen = ts.toISOString();
        if (now - ts.getTime() <= windowMs) s.packetsToday += 1;
      }
      if (!s.gps && rec.gps) s.gps = rec.gps;

      const hex = getHex(rec);
      if (!hex) continue;
      let decoded;
      try {
        decoded = MeshCoreDecoder.decode(String(hex).toUpperCase());
      } catch {
        continue;
      }
      const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
      if (payloadType !== "Advert") continue;
      const path = Array.isArray(decoded?.path) ? decoded.path.map(normalizePathHash) : [];
      const hopCount = path.length || (Number.isFinite(decoded?.pathLength) ? decoded.pathLength : 0);
      if (hopCount > 1) continue;
      const adv = decoded.payload?.decoded || decoded.decoded || decoded.payload || null;
      const pub = adv?.publicKey || adv?.pub || adv?.pubKey || null;
      if (!pub) continue;
      const rpt = repeatersByPub.get(String(pub).toUpperCase());
      if (rpt?.gps) s.repeaters.add(String(pub).toUpperCase());
      if (Number.isFinite(rec.rssi)) {
        if (!Number.isFinite(s.bestRepeaterRssi) || rec.rssi > s.bestRepeaterRssi) {
          s.bestRepeaterRssi = rec.rssi;
          s.bestRepeaterPub = String(pub).toUpperCase();
        }
      }
    }
  }

  function scoreFor(o) {
    const uptimeScore = clamp(o.uptimeHours / 48, 0, 1);
    const trafficScore = clamp(o.packetsToday / 2000, 0, 1);
    return Math.round((uptimeScore * 0.6 + trafficScore * 0.4) * 100);
  }

  function colorForAge(ageHours) {
    if (ageHours <= 1) return "#34c759";
    if (ageHours <= 6) return "#ff9500";
    if (ageHours <= 48) return "#ff3b30";
    return "#8e8e93";
  }

  const items = [];
    for (const s of stats.values()) {
      const lastSeen = s.lastSeen ? new Date(s.lastSeen).getTime() : 0;
      const firstSeen = s.firstSeen ? new Date(s.firstSeen).getTime() : 0;
      const ageHours = lastSeen ? (now - lastSeen) / 3600000 : 999;
      if (ageHours > 48) continue;
      const rawUptimeHours = firstSeen ? (now - firstSeen) / 3600000 : 0;
      const offline = ageHours > OBSERVER_OFFLINE_HOURS;
      const uptimeHours = offline ? 0 : rawUptimeHours;
    let gps = s.gps && Number.isFinite(s.gps.lat) && Number.isFinite(s.gps.lon) ? s.gps : null;
    if (gps && gps.lat === 0 && gps.lon === 0) gps = null;
    let locSource = s.locSource;
    if (!gps && s.bestRepeaterPub && repeatersByPub.has(String(s.bestRepeaterPub).toUpperCase())) {
      const rptGps = repeatersByPub.get(String(s.bestRepeaterPub).toUpperCase())?.gps || null;
      if (rptGps && Number.isFinite(rptGps.lat) && Number.isFinite(rptGps.lon) && !(rptGps.lat === 0 && rptGps.lon === 0)) {
        gps = rptGps;
      }
      locSource = locSource || s.bestRepeaterPub;
    }
    let coverageKm = 0;
    let nearestRepeaterName = null;
    let nearestRepeaterKm = null;
    let coverageCount = 0;
    if (gps && s.repeaters.size) {
      let maxKm = 0;
      for (const pub of s.repeaters) {
        const rpt = repeatersByPub.get(pub);
        const rptGps = rpt?.gps;
        if (!rptGps || !Number.isFinite(rptGps.lat) || !Number.isFinite(rptGps.lon)) continue;
        if (rptGps.lat === 0 && rptGps.lon === 0) continue;
        const km = haversineKm(gps.lat, gps.lon, rptGps.lat, rptGps.lon);
        if (km > OBSERVER_MAX_REPEATER_KM) continue;
        coverageCount += 1;
        if (km > maxKm) maxKm = km;
      }
      coverageKm = Math.round(maxKm * 10) / 10;
    }
    if (gps) {
      let bestKm = Infinity;
      let bestName = null;
      for (const rpt of repeatersByPub.values()) {
        if (!rpt?.gps || !Number.isFinite(rpt.gps.lat) || !Number.isFinite(rpt.gps.lon)) continue;
        if (rpt.gps.lat === 0 && rpt.gps.lon === 0) continue;
        const km = haversineKm(gps.lat, gps.lon, rpt.gps.lat, rpt.gps.lon);
        if (km > OBSERVER_MAX_REPEATER_KM) continue;
        if (km < bestKm) {
          bestKm = km;
          bestName = rpt.name || null;
        }
      }
      if (Number.isFinite(bestKm) && bestName) {
        nearestRepeaterName = bestName;
        nearestRepeaterKm = Math.round(bestKm * 10) / 10;
      }
    }
    const score = scoreFor({ uptimeHours, packetsToday: s.packetsToday });
    const scoreColor = colorForAge(ageHours);
    const isStale = ageHours > OBSERVER_OFFLINE_HOURS;
    const lowPacketRate = Number.isFinite(ageHours)
      ? ageHours > (OBSERVER_LOW_PACKET_MINUTES / 60) && ageHours <= OBSERVER_OFFLINE_HOURS
      : false;
    const lastSeenAgoMinutes = Number.isFinite(ageHours) ? Math.round(ageHours * 60) : null;
    items.push({
      id: s.id,
      name: s.name || s.id,
      gps,
      lastSeen: s.lastSeen,
      lastSeenAgoMinutes,
      firstSeen: s.firstSeen,
      ageHours,
      stale: isStale,
      lowPacketRate,
      uptimeHours,
      packetsToday: s.packetsToday,
      coverageKm,
      coverageCount,
      nearestRepeaterName,
      nearestRepeaterKm,
      locSource,
      score,
      scoreColor,
      offline
    });
  }

  items.sort((a, b) => (a.offline === b.offline ? 0 : a.offline ? 1 : -1) || (b.score - a.score) || (b.packetsToday - a.packetsToday));
  return { updatedAt: new Date().toISOString(), items };
}

async function buildObserverDebug(observerId) {
  const id = String(observerId || "").trim();
  if (!id) return { ok: false, error: "observerId required" };
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const seen = new Map();
  if (!fs.existsSync(observerPath)) return { ok: true, observerId: id, items: [] };
  const tail = await tailLines(observerPath, OBSERVER_DEBUG_TAIL_LINES);
  for (const line of tail) {
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    if (String(rec.observerId || "") !== id) continue;
    const hex = getHex(rec);
    if (!hex) continue;
    let decoded;
    try {
      decoded = MeshCoreDecoder.decode(String(hex).toUpperCase());
    } catch {
      continue;
    }
    const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
    if (payloadType !== "Advert") continue;
    const adv = decoded.payload?.decoded || decoded.decoded || decoded.payload || null;
    const pub = adv?.publicKey || adv?.pub || adv?.pubKey || null;
    if (!pub) continue;
    const key = String(pub).toUpperCase();
    if (!seen.has(key)) {
      const dev = byPub[key] || null;
      seen.set(key, {
        pub: key,
        name: dev?.name || adv?.name || null,
        hasGps: !!(dev?.gps && Number.isFinite(dev.gps.lat) && Number.isFinite(dev.gps.lon) && !(dev.gps.lat === 0 && dev.gps.lon === 0)),
        gps: (dev?.gps && !(dev.gps.lat === 0 && dev.gps.lon === 0)) ? dev.gps : null,
        count: 0
      });
    }
    const entry = seen.get(key);
    entry.count += 1;
  }
  const items = Array.from(seen.values()).sort((a, b) => b.count - a.count);
  return { ok: true, observerId: id, items };
}

async function buildMessageDebug(hash) {
  const key = String(hash || "").trim().toUpperCase();
  if (!key) return { ok: false, error: "hash required" };
  const observerMap = await getObserverHitsMap();
  const fromIndex = Array.from(observerMap.get(key) || []);
  const fromLog = new Set();
  if (fs.existsSync(observerPath)) {
    const tail = await tailLines(observerPath, MESSAGE_DEBUG_TAIL_LINES);
    for (const line of tail) {
      let rec;
      try { rec = JSON.parse(line); } catch { continue; }
      const observerId = rec.observerId || rec.observerName || null;
      if (!observerId) continue;
      const frameKey = rec.frameHash ? String(rec.frameHash).toUpperCase() : null;
      const hashKey = rec.hash ? String(rec.hash).toUpperCase() : null;
      const msgKey = rec.messageHash ? String(rec.messageHash).toUpperCase() : null;
      const payloadKey = rec.payloadHex ? sha256Hex(String(rec.payloadHex).toUpperCase()) : null;
      if (frameKey === key || hashKey === key || msgKey === key || payloadKey === key) {
        fromLog.add(observerId);
      }
    }
  }
  return {
    ok: true,
    hash: key,
    fromIndex,
    fromLog: Array.from(fromLog)
  };
}

async function buildMessageRouteHistory(hash, hours) {
  const key = String(hash || "").trim().toUpperCase();
  if (!key) return { ok: false, error: "hash required" };
  const hoursNum = Number.isFinite(Number(hours)) ? Math.max(1, Math.min(168, Number(hours))) : 24;
  const cacheKey = `${key}|${hoursNum}`;
  const cached = messageRouteCache.get(cacheKey);
  if (cached && (Date.now() - cached.builtAt) < MESSAGE_ROUTE_CACHE_MS) {
    return cached.payload;
  }

  const sourcePath = getRfSourcePath();
  if (!fs.existsSync(sourcePath)) {
    const payload = { ok: true, hash: key, hours: hoursNum, total: 0, routes: [], stop: null };
    messageRouteCache.set(cacheKey, { builtAt: Date.now(), payload });
    return payload;
  }

  const keyStore = buildKeyStore(loadKeys());
  const nodeMap = buildNodeHashMap();
  const cutoffMs = Date.now() - (hoursNum * 3600000);
  let baseNow = Date.now();
  try {
    const stat = fs.statSync(sourcePath);
    if (Number.isFinite(stat.mtimeMs)) baseNow = stat.mtimeMs;
  } catch {}

  let maxNumericTs = null;
  {
    const rl = readline.createInterface({
      input: fs.createReadStream(sourcePath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });
    for await (const line of rl) {
      try {
        const rec = JSON.parse(line);
        if (Number.isFinite(rec.ts)) {
          if (maxNumericTs === null || rec.ts > maxNumericTs) maxNumericTs = rec.ts;
        }
      } catch {}
    }
  }

  const routes = new Map();
  const stopCounts = new Map();
  let total = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(sourcePath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    const ts = resolveRecordTimestamp(rec, baseNow, maxNumericTs);
    if (!ts) continue;
    const tsMs = new Date(ts).getTime();
    if (!Number.isFinite(tsMs) || tsMs < cutoffMs) continue;

    const hex = getHex(rec);
    if (!hex) continue;

    let decoded;
    try {
      decoded = MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
    } catch {
      continue;
    }
    const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
    if (payloadType !== "GroupText") continue;
    const payload = decoded.payload?.decoded;
    if (!payload || !payload.decrypted) continue;

    const msgHash = String(
      decoded.messageHash ||
      rec.frameHash ||
      sha256Hex(hex) ||
      hex.slice(0, 16) ||
      "unknown"
    ).toUpperCase();
    const messageHash = decoded.messageHash ? String(decoded.messageHash).toUpperCase() : null;
    const frameHash = (rec.frameHash ? String(rec.frameHash) : sha256Hex(hex) || "").toUpperCase();
    if (key !== msgHash && key !== messageHash && key !== frameHash) continue;

    total += 1;
    const path = Array.isArray(decoded.path) ? decoded.path.map(normalizePathHash) : [];
    const hopCount = path.length || (Number.isFinite(decoded.pathLength) ? decoded.pathLength : 0);
    const pathPoints = path.map((h) => {
      const hit = nodeMap.get(h);
      return {
        hash: h,
        name: hit ? hit.name : h,
        gps: hit?.gps || null
      };
    });
    const pathNames = pathPoints.map((p) => p.name);
    const routeKey = path.length ? path.join(">") : "direct";
    const existing = routes.get(routeKey) || {
      key: routeKey,
      path,
      pathNames,
      hopCount,
      count: 0,
      lastSeen: ts
    };
    existing.count += 1;
    if (Number.isFinite(hopCount) && hopCount > (existing.hopCount || 0)) {
      existing.hopCount = hopCount;
      existing.path = path;
      existing.pathNames = pathNames;
    }
    if (!existing.lastSeen || new Date(ts) > new Date(existing.lastSeen)) {
      existing.lastSeen = ts;
    }
    routes.set(routeKey, existing);

    const stopName = pathNames.length ? pathNames[pathNames.length - 1] : "Direct";
    stopCounts.set(stopName, (stopCounts.get(stopName) || 0) + 1);
  }

  const routesList = Array.from(routes.values())
    .sort((a, b) => (b.count - a.count) || (new Date(b.lastSeen || 0) - new Date(a.lastSeen || 0)));

  let topStop = null;
  for (const [name, count] of stopCounts.entries()) {
    if (!topStop || count > topStop.count) topStop = { name, count };
  }

  const payload = {
    ok: true,
    hash: key,
    hours: hoursNum,
    total,
    routes: routesList,
    stop: topStop
  };
  messageRouteCache.set(cacheKey, { builtAt: Date.now(), payload });
  return payload;
}

async function buildConfidenceHistory(sender, channel, hours, limit) {
  const db = getDb();
  if (!hasMessagesDb(db)) return { ok: false, error: "messages db unavailable" };
  const senderName = String(sender || "").trim();
  if (!senderName) return { ok: false, error: "sender required" };
  const channelName = String(channel || "").trim();
  const hoursNum = Number.isFinite(Number(hours)) ? Math.max(1, Math.min(168, Number(hours))) : 168;
  const maxRows = Number.isFinite(Number(limit))
    ? Math.max(50, Math.min(CONFIDENCE_HISTORY_MAX_ROWS, Number(limit)))
    : CONFIDENCE_HISTORY_MAX_ROWS;
  const cutoff = new Date(Date.now() - hoursNum * 3600000).toISOString();
  const nodeMap = buildNodeHashMap();
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const flaggedHashes = new Set();
  for (const d of Object.values(byPub)) {
    if (!d?.isRepeater) continue;
    if (!d.gpsImplausible && !d.hiddenOnMap) continue;
    const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || null;
    const hash = nodeHashFromPub(pub);
    if (hash) flaggedHashes.add(String(hash).toUpperCase());
  }
  const observers = readObservers();
  const observerById = observers.byId || {};
  const destinationRepeaterHashes = new Set();
  const observerLinkedRepeaterHashes = new Set();
  const repeaters = Object.values(byPub).filter((d) =>
    d && d.isRepeater && d.gps && Number.isFinite(d.gps.lat) && Number.isFinite(d.gps.lon) && !(d.gps.lat === 0 && d.gps.lon === 0)
  );
  for (const obs of Object.values(observerById)) {
    const gps = obs?.gps;
    if (!gps || !Number.isFinite(gps.lat) || !Number.isFinite(gps.lon)) continue;
    if (gps.lat === 0 && gps.lon === 0) continue;
    for (const rpt of repeaters) {
      const km = haversineKm(gps.lat, gps.lon, rpt.gps.lat, rpt.gps.lon);
      if (km === 0) {
        const pub = rpt.pub || rpt.publicKey || rpt.pubKey || rpt.raw?.lastAdvert?.publicKey || null;
        const hash = nodeHashFromPub(pub);
        if (hash) destinationRepeaterHashes.add(hash);
      }
    }
  }
  if (fs.existsSync(observerPath)) {
    const tail = await tailLines(observerPath, OBSERVER_DEBUG_TAIL_LINES);
    for (const line of tail) {
      let rec;
      try { rec = JSON.parse(line); } catch { continue; }
      const hex = getHex(rec);
      if (!hex) continue;
      let decoded;
      try {
        decoded = MeshCoreDecoder.decode(String(hex).toUpperCase());
      } catch {
        continue;
      }
      const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
      if (payloadType !== "Advert") continue;
      const path = Array.isArray(decoded?.path) ? decoded.path.map(normalizePathHash) : [];
      const hopCount = path.length || (Number.isFinite(decoded?.pathLength) ? decoded.pathLength : 0);
      if (hopCount > 1) continue;
      const adv = decoded.payload?.decoded || decoded.decoded || decoded.payload || null;
      const pub = adv?.publicKey || adv?.pub || adv?.pubKey || null;
      if (!pub) continue;
      const entry = byPub[String(pub).toUpperCase()];
      if (!entry?.isRepeater) continue;
      const hash = nodeHashFromPub(pub);
      if (hash) observerLinkedRepeaterHashes.add(String(hash).toUpperCase());
    }
  }
  let rows = [];
  if (channelName) {
    rows = db.prepare(`
      SELECT message_hash, ts, path_json, path_text, path_length
      FROM messages
      WHERE sender = ? AND channel_name = ? AND ts >= ?
      ORDER BY ts DESC
      LIMIT ?
    `).all(senderName, channelName, cutoff, maxRows);
  } else {
    rows = db.prepare(`
      SELECT message_hash, ts, path_json, path_text, path_length
      FROM messages
      WHERE sender = ? AND ts >= ?
      ORDER BY ts DESC
      LIMIT ?
    `).all(senderName, cutoff, maxRows);
  }

  const observedMessages = new Set();
  if (rows.length) {
    const hashes = rows.map((r) => String(r.message_hash || "").toUpperCase()).filter(Boolean);
    if (hashes.length) {
      const placeholders = hashes.map(() => "?").join(", ");
      const seen = db.prepare(`
        SELECT DISTINCT message_hash
        FROM message_observers
        WHERE message_hash IN (${placeholders})
      `).all(...hashes);
      seen.forEach((row) => {
        if (row?.message_hash) observedMessages.add(String(row.message_hash).toUpperCase());
      });
    }
  }

  const paths = [];
  const deadEnds = new Map();
  rows.forEach((row) => {
    let parsed;
    if (row.path_text) {
      parsed = String(row.path_text)
        .split("|")
        .map(normalizePathHash)
        .filter(Boolean);
    } else if (row?.path_json) {
      try {
        parsed = JSON.parse(row.path_json);
      } catch {
        parsed = null;
      }
    }
    if (!Array.isArray(parsed) || !parsed.length) return;
    const path = parsed.map(normalizePathHash).filter(Boolean);
    if (!path.length) return;
    const pathPoints = path.map((code) => {
      if (flaggedHashes.has(String(code).toUpperCase())) return null;
      const hit = nodeMap.get(code);
      if (!hit?.gps || !Number.isFinite(hit.gps.lat) || !Number.isFinite(hit.gps.lon)) return null;
      if (hit.gps.lat === 0 && hit.gps.lon === 0) return null;
      return {
        hash: code,
        name: hit.name || code,
        lat: hit.gps.lat,
        lon: hit.gps.lon
      };
    }).filter(Boolean);
    if (pathPoints.length >= 2) {
      paths.push({
        ts: row.ts,
        hops: Number.isFinite(row.path_length) ? row.path_length : path.length,
        pathPoints,
        observed: observedMessages.has(String(row.message_hash || "").toUpperCase())
      });
    }
    const lastCode = path[path.length - 1];
    if (lastCode) {
      if (flaggedHashes.has(String(lastCode).toUpperCase())) return;
      if (observerLinkedRepeaterHashes.has(String(lastCode).toUpperCase())) return;
      if (destinationRepeaterHashes.has(String(lastCode).toUpperCase())) return;
      const hit = nodeMap.get(lastCode);
      const gps = hit?.gps || null;
      if (gps && Number.isFinite(gps.lat) && Number.isFinite(gps.lon) && gps.lat === 0 && gps.lon === 0) return;
      if (!gps || !Number.isFinite(gps.lat) || !Number.isFinite(gps.lon)) return;
      const label = hit?.name || lastCode;
      const key = String(lastCode).toUpperCase();
      const entry = deadEnds.get(key) || {
        hash: key,
        name: label,
        gps,
        count: 0
      };
      entry.count += 1;
      deadEnds.set(key, entry);
    }
  });

  paths.sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));
  const deadList = Array.from(deadEnds.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, CONFIDENCE_DEAD_END_LIMIT);

  return {
    ok: true,
    sender: senderName,
    channel: channelName || null,
    hours: hoursNum,
    total: rows.length,
    paths,
    deadEnds: deadList
  };
}

async function buildRepeaterRank() {
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const nodeMap = buildNodeHashMap();
  const isRepeaterPub = (pub) => {
    if (!pub) return false;
    const entry = byPub[String(pub).toUpperCase()];
    return !!entry?.isRepeater;
  };
  const repeatersByHash = new Map(); // hash -> [pub]
  const repeaterCandidatesByHash = new Map(); // hash -> [{pub, name, gps}]
  for (const d of Object.values(byPub)) {
    if (!d?.isRepeater) continue;
    const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || null;
    if (!pub) continue;
    const hash = nodeHashFromPub(pub);
    if (!hash) continue;
    if (!repeatersByHash.has(hash)) repeatersByHash.set(hash, []);
    repeatersByHash.get(hash).push(pub);
    if (
      d.gps && Number.isFinite(d.gps.lat) && Number.isFinite(d.gps.lon) &&
      !d.hiddenOnMap && !d.gpsImplausible && !d.gpsFlagged
    ) {
      if (!repeaterCandidatesByHash.has(hash)) repeaterCandidatesByHash.set(hash, []);
      repeaterCandidatesByHash.get(hash).push({
        hash,
        pub: String(pub).toUpperCase(),
        name: d.name || d.raw?.lastAdvert?.appData?.name || hash,
        gps: d.gps
      });
    }
  }

  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;
  const windowHours = windowMs / 3600000;
  const rankWindowMs = windowMs * 3;

  const stats = new Map(); // pub -> {total24h, msgCounts: Map, rssi: [], snr: [], bestRssi, bestSnr, zeroHopNeighbors, neighborRssi, clockDriftMs}
  const ensureRepeaterStat = (pub) => {
    if (!stats.has(pub)) {
      stats.set(pub, {
        total24h: 0,
        msgCounts: new Map(),
        lastSeenTs: null,
        zeroHopNeighbors: new Set(),
        neighborRssi: new Map(),
        clockDriftMs: null
      });
    }
    return stats.get(pub);
  };
  const addPathNeighbors = (path, rssi) => {
    if (!Array.isArray(path) || path.length < 2) return;
    for (let i = 0; i < path.length; i += 1) {
      const hash = path[i];
      const pubs = repeatersByHash.get(hash);
      if (!pubs || pubs.length === 0) continue;
      const neighbors = [];
      if (i > 0 && path[i - 1]) neighbors.push(path[i - 1]);
      if (i + 1 < path.length && path[i + 1]) neighbors.push(path[i + 1]);
      if (neighbors.length === 0) continue;
      for (const pub of pubs) {
        const s = ensureRepeaterStat(pub);
        for (const neighbor of neighbors) {
          if (Number.isFinite(rssi)) {
            const entry = s.neighborRssi.get(neighbor) || { sum: 0, count: 0, max: -999 };
            entry.sum += rssi;
            entry.count += 1;
            if (rssi > entry.max) entry.max = rssi;
            s.neighborRssi.set(neighbor, entry);
          }
        }
      }
    }
  };

  const NEIGHBOR_RADIUS_KM = 200;
  const NEIGHBOR_CLUSTER_RADIUS_KM = 60;
  const GREEN_RSSI_MIN = -75;
  const getStatsForPub = (pubKey) => {
    if (!pubKey) return null;
    const key = String(pubKey).toUpperCase();
    return stats.get(key) || stats.get(pubKey) || null;
  };
  const resolveZeroHopNeighbors = (targetGps, neighborHashes, targetPub, targetHash, overrides) => {
    if (!targetGps || !Number.isFinite(targetGps.lat) || !Number.isFinite(targetGps.lon)) return [];
    if (!Array.isArray(neighborHashes) || neighborHashes.length === 0) return [];

    const localCandidatesByHash = new Map();
    neighborHashes.forEach((hash) => {
      const candidates = repeaterCandidatesByHash.get(hash) || [];
      const local = candidates.filter((c) => {
        const km = haversineKm(targetGps.lat, targetGps.lon, c.gps.lat, c.gps.lon);
        return Number.isFinite(km) && km <= NEIGHBOR_RADIUS_KM;
      });
      if (local.length) localCandidatesByHash.set(hash, local);
    });

    const hashes = Array.from(new Set(neighborHashes));
    const results = [];
    hashes.forEach((hash) => {
      const overrideKey = targetPub ? `${targetPub}:${hash}` : null;
      const override = overrideKey ? overrides?.[overrideKey] : null;
      if (override?.pub) {
        const options = repeaterCandidatesByHash.get(hash) || [];
        const match = options.find((opt) => opt.pub === override.pub);
        if (match) {
          results.push({ hash, ...match, override: true, mutual: true, relation: "reciprocal" });
          return;
        }
      }
      const candidates = localCandidatesByHash.get(hash) || [];
      if (!candidates.length) return;
      const mutualCandidates = targetHash
        ? candidates.filter((cand) => {
          const candStat = getStatsForPub(cand.pub);
          if (!candStat?.zeroHopNeighbors) return false;
          return candStat.zeroHopNeighbors.has(targetHash);
        })
        : [];
      const pool = mutualCandidates.length ? mutualCandidates : candidates;
      const mutualFlag = mutualCandidates.length > 0;
      if (pool.length === 1) {
        results.push({ hash, ...pool[0], mutual: mutualFlag, relation: mutualFlag ? "reciprocal" : "handoff" });
        return;
      }
      let best = null;
      let bestDist = Infinity;
      let bestCluster = -Infinity;
      pool.forEach((cand) => {
        const dist = haversineKm(targetGps.lat, targetGps.lon, cand.gps.lat, cand.gps.lon);
        if (!Number.isFinite(dist)) return;
        const clusterCount = (candidates || []).reduce((sum, other) => {
          if (!other?.gps) return sum;
          const d = haversineKm(cand.gps.lat, cand.gps.lon, other.gps.lat, other.gps.lon);
          if (Number.isFinite(d) && d <= NEIGHBOR_CLUSTER_RADIUS_KM) return sum + 1;
          return sum;
        }, 0);
        if (clusterCount > bestCluster || (clusterCount === bestCluster && dist < bestDist)) {
          bestCluster = clusterCount;
          bestDist = dist;
          best = { hash, ...cand, mutual: mutualFlag, relation: mutualFlag ? "reciprocal" : "handoff" };
        }
      });
      if (best) results.push(best);
    });

    return results;
  };

  if (fs.existsSync(decodedPath)) {
    const rl = readline.createInterface({
      input: fs.createReadStream(decodedPath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      const t = line.trim();
      if (!t) continue;
      let rec;
      try { rec = JSON.parse(t); } catch { continue; }
      const decoded = rec.decoded || null;
      const payloadTypeName = rec.payloadType || (decoded ? Utils.getPayloadTypeName(decoded.payloadType) : null);
      const ts = parseIso(rec.ts);
      if (!ts) continue;
      if (now - ts.getTime() > windowMs) continue;
      const path = Array.isArray(decoded?.path) ? decoded.path.map(normalizePathHash).filter(Boolean) : [];
      addPathNeighbors(path, rec.rssi);
      if (payloadTypeName !== "Advert") continue;
        const adv = decoded?.payload?.decoded || decoded?.decoded || decoded;
        if (!adv || typeof adv !== "object") continue;
        const pub = adv.publicKey || adv.pub || adv.pubKey;
        if (!pub) continue;
        if (!isRepeaterPub(pub)) continue;
        const advTimestamp = Number.isFinite(adv.timestamp) ? Number(adv.timestamp) : null;
        const driftMs = Number.isFinite(advTimestamp)
          ? (advTimestamp < 1e12 ? advTimestamp * 1000 : advTimestamp) - ts.getTime()
          : null;

      const s = ensureRepeaterStat(pub);
      if (Number.isFinite(driftMs)) s.clockDriftMs = driftMs;
      s.total24h += 1;
      if (!s.lastSeenTs || ts > s.lastSeenTs) s.lastSeenTs = ts;
      if (!s.rssi) s.rssi = [];
      if (!s.snr) s.snr = [];
      if (Number.isFinite(rec.rssi)) s.rssi.push(rec.rssi);
      if (Number.isFinite(rec.snr)) s.snr.push(rec.snr);
      if (!Number.isFinite(s.bestRssi) || (Number.isFinite(rec.rssi) && rec.rssi > s.bestRssi)) s.bestRssi = rec.rssi;
      if (!Number.isFinite(s.bestSnr) || (Number.isFinite(rec.snr) && rec.snr > s.bestSnr)) s.bestSnr = rec.snr;
      const hopCount = path.length || (Number.isFinite(decoded?.pathLength) ? decoded.pathLength : 0);
        if (hopCount > 0 && path[0]) s.zeroHopNeighbors.add(path[0]);
      const msgKey = String(rec.messageHash || rec.hash || rec.id || "unknown");
      s.msgCounts.set(msgKey, (s.msgCounts.get(msgKey) || 0) + 1);
    }
  } else if (fs.existsSync(observerPath)) {
    const rl = readline.createInterface({
      input: fs.createReadStream(observerPath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });

    for await (const line of rl) {
      const t = line.trim();
      if (!t) continue;
      let rec;
      try { rec = JSON.parse(t); } catch { continue; }
      const hex = getHex(rec);
      if (!hex) continue;
      let decoded;
      try {
        decoded = MeshCoreDecoder.decode(String(hex).toUpperCase());
      } catch {
        continue;
      }
      const payloadTypeName = Utils.getPayloadTypeName(decoded.payloadType);
      const ts = parseIso(rec.archivedAt);
      if (!ts) continue;
      if (now - ts.getTime() > windowMs) continue;
      const path = Array.isArray(decoded?.path) ? decoded.path.map(normalizePathHash).filter(Boolean) : [];
      addPathNeighbors(path, rec.rssi);
      if (payloadTypeName !== "Advert") continue;
        const adv = decoded?.payload?.decoded || decoded?.decoded || decoded;
        if (!adv || typeof adv !== "object") continue;
        const pub = adv.publicKey || adv.pub || adv.pubKey;
        if (!pub) continue;
        if (!isRepeaterPub(pub)) continue;
        const advTimestamp = Number.isFinite(adv.timestamp) ? Number(adv.timestamp) : null;
        const driftMs = Number.isFinite(advTimestamp)
          ? (advTimestamp < 1e12 ? advTimestamp * 1000 : advTimestamp) - ts.getTime()
          : null;

      const s = ensureRepeaterStat(pub);
      if (Number.isFinite(driftMs)) s.clockDriftMs = driftMs;
      s.total24h += 1;
      if (!s.lastSeenTs || ts > s.lastSeenTs) s.lastSeenTs = ts;
      if (!s.rssi) s.rssi = [];
      if (!s.snr) s.snr = [];
      if (Number.isFinite(rec.rssi)) s.rssi.push(rec.rssi);
      if (Number.isFinite(rec.snr)) s.snr.push(rec.snr);
      if (!Number.isFinite(s.bestRssi) || (Number.isFinite(rec.rssi) && rec.rssi > s.bestRssi)) s.bestRssi = rec.rssi;
      if (!Number.isFinite(s.bestSnr) || (Number.isFinite(rec.snr) && rec.snr > s.bestSnr)) s.bestSnr = rec.snr;
      const hopCount = path.length || (Number.isFinite(decoded?.pathLength) ? decoded.pathLength : 0);
        if (hopCount > 0 && path[0]) s.zeroHopNeighbors.add(path[0]);
      const msgKey = String(rec.frameHash || hex.slice(0, 16) || "unknown");
      s.msgCounts.set(msgKey, (s.msgCounts.get(msgKey) || 0) + 1);
    }
  }

  function trimmedMean(list, trimRatio) {
    if (!Array.isArray(list) || list.length === 0) return null;
    const sorted = [...list].sort((a, b) => a - b);
    const trim = Math.floor(sorted.length * trimRatio);
    const slice = sorted.slice(trim, Math.max(trim + 1, sorted.length - trim));
    const sum = slice.reduce((acc, v) => acc + v, 0);
    return sum / slice.length;
  }

function neighborEstimate(stat, nodeLookup, pubKey) {
  const points = [];
  for (const hash of stat.zeroHopNeighbors || []) {
    const neighbor = nodeLookup.get(hash);
    if (neighbor?.hiddenOnMap || neighbor?.gpsImplausible || neighbor?.gpsFlagged) continue;
    const gps = neighbor?.gps;
      if (!gps || !Number.isFinite(gps.lat) || !Number.isFinite(gps.lon)) continue;
      if (gps.lat === 0 && gps.lon === 0) continue;
      points.push(gps);
    }
    if (!points.length) return null;
    if (points.length >= REPEATER_ESTIMATE_MIN_NEIGHBORS) {
      const avgLat = points.reduce((sum, p) => sum + p.lat, 0) / points.length;
      const avgLon = points.reduce((sum, p) => sum + p.lon, 0) / points.length;
      return { gps: { lat: avgLat, lon: avgLon }, neighbors: points.length, reason: "neighbors_3" };
    }
    if (points.length === 2) {
      return {
        gps: { lat: (points[0].lat + points[1].lat) / 2, lon: (points[0].lon + points[1].lon) / 2 },
        neighbors: 2,
        reason: "neighbors_2"
      };
    }
    const base = points[0];
    const hash = nodeHashFromPub(pubKey) || "00";
    const dir = (parseInt(hash, 16) % 2 === 0) ? 1 : -1;
    const offsetDeg = REPEATER_ESTIMATE_SINGLE_OFFSET_KM / 111;
    return {
      gps: { lat: base.lat + (dir * offsetDeg), lon: base.lon },
      neighbors: 1,
      reason: "neighbors_1"
    };
  }

  let hadEstimateUpdates = false;
  const nowMs = Date.now();
  for (const d of Object.values(byPub)) {
    if (!d?.pub || !d.gpsFlagged || d.gpsEstimated) continue;
    const flaggedAt = d.gpsFlaggedAt ? new Date(d.gpsFlaggedAt).getTime() : 0;
    if (flaggedAt && (nowMs - flaggedAt) < (REPEATER_FLAG_REVIEW_HOURS * 3600000)) continue;
    const s = stats.get(d.pub) || stats.get(d.publicKey) || stats.get(d.pubKey) || null;
    if (!s) continue;
    const estimate = neighborEstimate(s, nodeMap, d.pub);
    if (!estimate?.gps) continue;
    if (!isValidGps(estimate.gps.lat, estimate.gps.lon)) continue;
    d.gps = estimate.gps;
    d.gpsEstimated = true;
    d.gpsEstimateAt = new Date().toISOString();
    d.gpsEstimateNeighbors = estimate.neighbors;
    d.gpsEstimateReason = estimate.reason;
    d.gpsImplausible = true;
    d.hiddenOnMap = false;
    d.locSource = "estimated";
    hadEstimateUpdates = true;
  }
  if (hadEstimateUpdates) {
    devices.byPub = byPub;
    devices.updatedAt = new Date().toISOString();
    writeJsonSafe(devicesPath, devices);
    devicesCache = { readAt: 0, data: null };
  }

  const zeroHopOverrides = readZeroHopOverrides();
  const repeaters3d = buildRepeatersWithinWindow(byPub, rankWindowMs);
  const repeatEvidenceMap = buildRepeatEvidenceMap(REPEAT_EVIDENCE_WINDOW_MS);
  const included = [];
  const excluded = [];
  for (const repeater of repeaters3d.values()) {
    const { pub, entry: d, lastAdvertHeardMs, lastAdvertHeardIso } = repeater;
    const statsEntry = stats.get(pub) || {
      total24h: 0,
      msgCounts: new Map(),
      rssi: [],
      snr: [],
      zeroHopNeighbors: new Set(),
      neighborRssi: new Map(),
      bestRssi: null,
      bestSnr: null,
      clockDriftMs: null
    };
    const lastAdvertAgeHours = Number.isFinite(lastAdvertHeardMs)
      ? (now - lastAdvertHeardMs) / 3600000
      : null;
    const lastSeen = lastAdvertHeardIso || null;
    const isLive = lastAdvertAgeHours !== null && lastAdvertAgeHours <= windowHours;
    const isStale = lastAdvertAgeHours !== null && lastAdvertAgeHours >= windowHours;
    const liveState = isLive ? "live" : "offline";
    const ageHours = lastAdvertAgeHours !== null ? lastAdvertAgeHours : Infinity;
    const classification = classifyRepeaterQuality(d, statsEntry, lastAdvertHeardMs);
    const targetPub = String(pub || "").toUpperCase();
    const targetHash = nodeHashFromPub(pub);
    const evidence = summarizeRepeatEvidence(repeatEvidenceMap.get(targetHash));
    const excludedReasons = [];
    const addExcludedReason = (reason) => {
      if (!reason) return;
      if (excludedReasons.includes(reason)) return;
      excludedReasons.push(reason);
    };
    if (classification.quality !== "valid") {
      addExcludedReason(classification.quality);
      (Array.isArray(classification.reasons) ? classification.reasons : []).forEach(addExcludedReason);
    }
    if (isCompanionDevice(d)) addExcludedReason("companion_node");
    const evidenceOk = evidence.isTrueRepeater || !!d.raw?.meta?.backfilled;
    if (!evidenceOk) {
      addExcludedReason(evidence.repeatEvidenceReason || "insufficient_repeat_evidence");
    }
    if (excludedReasons.length) {
      excluded.push({
        pub: targetPub,
        hashByte: targetHash,
        name: d.name || d.raw?.lastAdvert?.appData?.name || null,
        gps: d.gps || null,
        lastAdvertIngestMs: Number.isFinite(lastAdvertHeardMs) ? lastAdvertHeardMs : null,
        lastAdvertIngestIso: lastAdvertHeardIso || null,
        isLive,
        liveState,
        lastAdvertAgeHours: lastAdvertAgeHours !== null ? Number(lastAdvertAgeHours.toFixed(2)) : null,
        quality: classification.quality,
        qualityReasons: classification.reasons,
        excludedReasons,
        repeatEvidenceMiddleHops24h: evidence.middleCount,
        repeatEvidenceUpstreamDistinct24h: evidence.upstreamCount,
        repeatEvidenceDownstreamDistinct24h: evidence.downstreamCount,
        isTrueRepeater: evidence.isTrueRepeater,
        repeatEvidenceReason: evidence.repeatEvidenceReason
      });
      continue;
    }

    const uniqueMsgs = statsEntry.msgCounts.size || 0;
    const avgRepeats = uniqueMsgs ? (statsEntry.total24h / uniqueMsgs) : 0;
    const neighborHashes = statsEntry.zeroHopNeighbors ? Array.from(statsEntry.zeroHopNeighbors) : [];
    const resolvedNeighbors = resolveZeroHopNeighbors(d.gps, neighborHashes, targetPub, targetHash, zeroHopOverrides);
    const zeroHopNeighborNames = resolvedNeighbors
      .map((n) => n.name || n.hash)
      .filter(Boolean);
    const zeroHopNeighborDetails = resolvedNeighbors.map((n) => {
      const rssiEntry = statsEntry.neighborRssi ? statsEntry.neighborRssi.get(n.hash) : null;
      const avg = rssiEntry && rssiEntry.count ? rssiEntry.sum / rssiEntry.count : null;
      const rssiAvg = Number.isFinite(avg) ? Number(avg.toFixed(1)) : null;
      const rssiMax = rssiEntry && Number.isFinite(rssiEntry.max) ? Number(rssiEntry.max.toFixed(1)) : null;
      const rssiValue = Number.isFinite(rssiAvg) ? rssiAvg : (Number.isFinite(rssiMax) ? rssiMax : null);
      const options = (repeaterCandidatesByHash.get(n.hash) || []).map((opt) => ({
        pub: opt.pub,
        name: opt.name || opt.hash,
        hash: opt.hash
      }));
      return {
        hash: n.hash,
        pub: n.pub,
        name: n.name || n.hash,
        gps: n.gps,
        rssiAvg,
        rssiMax,
        options,
        isGreen: Number.isFinite(rssiValue) && rssiValue >= GREEN_RSSI_MIN,
        mutual: n.mutual !== false,
        override: !!n.override,
        relation: n.relation || (n.mutual ? "reciprocal" : "handoff")
      };
    });
    const zeroHopNeighbors24h = zeroHopNeighborDetails.length;

    const avgRssi = trimmedMean(statsEntry.rssi, 0.1);
    const driftMinutes = Number.isFinite(statsEntry.clockDriftMs) ? Math.round(statsEntry.clockDriftMs / 60000) : null;
    const avgSnr = trimmedMean(statsEntry.snr, 0.1);
    const bestRssi = Number.isFinite(statsEntry.bestRssi) ? statsEntry.bestRssi : (Number.isFinite(d.stats?.bestRssi) ? d.stats.bestRssi : -120);
    const bestSnr = Number.isFinite(statsEntry.bestSnr) ? statsEntry.bestSnr : (Number.isFinite(d.stats?.bestSnr) ? d.stats.bestSnr : -20);

    const rssiBase = Number.isFinite(avgRssi) ? avgRssi : bestRssi;
    const snrBase = Number.isFinite(avgSnr) ? avgSnr : bestSnr;
    const rssiScore = clamp((rssiBase + 120) / 70, 0, 1);
    const snrScore = clamp((snrBase + 20) / 30, 0, 1);
    const bestRssiScore = clamp((bestRssi + 120) / 70, 0, 1);
    const bestSnrScore = clamp((bestSnr + 20) / 30, 0, 1);
    const throughputScore = clamp(statsEntry.total24h / 50, 0, 1);
    const repeatScore = clamp(avgRepeats / 5, 0, 1);
    const meshNeighborScore = clamp(zeroHopNeighbors24h / 5, 0, 1);

    let score = 100 * clamp(
      0.30 * rssiScore +
      0.10 * snrScore +
      0.10 * bestRssiScore +
      0.05 * bestSnrScore +
      0.25 * throughputScore +
      0.10 * repeatScore +
      0.10 * meshNeighborScore,
      0, 1
    );

    if (isStale) score = 0;

    let color = "#ff3b30";
    if (!isStale) {
      if (score >= 70) color = "#34c759";
      else if (score >= 45) color = "#ffcc00";
      else color = "#ff9500";
    }

    const effectiveGps = d.gps || null;

    included.push({
      pub,
      hashByte: targetHash,
      name: d.name || d.raw?.lastAdvert?.appData?.name || "Unknown",
      gps: effectiveGps,
      lastSeen,
      lastAdvertIngestMs: Number.isFinite(lastAdvertHeardMs) ? lastAdvertHeardMs : null,
      lastAdvertIngestIso: lastAdvertHeardIso || null,
      lastAdvertAgeHours: lastAdvertAgeHours !== null ? Number(lastAdvertAgeHours.toFixed(2)) : null,
      isLive,
      liveState,
      quality: classification.quality,
      qualityReason: classification.reasons,
      isObserver: !!d.isObserver,
      bestRssi,
      bestSnr,
      avgRssi: Number.isFinite(avgRssi) ? Number(avgRssi.toFixed(2)) : null,
      avgSnr: Number.isFinite(avgSnr) ? Number(avgSnr.toFixed(2)) : null,
      total24h: statsEntry.total24h,
      zeroHopNeighbors24h,
      zeroHopNeighborNames,
      zeroHopNeighborDetails,
      uniqueMsgs,
      avgRepeats: Number(avgRepeats.toFixed(2)),
      score: Math.round(score),
      stale: isStale,
      color,
      hiddenOnMap: !!d.hiddenOnMap,
      gpsImplausible: !!d.gpsImplausible,
      gpsFlagged: !!d.gpsFlagged,
      gpsEstimated: !!d.gpsEstimated,
      gpsEstimateReason: d.gpsEstimateReason || null,
      gpsEstimateNeighbors: d.gpsEstimateNeighbors || null,
      clockDriftMinutes: driftMinutes,
      repeatEvidenceMiddleHops24h: evidence.middleCount,
      repeatEvidenceUpstreamDistinct24h: evidence.upstreamCount,
      repeatEvidenceDownstreamDistinct24h: evidence.downstreamCount,
      isTrueRepeater: evidence.isTrueRepeater,
      repeatEvidenceReason: evidence.repeatEvidenceReason,
      excludedReasons: []
    });
  }

  const deduped = [];
  const seenByName = new Map();
  included.forEach((item) => {
    const key = String(item.name || item.pub || "").trim().toLowerCase() || item.pub;
    const existing = seenByName.get(key);
    if (!existing) {
      seenByName.set(key, item);
      deduped.push(item);
      return;
    }
    const better = chooseBetterRepeater(item, existing);
    if (better === item) {
      seenByName.set(key, item);
      const idx = deduped.indexOf(existing);
      if (idx >= 0) deduped[idx] = item;
    }
  });
  const items = deduped.sort((a, b) => (b.score - a.score) || (b.total24h - a.total24h));

  return {
    updatedAt: new Date().toISOString(),
    count: items.length,
    items,
    excluded
  };
}

async function buildNodeRank() {
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const all = Object.values(byPub).filter(Boolean);
  const companions = all.filter((d) => {
    if (!d || d.isRepeater) return false;
    const role = String(d.role || "").toLowerCase();
    if (role === "room_server" || role === "chat" || role === "repeater") return false;
    const roleName = String(d.appFlags?.roleName || "").toLowerCase();
    if (roleName === "room_server" || roleName === "chat" || roleName === "repeater") return false;
    if (role === "companion" || roleName === "companion") return true;
    const roleCode = Number.isFinite(d.appFlags?.roleCode) ? d.appFlags.roleCode : null;
    if (roleCode === 0) return true;
    return false;
  });
  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;
  const stats = new Map(); // pub -> {messages24h, msgCounts, lastMsgTs}
  const sourcePath = getRfSourcePath();
  const keyCfg = loadKeys();
  const keyStore = buildKeyStore(keyCfg);

  if (fs.existsSync(sourcePath)) {
    const tail = await tailLines(sourcePath, NODE_RANK_TAIL_LINES);
    for (const line of tail) {
      let rec;
      try { rec = JSON.parse(line); } catch { continue; }
      let decoded = rec.decoded || null;
      let payloadTypeName = rec.payloadType || (decoded ? Utils.getPayloadTypeName(decoded.payloadType) : null);
      if (!payloadTypeName) {
        const hex = getHex(rec);
        if (!hex) continue;
        try {
          decoded = MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
          payloadTypeName = Utils.getPayloadTypeName(decoded.payloadType);
        } catch {
          continue;
        }
      }
      if (payloadTypeName !== "GroupText") continue;
      const senderPublicKey = extractSenderPublicKey(decoded);
      if (!senderPublicKey) continue;
      const ts = parseIso(rec.ts || rec.archivedAt);
      if (!ts) continue;
      if (now - ts.getTime() > windowMs) continue;

      const pubKey = String(senderPublicKey).toUpperCase();
      const hex = getHex(rec);
      const hash = decoded?.messageHash
        ? String(decoded.messageHash).toUpperCase()
        : (rec.frameHash ? String(rec.frameHash).toUpperCase() : (sha256Hex(hex || "") || (hex ? hex.slice(0, 16) : "unknown")).toUpperCase());

      if (!stats.has(pubKey)) {
        stats.set(pubKey, { messages24h: 0, msgCounts: new Map(), lastMsgTs: null });
      }
      const s = stats.get(pubKey);
      s.messages24h += 1;
      s.msgCounts.set(hash, (s.msgCounts.get(hash) || 0) + 1);
      if (!s.lastMsgTs || ts > s.lastMsgTs) s.lastMsgTs = ts;
    }
  }

  const items = companions.map((d) => {
    const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey;
    const pubKey = pub ? String(pub).toUpperCase() : null;
    const s = pubKey && stats.has(pubKey) ? stats.get(pubKey) : { messages24h: 0, msgCounts: new Map(), lastMsgTs: null };
    const lastSeen = d.lastSeen || null;
    const lastSeenDate = parseIso(lastSeen);
    const ageHours = lastSeenDate ? (now - lastSeenDate.getTime()) / 3600000 : Infinity;
    if (ageHours > 24 * 7) return null;
    const messages24h = s.messages24h || 0;
    const uniqueMessages24h = s.msgCounts.size || 0;
    const activityScore = Number.isFinite(ageHours) ? clamp((24 - ageHours) / 24, 0, 1) : 0;
    const messageScore = clamp(messages24h / 200, 0, 1);
    const uniqueScore = clamp(uniqueMessages24h / 50, 0, 1);
    let score = 100 * clamp(0.5 * messageScore + 0.3 * uniqueScore + 0.2 * activityScore, 0, 1);
    if (!Number.isFinite(score)) score = 0;
    const color = ageHours <= 1 ? "#34c759" : (ageHours <= 6 ? "#ff9500" : "#ff3b30");

    return {
      pub: pubKey,
      name: d.name || d.raw?.lastAdvert?.appData?.name || "Unknown",
      model: d.model || d.raw?.lastAdvert?.appData?.model || null,
      role: d.role || "companion",
      gps: d.gps || null,
      lastSeen,
      ageHours,
      messages24h,
      uniqueMessages24h,
      lastMessageAt: s.lastMsgTs ? s.lastMsgTs.toISOString() : null,
      score: Math.round(score),
      scoreColor: color
    };
  }).filter(Boolean)
    .sort((a, b) => (b.score - a.score) || (b.messages24h - a.messages24h));

  return {
    updatedAt: new Date().toISOString(),
    count: items.length,
    items
  };
}

async function refreshRankCache(force) {
  const now = Date.now();
  const last = rankCache.updatedAt ? new Date(rankCache.updatedAt).getTime() : 0;
  if (!force && last && now - last < RANK_REFRESH_MS) return rankCache;
  if (rankRefreshInFlight) return rankRefreshInFlight;
  rankRefreshInFlight = (async () => {
    const data = await buildRepeaterRank();
    rankCache = { ...data, cachedAt: new Date().toISOString() };
    updateRankSummary(data.items || [], rankCache.updatedAt);
    persistRepeaterRankCache(rankCache);
    return rankCache;
  })();
  try {
    return await rankRefreshInFlight;
  } finally {
    rankRefreshInFlight = null;
  }
}

async function scheduleAutoRefresh() {
  setTimeout(async () => {
    try {
      await refreshRankCache(false);
      await refreshMeshScoreCache(false);
      await refreshObserverRankCache(false);
    } catch {}
    scheduleAutoRefresh().catch(() => {});
  }, AUTO_REFRESH_MS);
}

async function refreshMeshScoreCache(force) {
  const now = Date.now();
  const last = meshScoreCache.updatedAt ? new Date(meshScoreCache.updatedAt).getTime() : 0;
  if (!force && meshScoreCache.payload && last && (now - last) < MESH_REFRESH_MS) {
    return meshScoreCache.payload;
  }
  if (meshScoreRefresh) return meshScoreRefresh;
  meshScoreRefresh = (async () => {
    const payload = await buildMeshScore();
    meshScoreCache = { updatedAt: payload.updatedAt, payload };
    persistMeshScoreCache(payload);
    return payload;
  })();
  try {
    return await meshScoreRefresh;
  } finally {
    meshScoreRefresh = null;
  }
}

async function refreshObserverRankCache(force) {
  const now = Date.now();
  const last = observerRankCache.updatedAt ? new Date(observerRankCache.updatedAt).getTime() : 0;
  if (!force && observerRankCache.items?.length && last && (now - last) < OBSERVER_RANK_REFRESH_MS) {
    return observerRankCache;
  }
  if (observerRankRefresh) return observerRankRefresh;
  observerRankRefresh = buildObserverRank()
    .then((data) => {
      observerRankCache = data;
      persistObserverRankCache(data);
      return observerRankCache;
    })
    .finally(() => { observerRankRefresh = null; });
  return observerRankRefresh;
}

async function buildChannelMessages() {
  const db = getDb();
  if (hasMessagesDb(db)) {
    const countRow = db.prepare("SELECT COUNT(1) as count FROM messages").get();
    if ((countRow?.count || 0) > 0) {
      const nodeMap = buildNodeHashMap();
      const observerHitsMap = await getObserverHitsMap();
      const latestRows = db.prepare(`
        SELECT m.channel_name, m.sender, m.body, m.ts
        FROM messages m
        JOIN (
          SELECT channel_name, MAX(ts) AS max_ts
          FROM messages
          GROUP BY channel_name
        ) x
        ON m.channel_name = x.channel_name AND m.ts = x.max_ts
      `).all();
      const channels = latestRows
        .filter((row) => row.channel_name)
        .sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0))
        .map((row) => ({
          id: String(row.channel_name || "").replace(/^#/, ""),
          name: row.channel_name,
          snippet: String(row.body || "").slice(0, 48) || "No recent messages",
          time: row.ts ? new Date(row.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "--"
        }));

      const messages = [];
      for (const row of latestRows) {
        const limit = channelHistoryLimit(row.channel_name);
        const rows = readMessagesFromDb(row.channel_name, limit, null) || [];
        const hashes = rows.map((r) => String(r.message_hash || "").toUpperCase()).filter(Boolean);
        const observerAggMap = readMessageObserverAgg(db, hashes);
        const observerPathsMap = readMessageObserverPaths(db, hashes);
        const mapped = rows.map((r) => mapMessageRow(r, nodeMap, observerHitsMap, observerAggMap, observerPathsMap)).reverse();
        mapped.forEach((msg) => messages.push(msg));
      }
      return { channels, messages };
    }
  }

  const sourcePath = getRfSourcePath();
  let stat = null;
  try {
    stat = fs.statSync(sourcePath);
  } catch {
    return { channels: [], messages: [] };
  }
  const now = Date.now();
  if (channelMessagesCache.payload && (now - channelMessagesCache.builtAt) < CHANNEL_CACHE_MIN_MS) {
    return channelMessagesCache.payload;
  }
  if (channelMessagesCache.payload && channelMessagesCache.mtimeMs === stat.mtimeMs && channelMessagesCache.size === stat.size) {
    return channelMessagesCache.payload;
  }
  if (channelMessagesInFlight) {
    if (channelMessagesCache.payload && (now - channelMessagesCache.builtAt) < CHANNEL_CACHE_STALE_MS) {
      return channelMessagesCache.payload;
    }
    return channelMessagesInFlight;
  }
  channelMessagesInFlight = (async () => {
  const channelMap = new Map(); // name -> {name, lastTs, snippet}
  const messagesMap = new Map(); // key -> msg
  const keyCfg = loadKeys();
  const nodeMap = buildNodeHashMap();
  const observerMap = new Map(); // msgHash -> Set(observerName)
  const keyMap = {};
  for (const ch of keyCfg.channels || []) {
    const hb = typeof ch.hashByte === "string" ? ch.hashByte.toUpperCase() : null;
    const nm = typeof ch.name === "string" ? ch.name : null;
    if (hb && nm) keyMap[hb] = nm;
  }

  for (const ch of keyCfg.channels || []) {
    if (typeof ch.name === "string") {
      channelMap.set(ch.name, { name: ch.name, lastTs: null, snippet: "" });
    }
  }

  const keyStore = buildKeyStore(keyCfg);
  const tail = await tailLines(sourcePath, CHANNEL_TAIL_LINES);
  if (!tail.length) {
    const payload = { channels: Array.from(channelMap.values()), messages: [] };
    channelMessagesCache = { mtimeMs: stat.mtimeMs, size: stat.size, payload, builtAt: Date.now() };
    return payload;
  }
  const cachedObserverMap = await getObserverHitsMap();
  cachedObserverMap.forEach((set, key) => {
    observerMap.set(key, set);
  });
  let baseNow = Date.now();
  try {
    const stat = fs.statSync(sourcePath);
    if (Number.isFinite(stat.mtimeMs)) baseNow = stat.mtimeMs;
  } catch {}
  let maxNumericTs = null;
  for (const line of tail) {
    try {
      const rec = JSON.parse(line);
      if (Number.isFinite(rec.ts)) {
        if (maxNumericTs === null || rec.ts > maxNumericTs) maxNumericTs = rec.ts;
      }
    } catch {}
  }

  for (const line of tail) {
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    const hex = getHex(rec);
    if (!hex) continue;

    let decoded;
    try {
      decoded = MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
    } catch {
      continue;
    }

    const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
    if (payloadType !== "GroupText") continue;
    const payload = decoded.payload?.decoded;
    if (!payload || !payload.decrypted) continue;

    const chHash = typeof payload.channelHash === "string" ? payload.channelHash.toUpperCase() : null;
    const chName = chHash && keyMap[chHash] ? keyMap[chHash] : null;
    if (!chName) continue;

      const msgHash = String(
        decoded.messageHash ||
        rec.frameHash ||
        sha256Hex(hex) ||
        hex.slice(0, 16) ||
        "unknown"
      ).toUpperCase();
      const messageHash = decoded.messageHash ? String(decoded.messageHash).toUpperCase() : null;
      const frameHash = (rec.frameHash ? String(rec.frameHash) : sha256Hex(hex) || "").toUpperCase();
      const observerSet = new Set();
      let directObserver = rec.observerId || "";
      if (!directObserver && rec.topic) {
        const m = String(rec.topic).match(/observers\/([^/]+)\//i);
        if (m) directObserver = m[1];
      }
      if (!directObserver) directObserver = rec.observerName || "";
      directObserver = String(directObserver).trim();
      if (directObserver) observerSet.add(directObserver);
      const hits = observerMap.get(msgHash);
      if (hits) hits.forEach((o) => observerSet.add(o));
      if (frameHash && frameHash !== msgHash) {
        const frameHits = observerMap.get(frameHash);
        if (frameHits) frameHits.forEach((o) => observerSet.add(o));
      }
      if (messageHash && messageHash !== msgHash) {
        const msgHits = observerMap.get(messageHash);
        if (msgHits) msgHits.forEach((o) => observerSet.add(o));
      }
      const observerHits = Array.from(observerSet);
      const observerCount = observerHits.length;
      const msgKey = chName + "|" + msgHash;
      const repeatCount = Math.max(hopCount, observerCount || 0);
    const body = String(payload.decrypted.message || "");
    const sender = String(payload.decrypted.sender || "unknown");
    let ts = null;
    if (typeof rec.archivedAt === "string" && parseIso(rec.archivedAt)) {
      ts = rec.archivedAt;
    } else if (typeof rec.ts === "string" && parseIso(rec.ts)) {
      ts = rec.ts;
    } else if (Number.isFinite(rec.ts) && maxNumericTs !== null) {
      const approx = baseNow - (maxNumericTs - rec.ts);
      ts = new Date(approx).toISOString();
    }
    if (!ts) continue;

    const path = Array.isArray(decoded.path) ? decoded.path.map(normalizePathHash) : [];
    const hopCount = path.length || (Number.isFinite(decoded.pathLength) ? decoded.pathLength : 0);
    const pathPoints = path.map((h) => {
      const hit = nodeMap.get(h);
      return {
        hash: h,
        name: hit ? hit.name : "#unknown",
        gps: hit?.gps || null
      };
    });
    const pathNames = pathPoints.map((p) => p.name);

    if (!messagesMap.has(msgKey)) {
        messagesMap.set(msgKey, {
          id: msgHash,
          frameHash,
          messageHash,
          channelName: chName,
          sender,
          body,
          ts,
          repeats: repeatCount,
          path,
          pathNames,
          pathPoints,
          observerHits,
          observerCount
        });
      } else {
        const m = messagesMap.get(msgKey);
        m.repeats = Math.max(m.repeats || 0, repeatCount);
        if (ts && (!m.ts || new Date(ts) > new Date(m.ts))) m.ts = ts;
        if (path.length > (m.path?.length || 0)) {
          m.path = path;
          m.pathNames = pathNames;
          m.pathPoints = pathPoints;
        }
        const mergedHits = new Set([...(m.observerHits || []), ...observerHits]);
        m.observerHits = Array.from(mergedHits);
        m.observerCount = m.observerHits.length;
      }

    const ch = channelMap.get(chName) || { name: chName, lastTs: null, snippet: "" };
    if (ts && (!ch.lastTs || new Date(ts) > new Date(ch.lastTs))) {
      ch.lastTs = ts;
      ch.snippet = body.slice(0, 48);
    }
    channelMap.set(chName, ch);
  }

  const channels = Array.from(channelMap.values())
    .sort((a, b) => new Date(b.lastTs || 0) - new Date(a.lastTs || 0))
    .map((c) => ({
      id: c.name.replace(/^#/, ""),
      name: c.name,
      snippet: c.snippet || "No recent messages",
      time: c.lastTs ? new Date(c.lastTs).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "--"
    }));

  const messages = Array.from(messagesMap.values())
    .sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));

  const payload = { channels, messages };
  channelMessagesCache = { mtimeMs: stat.mtimeMs, size: stat.size, payload, builtAt: Date.now() };
  return payload;
  })();
  try {
    if (channelMessagesCache.payload && (now - channelMessagesCache.builtAt) < CHANNEL_CACHE_STALE_MS) {
      return channelMessagesCache.payload;
    }
    return await channelMessagesInFlight;
  } finally {
    channelMessagesInFlight = null;
  }
}

async function buildChannelSummary() {
  const db = getDb();
  if (hasMessagesDb(db)) {
    const countRow = db.prepare("SELECT COUNT(1) as count FROM messages").get();
    if ((countRow?.count || 0) > 0) {
      const latestRows = db.prepare(`
        SELECT m.channel_name, m.body, m.ts
        FROM messages m
        JOIN (
          SELECT channel_name, MAX(ts) AS max_ts
          FROM messages
          GROUP BY channel_name
        ) x
        ON m.channel_name = x.channel_name AND m.ts = x.max_ts
      `).all();
      return latestRows
        .filter((row) => row.channel_name)
        .sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0))
        .map((row) => ({
          id: String(row.channel_name || "").replace(/^#/, ""),
          name: row.channel_name,
          snippet: String(row.body || "").slice(0, 48) || "No recent messages",
          time: row.ts ? new Date(row.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "--"
        }));
    }
  }
  const payload = await buildChannelMessages();
  return payload.channels || [];
}

function normalizeChannelName(name) {
  const raw = String(name || "").trim();
  if (!raw) return null;
  return raw.startsWith("#") ? raw : `#${raw}`;
}

function normalizeChannelId(name) {
  const raw = String(name || "").trim();
  if (!raw) return "";
  return raw.replace(/^#/, "").toLowerCase();
}

function loadBlockedChannels(db) {
  const rows = db.prepare("SELECT name FROM channel_blocks").all();
  const set = new Set();
  rows.forEach((row) => {
    const name = normalizeChannelName(row.name);
    if (name) set.add(name);
  });
  return set;
}

function isChannelBlocked(name, blockedSet) {
  if (!name) return false;
  const normalized = normalizeChannelName(name);
  return !!(normalized && blockedSet?.has(normalized));
}

function recordPacketEvents(count) {
  if (!Number.isFinite(count) || count <= 0) return;
  const now = Date.now();
  packetTimeline.push({ ts: now, count });
  const cutoff = now - PACKET_TREND_WINDOW_MS;
  while (packetTimeline.length && packetTimeline[0].ts < cutoff) {
    packetTimeline.shift();
  }
}

function getRecentPacketRate(durationMs = PACKET_RATE_DURATION_MS) {
  const now = Date.now();
  const cutoff = now - durationMs;
  let sum = 0;
  for (const entry of packetTimeline) {
    if (entry.ts >= cutoff) sum += entry.count;
  }
  const rate = durationMs ? (sum * 60000) / durationMs : sum;
  return Math.round(rate);
}

function getDailyMeshTrendPayload() {
  const now = Date.now();
  if (meshTrendCache.payload && (now - meshTrendCache.updatedAt) < MESH_TREND_CACHE_MS) {
    return meshTrendCache.payload;
  }
  const db = getDb();
  let payload = { today: null, yesterday: null, diff: {} };
  try {
    const rows = db.prepare(`
      SELECT day, score, messages, avg_repeats
      FROM meshscore_daily
      ORDER BY day DESC
      LIMIT 2
    `).all();
    const today = rows[0] || null;
    const yesterday = rows[1] || null;
    const diff = {
      score: Number.isFinite(today?.score) && Number.isFinite(yesterday?.score)
        ? Number((today.score - yesterday.score).toFixed(1))
        : null,
      messages: Number.isFinite(today?.messages) && Number.isFinite(yesterday?.messages)
        ? today.messages - yesterday.messages
        : null,
      avgRepeats: Number.isFinite(today?.avg_repeats) && Number.isFinite(yesterday?.avg_repeats)
        ? Number((today.avg_repeats - yesterday.avg_repeats).toFixed(2))
        : null
    };
    payload = { today, yesterday, diff };
  } catch {}
  meshTrendCache = { payload, updatedAt: now };
  return payload;
}

async function buildMeshHeatPayload() {
  const rank = rankCache.items || [];
  const repeaters = rank
    .filter((item) => item.gps && Number.isFinite(item.gps.lat) && Number.isFinite(item.gps.lon))
    .slice(0, 220)
    .map((item) => ({
      pub: item.pub,
      hash: item.hashByte || null,
      name: item.name,
      lat: item.gps.lat,
      lon: item.gps.lon,
      score: item.score,
      total24h: item.total24h,
      avgRssi: item.avgRssi,
      bestRssi: item.bestRssi,
      lastSeen: item.lastSeen,
      zeroHopNeighbors: item.zeroHopNeighbors24h,
      color: item.color
    }));
  const packetRate = getRecentPacketRate();
  const daily = getDailyMeshTrendPayload();
  return {
    ts: Date.now(),
    repeaters,
    packetRate,
    daily
  };
}

const NEW_CHANNEL_WINDOW_MS = 5 * 24 * 60 * 60 * 1000;
const CHANNEL_GROUPS = [
  "General Chat",
  "Regional",
  "Specialist",
  "Clubs & Societies",
  "Testing",
  "Ham Radio",
  "Comedy",
  "Sports",
  "Music",
  "Other"
];

const FIXED_CHANNELS = ["#public", "#meshranksuggestions"];

function resetUserChannelsToFixed(db) {
  const marker = path.join(dataDir, "channels_default_v3.flag");
  if (fs.existsSync(marker)) return;
  const lowered = FIXED_CHANNELS.map((ch) => ch.toLowerCase());
  const placeholders = lowered.map(() => "?").join(",");
  db.prepare(`DELETE FROM user_channels WHERE lower(channel_name) NOT IN (${placeholders})`).run(...lowered);
  fs.writeFileSync(marker, new Date().toISOString());
}

function ensureDefaultUserChannels(userId) {
  if (!userId) return;
  const db = getDb();
  const existing = db.prepare("SELECT channel_name FROM user_channels WHERE user_id = ? LIMIT 1").get(userId);
  if (existing) return;
  const now = new Date().toISOString();
  db.prepare("INSERT OR IGNORE INTO user_channels (user_id, channel_name, created_at) VALUES (?, ?, ?)")
    .run(userId, "#public", now);
  db.prepare("INSERT OR IGNORE INTO user_channels (user_id, channel_name, created_at) VALUES (?, ?, ?)")
    .run(userId, "#meshranksuggestions", now);
}

function getUserChannels(userId) {
  if (!userId) return [];
  const db = getDb();
  const rows = db.prepare("SELECT channel_name FROM user_channels WHERE user_id = ? ORDER BY channel_name ASC").all(userId);
  return rows.map((row) => row.channel_name).filter(Boolean);
}

async function buildUserChannelSummary(userId) {
  ensureDefaultUserChannels(userId);
  const userChannels = new Set(getUserChannels(userId));
  const channels = await buildChannelSummary();
  const filtered = channels.filter((ch) => userChannels.has(normalizeChannelName(ch.name)));
  return filtered;
}

function buildDefaultChannelGroups() {
  return {
    "General Chat": [
      { name: "#chat", emoji: "💬", description: "" },
      { name: "#hashtags", emoji: "🏷️", description: "" }
    ],
    Regional: [
      { name: "#yorkshire", emoji: "📍", description: "" },
      { name: "#northwest", emoji: "📍", description: "" },
      { name: "#london", emoji: "📍", description: "" },
      { name: "#reddirch-hams", emoji: "📍", description: "" },
      { name: "#eastofengland", emoji: "📍", description: "" },
      { name: "#isleofwight", emoji: "📍", description: "" },
      { name: "#morley", emoji: "📍", description: "" },
      { name: "#leeds", emoji: "📍", description: "" },
      { name: "#walesandwest-chat", emoji: "📍", description: "" }
    ],
    Specialist: [],
    "Clubs & Societies": [
      { name: "#oarc", emoji: "🎯", description: "" }
    ],
    Specialist: [],
    Testing: [
      { name: "#test", emoji: "🧪", description: "" },
      { name: "#m3sh", emoji: "🧪", description: "" }
    ],
    "Ham Radio": [
      { name: "#hamradio", emoji: "📻", description: "" },
      { name: "#hubnet", emoji: "🛰️", description: "" }
    ],
    Comedy: [
      { name: "#jokes", emoji: "😂", description: "" }
    ],
    Sports: [
      { name: "#football", emoji: "⚽", description: "" },
      { name: "#lufc", emoji: "⚽", description: "" }
    ],
    Music: [
      { name: "#midi", emoji: "🎵", description: "" }
    ],
    Other: []
  };
}

function loadChannelCatalog() {
  const db = getDb();
  const rows = db.prepare(`
    SELECT name, code, emoji, group_name, allow_popular, created_at
    FROM channels_catalog
    ORDER BY group_name, name
  `).all();
  return rows.map((row) => ({
    name: row.name,
    code: row.code,
    emoji: row.emoji,
    group: row.group_name,
    allowPopular: row.allow_popular !== 0,
    createdAt: row.created_at
  }));
}

async function buildChannelDirectoryGroups(blockedSet = new Set(), includeBlocked = false) {
  const defaults = buildDefaultChannelGroups();
  const keys = loadKeys();
  const keyMap = new Map((keys.channels || []).map((row) => [normalizeChannelName(row.name), row.secretHex || row.hashByte || ""]));
  const catalog = loadChannelCatalog();
  const counts24h = getChannelCounts24h();
  const grouped = {};
  const groupFlags = {};
  CHANNEL_GROUPS.forEach((g) => {
    grouped[g] = [];
    groupFlags[g] = { newChannelCount: 0 };
  });
  const now = Date.now();
  const markNew = (group, createdAt) => {
    if (!group || !createdAt) return;
    const ts = Date.parse(createdAt);
    if (Number.isNaN(ts)) return;
    if ((now - ts) <= NEW_CHANNEL_WINDOW_MS) {
      if (!groupFlags[group]) groupFlags[group] = { newChannelCount: 0 };
      groupFlags[group].newChannelCount += 1;
    }
  };
  Object.entries(defaults).forEach(([group, list]) => {
    grouped[group] = (grouped[group] || []).concat(list.map((item) => ({
      ...item,
      code: item.code || keyMap.get(normalizeChannelName(item.name)) || "",
      allowPopular: true,
      blocked: isChannelBlocked(item.name, blockedSet),
      messageCount24h: counts24h[normalizeChannelId(item.name)] || 0
    })));
  });

  const fixedChannels = new Set(["#public", "#meshranksuggestions"]);
  const summary = await buildChannelSummary();
  summary.forEach((ch) => {
    const name = normalizeChannelName(ch.name || "");
    if (!name || fixedChannels.has(name)) return;
    if (!includeBlocked && isChannelBlocked(name, blockedSet)) return;
    const listed = Object.values(defaults).some((list) =>
      list.some((item) => normalizeChannelName(item.name) === name)
    );
    if (listed) return;
    if (!grouped.Other) grouped.Other = [];
    const exists = grouped.Other.some((item) => normalizeChannelName(item.name) === name);
    if (!exists) {
      grouped.Other.push({
        name,
        emoji: "💬",
        code: keyMap.get(name) || "",
        description: "",
        allowPopular: true,
        blocked: isChannelBlocked(name, blockedSet),
        messageCount24h: counts24h[normalizeChannelId(name)] || 0
      });
    }
  });
  catalog.forEach((entry) => {
    if (fixedChannels.has(normalizeChannelName(entry.name))) return;
    if (!includeBlocked && isChannelBlocked(entry.name, blockedSet)) return;
    const group = CHANNEL_GROUPS.includes(entry.group) ? entry.group : "Other";
    const exists = grouped[group].some((item) => normalizeChannelName(item.name) === normalizeChannelName(entry.name));
    if (!exists) {
      grouped[group].push({
        name: entry.name,
        emoji: entry.emoji || "💬",
        code: entry.code || keyMap.get(normalizeChannelName(entry.name)) || "Custom channel",
        description: entry.code || "Custom channel",
        allowPopular: entry.allowPopular !== false,
        blocked: isChannelBlocked(entry.name, blockedSet),
        messageCount24h: counts24h[normalizeChannelId(entry.name)] || 0
      });
      markNew(group, entry.createdAt);
    } else {
      grouped[group] = grouped[group].map((item) => {
        if (normalizeChannelName(item.name) !== normalizeChannelName(entry.name)) return item;
        return {
          ...item,
          emoji: entry.emoji || item.emoji || "💬",
          code: entry.code || item.code || keyMap.get(normalizeChannelName(entry.name)) || item.description || "",
          description: entry.code || item.description || "",
          allowPopular: entry.allowPopular !== false,
          blocked: isChannelBlocked(entry.name, blockedSet),
          messageCount24h: counts24h[normalizeChannelId(entry.name)] || item.messageCount24h || 0
        };
      });
    }
  });

  return { groups: grouped, groupFlags };
}

function buildPopularChannels(limit = 10, excludeChannels = [], blockedSet = new Set()) {
  const db = getDb();
  if (!hasMessagesDb(db)) return [];
  const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const exclude = (excludeChannels || []).map(normalizeChannelName).filter(Boolean);
  const placeholders = exclude.length ? exclude.map(() => "?").join(",") : "";
  const rows = db.prepare(`
    SELECT channel_name, COUNT(1) AS messages, MAX(ts) AS last_ts
    FROM messages
    WHERE channel_name IS NOT NULL AND ts >= ?
    ${exclude.length ? `AND channel_name NOT IN (${placeholders})` : ""}
    GROUP BY channel_name
    ORDER BY messages DESC
    LIMIT ?
  `).all(cutoff, ...(exclude.length ? exclude : []), limit);
  const catalog = loadChannelCatalog();
  const allowMap = new Map(
    catalog.map((entry) => [normalizeChannelName(entry.name), entry.allowPopular !== false])
  );
  return rows.filter((row) => {
    const name = normalizeChannelName(row.channel_name);
    if (!name) return false;
    if (blockedSet.has(name)) return false;
    if (!allowMap.has(name)) return true;
    return allowMap.get(name) !== false;
  }).map((row) => ({
    name: row.channel_name,
    emoji: catalog.find((c) => normalizeChannelName(c.name) === normalizeChannelName(row.channel_name))?.emoji || "💬",
    messageCount24h: row.messages || 0,
    lastMessageAt: row.last_ts || null
  }));
}

function getChannelCounts24h() {
  const db = getDb();
  if (!hasMessagesDb(db)) return {};
  const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const rows = db.prepare(`
    SELECT channel_name, COUNT(1) AS messages
    FROM messages
    WHERE channel_name IS NOT NULL AND ts >= ?
    GROUP BY channel_name
  `).all(cutoff);
  const map = {};
  rows.forEach((row) => {
    const id = normalizeChannelId(row.channel_name);
    if (!id) return;
    map[id] = row.messages || 0;
  });
  return map;
}

async function buildChannelDirectory(userChannels = [], includeBlocked = false) {
  const db = getDb();
  const blockedSet = loadBlockedChannels(db);
  const popular = buildPopularChannels(12, [
    ...(userChannels || []),
    ...FIXED_CHANNELS
  ], blockedSet);
  const { groups, groupFlags } = await buildChannelDirectoryGroups(blockedSet, includeBlocked);
  const channelCounts24h = getChannelCounts24h();
  return { popular, groups, channelCounts24h, groupFlags };
}

function claimUserNode(db, userId, pub, nickname) {
  const now = new Date().toISOString();
  db.prepare("INSERT OR IGNORE INTO user_nodes (user_id, public_id, nickname, created_at) VALUES (?, ?, ?, ?)")
    .run(userId, pub, nickname || null, now);
  return db.prepare("SELECT public_id, nickname, created_at FROM user_nodes WHERE user_id = ? ORDER BY created_at DESC").all(userId);
}

async function refreshNodeRankCache(force) {
  const now = Date.now();
  const last = nodeRankCache.updatedAt ? new Date(nodeRankCache.updatedAt).getTime() : 0;
  if (!force && last && now - last < NODE_RANK_REFRESH_MS) return nodeRankCache;
  if (nodeRankRefreshInFlight) return nodeRankRefreshInFlight;
  nodeRankRefreshInFlight = (async () => {
    const data = await buildNodeRank();
    nodeRankCache = { ...data, cachedAt: new Date().toISOString() };
    return nodeRankCache;
  })();
  try {
    return await nodeRankRefreshInFlight;
  } finally {
    nodeRankRefreshInFlight = null;
  }
}

async function buildRepeaterRankSummary() {
  const now = Date.now();
  const last = rankSummaryCache.updatedAt ? new Date(rankSummaryCache.updatedAt).getTime() : 0;
  if (!last || now - last >= RANK_REFRESH_MS) {
    await refreshRankCache(true);
  }
  return {
    updatedAt: rankSummaryCache.updatedAt,
    count: rankSummaryCache.totals ? rankSummaryCache.totals.total : rankCache.count,
    totals: rankSummaryCache.totals || { total: rankCache.count, active: 0, total24h: 0 }
  };
}

async function buildNodeRankSummary() {
  const now = Date.now();
  const last = nodeRankCache.updatedAt ? new Date(nodeRankCache.updatedAt).getTime() : 0;
  if (!last || now - last >= NODE_RANK_REFRESH_MS) {
    await refreshNodeRankCache(true);
  }
  return {
    updatedAt: nodeRankCache.updatedAt,
    count: nodeRankCache.count,
    totals: {
      active: nodeRankCache.items.filter((n) => Number.isFinite(n.ageHours) && n.ageHours < 24).length,
      messages24h: nodeRankCache.items.reduce((sum, n) => sum + (n.messages24h || 0), 0)
    }
  };
}

async function buildObserverRankSummary() {
  const now = Date.now();
  const last = observerRankCache.updatedAt ? new Date(observerRankCache.updatedAt).getTime() : 0;
  if (!observerRankCache.items?.length) {
    await refreshObserverRankCache(true);
  } else if (!last || (now - last) >= OBSERVER_RANK_REFRESH_MS) {
    await refreshObserverRankCache(true);
  }
  return {
    updatedAt: observerRankCache.updatedAt,
    count: observerRankCache.items.length,
    totals: {
      active: observerRankCache.items.filter((o) => o.ageHours < OBSERVER_OFFLINE_HOURS).length,
      packetsToday: observerRankCache.items.reduce((sum, o) => sum + (o.packetsToday || 0), 0)
    }
  };
}

function floorStatsBucket(ts) {
  return ts - (ts % STATS_ROLLUP_WINDOW_MS);
}

function updateStatsRollup() {
  const db = getDb();
  const now = Date.now();
  const currentBucket = floorStatsBucket(now);
  const row = db.prepare("SELECT MAX(bucket_start) AS max_bucket FROM stats_5m").get();
  let startBucket = Number.isFinite(row?.max_bucket)
    ? row.max_bucket
    : currentBucket - (STATS_ROLLUP_WINDOW_MS * STATS_ROLLUP_SEED_BUCKETS);
  if (startBucket > currentBucket) startBucket = currentBucket;

  const insert = db.prepare(`
    INSERT INTO stats_5m (bucket_start, channel_name, messages, unique_senders, unique_messages, updated_at)
    SELECT ?, channel_name, COUNT(1), COUNT(DISTINCT sender), COUNT(DISTINCT message_hash), ?
    FROM messages
    WHERE ts >= ? AND ts < ?
    GROUP BY channel_name
    ON CONFLICT(bucket_start, channel_name) DO UPDATE SET
      messages = excluded.messages,
      unique_senders = excluded.unique_senders,
      unique_messages = excluded.unique_messages,
      updated_at = excluded.updated_at
  `);
  const insertTotals = db.prepare(`
    INSERT INTO stats_5m (bucket_start, channel_name, messages, unique_senders, unique_messages, updated_at)
    SELECT ?, '__all__', COUNT(1), COUNT(DISTINCT sender), COUNT(DISTINCT message_hash), ?
    FROM messages
    WHERE ts >= ? AND ts < ?
    ON CONFLICT(bucket_start, channel_name) DO UPDATE SET
      messages = excluded.messages,
      unique_senders = excluded.unique_senders,
      unique_messages = excluded.unique_messages,
      updated_at = excluded.updated_at
  `);
  const updatedAt = new Date().toISOString();
  const tx = db.transaction(() => {
    for (let bucket = startBucket; bucket <= currentBucket; bucket += STATS_ROLLUP_WINDOW_MS) {
      const startIso = new Date(bucket).toISOString();
      const endIso = new Date(bucket + STATS_ROLLUP_WINDOW_MS).toISOString();
      insert.run(bucket, updatedAt, startIso, endIso);
      insertTotals.run(bucket, updatedAt, startIso, endIso);
    }
  });
  tx();
}

function maybeUpdateStatsRollup() {
  const now = Date.now();
  if (statsRollupLastAt && (now - statsRollupLastAt) < STATS_ROLLUP_MIN_INTERVAL_MS) return;
  statsRollupLastAt = now;
  try {
    updateStatsRollup();
  } catch {}
}

function readLatestStatsRollup() {
  const db = getDb();
  const row = db.prepare("SELECT MAX(bucket_start) AS max_bucket FROM stats_5m").get();
  if (!Number.isFinite(row?.max_bucket)) return null;
  const items = db.prepare(`
    SELECT channel_name, messages, unique_senders, unique_messages
    FROM stats_5m
    WHERE bucket_start = ?
  `).all(row.max_bucket);
  const totals = items.find((item) => item.channel_name === "__all__") || null;
  const perChannel = items.filter((item) => item.channel_name !== "__all__");
  return { bucketStart: row.max_bucket, totals, perChannel };
}

function startStatsRollupUpdater() {
  if (startStatsRollupUpdater.started) return;
  startStatsRollupUpdater.started = true;
  maybeUpdateStatsRollup();
  setInterval(() => {
    maybeUpdateStatsRollup();
  }, STATS_ROLLUP_INTERVAL_MS);
}

async function buildChannelMessagesBefore(channelName, beforeTs, limit) {
  const db = getDb();
  if (hasMessagesDb(db)) {
    const rows = readMessagesFromDb(channelName, limit, beforeTs) || [];
    if (rows.length) {
      const nodeMap = buildNodeHashMap();
      const observerHitsMap = await getObserverHitsMap();
      const hashes = rows.map((r) => String(r.message_hash || "").toUpperCase()).filter(Boolean);
      const observerAggMap = readMessageObserverAgg(db, hashes);
      const observerPathsMap = readMessageObserverPaths(db, hashes);
      return rows.map((row) => mapMessageRow(row, nodeMap, observerHitsMap, observerAggMap, observerPathsMap)).reverse();
    }
  }
  const sourcePath = getRfSourcePath();
  if (!fs.existsSync(sourcePath)) return [];
  const keyCfg = loadKeys();
  const nodeMap = buildNodeHashMap();
  const observerMap = await getObserverHitsMap();
  const keyMap = {};
  for (const ch of keyCfg.channels || []) {
    const hb = typeof ch.hashByte === "string" ? ch.hashByte.toUpperCase() : null;
    const nm = typeof ch.name === "string" ? ch.name : null;
    if (hb && nm) keyMap[hb] = nm;
  }
  const keyStore = buildKeyStore(keyCfg);
  const beforeMs = beforeTs instanceof Date ? beforeTs.getTime() : Number(beforeTs);

  let baseNow = Date.now();
  try {
    const stat = fs.statSync(sourcePath);
    if (Number.isFinite(stat.mtimeMs)) baseNow = stat.mtimeMs;
  } catch {}

  let maxNumericTs = null;
  {
    const rl = readline.createInterface({
      input: fs.createReadStream(sourcePath, { encoding: "utf8" }),
      crlfDelay: Infinity
    });
    for await (const line of rl) {
      try {
        const rec = JSON.parse(line);
        if (Number.isFinite(rec.ts)) {
          if (maxNumericTs === null || rec.ts > maxNumericTs) maxNumericTs = rec.ts;
        }
      } catch {}
    }
  }

  const messagesMap = new Map();
  const rl = readline.createInterface({
    input: fs.createReadStream(sourcePath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    const hex = getHex(rec);
    if (!hex) continue;

    let decoded;
    try {
      decoded = MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
    } catch {
      continue;
    }

    const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
    if (payloadType !== "GroupText") continue;
    const payload = decoded.payload?.decoded;
    if (!payload || !payload.decrypted) continue;

    const chHash = typeof payload.channelHash === "string" ? payload.channelHash.toUpperCase() : null;
    const chName = chHash && keyMap[chHash] ? keyMap[chHash] : null;
    if (!chName || chName !== channelName) continue;

    let ts = null;
    if (typeof rec.archivedAt === "string" && parseIso(rec.archivedAt)) {
      ts = rec.archivedAt;
    } else if (typeof rec.ts === "string" && parseIso(rec.ts)) {
      ts = rec.ts;
    } else if (Number.isFinite(rec.ts) && maxNumericTs !== null) {
      const approx = baseNow - (maxNumericTs - rec.ts);
      ts = new Date(approx).toISOString();
    }
    if (!ts) continue;
    const tsMs = new Date(ts).getTime();
    if (!Number.isFinite(tsMs) || (Number.isFinite(beforeMs) && tsMs >= beforeMs)) continue;

    const msgHash = String(
      decoded.messageHash ||
      rec.frameHash ||
      sha256Hex(hex) ||
      hex.slice(0, 16) ||
      "unknown"
    ).toUpperCase();
    const messageHash = decoded.messageHash ? String(decoded.messageHash).toUpperCase() : null;
    const frameHash = (rec.frameHash ? String(rec.frameHash) : sha256Hex(hex) || "").toUpperCase();

    const observerSet = new Set();
    const directObserver = String(rec.observerId || rec.observerName || "").trim();
    if (directObserver) observerSet.add(directObserver);
    const hits = observerMap.get(msgHash);
    if (hits) hits.forEach((o) => observerSet.add(o));
    if (frameHash && frameHash !== msgHash) {
      const frameHits = observerMap.get(frameHash);
      if (frameHits) frameHits.forEach((o) => observerSet.add(o));
    }
    if (messageHash && messageHash !== msgHash) {
      const msgHits = observerMap.get(messageHash);
      if (msgHits) msgHits.forEach((o) => observerSet.add(o));
    }
    const observerHits = Array.from(messagesMap.get(chName + "|" + msgHash)?.observerHits || observerSet);
    const observerCount = observerHits.length;

    const body = String(payload.decrypted.message || "");
    const sender = String(payload.decrypted.sender || "unknown");
    const path = Array.isArray(decoded.path) ? decoded.path.map(normalizePathHash) : [];
    const hopCount = path.length || (Number.isFinite(decoded.pathLength) ? decoded.pathLength : 0);
    const pathPoints = path.map((h) => {
      const hit = nodeMap.get(h);
      return {
        hash: h,
        name: hit ? hit.name : "#unknown",
        gps: hit?.gps || null
      };
    });
    const pathNames = pathPoints.map((p) => p.name);
    const msgKey = chName + "|" + msgHash;
    const repeatCount = Math.max(hopCount, observerCount || 0);

    if (!messagesMap.has(msgKey)) {
      messagesMap.set(msgKey, {
        id: msgHash,
        frameHash,
        messageHash,
        channelName: chName,
        sender,
        body,
        ts,
        repeats: repeatCount,
        path,
        pathNames,
        pathPoints,
        observerHits,
        observerCount
      });
    } else {
      const m = messagesMap.get(msgKey);
      m.repeats = Math.max(m.repeats || 0, repeatCount);
      if (ts && (!m.ts || new Date(ts) > new Date(m.ts))) m.ts = ts;
      if (path.length > (m.path?.length || 0)) {
        m.path = path;
        m.pathNames = pathNames;
        m.pathPoints = pathPoints;
      }
      const mergedHits = new Set([...(m.observerHits || []), ...observerHits]);
      m.observerHits = Array.from(mergedHits);
      m.observerCount = m.observerHits.length;
    }
  }

  let list = Array.from(messagesMap.values())
    .sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));
  if (Number.isFinite(limit) && limit > 0 && list.length > limit) {
    list = list.slice(Math.max(0, list.length - limit));
  }
  return list;
}

async function buildRotmData() {
  const now = Date.now();
  if (rotmCache.payload && (now - rotmCache.builtAt) < ROTM_CACHE_MS) {
    return rotmCache.payload;
  }

  const db = getDb();
  if (!hasMessagesDb(db)) {
    const payload = { updatedAt: new Date().toISOString(), feed: [], leaderboard: [], qsos: 0 };
    rotmCache = { builtAt: now, payload };
    return payload;
  }

  const cutoff = new Date(now - ROTM_WINDOW_MS).toISOString();
  const cfg = readRotmConfig();
  const channelName = normalizeRotmChannel(cfg.channel || "#rotm");
  const channelPlain = channelName.replace(/^#/, "");
  const rows = db.prepare(`
    SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_text, path_length, repeats
    FROM messages
    WHERE lower(channel_name) IN (lower(?), lower(?)) AND ts >= ?
    ORDER BY ts DESC
    LIMIT ?
  `).all(channelName, channelPlain, cutoff, ROTM_DB_LIMIT);

  if (!rows.length) {
    const payload = { updatedAt: new Date().toISOString(), feed: [], leaderboard: [], qsos: 0 };
    rotmCache = { builtAt: now, payload };
    return payload;
  }

  const nodeMap = buildNodeHashMap();
  const rotmCandidatesByHash = buildRotmCandidatesByHash(readDevices());
  const observerHitsMap = await getObserverHitsMap();
  const hashes = rows.map((r) => String(r.message_hash || "").toUpperCase()).filter(Boolean);
  const observerAggMap = readMessageObserverAgg(db, hashes);
  const observerPathsMap = readMessageObserverPaths(db, hashes);
  const messagesAsc = rows
    .map((row) => mapMessageRow(row, nodeMap, observerHitsMap, observerAggMap, observerPathsMap))
    .filter((m) => m && m.ts)
    .sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));

  const cqBySender = new Map(); // senderKey -> [{ msg, ts, confidenceOk, repeater }]
  const infoById = new Map(); // msgId -> info
  const qsos = [];
  const qsoLogByNode = new Map(); // nodeKey -> [{ts, cq, response, repeater}]
  const claims = new Map(); // nodeKey -> entry

  const addClaim = (nodeName, nodeKey, repeater, ts, countRepeater) => {
    if (!nodeKey) return;
    if (!claims.has(nodeKey)) {
      claims.set(nodeKey, {
        nodeKey,
        node: nodeName || nodeKey,
        qsos: 0,
        lastActivity: null,
        repeaters: new Map()
      });
    }
    const entry = claims.get(nodeKey);
    entry.qsos += 1;
    if (!entry.lastActivity || ts > entry.lastActivity) entry.lastActivity = ts;
    if (countRepeater && (repeater?.hash || repeater?.name)) {
      const key = repeater.hash || repeater.name;
      if (!entry.repeaters.has(key)) {
        entry.repeaters.set(key, {
          hash: repeater.hash || null,
          name: repeater.name || key,
          gps: repeater.gps || null
        });
      }
    }
    if (nodeName) entry.node = nodeName;
  };

  const overrides = readRotmOverrides();
  for (const msg of messagesAsc) {
    const sender = String(msg.sender || "unknown");
    const senderKeys = buildRotmKeys(sender);
    const senderKey = senderKeys[0] || "";
    const body = String(msg.body || "");
    const isCq = /^CQ\b/i.test(body.trim());
    const mentions = extractRotmMentions(body);
    const confidenceOk = (msg.observerCount || 0) >= ROTM_MIN_OBSERVER_HITS;
    const repeater = resolveRotmRepeaterFromPath(msg.path, rotmCandidatesByHash);
    const ts = new Date(msg.ts);
    infoById.set(msg.id, {
      sender,
      senderKey,
      senderKeys,
      body,
      isCq,
      mentions,
      confidenceOk,
      repeater,
      ts,
      confirmed: false,
      response: false,
      repeaterOptions: repeater?.hash ? (rotmCandidatesByHash.get(repeater.hash) || []) : []
    });

    if (isCq && senderKeys.length) {
      senderKeys.forEach((key) => {
        if (!cqBySender.has(key)) cqBySender.set(key, []);
        cqBySender.get(key).push({ msg, ts, confidenceOk, repeater });
      });
      continue;
    }
  }

  for (const msg of messagesAsc) {
    const info = infoById.get(msg.id);
    if (!info || info.isCq) continue;
    const ts = info.ts;
    if (!info.mentions.length || !info.senderKeys?.length) continue;
    info.response = true;

    let matched = null;
    let matchedSenderKey = null;
    for (const mention of info.mentions) {
      if (info.senderKeys.includes(mention)) continue;
      const cqList = cqBySender.get(mention) || [];
      for (let i = cqList.length - 1; i >= 0; i -= 1) {
        const candidate = cqList[i];
        const diff = ts - candidate.ts;
        if (diff < 0) continue;
        if (diff > ROTM_QSO_WINDOW_MS) break;
        matched = candidate;
        matchedSenderKey = mention;
        break;
      }
      if (matched) break;
    }
    if (!matched) continue;

    const cqMsg = matched.msg;
    const cqInfo = infoById.get(cqMsg.id);
    if (!cqInfo) continue;
    if (!cqInfo.confidenceOk || !info.confidenceOk) continue;

    let repeater = cqInfo.repeater || info.repeater || null;
    if (overrides && overrides[cqMsg.id]) {
      const override = overrides[cqMsg.id];
      const options = override?.hash ? (rotmCandidatesByHash.get(override.hash) || []) : [];
      const match = options.find((opt) => opt.pub === override.pub);
      if (match) {
        repeater = {
          hash: override.hash,
          name: match.name || override.hash,
          gps: match.gps,
          pub: match.pub
        };
      }
    }
    if (!repeater || !repeater.gps) continue;
    const qso = {
      cqId: cqMsg.id,
      responseId: msg.id,
      senderA: cqInfo.sender,
      senderB: info.sender,
      repeater,
      ts: ts.toISOString()
    };
    qsos.push(qso);
    cqInfo.confirmed = true;
    cqInfo.confirmedAt = ts;
    cqInfo.responseSender = info.sender;
    cqInfo.responseBody = info.body;
    cqInfo.responseViaRepeater = info.repeater?.name || null;
    cqInfo.responseViaRepeaterHash = info.repeater?.hash || null;
    cqInfo.responseViaRepeaterGps = info.repeater?.gps || null;
    info.confirmed = true;
    addClaim(cqInfo.sender, cqInfo.senderKey, repeater, ts, true);
    addClaim(info.sender, info.senderKey, repeater, ts, false);

    const logEntry = {
      cqId: cqMsg.id,
      ts: ts.toISOString(),
      cqSender: cqInfo.sender,
      cqBody: cqInfo.body || "",
      responseSender: info.sender,
      responseBody: info.body || "",
      repeater: repeater?.name || "Unknown",
      repeaterHash: repeater?.hash || null,
      repeaterPub: repeater?.pub || null,
      repeaterOptions: repeater?.hash ? (rotmCandidatesByHash.get(repeater.hash) || []) : []
    };
    const cqLog = qsoLogByNode.get(cqInfo.senderKey) || [];
    cqLog.push(logEntry);
    qsoLogByNode.set(cqInfo.senderKey, cqLog);
    const respLog = qsoLogByNode.get(info.senderKey) || [];
    respLog.push(logEntry);
    qsoLogByNode.set(info.senderKey, respLog);
  }

  const cqFeed = [];
  infoById.forEach((info, id) => {
    if (!info.isCq) return;
    const msg = messagesAsc.find((m) => m.id === id);
    if (!msg) return;
    const createdAt = new Date(msg.ts || 0).getTime();
    if (!Number.isFinite(createdAt)) return;
    const nowMs = Date.now();
    if (!info.confirmed) {
      if (nowMs - createdAt > ROTM_QSO_WINDOW_MS) return;
    } else {
      const confirmedAt = info.confirmedAt ? new Date(info.confirmedAt).getTime() : nowMs;
      if (nowMs - confirmedAt > 30 * 1000) return;
    }
    const hopCount = Number.isFinite(msg.hopCount) ? msg.hopCount : (Array.isArray(msg.path) ? msg.path.length : 0);
    cqFeed.push({
      id,
      ts: msg.ts,
      channel: msg.channelName,
      sender: msg.sender,
      body: msg.body,
      responseSender: info.responseSender || null,
      responseBody: info.responseBody || null,
      responseViaRepeater: info.responseViaRepeater || null,
      responseViaRepeaterHash: info.responseViaRepeaterHash || null,
      responseViaRepeaterGps: info.responseViaRepeaterGps || null,
      confirmed: !!info.confirmed,
      viaRepeater: info.repeater?.name || "Unknown",
      viaRepeaterHash: info.repeater?.hash || null,
      viaRepeaterGps: info.repeater?.gps || null,
      viaRepeaterOptions: info.repeaterOptions || [],
      observerCount: msg.observerCount || 0,
      hopCount,
      confidenceOk: (msg.observerCount || 0) >= ROTM_MIN_OBSERVER_HITS,
      badge: info.confirmed ? "Confirmed QSO!" : "CQ",
      tone: info.confirmed ? "ok" : "cq"
    });
  });

  const feed = cqFeed
    .sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0))
    .slice(0, ROTM_FEED_LIMIT);

  const leaderboard = Array.from(claims.values())
    .map((entry) => ({
      node: entry.node,
      nodeKey: entry.nodeKey,
      uniqueRepeaters: entry.repeaters.size,
      qsos: entry.qsos,
      lastActivity: entry.lastActivity ? entry.lastActivity.toISOString() : null,
      repeaters: Array.from(entry.repeaters.values()),
      qsoLog: (qsoLogByNode.get(entry.nodeKey) || []).slice(-50).reverse()
    }))
    .sort((a, b) => (b.uniqueRepeaters - a.uniqueRepeaters) || (b.qsos - a.qsos) || (new Date(b.lastActivity || 0) - new Date(a.lastActivity || 0)));

  const payload = {
    updatedAt: new Date().toISOString(),
    feed,
    leaderboard,
    qsos: qsos.length
  };
  rotmCache = { builtAt: now, payload };
  return payload;
}

async function buildMeshScore() {
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const all = Object.values(byPub).filter(Boolean);
  const repeaters = all.filter((d) => d.isRepeater);
  const roomServers = all.filter((d) => d.role === "room_server");
  const chatNodes = all.filter((d) => d.role === "chat");
  const companions = all.filter((d) => !d.isRepeater && d.role !== "room_server" && d.role !== "chat");
  const now = Date.now();
  const activeRepeaters = repeaters.filter((d) => {
    const ts = parseIso(d.lastSeen);
    return ts && (now - ts.getTime()) <= 24 * 60 * 60 * 1000;
  });

  const sourcePath = getRfSourcePath();
  const lines = await tailLines(sourcePath, 5000);
  const buckets = new Map(); // date -> {messages, msgCounts}
  const keyCfg = loadKeys();
  const keyStore = buildKeyStore(keyCfg);

  for (const line of lines) {
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    const hex = getHex(rec);
    if (!hex) continue;
    let decoded;
    try {
      decoded = MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
    } catch {
      continue;
    }
    const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
    if (payloadType !== "GroupText") continue;
    const payload = decoded.payload?.decoded;
    if (!payload || !payload.decrypted) continue;
    const ts = rec.archivedAt || rec.ts || null;
    if (!ts) continue;
    const day = new Date(ts);
    if (!Number.isFinite(day.getTime())) continue;
    const key = day.toISOString().slice(0, 10);
    if (!buckets.has(key)) buckets.set(key, { messages: 0, msgCounts: new Map() });
    const b = buckets.get(key);
    const msgHash = String(decoded.messageHash || hex.slice(0, 16) || "unknown");
    b.messages += 1;
    b.msgCounts.set(msgHash, (b.msgCounts.get(msgHash) || 0) + 1);
  }

  const activeRatio = repeaters.length ? activeRepeaters.length / repeaters.length : 0;
  const nodeScore = all.length ? clamp((repeaters.length + roomServers.length + chatNodes.length + companions.length) / 200, 0, 1) : 0;
  const series = Array.from(buckets.entries()).map(([date, b]) => {
    const uniqueMsgs = b.msgCounts.size || 1;
    const avgRepeats = b.messages / uniqueMsgs;
    const messageScore = clamp(b.messages / 200, 0, 1);
    const repeatScore = clamp(avgRepeats / 5, 0, 1);
    const score = Math.round(100 * clamp(
      0.35 * activeRatio +
      0.30 * messageScore +
      0.20 * repeatScore +
      0.15 * nodeScore,
      0, 1
    ));
    return { date, score, messages: b.messages, avgRepeats: Number(avgRepeats.toFixed(2)) };
  }).sort((a, b) => a.date.localeCompare(b.date));

  let mergedSeries = series;
  try {
    const db = getDb();
    const upsert = db.prepare(`
      INSERT INTO meshscore_daily (day, score, messages, avg_repeats, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(day) DO UPDATE SET score=excluded.score, messages=excluded.messages,
        avg_repeats=excluded.avg_repeats, updated_at=excluded.updated_at
    `);
    const nowIso = new Date().toISOString();
    db.transaction(() => {
      for (const row of series) {
        upsert.run(row.date, row.score, row.messages, row.avgRepeats, nowIso);
      }
    })();
    const rows = db.prepare("SELECT day, score, messages, avg_repeats FROM meshscore_daily ORDER BY day ASC").all();
    if (rows.length) {
      mergedSeries = rows.map((r) => ({
        date: r.day,
        score: r.score,
        messages: r.messages,
        avgRepeats: Number(Number(r.avg_repeats || 0).toFixed(2))
      }));
    }
  } catch {}

  const latestKey = mergedSeries.length ? mergedSeries[mergedSeries.length - 1].date : new Date().toISOString().slice(0, 10);
  const latestIndex = mergedSeries.findIndex((s) => s.date === latestKey);
  const today = mergedSeries.find((s) => s.date === latestKey) || { score: 0, messages: 0, avgRepeats: 0 };
  const yesterday = latestIndex > 0 ? mergedSeries[latestIndex - 1] : { score: 0, messages: 0, avgRepeats: 0 };

  return {
    updatedAt: new Date().toISOString(),
    totals: {
      devices: all.length,
      repeaters: repeaters.length,
      activeRepeaters: activeRepeaters.length,
      roomServers: roomServers.length,
      chatNodes: chatNodes.length,
      companions: companions.length
    },
    messages: {
      meshToday: today.messages,
      meshYesterday: yesterday.messages,
      observerToday: 0
    },
    scores: {
      today: today.score,
      yesterday: yesterday.score,
      delta: Math.round(today.score - yesterday.score)
    },
    series: mergedSeries
  };
}

async function readRfRecords(limit) {
  const max = Number.isFinite(limit) ? limit : 80;
  try {
    const db = getDb();
    const has = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='rf_packets'").get();
    if (has) {
      const rows = db.prepare(`
        SELECT ts, payload_hex, frame_hash, rssi, snr, crc, observer_id, observer_name,
               len, payload_len, packet_type, topic, route, path
        FROM rf_packets
        ORDER BY id DESC
        LIMIT ?
      `).all(max);
      if (rows.length) {
        return rows.reverse().map((row) => {
          let route = null;
          let path = null;
          try { route = row.route ? JSON.parse(row.route) : null; } catch {}
          try { path = row.path ? JSON.parse(row.path) : null; } catch {}
          return {
            archivedAt: row.ts,
            payloadHex: row.payload_hex,
            frameHash: row.frame_hash,
            rssi: row.rssi,
            snr: row.snr,
            crc: row.crc === null ? null : !!row.crc,
            observerId: row.observer_id,
            observerName: row.observer_name,
            len: row.len,
            payloadLen: row.payload_len,
            packetType: row.packet_type,
            topic: row.topic,
            route,
            path
          };
        });
      }
    }
  } catch {}

  const sourcePath = getRfSourcePath();
  const lines = await tailLines(sourcePath, max);
  const records = [];
  for (const line of lines) {
    try {
      records.push(JSON.parse(line));
    } catch {}
  }
  return records;
}

async function buildRfLatest(limit) {
  const records = await readRfRecords(limit || 80);
  const keyCfg = loadKeys();
  const keyStore = buildKeyStore(keyCfg);
  const devices = readDevices();
  const byPub = devices.byPub || {};
  const keyMap = {};
  for (const ch of keyCfg.channels || []) {
    if (ch?.hashByte && ch?.name) keyMap[String(ch.hashByte).toUpperCase()] = String(ch.name);
  }
  const nodeMap = buildNodeHashMap();
  const items = [];
  for (const rec of records) {
    const hex = getHex(rec);
    if (!hex) continue;
    let decoded = null;
    try {
      decoded = MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
    } catch {
      decoded = null;
    }
    const payloadType = decoded ? Utils.getPayloadTypeName(decoded.payloadType) : null;
    const routeTypeName = decoded ? Utils.getRouteTypeName(decoded.routeType) : null;
    const payloadDecoded = decoded?.payload?.decoded || decoded?.decoded || null;
    const senderPublicKeyRaw = payloadDecoded?.senderPublicKey || payloadDecoded?.publicKey || payloadDecoded?.senderPub || null;
    const senderPublicKey = senderPublicKeyRaw ? String(senderPublicKeyRaw).toUpperCase() : null;
    const senderName = senderPublicKey
      ? (byPub[senderPublicKey]?.name ||
         byPub[senderPublicKey]?.raw?.lastAdvert?.appData?.name ||
         payloadDecoded?.senderName ||
         payloadDecoded?.name ||
         null)
      : (payloadDecoded?.senderName || payloadDecoded?.name || null);
    const destinationHash = payloadDecoded?.destinationHash || payloadDecoded?.destHash || payloadDecoded?.destination || null;
    const channelHash = typeof payloadDecoded?.channelHash === "string" ? payloadDecoded.channelHash.toUpperCase() : null;
    const channelName = channelHash && keyMap[channelHash] ? keyMap[channelHash] : null;
    const pathRaw = Array.isArray(decoded?.path) ? decoded.path : [];
    const path = pathRaw.map(normalizePathHash);
    const pathNames = path.map((hash) => nodeMap.get(hash)?.name || hash);
    const appData = payloadDecoded?.appData || null;
    const appFlags = typeof appData?.flags === "number" ? appData.flags : null;
    const advertName = typeof appData?.name === "string" ? appData.name : null;
    const hash = (decoded && decoded.messageHash)
      ? String(decoded.messageHash)
      : (rec.frameHash || rec.fp || hex.slice(0, 16) || null);
    const len = rec.len ?? rec.reported_len ?? (hex ? Math.floor(hex.length / 2) : null);
    const payloadBytes = decoded?.payload?.raw ? Math.floor(decoded.payload.raw.length / 2) : null;
    const observerId = rec.observerId || rec.observerName || null;
    const observerName = rec.observerName || rec.observerId || null;
    items.push({
      ts: rec.archivedAt || rec.ts || null,
      fp: rec.fp || null,
      crc: rec.crc ?? null,
      rssi: rec.rssi ?? null,
      snr: rec.snr ?? null,
      len,
      payloadType,
      routeTypeName,
      pathLength: decoded?.pathLength ?? path.length,
      path,
      pathNames,
      senderPublicKey,
      senderName,
      destinationHash,
      channelHash,
      channelName,
      advertName,
      appFlags,
      payloadBytes,
      decrypted: payloadDecoded?.decrypted === true,
      isValid: decoded?.isValid ?? null,
      hash,
      observerId,
      observerName
    });
  }
  const grouped = new Map();
  for (const item of items) {
    if (!item.hash) continue;
    const key = String(item.hash);
    const observerLabel = item.observerName || item.observerId || null;
    const existing = grouped.get(key);
    if (!existing) {
      const clone = { ...item };
      clone.observerHits = new Set();
      clone.observerEntries = new Map();
      clone.hopCodes = new Set(Array.isArray(item.path) ? item.path : []);
      if (observerLabel) clone.observerHits.add(observerLabel);
      if (observerLabel) {
        clone.observerEntries.set(observerLabel, {
          observerId: item.observerId || null,
          observerName: item.observerName || item.observerId || observerLabel,
          ts: item.ts || null,
          rssi: item.rssi ?? null,
          snr: item.snr ?? null,
          len: item.len ?? null,
          path: Array.isArray(item.path) ? item.path : [],
          pathNames: Array.isArray(item.pathNames) ? item.pathNames : [],
          pathLength: item.pathLength ?? 0
        });
      }
      grouped.set(key, clone);
      continue;
    }
    if (observerLabel) existing.observerHits.add(observerLabel);
    if (observerLabel) {
      const entry = existing.observerEntries.get(observerLabel) || {
        observerId: item.observerId || null,
        observerName: item.observerName || item.observerId || observerLabel,
        ts: item.ts || null,
        rssi: item.rssi ?? null,
        snr: item.snr ?? null,
        len: item.len ?? null,
        path: Array.isArray(item.path) ? item.path : [],
        pathNames: Array.isArray(item.pathNames) ? item.pathNames : [],
        pathLength: item.pathLength ?? 0
      };
      if (item.ts && (!entry.ts || new Date(item.ts) > new Date(entry.ts))) {
        entry.ts = item.ts;
        entry.rssi = item.rssi ?? entry.rssi;
        entry.snr = item.snr ?? entry.snr;
        entry.len = item.len ?? entry.len;
        entry.path = Array.isArray(item.path) ? item.path : entry.path;
        entry.pathNames = Array.isArray(item.pathNames) ? item.pathNames : entry.pathNames;
        entry.pathLength = item.pathLength ?? entry.pathLength;
      }
      existing.observerEntries.set(observerLabel, entry);
    }
    if (Array.isArray(item.path)) {
      item.path.forEach((code) => existing.hopCodes.add(code));
    }
    if (item.ts && (!existing.ts || new Date(item.ts) > new Date(existing.ts))) {
      existing.ts = item.ts;
    }
    if (Number.isFinite(item.rssi) && (!Number.isFinite(existing.rssi) || item.rssi > existing.rssi)) {
      existing.rssi = item.rssi;
    }
    if (Number.isFinite(item.snr) && (!Number.isFinite(existing.snr) || item.snr > existing.snr)) {
      existing.snr = item.snr;
    }
    if (Number.isFinite(item.len) && (!Number.isFinite(existing.len) || item.len > existing.len)) {
      existing.len = item.len;
    }
    if (item.pathLength > existing.pathLength) {
      existing.pathLength = item.pathLength;
      existing.path = item.path;
      existing.pathNames = item.pathNames;
    }
  }
  const merged = Array.from(grouped.values()).map((item) => {
    const hits = item.observerHits ? Array.from(item.observerHits) : [];
    const entries = item.observerEntries ? Array.from(item.observerEntries.values()) : [];
    const hopCount = item.hopCodes ? item.hopCodes.size : (item.pathLength ?? 0);
    return {
      ...item,
      observerHits: hits,
      observerCount: hits.length,
      observerEntries: entries,
      hopCount
    };
  });
  merged.sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));
  return { updatedAt: new Date().toISOString(), items: merged };
}

async function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 1_000_000) {
        reject(new Error("payload too large"));
      }
    });
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

async function buildMessagesPayload({ channel, limit, beforeTs, restrictChannels, blockedSet, includeBlocked }) {
  const payload = await buildChannelMessages();
  const channelName = channel ? (String(channel).startsWith("#") ? channel : `#${channel}`) : null;
  let list = payload.messages;
  const before = typeof beforeTs === "string" ? beforeTs : (beforeTs ? new Date(beforeTs).toISOString() : null);
  let max = Number.isFinite(limit) ? limit : null;

  if (channelName) {
    if (!includeBlocked && blockedSet && blockedSet.has(normalizeChannelName(channelName))) {
      return { channels: [], messages: [] };
    }
    const channelLimit = channelHistoryLimit(channelName);
    if (!Number.isFinite(max) || max <= 0 || max < channelLimit) {
      max = channelLimit;
    }
    const cutoff = before ? new Date(before).getTime() : Date.now();
    list = await buildChannelMessagesBefore(channelName, cutoff, max);
  } else {
    if (before) {
      list = list.filter((m) => {
        const ts = m.ts ? new Date(m.ts).getTime() : 0;
        return ts && ts < new Date(before).getTime();
      });
    }
    list.sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));
  }

  if (!channelName && Array.isArray(restrictChannels) && restrictChannels.length) {
    const allowed = new Set(restrictChannels.map(normalizeChannelName).filter(Boolean));
    list = list.filter((m) => allowed.has(normalizeChannelName(m.channelName)));
  }
  if (!includeBlocked && blockedSet && blockedSet.size) {
    list = list.filter((m) => !blockedSet.has(normalizeChannelName(m.channelName)));
  }

  if (!channelName) {
    const perChannel = new Map();
    for (const m of list) {
      const key = m.channelName || "#unknown";
      const bucket = perChannel.get(key) || [];
      bucket.push(m);
      perChannel.set(key, bucket);
    }
    const trimmed = [];
    for (const [key, bucket] of perChannel.entries()) {
      const limitForChannel = channelHistoryLimit(key);
      const keep = bucket.slice(Math.max(0, bucket.length - limitForChannel));
      keep.forEach((item) => trimmed.push(item));
    }
    list = trimmed.sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));
  } else if (Number.isFinite(max) && max > 0 && list.length > max) {
    list = list.slice(Math.max(0, list.length - max));
  }

  let channels = payload.channels;
  if (Array.isArray(restrictChannels) && restrictChannels.length) {
    const allowed = new Set(restrictChannels.map(normalizeChannelName).filter(Boolean));
    channels = (channels || []).filter((ch) => allowed.has(normalizeChannelName(ch.name || ch.id)));
  }
  if (!includeBlocked && blockedSet && blockedSet.size) {
    channels = (channels || []).filter((ch) => !blockedSet.has(normalizeChannelName(ch.name || ch.id)));
  }
  return { channels, messages: list };
}

function send(res, status, contentType, body) {
  res.writeHead(status, {
    "Content-Type": contentType,
    "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
    "Pragma": "no-cache",
    "Expires": "0"
  });
  res.end(body);
}

function contentTypeFor(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".png") return "image/png";
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".svg") return "image/svg+xml";
  if (ext === ".json") return "application/json; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".js") return "application/javascript; charset=utf-8";
  return "application/octet-stream";
}

refreshLastHeardCache();
setInterval(refreshLastHeardCache, GEO_SCORE_LAST_HEARD_REFRESH_MS);

const server = http.createServer(async (req, res) => {
  let perfStart = null;
  if (DEBUG_PERF) {
    perfStart = process.hrtime.bigint();
    let bytes = 0;
    const origWrite = res.write.bind(res);
    const origEnd = res.end.bind(res);
    res.write = (chunk, encoding, cb) => {
      if (chunk) {
        bytes += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(String(chunk), encoding);
      }
      return origWrite(chunk, encoding, cb);
    };
    res.end = (chunk, encoding, cb) => {
      if (chunk) {
        bytes += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(String(chunk), encoding);
      }
      return origEnd(chunk, encoding, cb);
    };
    res.on("finish", () => {
      if (req.url && req.url.startsWith("/api/message-stream")) return;
      const elapsedMs = Number(process.hrtime.bigint() - perfStart) / 1e6;
      console.log(`[perf] ${req.method} ${req.url} ${res.statusCode} ${elapsedMs.toFixed(1)}ms ${bytes}b`);
    });
  }
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname === "/") {
    let html = fs.readFileSync(indexPath, "utf8");
    html = html.replace(/__GOOGLE_CLIENT_ID__/g, GOOGLE_CLIENT_ID);
    return send(res, 200, "text/html; charset=utf-8", html);
  }
  const rawPath = decodeURIComponent(u.pathname || "");
  const trimmed = rawPath.replace(/^[\\/]+/, "");
  const staticPath = path.normalize(trimmed).replace(/^(\.\.[\\/])+/, "");
  if (staticPath) {
    const absPath = path.join(staticDir, staticPath);
    if (absPath.startsWith(staticDir) && fs.existsSync(absPath) && fs.statSync(absPath).isFile()) {
      const body = fs.readFileSync(absPath);
      return send(res, 200, contentTypeFor(absPath), body);
    }
  }

  if (u.pathname === "/api/health") {
    try {
      await refreshRankCache(false);
      const payload = buildHealthPayload();
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, ...payload }));
    } catch (err) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/dashboard") {
    const channelRaw = String(u.searchParams.get("channel") || "").trim();
    const channel = channelRaw ? (channelRaw.startsWith("#") ? channelRaw : `#${channelRaw}`) : null;
    const limitRaw = u.searchParams.get("limit");
    const beforeRaw = u.searchParams.get("before");
    const limit = limitRaw ? Number(limitRaw) : null;
    let beforeTs = null;
    if (beforeRaw) {
      const beforeDate = parseIso(beforeRaw);
      if (beforeDate) beforeTs = beforeDate.getTime();
    }

    const user = getSessionUser(req);
    const db = getDb();
    const blockedSet = loadBlockedChannels(db);
    const row = db.prepare("SELECT id FROM users WHERE is_admin = 1 LIMIT 1").get();
    let userChannels = [];
    let channelsPayload = null;
    if (user) {
      userChannels = getUserChannels(user.id);
      channelsPayload = await buildUserChannelSummary(user.id);
    }
    const messagesPayload = await buildMessagesPayload({
      channel,
      limit,
      beforeTs,
      restrictChannels: userChannels.length ? userChannels : null,
      blockedSet,
      includeBlocked: !!user?.isAdmin
    });
    maybeUpdateStatsRollup();
    const stats = readLatestStatsRollup();
    const meshscore = await refreshMeshScoreCache(false);
    const rotm = await buildRotmData();
    const rotmConfig = user?.isAdmin
      ? { channel: normalizeRotmChannel(readRotmConfig().channel || "#rotm") }
      : null;
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
      ok: true,
      user,
      hasAdmin: !!row,
      channels: channelsPayload || messagesPayload.channels,
      messages: messagesPayload.messages,
      userChannels,
      channelCounts24h: getChannelCounts24h(),
      stats,
      meshscore,
      rotm,
      rotmConfig
    }));
  }

  if (u.pathname === "/api/auth/exists") {
    const email = String(u.searchParams.get("email") || "").trim();
    if (!email) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "email required" }));
    }
    const db = getDb();
    const row = db.prepare("SELECT id FROM users WHERE lower(username) = lower(?)").get(email);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, exists: !!row }));
  }

  if (u.pathname === "/api/auth/oauth/google") {
    if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REDIRECT_URI) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "google oauth not configured" }));
    }
    const state = createOauthState();
    const params = new URLSearchParams({
      client_id: GOOGLE_CLIENT_ID,
      redirect_uri: GOOGLE_REDIRECT_URI,
      response_type: "code",
      scope: "openid email profile",
      state,
      include_granted_scopes: "true",
      prompt: "consent"
    });
    res.statusCode = 302;
    res.setHeader("Location", `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`);
    return res.end();
  }

  if (u.pathname === "/api/auth/oauth/google/callback") {
    const code = String(u.searchParams.get("code") || "");
    const state = String(u.searchParams.get("state") || "");
    if (!code || !state) {
      res.statusCode = 302;
      res.setHeader("Location", "/?auth=failed");
      return res.end();
    }
    const stateEntry = consumeOauthState(state);
    if (!stateEntry) {
      res.statusCode = 302;
      res.setHeader("Location", "/?auth=expired");
      return res.end();
    }
    try {
      const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          code,
          client_id: GOOGLE_CLIENT_ID,
          client_secret: GOOGLE_CLIENT_SECRET,
          redirect_uri: GOOGLE_REDIRECT_URI,
          grant_type: "authorization_code"
        })
      });
      const tokenData = await tokenRes.json().catch(() => ({}));
      if (!tokenRes.ok || !tokenData.id_token) throw new Error("OAuth token exchange failed.");
      const tokenInfo = await verifyGoogleIdToken(tokenData.id_token);
      const db = getDb();
      createGoogleSession(db, tokenInfo, req, res);
      res.statusCode = 302;
      res.setHeader("Location", "/?auth=google");
      return res.end();
    } catch {
      res.statusCode = 302;
      res.setHeader("Location", "/?auth=failed");
      return res.end();
    }
  }

  if (u.pathname === "/api/auth/google-id-token" && req.method === "POST") {
    if (!GOOGLE_CLIENT_ID) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "google oauth not configured" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const credential = String(body.credential || "").trim();
      if (!credential) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "credential missing" }));
      }
      const tokenInfo = await verifyGoogleIdToken(credential);
      const db = getDb();
      const { session, user } = createGoogleSession(db, tokenInfo, req, res);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        ok: true,
        token: session.token,
        user: {
          id: user.id,
          username: user.username,
          displayName: user.display_name || null,
          isAdmin: !!user.is_admin
        }
      }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/auth/me") {
    const user = getSessionUser(req);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, user }));
  }

  if (u.pathname === "/api/admin/status") {
    const db = getDb();
    const row = db.prepare("SELECT id FROM users WHERE is_admin = 1 LIMIT 1").get();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, hasAdmin: !!row }));
  }

  if (u.pathname === "/api/admin/bootstrap" && req.method === "POST") {
    try {
      const db = getDb();
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const username = String(body.username || "").trim();
      const password = String(body.password || "");
      if (!username || password.length < 8) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid credentials" }));
      }
      const existing = db.prepare("SELECT id FROM users WHERE is_admin = 1 LIMIT 1").get();
      if (existing) {
        return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin already exists" }));
      }
      const hash = bcrypt.hashSync(password, 10);
      const now = new Date().toISOString();
      const info = db.prepare("INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, 1, ?)")
        .run(username, hash, now);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, id: info.lastInsertRowid }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/auth/register" && req.method === "POST") {
    try {
      const db = getDb();
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const username = String(body.username || body.email || "").trim();
      const displayName = String(body.displayName || "").trim();
      const password = String(body.password || "");
      if (!username || password.length < 8) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid credentials" }));
      }
      const existing = db.prepare("SELECT id FROM users WHERE username = ?").get(username);
      if (existing) {
        return send(res, 409, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "username already exists" }));
      }
      const hash = bcrypt.hashSync(password, 10);
      const now = new Date().toISOString();
      const info = db.prepare("INSERT INTO users (username, password_hash, display_name, created_at) VALUES (?, ?, ?, ?)")
        .run(username, hash, displayName || null, now);
      const session = createSession(db, info.lastInsertRowid);
      res.setHeader("Set-Cookie", buildSessionCookie(req, session.token));
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        ok: true,
        token: session.token,
        user: { id: info.lastInsertRowid, username, displayName: displayName || null, isAdmin: false }
      }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/auth/login" && req.method === "POST") {
    try {
      const db = getDb();
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const username = String(body.username || "").trim();
      const password = String(body.password || "");
      const user = db.prepare("SELECT id, username, display_name, password_hash, is_admin FROM users WHERE username = ?").get(username);
      if (!user || !bcrypt.compareSync(password, user.password_hash)) {
        return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid credentials" }));
      }
      const session = createSession(db, user.id);
      db.prepare("UPDATE users SET last_login = ? WHERE id = ?").run(new Date().toISOString(), user.id);
      res.setHeader("Set-Cookie", buildSessionCookie(req, session.token));
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        ok: true,
        token: session.token,
        user: { id: user.id, username: user.username, displayName: user.display_name || null, isAdmin: !!user.is_admin }
      }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/auth/logout" && req.method === "POST") {
    const token = getAuthToken(req);
    if (token) {
      try {
        const db = getDb();
        db.prepare("DELETE FROM sessions WHERE token = ?").run(token);
      } catch {}
    }
    res.setHeader("Set-Cookie", "mesh_session=; Path=/; Max-Age=0; SameSite=Lax");
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
  }

  if (u.pathname === "/api/node-profile") {
    const db = getDb();
    if (req.method === "GET") {
      const pub = String(u.searchParams.get("pub") || "").trim().toUpperCase();
      if (!pub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub required" }));
      }
      const row = db.prepare("SELECT pub, name, bio, photo_url, owner_user_id, updated_at FROM node_profiles WHERE pub = ?").get(pub);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, profile: row || null }));
    }
    if (req.method === "POST") {
      const user = getSessionUser(req);
      if (!user) {
        return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
      }
      try {
        const raw = await readBody(req);
        const body = JSON.parse(raw || "{}");
        const pub = String(body.pub || "").trim().toUpperCase();
        const name = String(body.name || "").trim();
        const bio = String(body.bio || "").trim();
        const photoUrl = String(body.photoUrl || "").trim();
        if (!pub) {
          return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub required" }));
        }
        const existing = db.prepare("SELECT owner_user_id FROM node_profiles WHERE pub = ?").get(pub);
        if (existing && existing.owner_user_id && existing.owner_user_id !== user.id && !user.isAdmin) {
          return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "not authorized" }));
        }
        const updatedAt = new Date().toISOString();
        const ownerId = existing?.owner_user_id || user.id;
        db.prepare(`
          INSERT INTO node_profiles (pub, name, bio, photo_url, owner_user_id, updated_at)
          VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(pub) DO UPDATE SET name=excluded.name, bio=excluded.bio, photo_url=excluded.photo_url,
          owner_user_id=excluded.owner_user_id, updated_at=excluded.updated_at
        `).run(pub, name, bio, photoUrl, ownerId, updatedAt);
        return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
      } catch (err) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
      }
    }
  }

  if (u.pathname === "/api/user/nodes" && req.method === "GET") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    const db = getDb();
    const rows = db.prepare("SELECT public_id, nickname, created_at FROM user_nodes WHERE user_id = ? ORDER BY created_at DESC").all(user.id);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, nodes: rows }));
  }

  if (u.pathname === "/api/user/nodes/claim" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const pub = String(body.publicNodeId || body.pub || "").trim().toUpperCase();
      const nickname = String(body.nickname || "").trim();
      if (!pub || pub.length < 6) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "public id required" }));
      }
      const db = getDb();
      const rows = claimUserNode(db, user.id, pub, nickname);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, nodes: rows }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/user/nodes/unclaim" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const pub = String(body.publicNodeId || body.pub || "").trim().toUpperCase();
      if (!pub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "public id required" }));
      }
      const db = getDb();
      db.prepare("DELETE FROM user_nodes WHERE user_id = ? AND public_id = ?").run(user.id, pub);
      const rows = db.prepare("SELECT public_id, nickname, created_at FROM user_nodes WHERE user_id = ? ORDER BY created_at DESC").all(user.id);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, nodes: rows }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/node-claim" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const pub = String(body.publicNodeId || body.pub || "").trim().toUpperCase();
      const nickname = String(body.nickname || "").trim();
      if (!pub || pub.length < 6) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "public id required" }));
      }
      const db = getDb();
      const rows = claimUserNode(db, user.id, pub, nickname);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, nodes: rows }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/channels/popular") {
    const limit = Number(u.searchParams.get("limit") || 6);
    const user = getSessionUser(req);
    const exclude = user ? getUserChannels(user.id) : [];
    const blockedSet = loadBlockedChannels(getDb());
    const popular = buildPopularChannels(limit, exclude, blockedSet);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, popular }));
  }

  if (u.pathname === "/api/channels/create" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const group = String(body.group || "").trim();
      const name = normalizeChannelName(body.name || "");
      const code = String(body.code || "").trim();
      const emojiRaw = String(body.emoji || "").trim();
      const emoji = emojiRaw || "💬";
      const allowPopular = user.isAdmin ? (body.allowPopular !== false) : true;
      if (!CHANNEL_GROUPS.includes(group)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid group" }));
      }
      if (!name || !code) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "name and code required" }));
      }
      const blockedSet = loadBlockedChannels(getDb());
      if (blockedSet.has(name) && !user.isAdmin) {
        return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel is blocked" }));
      }
      const db = getDb();
      const existing = db.prepare("SELECT name FROM channels_catalog WHERE lower(name) = lower(?)").get(name);
      if (existing) {
        return send(res, 409, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel already exists" }));
      }
      const now = new Date().toISOString();
      db.prepare(`
        INSERT INTO channels_catalog (name, code, emoji, group_name, allow_popular, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(name, code, emoji, group, allowPopular ? 1 : 0, user.id, now);
      const payload = await buildChannelDirectory(getUserChannels(user.id));
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, directory: payload }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/channel-directory") {
    const user = getSessionUser(req);
    const payload = await buildChannelDirectory(user ? getUserChannels(user.id) : [], !!user?.isAdmin);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/geoscore/observers") {
    const profiles = readGeoscoreObserverProfiles();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, profiles, count: profiles.length }));
  }

  if (u.pathname === "/api/geoscore/status") {
    const status = readGeoscoreStatus();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, status }));
  }

  if (u.pathname === "/api/geoscore/diagnostics") {
    const diagnostics = readGeoscoreDiagnostics();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, diagnostics }));
  }

  if (u.pathname === "/api/health") {
    try {
      const db = getDb();
      const metrics = readIngestMetrics(db);
      const parsedMetrics = {
        countAdvertsSeenLast10m: metrics.countAdvertsSeenLast10m ? Number(metrics.countAdvertsSeenLast10m) : 0,
        lastAdvertSeenAtIso: metrics.lastAdvertSeenAtIso || null
      };
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        ok: true,
        dbInfo: dbInfoCache || {
          resolvedPath: path.resolve(dbPath),
          cwd: process.cwd(),
          databaseList: db.pragma("database_list") || [],
          mainFile: db.pragma("database_list")?.[0]?.file || null
        },
        ingestMetrics: parsedMetrics
      }));
    } catch (err) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/geoscore/rebuild-observer-profiles" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user?.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
    }
    try {
      const result = refreshGeoscoreObserverProfiles();
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, rebuilt: result.count }));
    } catch (err) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/geoscore/debug") {
    const targets = [
      { key: "observers", primary: observersPath, fallback: observersPathAlt },
      { key: "devices", primary: devicesPath, fallback: devicesPathAlt },
      { key: "db", primary: dbPath }
    ];
    const detail = targets.map((target) => {
      const primary = path.resolve(target.primary || "");
      const fallback = target.fallback ? path.resolve(target.fallback) : null;
      const nodes = [primary];
      if (fallback && fallback !== primary) nodes.push(fallback);
      return nodes.map((filePath) => {
        const exists = fs.existsSync(filePath);
        const stats = exists ? fs.statSync(filePath) : null;
        let head = null;
        if (exists && (target.key === "observers" || target.key === "devices")) {
          const json = readJsonSafe(filePath, {});
          head = Array.isArray(json)
            ? json.slice(0, 2)
            : Object.keys(json).slice(0, 2);
        }
        return {
          path: filePath,
          exists,
          size: stats ? stats.size : null,
          head
        };
      });
    });
    return send(
      res,
      200,
      "application/json; charset=utf-8",
      JSON.stringify({
        ok: true,
        cwd: process.cwd(),
        dir: __dirname,
        targets: detail
      })
    );
  }

  if (u.pathname === "/api/channels/update" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "not authorized" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const name = normalizeChannelName(body.name || "");
      const group = String(body.group || "").trim();
      const code = String(body.code || "").trim();
      const emojiRaw = String(body.emoji || "").trim();
      const emoji = emojiRaw || "💬";
      const allowPopular = body.allowPopular !== false;
      if (!name || !CHANNEL_GROUPS.includes(group)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid channel or group" }));
      }
      const db = getDb();
      const existing = db.prepare("SELECT name, code FROM channels_catalog WHERE lower(name) = lower(?)").get(name);
      const now = new Date().toISOString();
      if (existing) {
        const nextCode = code || existing.code || "unknown";
        db.prepare(`
          UPDATE channels_catalog
          SET code = ?, emoji = ?, group_name = ?, allow_popular = ?
          WHERE lower(name) = lower(?)
        `).run(nextCode, emoji, group, allowPopular ? 1 : 0, name);
      } else {
        db.prepare(`
          INSERT INTO channels_catalog (name, code, emoji, group_name, allow_popular, created_by, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(name, code || "unknown", emoji, group, allowPopular ? 1 : 0, user.id, now);
      }
      const payload = await buildChannelDirectory(getUserChannels(user.id));
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, directory: payload }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/user/channels") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    if (req.method === "GET") {
      const blockedSet = loadBlockedChannels(getDb());
      let channels = await buildUserChannelSummary(user.id);
      if (!user.isAdmin) {
        channels = (channels || []).filter((ch) => !blockedSet.has(normalizeChannelName(ch.name || ch.id)));
      }
      const names = getUserChannels(user.id);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channels, userChannels: names }));
    }
    return send(res, 405, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "method not allowed" }));
  }

  if (u.pathname === "/api/user/channels/join" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const channelName = normalizeChannelName(body.channelName || body.channel);
      if (!channelName) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
      }
      const blockedSet = loadBlockedChannels(getDb());
      if (blockedSet.has(channelName) && !user.isAdmin) {
        return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel is blocked" }));
      }
      const now = new Date().toISOString();
      const db = getDb();
      db.prepare("INSERT OR IGNORE INTO user_channels (user_id, channel_name, created_at) VALUES (?, ?, ?)")
        .run(user.id, channelName, now);
      const channels = await buildUserChannelSummary(user.id);
      const names = getUserChannels(user.id);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channels, userChannels: names }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/user/channels/leave" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const channelName = normalizeChannelName(body.channelName || body.channel);
      if (!channelName) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
      }
      if (channelName === "#public" || channelName === "#meshranksuggestions") {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "#public cannot be removed" }));
      }
      const db = getDb();
      db.prepare("DELETE FROM user_channels WHERE user_id = ? AND lower(channel_name) = lower(?)")
        .run(user.id, channelName);
      const channels = await buildUserChannelSummary(user.id);
      const names = getUserChannels(user.id);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channels, userChannels: names }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/user-channels") {
    // Backward-compatible alias
    const user = getSessionUser(req);
    if (!user) {
      return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "login required" }));
    }
    if (req.method === "GET") {
      const channels = await buildUserChannelSummary(user.id);
      const names = getUserChannels(user.id);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channels, userChannels: names }));
    }
    if (req.method === "POST") {
      try {
        const raw = await readBody(req);
        const body = JSON.parse(raw || "{}");
        const channelName = normalizeChannelName(body.channel || body.channelName);
        if (!channelName) {
          return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
        }
        const now = new Date().toISOString();
        const db = getDb();
        db.prepare("INSERT OR IGNORE INTO user_channels (user_id, channel_name, created_at) VALUES (?, ?, ?)")
          .run(user.id, channelName, now);
        const channels = await buildUserChannelSummary(user.id);
        const names = getUserChannels(user.id);
        return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channels, userChannels: names }));
      } catch (err) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
      }
    }
    if (req.method === "DELETE") {
      const channelName = normalizeChannelName(u.searchParams.get("channel"));
      if (!channelName) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
      }
      const db = getDb();
      db.prepare("DELETE FROM user_channels WHERE user_id = ? AND channel_name = ?").run(user.id, channelName);
      const channels = await buildUserChannelSummary(user.id);
      const names = getUserChannels(user.id);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channels, userChannels: names }));
    }
    return send(res, 405, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "method not allowed" }));
  }

  if (u.pathname === "/api/channels/move" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "not authorized" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const name = normalizeChannelName(body.name || "");
      const group = String(body.group || "").trim();
      if (!name || !CHANNEL_GROUPS.includes(group)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid channel or group" }));
      }
      const db = getDb();
      const existing = db.prepare("SELECT name FROM channels_catalog WHERE lower(name) = lower(?)").get(name);
      const now = new Date().toISOString();
      if (existing) {
        db.prepare("UPDATE channels_catalog SET group_name = ? WHERE lower(name) = lower(?)").run(group, name);
      } else {
        db.prepare(`
          INSERT INTO channels_catalog (name, code, emoji, group_name, created_by, created_at)
          VALUES (?, ?, ?, ?, ?, ?)
        `).run(name, "unknown", "💬", group, user.id, now);
      }
      const payload = await buildChannelDirectory(getUserChannels(user.id));
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, directory: payload }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/channels/block" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const name = normalizeChannelName(body.name || body.channelName || body.channel);
      if (!name) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
      }
      const db = getDb();
      const now = new Date().toISOString();
      db.prepare("INSERT OR REPLACE INTO channel_blocks (name, blocked_by, created_at) VALUES (?, ?, ?)")
        .run(name, user.id, now);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/channels/unblock" && req.method === "POST") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const name = normalizeChannelName(body.name || body.channelName || body.channel);
      if (!name) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
      }
      const db = getDb();
      db.prepare("DELETE FROM channel_blocks WHERE lower(name) = lower(?)").run(name);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/channels") {
    if (req.method === "GET") {
      const user = getSessionUser(req);
      const blockedSet = loadBlockedChannels(getDb());
      let channels = await buildChannelSummary();
      if (!user?.isAdmin) {
        channels = (channels || []).filter((ch) => !blockedSet.has(normalizeChannelName(ch.name || ch.id)));
      }
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ channels }));
    }
    if (req.method === "DELETE") {
      try {
        const user = getSessionUser(req);
        if (!user || !user.isAdmin) {
          return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
        }
        const name = String(u.searchParams.get("name") || "").trim();
        if (!name) {
          return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "name required" }));
        }
        const cfg = loadKeys();
        const before = cfg.channels || [];
        const after = before.filter((c) => String(c.name || "") !== name);
        cfg.channels = after;
        saveKeys(cfg);
        return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, removed: before.length - after.length }));
      } catch (err) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
      }
    }
    if (req.method === "POST") {
      if (!ChannelCrypto) {
        return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel crypto unavailable" }));
      }
      try {
        const raw = await readBody(req);
        const body = JSON.parse(raw || "{}");
        const name = String(body.name || "").trim();
        const secretHex = String(body.secretHex || "").trim();
        if (!name.startsWith("#")) {
          return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "name must start with #" }));
        }
        if (!/^[0-9a-fA-F]{32}$/.test(secretHex)) {
          return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "secret must be 32 hex chars" }));
        }

        const hashByte = ChannelCrypto.calculateChannelHash(secretHex).toUpperCase();
        const cfg = loadKeys();
        const exists = (cfg.channels || []).some((c) =>
          String(c.secretHex || "").toUpperCase() === secretHex.toUpperCase() ||
          (String(c.name || "") === name && String(c.hashByte || "").toUpperCase() === hashByte)
        );

        if (!exists) {
          cfg.channels = cfg.channels || [];
          cfg.channels.push({ hashByte, name, secretHex });
          saveKeys(cfg);
        }
        return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, hashByte }));
      } catch (err) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
      }
    }
    return send(res, 405, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "method not allowed" }));
  }

  if (u.pathname === "/api/message-stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-store, must-revalidate",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no"
    });
    res.write("\n");
    let closed = false;
    const db = getDb();
    let lastRowId = 0;
    try {
      const row = db.prepare("SELECT MAX(rowid) AS max_id FROM message_observers").get();
      lastRowId = row?.max_id || 0;
    } catch {}
    const sendEvent = (name, payload) => {
      if (closed) return;
      res.write(`event: ${name}\n`);
      res.write(`data: ${JSON.stringify(payload || {})}\n\n`);
    };
    sendEvent("ready", { ok: true, lastRowId });
    let countersBusy = false;
    let ranksBusy = false;
    const sendCounters = async () => {
      if (closed || countersBusy) return;
      countersBusy = true;
      try {
        maybeUpdateStatsRollup();
        const channels = await buildChannelSummary();
        const rotm = await buildRotmData();
        const stats = readLatestStatsRollup();
        sendEvent("counters", { ts: Date.now(), channels, rotm, stats });
      } catch (err) {
        sendEvent("error", { error: String(err?.message || err) });
      } finally {
        countersBusy = false;
      }
    };
    const sendRanks = async () => {
      if (closed || ranksBusy) return;
      ranksBusy = true;
      try {
        const [repeater, node, observer, meshscore] = await Promise.all([
          buildRepeaterRankSummary(),
          buildNodeRankSummary(),
          buildObserverRankSummary(),
          refreshMeshScoreCache(false)
        ]);
        sendEvent("ranks", { ts: Date.now(), repeater, node, observer, meshscore });
      } catch (err) {
        sendEvent("error", { error: String(err?.message || err) });
      } finally {
        ranksBusy = false;
      }
    };
    sendCounters().catch(() => {});
    sendRanks().catch(() => {});
    const countersInterval = setInterval(() => {
      sendCounters().catch(() => {});
    }, MESSAGE_STREAM_COUNTERS_MS);
    const ranksInterval = setInterval(() => {
      sendRanks().catch(() => {});
    }, MESSAGE_STREAM_RANKS_MS);
    const healthInterval = setInterval(() => {
      sendEvent("health", { ts: Date.now(), uptime: Math.round(process.uptime()) });
    }, MESSAGE_STREAM_HEALTH_MS);
    const poll = setInterval(() => {
      if (closed) return;
      try {
        const { updates, lastRowId: nextRowId } = readMessageObserverUpdatesSince(
          db,
          lastRowId,
          MESSAGE_OBSERVER_STREAM_MAX_ROWS
        );
        if (updates.length) {
          lastRowId = nextRowId;
          const hashes = updates.map((u) => String(u.messageHash || "").toUpperCase()).filter(Boolean);
          const pathRows = fetchGeoscorePathRows(db, hashes);
          pathRows.forEach((row) => {
            const tokens = row.path_text
              ? parsePathTokens(row.path_text)
              : parsePathJsonTokens(row.path_json);
            if (!tokens.length) return;
            const msgKey = String(row.message_hash || row.frame_hash || "").toUpperCase();
            if (!msgKey) return;
            const tsMs = Number.isFinite(row.ts_ms) ? row.ts_ms : (row.ts ? Date.parse(row.ts) : null);
            enqueueGeoscoreRoute({
              msgKey,
              ts: Number.isFinite(tsMs) ? tsMs : Date.now(),
              observerId: String(row.observer_id || "").toUpperCase(),
              pathTokens: tokens
            });
          });
          recordPacketEvents(updates.length);
          sendEvent("packet", { updates, lastRowId });
        } else {
          lastRowId = nextRowId;
        }
      } catch (err) {
        sendEvent("error", { error: String(err?.message || err) });
      }
    }, MESSAGE_OBSERVER_STREAM_POLL_MS);
    const ping = setInterval(() => {
      if (!closed) res.write("event: ping\ndata: {}\n\n");
    }, MESSAGE_OBSERVER_STREAM_PING_MS);
    req.on("close", () => {
      closed = true;
      clearInterval(countersInterval);
      clearInterval(ranksInterval);
      clearInterval(healthInterval);
      clearInterval(poll);
      clearInterval(ping);
    });
    return;
  }

  if (u.pathname === "/api/messages") {
    const channel = u.searchParams.get("channel");
    const limitRaw = u.searchParams.get("limit");
    const beforeRaw = u.searchParams.get("before");
    const limit = limitRaw ? Number(limitRaw) : null;
    let beforeTs = null;
    if (beforeRaw) {
      const beforeDate = parseIso(beforeRaw);
      if (beforeDate) beforeTs = beforeDate.getTime();
    }
    const user = getSessionUser(req);
    const db = getDb();
    const blockedSet = loadBlockedChannels(db);
    const restrictChannels = !channel && user ? getUserChannels(user.id) : null;
    const payload = await buildMessagesPayload({
      channel,
      limit,
      beforeTs,
      restrictChannels,
      blockedSet,
      includeBlocked: !!user?.isAdmin
    });
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/rotm") {
    const payload = await buildRotmData();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/rotm-config") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "not authorized" }));
    }
    if (req.method === "GET") {
      const cfg = readRotmConfig();
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channel: normalizeRotmChannel(cfg.channel || "#rotm") }));
    }
    if (req.method === "POST") {
      if (!ChannelCrypto) {
        return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel crypto unavailable" }));
      }
      try {
        const raw = await readBody(req);
        const body = JSON.parse(raw || "{}");
        const nameRaw = String(body.channel || "").trim();
        const secretHex = String(body.secretHex || "").trim();
        if (!nameRaw) {
          return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "channel required" }));
        }
        const name = normalizeRotmChannel(nameRaw);
        let hashByte = null;
        if (secretHex) {
          if (!/^[0-9a-fA-F]{32}$/.test(secretHex)) {
            return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "secret must be 32 hex chars" }));
          }
          hashByte = ChannelCrypto.calculateChannelHash(secretHex).toUpperCase();
          const cfg = loadKeys();
          const exists = (cfg.channels || []).some((c) =>
            String(c.secretHex || "").toUpperCase() === secretHex.toUpperCase() ||
            (String(c.name || "") === name && String(c.hashByte || "").toUpperCase() === hashByte)
          );
          if (!exists) {
            cfg.channels = cfg.channels || [];
            cfg.channels.push({ hashByte, name, secretHex });
            saveKeys(cfg);
          }
        }
        writeJsonSafe(rotmConfigPath, { channel: name });
        rotmCache = { builtAt: 0, payload: null };
        return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, channel: name, hashByte }));
      } catch (err) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
      }
    }
    return send(res, 405, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "method not allowed" }));
  }

  if (u.pathname === "/api/repeater-rank") {
    const now = Date.now();
    const last = rankCache.updatedAt ? new Date(rankCache.updatedAt).getTime() : 0;
    const force = u.searchParams.get("refresh") === "1";
    const limitRaw = u.searchParams.get("_limit");
    const skipRaw = u.searchParams.get("_skip");
    const limit = limitRaw ? Number(limitRaw) : 0;
    const skip = skipRaw ? Number(skipRaw) : 0;
    if (force || !last) {
      await refreshRankCache(true);
    } else if (now - last >= RANK_REFRESH_MS) {
      refreshRankCache(false).catch(() => {});
    }
    let payloadItems = rankCache.items;
    if (limit > 0) {
      const start = Number.isFinite(skip) && skip > 0 ? skip : 0;
      payloadItems = rankCache.items.slice(start, start + limit);
    }
    const payload = limit > 0
      ? { ...rankCache, items: payloadItems }
      : rankCache;
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }
  if (u.pathname === "/api/repeater-rank-excluded") {
    const now = Date.now();
    const last = rankCache.updatedAt ? new Date(rankCache.updatedAt).getTime() : 0;
    const force = u.searchParams.get("refresh") === "1";
    const limitRaw = u.searchParams.get("limit");
    const limit = limitRaw ? Number(limitRaw) : 100;
    if (force || !last) {
      await refreshRankCache(true);
    } else if (now - last >= RANK_REFRESH_MS) {
      refreshRankCache(false).catch(() => {});
    }
    const excluded = Array.isArray(rankCache.excluded) ? rankCache.excluded : [];
    const payload = {
      updatedAt: rankCache.updatedAt,
      count: excluded.length,
      excluded: limit > 0 ? excluded.slice(0, limit) : excluded
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }
  if (u.pathname === "/api/node-rank") {
    const now = Date.now();
    const last = nodeRankCache.updatedAt ? new Date(nodeRankCache.updatedAt).getTime() : 0;
    const force = u.searchParams.get("refresh") === "1";
    const limitRaw = u.searchParams.get("_limit");
    const limit = limitRaw ? Number(limitRaw) : 0;
    if (force || !last) {
      await refreshNodeRankCache(true);
    } else if (now - last >= NODE_RANK_REFRESH_MS) {
      refreshNodeRankCache(false).catch(() => {});
    }
    const payload = limit > 0
      ? { ...nodeRankCache, items: nodeRankCache.items.slice(0, limit) }
      : nodeRankCache;
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }
  if (u.pathname === "/api/node-rank-summary") {
    const now = Date.now();
    const last = nodeRankCache.updatedAt ? new Date(nodeRankCache.updatedAt).getTime() : 0;
    if (!last || now - last >= NODE_RANK_REFRESH_MS) {
      await refreshNodeRankCache(true);
    }
    const summary = {
      updatedAt: nodeRankCache.updatedAt,
      count: nodeRankCache.count,
      totals: {
        active: nodeRankCache.items.filter((n) => Number.isFinite(n.ageHours) && n.ageHours < 24).length,
        messages24h: nodeRankCache.items.reduce((sum, n) => sum + (n.messages24h || 0), 0)
      }
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(summary));
  }
  if (u.pathname === "/api/repeater-rank-summary") {
    const now = Date.now();
    const last = rankSummaryCache.updatedAt ? new Date(rankSummaryCache.updatedAt).getTime() : 0;
    if (!last || now - last >= RANK_REFRESH_MS) {
      await refreshRankCache(true);
    }
    const summary = {
      updatedAt: rankSummaryCache.updatedAt,
      count: rankSummaryCache.totals ? rankSummaryCache.totals.total : rankCache.count,
      totals: rankSummaryCache.totals || { total: rankCache.count, active: 0, total24h: 0 }
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(summary));
  }

  if (u.pathname === "/api/repeater-rank-history") {
    try {
      const limit = Number(u.searchParams.get("limit") || 100);
      const db = getDb();
      const rows = db.prepare(
        `SELECT recorded_at, total, active, total24h, cached_at
         FROM repeater_rank_history
         ORDER BY recorded_at DESC
         LIMIT ?`
      ).all(limit);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, history: rows }));
    } catch (err) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/observer-hop-diagnostics") {
    const observerId = String(u.searchParams.get("observerId") || "").trim().toUpperCase();
    if (!observerId) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "observerId required" }));
    }
    const limitRaw = Number(u.searchParams.get("limit") || 50);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 50;
    const db = getDb();
    if (!hasMessageObserversDb(db)) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "message_observers table unavailable" }));
    }
  let { linkedPub, linkedHash, source: linkSource } = getObserverLink(observerId);
  if (!linkedPub) {
    const profileRow = db.prepare("SELECT linked_repeater_pub, source FROM geoscore_observer_profiles WHERE observer_id = ? LIMIT 1").get(observerId);
    const fallbackPub = sanitizeRepeaterPub(profileRow?.linked_repeater_pub || "");
    if (fallbackPub) {
      linkedPub = fallbackPub;
      linkedHash = nodeHashFromPub(fallbackPub);
      linkSource = profileRow?.source || "geoscore_observer_profiles";
    } else {
      linkSource = linkSource || "missing";
    }
  }
  const rows = db.prepare(`
    SELECT mo.message_hash, mo.ts, mo.path_json, mo.path_text, mo.path_length, m.frame_hash
    FROM message_observers mo
      LEFT JOIN messages m ON m.message_hash = mo.message_hash
      WHERE mo.observer_id = ?
      ORDER BY mo.rowid DESC
      LIMIT ?
    `).all(observerId, limit);
    const events = rows.map((row) => {
      const tokens = row.path_json
        ? parsePathJsonTokens(row.path_json)
        : parsePathTokens(row.path_text);
      const hasLinkedHash = Boolean(linkedHash);
      let computedHopCount = null;
      let directHeard = false;
      let hopNote = null;
      if (!tokens.length) {
        computedHopCount = 0;
        directHeard = true;
        hopNote = "empty_path";
      } else if (hasLinkedHash && tokens[0] === linkedHash) {
        computedHopCount = 0;
        directHeard = true;
        hopNote = "linked_first";
      } else if (hasLinkedHash) {
        const idx = tokens.indexOf(linkedHash);
        if (idx >= 0) {
          computedHopCount = Math.max(0, idx);
          directHeard = idx === 0;
        } else {
          computedHopCount = tokens.length;
          hopNote = "linked_missing";
        }
      } else {
        computedHopCount = tokens.length;
        hopNote = "no_linked_hash";
      }
      return {
        msg_key: String((row.message_hash || row.frame_hash || "")).toUpperCase(),
        ts: row.ts || null,
        pathTokens: tokens,
        pathLength: Number.isFinite(row.path_length) ? row.path_length : tokens.length,
        linkedPub,
        linkedHash,
        computedHopCount,
        directHeard,
        hopNote
      };
    });
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
      ok: true,
      observerId,
      linkedPub,
      linkedHash,
      linkSource,
      count: events.length,
      events
    }));
  }

  if (u.pathname === "/api/mesh-live") {
    const now = Date.now();
    const last = rankCache.updatedAt ? new Date(rankCache.updatedAt).getTime() : 0;
    const force = u.searchParams.get("refresh") === "1";
    if (force || !last) {
      await refreshRankCache(true);
    } else if (now - last >= RANK_REFRESH_MS) {
      refreshRankCache(false).catch(() => {});
    }
    const payload = await buildMeshHeatPayload();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, ...payload }));
  }

  if (u.pathname === "/api/repeater-hide" && req.method === "POST") {
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const pub = String(body.pub || "").trim().toUpperCase();
      const hidden = !!body.hidden;
      if (!pub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub required" }));
      }
      const devices = readDevices();
      const byPub = devices.byPub || {};
      if (!byPub[pub]) {
        return send(res, 404, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub not found" }));
      }
      byPub[pub].hiddenOnMap = hidden;
      const updatedAt = new Date().toISOString();
      devices.updatedAt = updatedAt;
      const tmp = devicesPath + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(devices, null, 2));
      fs.renameSync(tmp, devicesPath);
      try {
        const db = getDb();
        db.prepare(`
          INSERT INTO devices (pub, hidden_on_map, updated_at)
          VALUES (?, ?, ?)
          ON CONFLICT(pub) DO UPDATE SET
            hidden_on_map = excluded.hidden_on_map,
            updated_at = excluded.updated_at
        `).run(pub, hidden ? 1 : 0, updatedAt);
      } catch {}
      devicesCache = { readAt: 0, data: null };
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, hidden }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/rotm-override") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
    }
    if (req.method !== "POST") {
      return send(res, 405, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "method not allowed" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const cqId = Number(body.cqId);
      const hash = String(body.hash || "").toUpperCase();
      const pub = String(body.pub || "").toUpperCase();
      if (!Number.isFinite(cqId) || !hash || !pub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid payload" }));
      }
      const overrides = readRotmOverrides();
      overrides[cqId] = { hash, pub };
      writeJsonSafe(rotmOverridesPath, overrides);
      rotmCache = { builtAt: 0, payload: null };
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
    } catch (err) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "override failed" }));
    }
  }

  if (u.pathname === "/api/zero-hop-override") {
    const user = getSessionUser(req);
    if (!user || !user.isAdmin) {
      return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
    }
    if (req.method !== "POST") {
      return send(res, 405, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "method not allowed" }));
    }
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const targetPub = String(body.targetPub || "").toUpperCase();
      const hash = String(body.hash || "").toUpperCase();
      const pub = String(body.pub || "").toUpperCase();
      if (!targetPub || !hash || !pub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid payload" }));
      }
      const overrides = readZeroHopOverrides();
      overrides[`${targetPub}:${hash}`] = { pub };
      writeJsonSafe(zeroHopOverridesPath, overrides);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
    } catch (err) {
      return send(res, 500, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "override failed" }));
    }
  }

  if (u.pathname === "/api/repeater-flag" && req.method === "POST") {
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const pub = String(body.pub || "").trim().toUpperCase();
      const flagged = body.flagged === false ? false : true;
      if (!pub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub required" }));
      }
      const devices = readDevices();
      const byPub = devices.byPub || {};
      if (!byPub[pub]) {
        return send(res, 404, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub not found" }));
      }
      const entry = byPub[pub];
      entry.gpsFlagged = flagged;
      entry.gpsFlaggedAt = flagged ? new Date().toISOString() : null;
      entry.gpsImplausible = flagged;
      entry.gpsEstimated = false;
      entry.gpsEstimateAt = null;
      entry.gpsEstimateNeighbors = null;
      entry.gpsEstimateReason = null;
      entry.hiddenOnMap = flagged;
      byPub[pub] = entry;
      devices.byPub = byPub;
      devices.updatedAt = new Date().toISOString();
      writeJsonSafe(devicesPath, devices);
      try {
        const db = getDb();
        db.prepare(`
          INSERT INTO devices (pub, hidden_on_map, updated_at)
          VALUES (?, ?, ?)
          ON CONFLICT(pub) DO UPDATE SET
            hidden_on_map = excluded.hidden_on_map,
            updated_at = excluded.updated_at
        `).run(pub, flagged ? 1 : 0, devices.updatedAt);
      } catch {}
      devicesCache = { readAt: 0, data: null };
      rankCache = { updatedAt: null, count: 0, items: [], excluded: [], cachedAt: null };
      rankSummaryCache = { updatedAt: null, totals: null };
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, flagged }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/repeater-location" && req.method === "POST") {
    try {
      const user = getSessionUser(req);
      if (!user || !user.isAdmin) {
        return send(res, 403, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "admin required" }));
      }
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const pub = String(body.pub || "").trim().toUpperCase();
      const lat = Number(body.lat);
      const lon = Number(body.lon);
      if (!pub || !Number.isFinite(lat) || !Number.isFinite(lon)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub, lat, lon required" }));
      }
      const devices = readDevices();
      const byPub = devices.byPub || {};
      const entry = byPub[pub] || { pub };
      entry.gps = { lat, lon };
      entry.locSource = "manual";
      entry.manualLocation = true;
      entry.gpsEstimated = false;
      entry.gpsEstimateAt = null;
      entry.gpsEstimateNeighbors = null;
      entry.gpsEstimateReason = null;
      if (entry.gpsFlagged) entry.gpsImplausible = true;
      entry.hiddenOnMap = false;
      byPub[pub] = entry;
      devices.byPub = byPub;
      devices.updatedAt = new Date().toISOString();
      writeJsonSafe(devicesPath, devices);
      try {
        const db = getDb();
        db.prepare(`
          INSERT INTO devices (
            pub, name, is_repeater, is_observer, last_seen, observer_last_seen,
            gps_lat, gps_lon, raw_json, hidden_on_map, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(pub) DO UPDATE SET
            gps_lat = excluded.gps_lat,
            gps_lon = excluded.gps_lon,
            updated_at = excluded.updated_at
        `).run(
          pub,
          entry.name || null,
          entry.isRepeater ? 1 : 0,
          entry.isObserver ? 1 : 0,
          entry.lastSeen || null,
          entry.observerLastSeen || null,
          lat,
          lon,
          entry.raw ? JSON.stringify(entry.raw) : null,
          entry.hiddenOnMap ? 1 : 0,
          devices.updatedAt
        );
      } catch {}
      devicesCache = { readAt: 0, data: null };
      rankCache = { updatedAt: null, count: 0, items: [], excluded: [], cachedAt: null };
      rankSummaryCache = { updatedAt: null, totals: null };
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/meshscore") {
    const now = Date.now();
    const last = meshScoreCache.updatedAt ? new Date(meshScoreCache.updatedAt).getTime() : 0;
    if (!meshScoreCache.payload) {
      const payload = await refreshMeshScoreCache(true);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
    }
    if (!last || (now - last) >= MESH_REFRESH_MS) {
      refreshMeshScoreCache(false).catch(() => {});
    }
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(meshScoreCache.payload));
  }

  if (u.pathname === "/api/observers") {
    const payload = readObservers();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/observer-location" && req.method === "POST") {
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const id = String(body.id || "").trim();
      const lat = Number(body.lat);
      const lon = Number(body.lon);
      if (!id || !Number.isFinite(lat) || !Number.isFinite(lon)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "id, lat, lon required" }));
      }
      const data = readObservers();
      const byId = data.byId || {};
      const entry = byId[id] || { id, firstSeen: new Date().toISOString(), count: 0 };
      entry.gps = { lat, lon };
      entry.locSource = "manual";
      entry.manualLocation = true;
      entry.locValidated = true;
      entry.locValidatedAt = new Date().toISOString();
      entry.gpsApprox = null;
      entry.locApprox = false;
      entry.locApproxAt = null;
      byId[id] = entry;
      const updatedAt = new Date().toISOString();
      data.byId = byId;
      data.updatedAt = updatedAt;
      writeJsonSafe(observersPath, data);
      try {
        const db = getDb();
        db.prepare(`
          INSERT INTO observers (
            observer_id, name, first_seen, last_seen, count, gps_lat, gps_lon, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(observer_id) DO UPDATE SET
            name = COALESCE(observers.name, excluded.name),
            first_seen = COALESCE(observers.first_seen, excluded.first_seen),
            last_seen = COALESCE(observers.last_seen, excluded.last_seen),
            count = COALESCE(observers.count, excluded.count),
            gps_lat = excluded.gps_lat,
            gps_lon = excluded.gps_lon,
            updated_at = excluded.updated_at
        `).run(
          id,
          entry.name || null,
          entry.firstSeen || null,
          entry.lastSeen || null,
          entry.count || 0,
          lat,
          lon,
          updatedAt
        );
      } catch {}
      observersCache = { readAt: 0, data: null };
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/observer-debug") {
    const id = u.searchParams.get("id");
    const payload = await buildObserverDebug(id);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/message-debug") {
    const hash = u.searchParams.get("hash");
    const payload = await buildMessageDebug(hash);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/message-routes") {
    const hash = u.searchParams.get("hash");
    const hours = u.searchParams.get("hours");
    const payload = await buildMessageRouteHistory(hash, hours);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }
  if (u.pathname === "/api/confidence-history") {
    const sender = u.searchParams.get("sender");
    const channel = u.searchParams.get("channel");
    const hours = u.searchParams.get("hours");
    const limit = u.searchParams.get("limit");
    const payload = await buildConfidenceHistory(sender, channel, hours, limit);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/observer-rank") {
    const now = Date.now();
    const last = observerRankCache.updatedAt ? new Date(observerRankCache.updatedAt).getTime() : 0;
    const force = u.searchParams.get("refresh") === "1";
    const limitRaw = u.searchParams.get("_limit");
    const limit = limitRaw ? Number(limitRaw) : 0;
    if (!observerRankCache.items?.length) {
      await refreshObserverRankCache(true);
    } else if (force) {
      await refreshObserverRankCache(true);
    } else if (!last || (now - last) >= OBSERVER_RANK_REFRESH_MS) {
      refreshObserverRankCache(false).catch(() => {});
    }
    const payload = limit > 0
      ? { ...observerRankCache, items: observerRankCache.items.slice(0, limit) }
      : observerRankCache;
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }
  if (u.pathname === "/api/observer-rank-summary") {
    const now = Date.now();
    const last = observerRankCache.updatedAt ? new Date(observerRankCache.updatedAt).getTime() : 0;
    if (!observerRankCache.items?.length) {
      await refreshObserverRankCache(true);
    } else if (!last || (now - last) >= OBSERVER_RANK_REFRESH_MS) {
      refreshObserverRankCache(false).catch(() => {});
    }
    const summary = {
      updatedAt: observerRankCache.updatedAt,
      count: observerRankCache.items.length,
      totals: {
        active: observerRankCache.items.filter((o) => o.ageHours < OBSERVER_OFFLINE_HOURS).length,
        packetsToday: observerRankCache.items.reduce((sum, o) => sum + (o.packetsToday || 0), 0)
      }
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(summary));
  }

  if (u.pathname === "/api/route-suggestions") {
    const suggestions = getRouteSuggestions();
    const devices = readDevices();
    const byCode = calcSuggestionScores(suggestions, devices);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
      updatedAt: suggestions.updatedAt,
      byCode
    }));
  }

  if (u.pathname === "/api/route-suggest" && req.method === "POST") {
    try {
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const code = String(body.code || "").trim().toUpperCase();
      const repeaterPub = sanitizeRepeaterPub(body.repeaterPub || "");
      const repeaterName = String(body.repeaterName || "").trim();
      const voterId = String(body.voterId || "").trim();
      const pathGps = Array.isArray(body.pathGps) ? body.pathGps : [];
      if (!/^[0-9A-F]{2}$/.test(code)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid code" }));
      }
      if (!repeaterPub) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid repeater pub" }));
      }
      if (!/^[a-zA-Z0-9_-]{6,64}$/.test(voterId)) {
        return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid voter id" }));
      }
      const devices = readDevices();
      const byPub = devices.byPub || {};
      const candGps = byPub[repeaterPub]?.gps || null;
      const minDistanceKm = minDistanceKmFromPath(candGps, pathGps);

      const suggestions = getRouteSuggestions();
      if (!suggestions.byCode[code]) suggestions.byCode[code] = { candidates: {} };
      const bucket = suggestions.byCode[code];
      if (!bucket.candidates[repeaterPub]) {
        bucket.candidates[repeaterPub] = { name: repeaterName || byPub[repeaterPub]?.name || null, votes: 0, voters: {}, lastAt: null, minDistanceKm: null };
      }
      const cand = bucket.candidates[repeaterPub];
      cand.voters = cand.voters || {};
      if (!cand.voters[voterId]) {
        cand.voters[voterId] = 1;
        cand.votes = Number(cand.votes || 0) + 1;
      } else {
        cand.voters[voterId] += 1;
      }
      if (repeaterName) cand.name = repeaterName;
      if (Number.isFinite(minDistanceKm)) cand.minDistanceKm = minDistanceKm;
      cand.lastAt = new Date().toISOString();
      suggestions.updatedAt = cand.lastAt;
      writeJsonSafe(routeSuggestionsPath, suggestions);

      const byCode = calcSuggestionScores(suggestions, devices);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, best: byCode[code] || null }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/rf-latest") {
    const limit = Number(u.searchParams.get("limit") || 100);
    const payload = await buildRfLatest(limit);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  return send(res, 404, "text/plain; charset=utf-8", "Not found");
});

const exported = {
  refreshGeoscoreObserverProfiles,
  buildRepeaterRank,
  buildRepeatEvidenceMap,
  summarizeRepeatEvidence,
  classifyRepeaterQuality,
  isCompanionDevice,
  sanitizeRepeaterPub,
  nodeHashFromPub,
  buildGeoscoreRoutePayload,
  readGeoscoreStatus,
  parsePathTokens,
  parsePathJsonTokens,
  ensureGeoscoreRouteStmt,
  getDb,
  loadJsonFromPaths,
  devicesPath,
  observersPath,
  dbPath,
  isValidGps
};

if (require.main === module) {
  server.listen(port, host, () => {
    console.log(`(observer-demo) http://${host}:${port}`);
    startObserverHitsTailer();
    startStatsRollupUpdater();
    hydrateRepeaterRankCache();
    hydrateObserverRankCache();
    refreshRankCache(true).catch(() => {});
    hydrateMeshScoreCache();
    try {
      const updated = refreshGeoscoreObserverProfiles();
      console.log("(geoscore) observer profiles loaded", updated.count);
    } catch (err) {
      console.error("(geoscore) observer profile load failed", err);
    }
    // TODO M4: nightly recalibration + systemd timer hook
    scheduleAutoRefresh().catch(() => {});
  });
}

module.exports = exported;
