#!/usr/bin/env node
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const Database = require("better-sqlite3");
const bcrypt = require("bcryptjs");

const projectRoot = path.resolve(__dirname, "..", "..");
const dataDir = path.join(projectRoot, "data");
const devicesPath = path.join(dataDir, "devices.json");
const decodedPath = path.join(dataDir, "decoded.ndjson");
const rfPath = path.join(dataDir, "rf.ndjson");
const observerPath = path.join(dataDir, "observer.ndjson");
const observersPath = path.join(dataDir, "observers.json");
const ingestLogPath = path.join(dataDir, "ingest.log");
const routeSuggestionsPath = path.join(dataDir, "route_suggestions.json");
const indexPath = path.join(__dirname, "index.html");
const staticDir = __dirname;
const keysPath = path.join(projectRoot, "tools", "meshcore_keys.json");
const crypto = require("crypto");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");
const dbPath = path.join(dataDir, "meshrank.db");

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

const OBSERVER_HITS_MAX_BYTES = 2 * 1024 * 1024;
const OBSERVER_HITS_COOLDOWN_MS = 30 * 1000;
const OBSERVER_HITS_TAIL_INTERVAL_MS = 2000;
const MESSAGE_OBSERVER_STREAM_POLL_MS = 1000;
const MESSAGE_OBSERVER_STREAM_PING_MS = 15000;
const MESSAGE_OBSERVER_STREAM_MAX_ROWS = 200;
const RF_AGG_COOLDOWN_MS = 1500;
const RF_AGG_LIMIT = 800;
const REPEATER_FLAG_REVIEW_HOURS = 24;
const REPEATER_ESTIMATE_MIN_NEIGHBORS = 3;
const REPEATER_ESTIMATE_SINGLE_OFFSET_KM = 4.8; // ~3 miles

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

let rankCache = { updatedAt: null, count: 0, items: [], cachedAt: null };
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

let ChannelCrypto;
try {
  ({ ChannelCrypto } = require("@michaelhart/meshcore-decoder/dist/crypto/channel-crypto"));
} catch {
  ChannelCrypto = null;
}

const host = "0.0.0.0";
const port = 5199;
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
  if (row.path_json) {
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
        SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_length, repeats
        FROM messages
        WHERE channel_name = ? AND ts < ?
        ORDER BY ts DESC
        LIMIT ?
      `).all(channelName, before, max);
    } else {
      rows = db.prepare(`
        SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_length, repeats
        FROM messages
        WHERE channel_name = ?
        ORDER BY ts DESC
        LIMIT ?
      `).all(channelName, max);
    }
  } else {
    rows = db.prepare(`
      SELECT message_hash, frame_hash, channel_name, sender, body, ts, path_json, path_length, repeats
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
    SELECT message_hash, observer_id, observer_name, path_json
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
    if (row.path_json) {
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
    SELECT message_hash, observer_id, observer_name, path_json
    FROM message_observers
    WHERE message_hash IN (${placeholders})
  `).all(...list);
  const map = new Map();
  rows.forEach((row) => {
    const key = String(row.message_hash || "").toUpperCase();
    if (!key) return;
    if (!row.path_json) return;
    let parsed;
    try { parsed = JSON.parse(row.path_json); } catch { parsed = null; }
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

function readMessageObserverUpdatesSince(db, lastRowId, limit) {
  if (!hasMessageObserversDb(db)) return { updates: [], lastRowId };
  const max = Number.isFinite(limit) ? limit : MESSAGE_OBSERVER_STREAM_MAX_ROWS;
  const rows = db.prepare(`
    SELECT rowid, message_hash, observer_id, observer_name, path_length
    FROM message_observers
    WHERE rowid > ?
    ORDER BY rowid ASC
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
      entry = { messageHash: key, observerHits: new Set(), pathLength: 0 };
      map.set(key, entry);
    }
    const label = row.observer_name || row.observer_id;
    if (label) entry.observerHits.add(label);
    if (Number.isFinite(row.path_length)) {
      entry.pathLength = Math.max(entry.pathLength, row.path_length || 0);
    }
  });
  const updates = Array.from(map.values()).map((entry) => ({
    messageHash: entry.messageHash,
    observerHits: Array.from(entry.observerHits),
    pathLength: entry.pathLength || null
  }));
  return { updates, lastRowId: nextRowId };
}

function getDb() {
  if (db) return db;
  db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      is_admin INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      last_login TEXT
    );
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id INTEGER NOT NULL,
      expires_at TEXT NOT NULL
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
      path_length INTEGER,
      repeats INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_name, ts);
    CREATE TABLE IF NOT EXISTS message_observers (
      message_hash TEXT NOT NULL,
      observer_id TEXT NOT NULL,
      observer_name TEXT,
      ts TEXT,
      path_json TEXT,
      path_length INTEGER,
      PRIMARY KEY (message_hash, observer_id)
    );
    CREATE INDEX IF NOT EXISTS idx_message_observers_hash ON message_observers(message_hash);
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
  `);
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
    rankCache = { ...parsed, cachedAt: updatedAt };
    updateRankSummary(parsed.items, updatedAt);
  } catch {}
}

function persistRepeaterRankCache(payload) {
  try {
    if (!payload) return;
    const updatedAt = payload.updatedAt || new Date().toISOString();
    getDb()
      .prepare(`
        INSERT INTO repeater_rank_cache (id, updated_at, payload)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, payload=excluded.payload
      `)
      .run(updatedAt, JSON.stringify(payload));
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
    SELECT u.id, u.username, u.is_admin, s.expires_at
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
  return { id: row.id, username: row.username, isAdmin: !!row.is_admin };
}

function createSession(db, userId) {
  const token = crypto.randomBytes(24).toString("hex");
  const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
  db.prepare("INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)").run(token, userId, expiresAt);
  return { token, expiresAt };
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

function readDevicesJson() {
  return readJsonSafe(devicesPath, { byPub: {} });
}

function readObserversJson() {
  return readJsonSafe(observersPath, { byId: {} });
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
             gps_lat, gps_lon, raw_json, hidden_on_map, updated_at
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
        const entry = {
          pub,
          name: row.name || null,
          isRepeater: !!row.is_repeater,
          isObserver: !!row.is_observer,
          lastSeen: row.last_seen || null,
          observerLastSeen: row.observer_last_seen || null,
          gps,
          raw: entryRaw,
          hiddenOnMap: row.hidden_on_map ? true : false
        };
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

function parseIso(ts) {
  const d = new Date(ts);
  return Number.isFinite(d.getTime()) ? d : null;
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
    const prev = map.get(hash);
    if (!prev) {
      map.set(hash, { name, lastSeen, gps });
      continue;
    }
    const prevTs = prev.lastSeen ? new Date(prev.lastSeen).getTime() : 0;
    const nextTs = lastSeen ? new Date(lastSeen).getTime() : 0;
    const prefer = (gps && !prev.gps) || (nextTs >= prevTs);
    if (prefer) map.set(hash, { name, lastSeen, gps });
  }

  return map;
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
      SELECT message_hash, ts, path_json, path_length
      FROM messages
      WHERE sender = ? AND channel_name = ? AND ts >= ?
      ORDER BY ts DESC
      LIMIT ?
    `).all(senderName, channelName, cutoff, maxRows);
  } else {
    rows = db.prepare(`
      SELECT message_hash, ts, path_json, path_length
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
    if (!row?.path_json) return;
    let parsed;
    try {
      parsed = JSON.parse(row.path_json);
    } catch {
      parsed = null;
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
  for (const d of Object.values(byPub)) {
    if (!d?.isRepeater) continue;
    const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || null;
    if (!pub) continue;
    const hash = nodeHashFromPub(pub);
    if (!hash) continue;
    if (!repeatersByHash.has(hash)) repeatersByHash.set(hash, []);
    repeatersByHash.get(hash).push(pub);
  }

  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;

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
          s.zeroHopNeighbors.add(neighbor);
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

  const items = Object.entries(byPub)
    .filter(([, d]) => d && d.isRepeater)
    .map(([key, d]) => {
      const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || key;
      const s = stats.get(pub) || { total24h: 0, msgCounts: new Map(), rssi: [], snr: [], zeroHopNeighbors: new Set() };
      const lastSeen = s.lastSeenTs ? s.lastSeenTs.toISOString() : (d.lastSeen || null);
      const lastSeenDate = parseIso(lastSeen);
      const ageHours = lastSeenDate ? (now - lastSeenDate.getTime()) / 3600000 : Infinity;
        const isStale = ageHours > 48;
        // Keep stale repeaters visible even if lastSeen is old.
        const uniqueMsgs = s.msgCounts.size || 0;
        const avgRepeats = uniqueMsgs ? (s.total24h / uniqueMsgs) : 0;
        const zeroHopNeighborNames = s.zeroHopNeighbors
          ? Array.from(s.zeroHopNeighbors)
            .filter((hash) => {
              const node = nodeMap.get(hash);
              return !node?.hiddenOnMap && !node?.gpsImplausible;
            })
            .map((hash) => nodeMap.get(hash)?.name || hash)
            .filter(Boolean)
          : [];
        const zeroHopNeighborDetails = s.zeroHopNeighbors
          ? Array.from(s.zeroHopNeighbors)
            .filter((hash) => {
              const node = nodeMap.get(hash);
              return !node?.hiddenOnMap && !node?.gpsImplausible;
            })
            .map((hash) => {
              const node = nodeMap.get(hash);
              const rssiEntry = s.neighborRssi ? s.neighborRssi.get(hash) : null;
              const avg = rssiEntry && rssiEntry.count ? rssiEntry.sum / rssiEntry.count : null;
              return {
                hash,
                name: node?.name || hash,
                rssiAvg: Number.isFinite(avg) ? Number(avg.toFixed(1)) : null,
                rssiMax: rssiEntry && Number.isFinite(rssiEntry.max) ? Number(rssiEntry.max.toFixed(1)) : null
              };
            })
          : [];
        const zeroHopNeighbors24h = zeroHopNeighborDetails.length;

      const avgRssi = trimmedMean(s.rssi, 0.1);
      const driftMinutes = Number.isFinite(s.clockDriftMs) ? Math.round(s.clockDriftMs / 60000) : null;
      const avgSnr = trimmedMean(s.snr, 0.1);
      const bestRssi = Number.isFinite(s.bestRssi) ? s.bestRssi : (Number.isFinite(d.stats?.bestRssi) ? d.stats.bestRssi : -120);
      const bestSnr = Number.isFinite(s.bestSnr) ? s.bestSnr : (Number.isFinite(d.stats?.bestSnr) ? d.stats.bestSnr : -20);

      const rssiBase = Number.isFinite(avgRssi) ? avgRssi : bestRssi;
      const snrBase = Number.isFinite(avgSnr) ? avgSnr : bestSnr;
      const rssiScore = clamp((rssiBase + 120) / 70, 0, 1);
      const snrScore = clamp((snrBase + 20) / 30, 0, 1);
      const bestRssiScore = clamp((bestRssi + 120) / 70, 0, 1);
      const bestSnrScore = clamp((bestSnr + 20) / 30, 0, 1);
        const throughputScore = clamp(s.total24h / 50, 0, 1);
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

      return {
        pub,
        hashByte: nodeHashFromPub(pub),
        name: d.name || d.raw?.lastAdvert?.appData?.name || "Unknown",
        gps: effectiveGps,
        lastSeen,
        isObserver: !!d.isObserver,
        bestRssi,
        bestSnr,
        avgRssi: Number.isFinite(avgRssi) ? Number(avgRssi.toFixed(2)) : null,
        avgSnr: Number.isFinite(avgSnr) ? Number(avgSnr.toFixed(2)) : null,
          total24h: s.total24h,
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
        clockDriftMinutes: driftMinutes
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);

  return {
    updatedAt: new Date().toISOString(),
    count: items.length,
    items
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

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname === "/") {
    const html = fs.readFileSync(indexPath, "utf8");
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

  if (u.pathname === "/api/auth/login" && req.method === "POST") {
    try {
      const db = getDb();
      const raw = await readBody(req);
      const body = JSON.parse(raw || "{}");
      const username = String(body.username || "").trim();
      const password = String(body.password || "");
      const user = db.prepare("SELECT id, username, password_hash, is_admin FROM users WHERE username = ?").get(username);
      if (!user || !bcrypt.compareSync(password, user.password_hash)) {
        return send(res, 401, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "invalid credentials" }));
      }
      const session = createSession(db, user.id);
      db.prepare("UPDATE users SET last_login = ? WHERE id = ?").run(new Date().toISOString(), user.id);
      res.setHeader("Set-Cookie", `mesh_session=${session.token}; Path=/; Max-Age=2592000; SameSite=Lax`);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        ok: true,
        token: session.token,
        user: { id: user.id, username: user.username, isAdmin: !!user.is_admin }
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

  if (u.pathname === "/api/channels") {
    if (req.method === "GET") {
      const payload = await buildChannelMessages();
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ channels: payload.channels }));
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
          sendEvent("updates", { updates });
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
      clearInterval(poll);
      clearInterval(ping);
    });
    return;
  }

  if (u.pathname === "/api/messages") {
    const payload = await buildChannelMessages();
    const channel = u.searchParams.get("channel");
    const limitRaw = u.searchParams.get("limit");
    const beforeRaw = u.searchParams.get("before");
    let limit = limitRaw ? Number(limitRaw) : null;
    let beforeTs = null;
    if (beforeRaw) {
      const beforeDate = parseIso(beforeRaw);
      if (beforeDate) beforeTs = beforeDate.getTime();
    }

    let list = payload.messages;
    if (channel) {
      const channelLimit = channelHistoryLimit(channel);
      if (!Number.isFinite(limit) || limit <= 0 || limit < channelLimit) {
        limit = channelLimit;
      }
      const cutoff = Number.isFinite(beforeTs) ? beforeTs : Date.now();
      list = await buildChannelMessagesBefore(channel, cutoff, limit);
    } else {
      if (channel) {
        list = list.filter((m) => m.channelName === channel);
      }
      list = list.filter((m) => {
        if (!beforeTs) return true;
        const ts = m.ts ? new Date(m.ts).getTime() : 0;
        return ts && ts < beforeTs;
      });
      list.sort((a, b) => new Date(a.ts || 0) - new Date(b.ts || 0));
    }

    if (!channel) {
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
    } else if (Number.isFinite(limit) && limit > 0 && list.length > limit) {
      list = list.slice(Math.max(0, list.length - limit));
    }

    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ channels: payload.channels, messages: list }));
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
      rankCache = { updatedAt: null, count: 0, items: [], cachedAt: null };
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
      rankCache = { updatedAt: null, count: 0, items: [], cachedAt: null };
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

server.listen(port, host, () => {
  console.log(`(observer-demo) http://${host}:${port}`);
  startObserverHitsTailer();
  hydrateRepeaterRankCache();
  hydrateObserverRankCache();
  hydrateMeshScoreCache();
  scheduleAutoRefresh().catch(() => {});
});
