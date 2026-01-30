#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");

// Import the updateDeviceFromAdvert function logic
// We'll need to replicate the key parts since we can't easily import from mqtt_ingest.js
const Database = require("better-sqlite3");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");
const { getDbPath } = require("../tools/observer_demo/db_path");

const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");
const observerPath = path.join(dataDir, "observer.ndjson");
const dbPath = getDbPath();

// 30 hours lookback
const lookbackHours = 30;
const cutoffMs = Date.now() - (lookbackHours * 60 * 60 * 1000);

if (!fs.existsSync(observerPath)) {
  console.error(`observer.ndjson not found: ${observerPath}`);
  process.exit(1);
}

console.log(`Backfilling current_repeaters table from ${observerPath}`);
console.log(`Looking back ${lookbackHours} hours (cutoff: ${new Date(cutoffMs).toISOString()})`);

const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

// Ensure current_repeaters table exists
db.exec(`
  CREATE TABLE IF NOT EXISTS current_repeaters (
    pub TEXT PRIMARY KEY,
    name TEXT,
    gps_lat REAL,
    gps_lon REAL,
    last_advert_heard_ms INTEGER,
    hidden_on_map INTEGER DEFAULT 0,
    gps_implausible INTEGER DEFAULT 0,
    visible INTEGER DEFAULT 1,
    is_observer INTEGER DEFAULT 0,
    best_rssi REAL,
    best_snr REAL,
    avg_rssi REAL,
    avg_snr REAL,
    total24h INTEGER DEFAULT 0,
    score INTEGER,
    color TEXT,
    quality TEXT,
    quality_reason TEXT,
    is_live INTEGER DEFAULT 0,
    stale INTEGER DEFAULT 0,
    last_seen TEXT,
    updated_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_current_repeaters_last_advert ON current_repeaters(last_advert_heard_ms);
  CREATE INDEX IF NOT EXISTS idx_current_repeaters_score ON current_repeaters(score);
  CREATE INDEX IF NOT EXISTS idx_current_repeaters_visible ON current_repeaters(visible, last_advert_heard_ms);
`);

// Prepare upsert statement for current_repeaters
const currentRepeaterUpsert = db.prepare(`
  INSERT INTO current_repeaters (
    pub, name, gps_lat, gps_lon, last_advert_heard_ms,
    hidden_on_map, gps_implausible, visible, is_observer, last_seen, updated_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(pub) DO UPDATE SET
    name = COALESCE(excluded.name, current_repeaters.name),
    last_advert_heard_ms = CASE
      WHEN excluded.last_advert_heard_ms IS NULL THEN current_repeaters.last_advert_heard_ms
      WHEN current_repeaters.last_advert_heard_ms IS NULL THEN excluded.last_advert_heard_ms
      WHEN excluded.last_advert_heard_ms > current_repeaters.last_advert_heard_ms THEN excluded.last_advert_heard_ms
      ELSE current_repeaters.last_advert_heard_ms
    END,
    gps_lat = CASE
      WHEN current_repeaters.gps_implausible = 1 AND excluded.gps_lat = current_repeaters.gps_lat AND excluded.gps_lon = current_repeaters.gps_lon
        THEN current_repeaters.gps_lat
      ELSE COALESCE(excluded.gps_lat, current_repeaters.gps_lat)
    END,
    gps_lon = CASE
      WHEN current_repeaters.gps_implausible = 1 AND excluded.gps_lat = current_repeaters.gps_lat AND excluded.gps_lon = current_repeaters.gps_lon
        THEN current_repeaters.gps_lon
      ELSE COALESCE(excluded.gps_lon, current_repeaters.gps_lon)
    END,
    hidden_on_map = COALESCE(current_repeaters.hidden_on_map, excluded.hidden_on_map),
    gps_implausible = COALESCE(current_repeaters.gps_implausible, excluded.gps_implausible),
    visible = CASE
      WHEN excluded.last_advert_heard_ms IS NOT NULL AND excluded.last_advert_heard_ms > (strftime('%s', 'now') * 1000 - 30*60*60*1000)
        THEN 1
      ELSE current_repeaters.visible
    END,
    is_observer = MAX(current_repeaters.is_observer, excluded.is_observer),
    last_seen = CASE
      WHEN excluded.last_seen > current_repeaters.last_seen THEN excluded.last_seen
      ELSE current_repeaters.last_seen
    END,
    updated_at = excluded.updated_at
`);

// Helper functions from mqtt_ingest.js
function normalizePubKey(value) {
  if (!value) return null;
  const raw = String(value).trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(raw)) return null;
  return raw;
}

function parseArchivedAtMs(value) {
  if (!value) return null;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function decodeAppFlags(flags) {
  if (!Number.isInteger(flags)) return null;
  const roleCode = flags & 0x0f;
  let roleName = "unknown";
  if (roleCode === 0x00) roleName = "sensor";
  else if (roleCode === 0x01) roleName = "chat";
  else if (roleCode === 0x02) roleName = "repeater";
  else if (roleCode === 0x03) roleName = "room_server";
  const isRepeater = roleCode === 0x02;
  return {
    raw: flags,
    roleCode,
    roleName,
    isRepeater,
    hasLocation: !!(flags & 0x10),
    hasName: !!(flags & 0x80)
  };
}

function validateGps(value) {
  if (!value || typeof value !== "object") return { valid: false, reason: "missing" };
  const lat = Number(value.lat ?? value.latitude);
  const lon = Number(value.lon ?? value.lng ?? value.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return { valid: false, reason: "not_numeric" };
  if (lat === 0 && lon === 0) return { valid: false, reason: "zero_point" };
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return { valid: false, reason: "out_of_range" };
  return { valid: true, lat, lon };
}

function sanitizeRepeaterName(value) {
  const raw = value == null ? "" : String(value);
  const trimmed = raw.trim();
  if (!trimmed) return { ok: false, reason: "empty" };
  if (trimmed.includes("�")) return { ok: false, reason: "replacement_char" };
  if (trimmed.length < 2) return { ok: false, reason: "too_short" };
  const MAX_REPEATER_NAME_LENGTH = 32;
  const MAX_NAME_CONTROL_RATIO = 0.2;
  function isPrintableChar(codePoint) {
    return codePoint >= 0x20 && codePoint !== 0x7f;
  }
  const strippedChars = [...trimmed].filter((ch) => {
    const code = ch.codePointAt(0);
    return code !== undefined && isPrintableChar(code);
  });
  const stripped = strippedChars.join("");
  const controlRatio = trimmed.length ? (trimmed.length - stripped.length) / trimmed.length : 0;
  if (controlRatio > MAX_NAME_CONTROL_RATIO) return { ok: false, reason: "too_many_control_chars" };
  const cleaned = stripped.length > MAX_REPEATER_NAME_LENGTH
    ? stripped.slice(0, MAX_REPEATER_NAME_LENGTH).trimEnd()
    : stripped;
  if (!cleaned) return { ok: false, reason: "empty_after_clean" };
  return { ok: true, cleaned };
}

function isVerifiedAdvert(decoded, advert) {
  if (!decoded || !advert) return { ok: false, reason: "missing_payload" };
  const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
  if (payloadType !== "Advert") return { ok: false, reason: "payload_not_advert" };
  const pub = normalizePubKey(advert.publicKey || advert.pub || advert.pubKey);
  if (!pub) return { ok: false, reason: "invalid_pub" };
  const hasAppData = advert.appData && typeof advert.appData === "object";
  const hasFlags = Number.isFinite(advert.flags) || Number.isFinite(advert.appFlags) || Number.isFinite(advert.appData?.flags);
  const nameCandidate = advert.appData?.name || advert.name || null;
  let nameResult = { ok: true };
  if (nameCandidate) {
    nameResult = sanitizeRepeaterName(nameCandidate);
    if (!nameResult.ok) return { ok: false, reason: `invalid_name_${nameResult.reason}` };
  }
  const gpsCandidate = advert.gps || advert.appData?.location || null;
  const gpsCheck = validateGps(gpsCandidate);
  const gpsValid = gpsCheck.valid;
  const structureProof = hasAppData && (hasFlags || nameCandidate || gpsValid);
  if (!structureProof) return { ok: false, reason: "missing_structure" };
  return {
    ok: true,
    reason: null,
    pub,
    cleanedName: nameResult.ok ? nameResult.cleaned : null,
    gps: gpsValid ? { lat: gpsCheck.lat, lon: gpsCheck.lon } : null,
    gpsInvalidReason: (gpsCandidate && !gpsValid) ? gpsCheck.reason : null
  };
}

function getHex(rec) {
  const hex = rec.payloadHex || rec.hex || rec.payload?.payloadHex || rec.payload?.hex || rec.raw;
  if (!hex) return null;
  const normalized = String(hex).trim().toUpperCase();
  if (!/^[0-9A-F]+$/.test(normalized)) return null;
  return normalized;
}

// Process a record (similar to updateDeviceFromAdvert but only updates current_repeaters)
function processAdvertRecord(record) {
  if (!record?.payloadHex) return false;
  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(String(record.payloadHex).toUpperCase());
  } catch {
    return false;
  }
  const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
  if (payloadType !== "Advert") return false;
  const adv = decoded.payload?.decoded || decoded.decoded || decoded.payload || null;
  if (!adv) return false;
  const verification = isVerifiedAdvert(decoded, adv);
  if (!verification.ok) return false;
  
  const key = verification.pub;
  const heardMs = parseArchivedAtMs(record.archivedAt);
  if (!Number.isFinite(heardMs) || heardMs < cutoffMs) return false;
  
  // Decode app flags to check if it's a repeater
  const appFlagsRaw =
    (Number.isInteger(adv.flags) ? adv.flags : null) ??
    (Number.isInteger(adv.appFlags) ? adv.appFlags : null) ??
    (Number.isInteger(adv.appData?.flags) ? adv.appData.flags : null);
  const decodedFlags = Number.isFinite(appFlagsRaw) ? decodeAppFlags(appFlagsRaw) : null;
  const heuristicIsRepeater =
    adv.isRepeater ||
    adv.appData?.isRepeater ||
    adv.appData?.deviceRole === 2 ||
    adv.appData?.nodeType === "repeater" ||
    adv.appData?.type === "repeater";
  
  const isRepeater = decodedFlags ? decodedFlags.isRepeater : heuristicIsRepeater;
  if (!isRepeater) return false;
  
  // Check if this is a room server - exclude from current_repeaters
  const role = decodedFlags ? decodedFlags.roleName : null;
  const roleCode = decodedFlags ? decodedFlags.roleCode : null;
  const isRoomServer = role === "room_server" || roleCode === 0x03;
  if (isRoomServer) return false;
  
  // Prepare data for current_repeaters
  const name = verification.cleanedName || null;
  const gps = verification.gps;
  const gpsImplausible = 0; // Will be set later if flagged
  const hasHideEmoji = name && String(name).includes("🚫");
  const hiddenOnMap = hasHideEmoji ? 1 : 0;
  const visible = 1; // All backfilled repeaters are visible (within 72h window)
  const isObserver = 0; // Will be updated later if needed
  const lastSeen = record.archivedAt || null;
  
  // Upsert to current_repeaters
  try {
    currentRepeaterUpsert.run(
      key,
      name,
      gps ? gps.lat : null,
      gps ? gps.lon : null,
      heardMs,
      hiddenOnMap,
      gpsImplausible,
      visible,
      isObserver,
      lastSeen,
      new Date().toISOString()
    );
    return true;
  } catch (err) {
    console.error(`Failed to upsert ${key}:`, err?.message || err);
    return false;
  }
}

// Main processing
let processed = 0;
let adverts = 0;
let repeaters = 0;
let errors = 0;

const stream = fs.createReadStream(observerPath, { encoding: "utf8" });
const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

rl.on("line", (line) => {
  processed++;
  const trimmed = String(line || "").trim();
  if (!trimmed) return;
  
  let record;
  try {
    record = JSON.parse(trimmed);
  } catch {
    errors++;
    return;
  }
  
  const archivedMs = parseArchivedAtMs(record.archivedAt || record.ts);
  if (!Number.isFinite(archivedMs) || archivedMs < cutoffMs) return;
  
  const hex = getHex(record);
  if (!hex) return;
  
  if (processAdvertRecord(record)) {
    repeaters++;
  }
  adverts++;
  
  if (processed % 10000 === 0) {
    console.log(`Processed ${processed} records, found ${adverts} adverts, ${repeaters} repeaters...`);
  }
});

rl.on("close", () => {
  console.log(`\nBackfill complete:`);
  console.log(`  Total records processed: ${processed}`);
  console.log(`  Adverts found: ${adverts}`);
  console.log(`  Repeaters added/updated: ${repeaters}`);
  console.log(`  Errors: ${errors}`);
  
  // Check final count
  const count = db.prepare("SELECT COUNT(*) as count FROM current_repeaters").get();
  console.log(`  Total repeaters in current_repeaters table: ${count.count}`);
  
  db.close();
  process.exit(0);
});

rl.on("error", (err) => {
  console.error("Error reading file:", err);
  db.close();
  process.exit(1);
});
