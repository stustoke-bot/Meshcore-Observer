#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const Database = require("better-sqlite3");
const { getDbPath } = require("../tools/observer_demo/db_path");

let meshcore;
try {
  meshcore = require("@michaelhart/meshcore-decoder");
} catch (err) {
  console.error("missing @michaelhart/meshcore-decoder; run npm install");
  process.exit(1);
}

const { MeshCoreDecoder, Utils } = meshcore;
if (!MeshCoreDecoder || typeof MeshCoreDecoder.decode !== "function") {
  console.error("MeshCoreDecoder decode not available from @michaelhart/meshcore-decoder");
  process.exit(1);
}

const argv = process.argv.slice(2);
const rawFilePath = argv[0]
  ? path.resolve(argv[0])
  : path.resolve("data/observer.ndjson");
const lookbackHours = Number(argv[1]) || 24;
const cutoffMs = Date.now() - Math.max(1, lookbackHours) * 60 * 60 * 1000;

if (!fs.existsSync(rawFilePath)) {
  console.error(`raw file not found: ${rawFilePath}`);
  process.exit(1);
}

const dbPath = getDbPath();
const db = new Database(dbPath);
const upsertDevices = db.prepare(`
  INSERT INTO devices
    (pub, name, is_repeater, is_observer, last_seen, observer_last_seen,
     last_advert_heard_ms, gps_lat, gps_lon, raw_json, hidden_on_map, updated_at)
  VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 0, ?)
  ON CONFLICT(pub) DO UPDATE SET
    name = COALESCE(excluded.name, devices.name),
    is_repeater = MAX(devices.is_repeater, excluded.is_repeater),
    last_seen = CASE
      WHEN excluded.last_seen IS NOT NULL AND (devices.last_seen IS NULL OR excluded.last_seen > devices.last_seen)
        THEN excluded.last_seen
      ELSE devices.last_seen
    END,
    last_advert_heard_ms = CASE
      WHEN excluded.last_advert_heard_ms IS NULL THEN devices.last_advert_heard_ms
      WHEN devices.last_advert_heard_ms IS NULL THEN excluded.last_advert_heard_ms
      WHEN excluded.last_advert_heard_ms > devices.last_advert_heard_ms THEN excluded.last_advert_heard_ms
      ELSE devices.last_advert_heard_ms
    END,
    gps_lat = COALESCE(excluded.gps_lat, devices.gps_lat),
    gps_lon = COALESCE(excluded.gps_lon, devices.gps_lon),
    raw_json = COALESCE(excluded.raw_json, devices.raw_json),
    updated_at = excluded.updated_at
`);

function parseArchivedAtMs(value) {
  if (!value) return null;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePubKey(value) {
  if (!value) return null;
  const raw = String(value).replace(/[^0-9a-fA-F]/g, "").toUpperCase();
  return /^[0-9A-F]{64}$/.test(raw) ? raw : null;
}

function validateGps(value) {
  if (!value || typeof value !== "object") return null;
  const lat = Number(value.lat ?? value.latitude);
  const lon = Number(value.lon ?? value.lng ?? value.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (lat === 0 && lon === 0) return null;
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return null;
  return { lat, lon };
}

function decodeAppFlags(flags) {
  if (!Number.isInteger(flags)) return null;
  const roleCode = flags & 0x0f;
  const roleName =
    roleCode === 0x00 ? "sensor"
      : roleCode === 0x01 ? "chat"
        : roleCode === 0x02 ? "repeater"
          : roleCode === 0x03 ? "room_server"
            : `roleCode ${roleCode}`;
  const isRepeater = roleCode === 0x02;
  return { raw: flags, roleCode, roleName, isRepeater };
}

function normalizeHex(value) {
  if (!value) return null;
  const candidate = String(value).replace(/[^0-9a-fA-F]/g, "").toUpperCase();
  return candidate.length ? candidate : null;
}

const stream = fs.createReadStream(rawFilePath, { encoding: "utf8" });
const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
const repeaterMap = new Map();
let lines = 0;
let adverts = 0;
let repeaters = 0;

rl.on("line", (line) => {
  lines += 1;
  const trimmed = String(line || "").trim();
  if (!trimmed) return;
  let record;
  try {
    record = JSON.parse(trimmed);
  } catch {
    return;
  }

  const archivedMs = parseArchivedAtMs(record.archivedAt || record.ts);
  if (!Number.isFinite(archivedMs) || archivedMs < cutoffMs) return;

  const hex = normalizeHex(record.payloadHex || record.payload?.payloadHex || record.raw?.payloadHex || record.raw?.hex);
  if (!hex) return;

  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(hex);
  } catch {
    return;
  }

  const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
  if (payloadType !== "Advert") return;

  const advert = decoded.payload?.decoded || decoded.decoded || decoded.payload;
  if (!advert || typeof advert !== "object") return;

  adverts += 1;

  const flags =
    Number.isFinite(advert.flags) ? Number(advert.flags) :
    Number.isFinite(advert.appFlags) ? Number(advert.appFlags) :
    Number.isFinite(advert.appData?.flags) ? Number(advert.appData.flags) :
    null;
  let isRepeater = false;
  if (Number.isInteger(flags)) {
    const decodedFlags = decodeAppFlags(flags);
    isRepeater = !!decodedFlags?.isRepeater;
  }
  if (!isRepeater) {
    isRepeater =
      advert.isRepeater ||
      advert.appData?.isRepeater ||
      advert.appData?.deviceRole === 2 ||
      advert.appData?.nodeType === "repeater" ||
      advert.appData?.type === "repeater";
  }
  if (!isRepeater) return;

  const pub = normalizePubKey(advert.publicKey || advert.pub || advert.pubKey || record.publicKey || record.pub);
  if (!pub) return;

  const gps = validateGps(advert.gps || advert.appData?.location || record.gps);
  const name = String(advert.appData?.name || advert.name || record.nodeName || record.name || "").trim() || null;
  const appDataClone = advert.appData ? JSON.parse(JSON.stringify(advert.appData)) : null;
  const lastAdvert = { publicKey: pub };
  if (appDataClone) lastAdvert.appData = appDataClone;
  if (gps) lastAdvert.gps = { lat: gps.lat, lon: gps.lon };
  if (Number.isFinite(flags)) lastAdvert.flags = Number(flags);
  if (advert.appData?.deviceRole !== undefined) lastAdvert.deviceRole = advert.appData.deviceRole;
  if (advert.appData?.type) lastAdvert.type = advert.appData.type;
  const meta = {
    verifiedAdvert: true,
    nameValid: !!name,
    nameInvalidReason: name ? null : "missing",
    gpsInvalidReason: gps ? null : "missing_gps",
    backfilled: true
  };

  const existing = repeaterMap.get(pub);
  if (!existing || archivedMs > existing.lastMs) {
    repeaterMap.set(pub, {
      lastMs: archivedMs,
      name,
      gps,
      updatedAt: new Date().toISOString(),
      lastSeenIso: new Date(archivedMs).toISOString(),
      lastAdvert,
      meta
    });
  }
});

rl.on("close", () => {
  const repeaterCount = repeaterMap.size;
  console.log(`scanned ${lines} lines, adverts decoded ${adverts}, repeaters sighted ${repeaterCount}`);
  if (!repeaterMap.size) {
    console.log("no repeater adverts found in the selected window");
    process.exit(0);
  }

  let written = 0;
  repeaterMap.forEach((entry, pub) => {
    const raw = {
      lastAdvert: entry.lastAdvert,
      meta: entry.meta
    };
    upsertDevices.run(
      pub,
      entry.name,
      1,
      entry.lastSeenIso,
      null,
      entry.lastMs,
      entry.gps ? entry.gps.lat : null,
      entry.gps ? entry.gps.lon : null,
      JSON.stringify(raw),
      entry.updatedAt
    );
    written += 1;
  });

  console.log(`backfilled ${written} repeaters into devices.last_advert_heard_ms`);
  process.exit(0);
});

rl.on("error", (err) => {
  console.error(`failed to read ${rawFilePath}:`, err);
  process.exit(1);
});
