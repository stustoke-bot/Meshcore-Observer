#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const mqtt = require("mqtt");
const Database = require("better-sqlite3");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");
const { getDbPath, logDbInfo } = require("./db_path");

const projectRoot = path.resolve(__dirname, "..", "..");
const dataDir = path.join(projectRoot, "data");
const observerPath = path.join(dataDir, "observer.ndjson");
const observerStatusPath = path.join(dataDir, "observers.json");
const devicesPath = path.join(dataDir, "devices.json");
const ingestLogPath = path.join(dataDir, "ingest.log");
const keysPath = path.join(projectRoot, "tools", "meshcore_keys.json");
const dbPath = getDbPath();
const GPS_WARN_KM = 50;
const RF_MAX_ROWS = 50000;
const RF_CLEAN_INTERVAL = 500;

let rfDb = null;
let rfInsert = null;
let rfPrune = null;
let rfInsertCount = 0;
let msgInsert = null;
let msgObserverInsert = null;
let deviceUpsert = null;
let observerUpsert = null;
let rejectAdvertInsert = null;
let ingestMetricUpsert = null;
let ingestDbInfoLogged = false;
let advertTimestamps = [];
let lastAdvertSeenAtIso = null;
let keysMtime = 0;
let keyStore = null;
let keyMap = {};

const mqttUrl = process.env.MESHRANK_MQTT_URL || "mqtts://meshrank.net:8883";
const mqttTopic = process.env.MESHRANK_MQTT_TOPIC || "meshrank/observers/+/packets";
const mqttUser = process.env.MESHRANK_MQTT_USER || undefined;
const mqttPass = process.env.MESHRANK_MQTT_PASS || undefined;
const INGEST_WINDOW_MS = 10 * 60 * 1000;

function ensureDataDir() {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
}

function sha256Hex(hex) {
  try {
    const buf = Buffer.from(hex, "hex");
    return crypto.createHash("sha256").update(buf).digest("hex").toUpperCase();
  } catch {
    return null;
  }
}

function parseTopicInfo(topic) {
  const parts = String(topic || "").split("/");
  if (parts.length >= 4 && parts[0] === "meshcore") {
    return { iata: parts[1], pub: parts[2] };
  }
  return { iata: null, pub: null };
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseArchivedAtMs(value) {
  if (!value) return null;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

const MAX_REPEATER_NAME_LENGTH = 32;
const MAX_NAME_CONTROL_RATIO = 0.2;

function isPrintableChar(codePoint) {
  return codePoint >= 0x20 && codePoint !== 0x7f;
}

function sanitizeRepeaterName(value) {
  const raw = value == null ? "" : String(value);
  const trimmed = raw.trim();
  if (!trimmed) return { ok: false, reason: "empty" };
  if (trimmed.includes("�")) return { ok: false, reason: "replacement_char" };
  if (trimmed.length < 2) return { ok: false, reason: "too_short" };
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

function normalizePubKey(value) {
  if (!value) return null;
  const raw = String(value).trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(raw)) return null;
  return raw;
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

function resolveObserverId(record) {
  if (!record) return null;
  let observerId = record.observerId || "";
  if (!observerId && record.topic) {
    const match = String(record.topic).match(/observers\/([^/]+)\//i);
    if (match) observerId = match[1];
  }
  observerId = String(observerId || record.observerName || "").trim();
  return observerId || null;
}

function recordRejectedAdvert(pub, reason, record) {
  if (!rejectAdvertInsert) return;
  try {
    const normalizedPub = pub ? String(pub).toUpperCase() : null;
    const observerId = resolveObserverId(record);
    const heardMs = parseArchivedAtMs(record?.archivedAt);
    const sample = JSON.stringify({
      observerId,
      topic: record?.topic || null,
      payloadHex: record?.payloadHex || null,
      archivedAt: record?.archivedAt || null,
      reason: reason || null
    });
    const sampleJson = sample.length > 1024 ? sample.slice(0, 1024) : sample;
    rejectAdvertInsert.run(
      normalizedPub,
      observerId,
      Number.isFinite(heardMs) ? heardMs : null,
      String(reason || "unknown"),
      sampleJson
    );
  } catch (err) {
    logIngest("ERROR", `rejected advert insert failed ${err?.message || err}`);
  }
}

function persistIngestMetric(key, value) {
  if (!ingestMetricUpsert) return;
  try {
    ingestMetricUpsert.run(String(key), value, new Date().toISOString());
  } catch (err) {
    logIngest("ERROR", `ingest metric persist failed ${err?.message || err}`);
  }
}

function recordAdvertSeen(heardMs) {
  const nowMs = Number.isFinite(heardMs) ? heardMs : Date.now();
  const cutoff = nowMs - INGEST_WINDOW_MS;
  advertTimestamps = advertTimestamps.filter((ts) => ts >= cutoff);
  advertTimestamps.push(nowMs);
  const count = advertTimestamps.length;
  lastAdvertSeenAtIso = new Date(nowMs).toISOString();
  persistIngestMetric("countAdvertsSeenLast10m", String(count));
  persistIngestMetric("lastAdvertSeenAtIso", lastAdvertSeenAtIso);
}

function logIngestDbInfo(db) {
  if (ingestDbInfoLogged || !db) return;
  try {
    logDbInfo(db);
    const dbList = db.pragma("database_list");
    logIngest("INFO", `ingest db info ${JSON.stringify({
      resolvedPath: getDbPath(),
      cwd: process.cwd(),
      databaseList: dbList || []
    })}`);
  } catch (err) {
    logIngest("ERROR", `ingest db info failed ${err?.message || err}`);
  } finally {
    ingestDbInfoLogged = true;
  }
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

function appendObserver(record) {
  ensureDataDir();
  fs.appendFileSync(observerPath, JSON.stringify(record) + "\n");
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

function refreshKeysIfNeeded() {
  try {
    if (!fs.existsSync(keysPath)) return;
    const stat = fs.statSync(keysPath);
    if (keysMtime && stat.mtimeMs === keysMtime) return;
    keysMtime = stat.mtimeMs;
    const cfg = loadKeys();
    keyStore = MeshCoreDecoder.createKeyStore({
      channelSecrets: (cfg.channels || []).map((c) => c.secretHex).filter(Boolean)
    });
    keyMap = {};
    for (const ch of cfg.channels || []) {
      if (ch?.hashByte && ch?.name) {
        keyMap[String(ch.hashByte).toUpperCase()] = String(ch.name);
      }
    }
  } catch (err) {
    logIngest("ERROR", `keys reload failed ${err?.message || err}`);
  }
}

function ensureColumn(db, tableName, columnName, columnType) {
  const cols = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (cols.some((c) => c.name === columnName)) return;
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`);
}

function initRfDb() {
  if (rfDb) return true;
  try {
    ensureDataDir();
    rfDb = new Database(dbPath);
    logIngestDbInfo(rfDb);
    rfDb.pragma("journal_mode = WAL");
    rfDb.pragma("synchronous = NORMAL");
    rfDb.pragma("temp_store = MEMORY");
    rfDb.pragma("cache_size = -64000");
    rfDb.pragma("foreign_keys = ON");
    rfDb.exec(`
      CREATE TABLE IF NOT EXISTS rf_packets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        payload_hex TEXT,
        frame_hash TEXT,
        rssi REAL,
        snr REAL,
        crc INTEGER,
        observer_id TEXT,
        observer_name TEXT,
        len INTEGER,
        payload_len INTEGER,
        packet_type TEXT,
        topic TEXT,
        route TEXT,
        path TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_rf_packets_ts ON rf_packets(ts);
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
      CREATE TABLE IF NOT EXISTS devices (
        pub TEXT PRIMARY KEY,
        name TEXT,
        is_repeater INTEGER,
        is_observer INTEGER,
        last_seen TEXT,
        observer_last_seen TEXT,
        last_advert_heard_ms INTEGER,
        gps_lat REAL,
        gps_lon REAL,
        raw_json TEXT,
        hidden_on_map INTEGER,
        updated_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_devices_last_advert_heard_ms ON devices(last_advert_heard_ms);
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
      CREATE INDEX IF NOT EXISTS idx_rejected_adverts_heard_ms ON rejected_adverts(heard_ms);
      CREATE TABLE IF NOT EXISTS ingest_metrics (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT
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
    `);
    ensureColumn(rfDb, "devices", "last_advert_heard_ms", "INTEGER");
    ensureColumn(rfDb, "messages", "path_text", "TEXT");
    ensureColumn(rfDb, "message_observers", "path_text", "TEXT");
    ensureColumn(rfDb, "message_observers", "ts_ms", "INTEGER");
    rfInsert = rfDb.prepare(`
      INSERT INTO rf_packets (
        ts, payload_hex, frame_hash, rssi, snr, crc, observer_id, observer_name,
        len, payload_len, packet_type, topic, route, path
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    rfPrune = rfDb.prepare(`
      DELETE FROM rf_packets
      WHERE id <= (SELECT id FROM rf_packets ORDER BY id DESC LIMIT 1 OFFSET ?)
    `);
    rejectAdvertInsert = rfDb.prepare(`
      INSERT INTO rejected_adverts (pub, observer_id, heard_ms, reason, sample_json)
      VALUES (?, ?, ?, ?, ?)
    `);
    ingestMetricUpsert = rfDb.prepare(`
      INSERT INTO ingest_metrics (key, value, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    `);
    msgInsert = rfDb.prepare(`
      INSERT INTO messages (
        message_hash, frame_hash, channel_name, channel_hash, sender, sender_pub,
        body, ts, path_json, path_text, path_length, repeats
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(message_hash) DO UPDATE SET
        ts = CASE WHEN excluded.ts > messages.ts THEN excluded.ts ELSE messages.ts END,
        frame_hash = COALESCE(messages.frame_hash, excluded.frame_hash),
        channel_name = COALESCE(messages.channel_name, excluded.channel_name),
        channel_hash = COALESCE(messages.channel_hash, excluded.channel_hash),
        sender = COALESCE(messages.sender, excluded.sender),
        sender_pub = COALESCE(messages.sender_pub, excluded.sender_pub),
        body = COALESCE(messages.body, excluded.body),
        path_length = MAX(messages.path_length, excluded.path_length),
        repeats = MAX(messages.repeats, excluded.repeats),
        path_json = CASE
          WHEN excluded.path_length > messages.path_length THEN excluded.path_json
          ELSE messages.path_json
        END,
        path_text = CASE
          WHEN excluded.path_length > messages.path_length THEN excluded.path_text
          ELSE messages.path_text
        END
    `);
    msgObserverInsert = rfDb.prepare(`
      INSERT INTO message_observers (
        message_hash, observer_id, observer_name, ts, ts_ms, path_json, path_text, path_length
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(message_hash, observer_id) DO UPDATE SET
        ts = CASE WHEN excluded.ts > message_observers.ts THEN excluded.ts ELSE message_observers.ts END,
        ts_ms = CASE WHEN excluded.ts_ms > message_observers.ts_ms THEN excluded.ts_ms ELSE message_observers.ts_ms END,
        observer_name = COALESCE(message_observers.observer_name, excluded.observer_name),
        path_length = MAX(message_observers.path_length, excluded.path_length),
        path_json = CASE
          WHEN excluded.path_length > message_observers.path_length THEN excluded.path_json
          ELSE message_observers.path_json
        END,
        path_text = CASE
          WHEN excluded.path_length > message_observers.path_length THEN excluded.path_text
          ELSE message_observers.path_text
        END
    `);
    deviceUpsert = rfDb.prepare(`
      INSERT INTO devices (
        pub, name, is_repeater, is_observer, last_seen, observer_last_seen,
        last_advert_heard_ms, gps_lat, gps_lon, raw_json, hidden_on_map, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(pub) DO UPDATE SET
        name = COALESCE(excluded.name, devices.name),
        is_repeater = MAX(devices.is_repeater, excluded.is_repeater),
        is_observer = MAX(devices.is_observer, excluded.is_observer),
        last_seen = CASE
          WHEN excluded.last_seen > devices.last_seen THEN excluded.last_seen
          ELSE devices.last_seen
        END,
        observer_last_seen = CASE
          WHEN excluded.observer_last_seen > devices.observer_last_seen THEN excluded.observer_last_seen
          ELSE devices.observer_last_seen
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
        hidden_on_map = COALESCE(devices.hidden_on_map, excluded.hidden_on_map),
        updated_at = excluded.updated_at
    `);
    observerUpsert = rfDb.prepare(`
      INSERT INTO observers (
        observer_id, name, first_seen, last_seen, count, gps_lat, gps_lon, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(observer_id) DO UPDATE SET
        name = COALESCE(excluded.name, observers.name),
        last_seen = CASE
          WHEN excluded.last_seen > observers.last_seen THEN excluded.last_seen
          ELSE observers.last_seen
        END,
        count = MAX(observers.count, excluded.count),
        gps_lat = COALESCE(excluded.gps_lat, observers.gps_lat),
        gps_lon = COALESCE(excluded.gps_lon, observers.gps_lon),
        updated_at = excluded.updated_at
    `);
    return true;
  } catch (err) {
    logIngest("ERROR", `rf db init failed ${err?.message || err}`);
    rfDb = null;
    rfInsert = null;
    rfPrune = null;
    msgInsert = null;
    msgObserverInsert = null;
    deviceUpsert = null;
    observerUpsert = null;
    return false;
  }
}

function storeRfPacket(record) {
  if (!initRfDb() || !rfInsert) return;
  try {
    rfInsert.run(
      record.archivedAt || null,
      record.payloadHex || null,
      record.frameHash || null,
      record.rssi ?? null,
      record.snr ?? null,
      record.crc ? 1 : 0,
      record.observerId || null,
      record.observerName || null,
      record.len ?? null,
      record.payloadLen ?? null,
      record.packetType || null,
      record.topic || null,
      record.route ? JSON.stringify(record.route) : null,
      record.path ? JSON.stringify(record.path) : null
    );
    rfInsertCount += 1;
    if (rfPrune && rfInsertCount % RF_CLEAN_INTERVAL === 0) {
      rfPrune.run(RF_MAX_ROWS);
    }
  } catch (err) {
    logIngest("ERROR", `rf db insert failed ${err?.message || err}`);
  }
}

function normalizePathHash(value) {
  if (!value) return null;
  const clean = String(value).trim().toUpperCase();
  if (/^[0-9A-F]{2}$/.test(clean)) return clean;
  return clean;
}

function storeMessage(record) {
  if (!initRfDb() || !msgInsert) return;
  refreshKeysIfNeeded();
  if (!keyStore) return;
  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(String(record.payloadHex).toUpperCase(), { keyStore });
  } catch {
    return;
  }
  const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
  if (payloadType !== "GroupText") return;
  const payload = decoded.payload?.decoded;
  if (!payload || !payload.decrypted) return;

  const channelHash = typeof payload.channelHash === "string" ? payload.channelHash.toUpperCase() : null;
  const channelName = channelHash && keyMap[channelHash] ? keyMap[channelHash] : null;
  const sender = String(payload.decrypted.sender || "unknown");
  const senderPub = payload.senderPublicKey || payload.publicKey || payload.senderPub || null;
  const body = String(payload.decrypted.message || "");
  const msgHash = String(
    decoded.messageHash ||
    record.frameHash ||
    sha256Hex(String(record.payloadHex).toUpperCase()) ||
    "unknown"
  ).toUpperCase();
  const pathRaw = Array.isArray(decoded.path) ? decoded.path : [];
  const path = pathRaw.map(normalizePathHash).filter(Boolean);
  const pathText = path.length ? path.join("|") : null;
  const pathLength = path.length || (Number.isFinite(decoded.pathLength) ? decoded.pathLength : 0);
  const hopCount = pathLength || 0;

  try {
    msgInsert.run(
      msgHash,
      record.frameHash || null,
      channelName || null,
      channelHash || null,
      sender || null,
      senderPub ? String(senderPub).toUpperCase() : null,
      body || null,
      record.archivedAt || null,
      path.length ? JSON.stringify(path) : null,
      pathText,
      pathLength,
      hopCount
    );
    if (msgObserverInsert) {
      const observerId = resolveObserverId(record);
      if (observerId) {
        const archivedMs = parseArchivedAtMs(record.archivedAt);
        msgObserverInsert.run(
          msgHash,
          observerId,
          record.observerName || observerId,
          record.archivedAt || null,
          Number.isFinite(archivedMs) ? archivedMs : null,
          path.length ? JSON.stringify(path) : null,
          pathText,
          pathLength
        );
      }
    }
  } catch (err) {
    logIngest("ERROR", `message insert failed ${err?.message || err}`);
  }
}

function updateDeviceFromAdvert(record) {
  if (!record?.payloadHex) return;
  if (!initRfDb()) return;
  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(String(record.payloadHex).toUpperCase());
  } catch {
    return;
  }
  const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
  if (payloadType !== "Advert") return;
  const adv = decoded.payload?.decoded || decoded.decoded || decoded.payload || null;
  if (!adv) return;
  const verification = isVerifiedAdvert(decoded, adv);
  if (!verification.ok) {
    recordRejectedAdvert(verification.pub || adv.publicKey || adv.pub || adv.pubKey, verification.reason, record);
    return;
  }
  const key = verification.pub;

  const devices = readJsonSafe(devicesPath, { byPub: {} });
  const byPub = devices.byPub || {};
  const entry = byPub[key] || { pub: key };
  entry.lastSeen = record.archivedAt || entry.lastSeen || null;
  entry.verifiedAdvert = true;
  entry.raw = entry.raw || {};
  entry.meta = entry.meta || {};
  entry.raw.meta = entry.raw.meta || {};
  entry.meta.verifiedAdvert = true;
  entry.raw.meta.verifiedAdvert = true;
  const heardMs = parseArchivedAtMs(record.archivedAt);
  if (Number.isFinite(heardMs)) {
    if (!Number.isFinite(entry.lastAdvertHeardMs) || heardMs > entry.lastAdvertHeardMs) {
      entry.lastAdvertHeardMs = heardMs;
    }
  }
  entry.raw.lastAdvert = adv;
  entry.lastNameInvalid = entry.lastNameInvalid || false;
  const hadValidName = !!entry.name && entry.nameValid;
  if (verification.cleanedName) {
    entry.name = verification.cleanedName;
    entry.nameValid = true;
    entry.nameInvalidReason = null;
  } else if (!hadValidName) {
    entry.nameValid = false;
    entry.nameInvalidReason = "missing_name";
  }
  entry.nameInvalid = !entry.nameValid;

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
  if (decodedFlags) {
    entry.appFlags = decodedFlags;
    entry.role = decodedFlags.roleName;
    entry.isRepeater = decodedFlags.isRepeater;
  } else if (heuristicIsRepeater) {
    entry.isRepeater = true;
  }

  entry.gpsInvalidReason = verification.gpsInvalidReason || null;
  entry.meta.gpsInvalidReason = entry.gpsInvalidReason;
  entry.raw.meta.gpsInvalidReason = entry.gpsInvalidReason;
  const gps = verification.gps || null;
  if (gps && Number.isFinite(gps.lat) && Number.isFinite(gps.lon)) {
    const prev = entry.gpsReported || null;
    const changed = !prev || prev.lat !== gps.lat || prev.lon !== gps.lon;
    entry.gpsReported = gps;
    if (changed) {
      entry.gps = gps;
      entry.manualLocation = false;
      entry.locSource = null;
      entry.gpsFlagged = false;
      entry.gpsFlaggedAt = null;
      entry.gpsEstimated = false;
      entry.gpsEstimateAt = null;
      entry.gpsEstimateNeighbors = null;
      entry.gpsEstimateReason = null;
      entry.gpsImplausible = false;
      entry.hiddenOnMap = false;
    } else if (!entry.manualLocation) {
      entry.gps = gps;
    }
  }

  entry.raw.meta.nameValid = !!entry.nameValid;
  entry.raw.meta.nameInvalidReason = entry.nameInvalidReason || null;
  entry.meta.nameValid = !!entry.nameValid;
  entry.meta.nameInvalidReason = entry.nameInvalidReason || null;
  if (entry.name) {
    entry.meta.name = entry.name;
    entry.raw.meta.name = entry.name;
  }
  recordAdvertSeen(heardMs);
  byPub[key] = entry;
  devices.byPub = byPub;
  devices.updatedAt = new Date().toISOString();
  writeJsonSafe(devicesPath, devices);
  if (deviceUpsert) {
    const gps = entry.gps && Number.isFinite(entry.gps.lat) && Number.isFinite(entry.gps.lon) ? entry.gps : null;
    deviceUpsert.run(
      key,
      entry.name || null,
      entry.isRepeater ? 1 : 0,
      entry.isObserver ? 1 : 0,
      entry.lastSeen || null,
      entry.observerLastSeen || null,
      entry.lastAdvertHeardMs ?? null,
      gps ? gps.lat : null,
      gps ? gps.lon : null,
      entry.raw ? JSON.stringify(entry.raw) : null,
      null,
      new Date().toISOString()
    );
  }
}

function logIngest(level, message) {
  ensureDataDir();
  const line = `${new Date().toISOString()} ${level} ${message}`;
  fs.appendFileSync(ingestLogPath, line + "\n");
}

function readJsonSafe(p, def) {
  try {
    if (!fs.existsSync(p)) return def;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return def;
  }
}

function writeJsonSafe(p, obj) {
  const dir = path.dirname(p);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = `${p}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  try {
    fs.renameSync(tmp, p);
  } catch {
    fs.writeFileSync(p, JSON.stringify(obj, null, 2));
    try { fs.unlinkSync(tmp); } catch {}
  }
}

function updateObserverStatus(record) {
  const data = readJsonSafe(observerStatusPath, { byId: {} });
  const byId = data.byId || {};
  const id = String(record.observerId || "observer");
  const entry = byId[id] || { id, firstSeen: record.archivedAt, count: 0 };
  entry.lastSeen = record.archivedAt;
  entry.count = (entry.count || 0) + 1;
  if (record.observerName) entry.name = record.observerName;
  if (record.gps && Number.isFinite(record.gps.lat) && Number.isFinite(record.gps.lon)) {
    entry.gps = record.gps;
  }
  const devices = readJsonSafe(devicesPath, { byPub: {} });
  const byPub = devices.byPub || {};
  if (entry.gps && Number.isFinite(entry.gps.lat) && Number.isFinite(entry.gps.lon)) {
    const repeaters = Object.values(byPub).filter((d) =>
      d && d.isRepeater && d.gps && Number.isFinite(d.gps.lat) && Number.isFinite(d.gps.lon)
    );
    if (repeaters.length) {
      let bestKm = Infinity;
      for (const rpt of repeaters) {
        const km = haversineKm(entry.gps.lat, entry.gps.lon, rpt.gps.lat, rpt.gps.lon);
        if (km < bestKm) bestKm = km;
      }
      entry.gpsNearestKm = Math.round(bestKm * 10) / 10;
      entry.gpsWarn = bestKm > GPS_WARN_KM;
      entry.gpsNote = entry.gpsWarn ? `No repeater within ${GPS_WARN_KM}km` : null;
    }
  }
  byId[id] = entry;
  data.byId = byId;
  data.updatedAt = new Date().toISOString();
  writeJsonSafe(observerStatusPath, data);
  if (initRfDb() && observerUpsert) {
    const gps = entry.gps && Number.isFinite(entry.gps.lat) && Number.isFinite(entry.gps.lon) ? entry.gps : null;
    observerUpsert.run(
      id,
      entry.name || null,
      entry.firstSeen || null,
      entry.lastSeen || null,
      entry.count || 0,
      gps ? gps.lat : null,
      gps ? gps.lon : null,
      new Date().toISOString()
    );
  }

  if (record.observerPub) {
    const pub = String(record.observerPub).toUpperCase();
    if (byPub[pub]) {
      byPub[pub].isObserver = true;
      byPub[pub].observerLastSeen = record.archivedAt;
      devices.updatedAt = new Date().toISOString();
      writeJsonSafe(devicesPath, devices);
      if (initRfDb() && deviceUpsert) {
        const gps = byPub[pub].gps && Number.isFinite(byPub[pub].gps.lat) && Number.isFinite(byPub[pub].gps.lon)
          ? byPub[pub].gps
          : null;
        deviceUpsert.run(
          pub,
          byPub[pub].name || null,
          byPub[pub].isRepeater ? 1 : 0,
          1,
          byPub[pub].lastSeen || null,
          byPub[pub].observerLastSeen || null,
          byPub[pub].lastAdvertHeardMs ?? null,
          gps ? gps.lat : null,
          gps ? gps.lon : null,
          byPub[pub].raw ? JSON.stringify(byPub[pub].raw) : null,
          null,
          new Date().toISOString()
        );
      }
    }
  }
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

const client = mqtt.connect(mqttUrl, {
  username: mqttUser,
  password: mqttPass,
  reconnectPeriod: 5000
});

client.on("connect", () => {
  console.log(`(mqtt-ingest) connected ${mqttUrl}`);
  logIngest("INFO", `connected ${mqttUrl}`);
  client.subscribe(mqttTopic, { qos: 0 }, (err) => {
    if (err) {
      console.error("(mqtt-ingest) subscribe failed", err.message);
      logIngest("ERROR", `subscribe failed ${err.message}`);
      return;
    }
    console.log(`(mqtt-ingest) subscribed ${mqttTopic}`);
    logIngest("INFO", `subscribed ${mqttTopic}`);
  });
});

client.on("message", (topic, payload) => {
  let msg;
  try {
    msg = JSON.parse(String(payload || ""));
  } catch {
    return;
  }

  const rawHex = String(msg.payloadHex || msg.raw || "").trim();
  if (!rawHex) return;

  const topicInfo = parseTopicInfo(topic);
  const frameHash = String(msg.frameHash || msg.hash || "").trim() || sha256Hex(rawHex);
  const record = {
    archivedAt: new Date().toISOString(),
    type: "observer",
    source: "mqtt",
    observerId: String(msg.observerId || msg.origin || topicInfo.iata || "observer").trim(),
    observerName: String(msg.observerName || "").trim() || null,
    observerPub: String(msg.observerPub || topicInfo.pub || msg.origin_id || "").trim(),
    rssi: toNumber(msg.rssi ?? msg.RSSI),
    snr: toNumber(msg.snr ?? msg.SNR),
    crc: msg.crc !== undefined ? !!msg.crc : true,
    payloadHex: rawHex.toUpperCase(),
    frameHash: frameHash || null,
    route: msg.route || null,
    path: msg.path || null,
    len: toNumber(msg.len),
    payloadLen: toNumber(msg.payload_len ?? msg.payloadLen),
    packetType: msg.packet_type || msg.packetType || null,
    gps: msg.gps || null,
    topic: String(topic || "")
  };

  appendObserver(record);
  updateDeviceFromAdvert(record);
  updateObserverStatus(record);
  storeRfPacket(record);
  storeMessage(record);
  logIngest(
    "MSG",
    `observer=${record.observerId} rssi=${record.rssi ?? "?"} len=${record.len ?? "?"}`
  );
});

client.on("error", (err) => {
  console.error("(mqtt-ingest) error", err.message);
  logIngest("ERROR", err.message);
});
