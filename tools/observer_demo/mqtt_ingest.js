#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const mqtt = require("mqtt");
const Database = require("better-sqlite3");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");

const projectRoot = path.resolve(__dirname, "..", "..");
const dataDir = path.join(projectRoot, "data");
const observerPath = path.join(dataDir, "observer.ndjson");
const observerStatusPath = path.join(dataDir, "observers.json");
const devicesPath = path.join(dataDir, "devices.json");
const ingestLogPath = path.join(dataDir, "ingest.log");
const keysPath = path.join(projectRoot, "tools", "meshcore_keys.json");
const dbPath = path.join(dataDir, "meshrank.db");
const GPS_WARN_KM = 50;
const ESTIMATE_MIN_HOURS = 4;
const ESTIMATE_MIN_RSSI = -75;
const RF_MAX_ROWS = 50000;
const RF_CLEAN_INTERVAL = 500;

let rfDb = null;
let rfInsert = null;
let rfPrune = null;
let rfInsertCount = 0;
let msgInsert = null;
let keysMtime = 0;
let keyStore = null;
let keyMap = {};

const mqttUrl = process.env.MESHRANK_MQTT_URL || "mqtts://meshrank.net:8883";
const mqttTopic = process.env.MESHRANK_MQTT_TOPIC || "meshrank/observers/+/packets";
const mqttUser = process.env.MESHRANK_MQTT_USER || undefined;
const mqttPass = process.env.MESHRANK_MQTT_PASS || undefined;

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

function initRfDb() {
  if (rfDb) return true;
  try {
    ensureDataDir();
    rfDb = new Database(dbPath);
    rfDb.pragma("journal_mode = WAL");
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
        path_length INTEGER,
        repeats INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_name, ts);
    `);
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
    msgInsert = rfDb.prepare(`
      INSERT INTO messages (
        message_hash, frame_hash, channel_name, channel_hash, sender, sender_pub,
        body, ts, path_json, path_length, repeats
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        END
    `);
    return true;
  } catch (err) {
    logIngest("ERROR", `rf db init failed ${err?.message || err}`);
    rfDb = null;
    rfInsert = null;
    rfPrune = null;
    msgInsert = null;
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
      pathLength,
      hopCount
    );
  } catch (err) {
    logIngest("ERROR", `message insert failed ${err?.message || err}`);
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
  const manualLoc = entry.locSource === "manual" || entry.manualLocation;
  if (!manualLoc) {
    updateEstimatedLocation(record, entry, byPub);
  }
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

  if (record.observerPub) {
    const pub = String(record.observerPub).toUpperCase();
    if (byPub[pub]) {
      byPub[pub].isObserver = true;
      byPub[pub].observerLastSeen = record.archivedAt;
      devices.updatedAt = new Date().toISOString();
      writeJsonSafe(devicesPath, devices);
    }
  }
}

function updateEstimatedLocation(record, entry, byPub) {
  if (!record.payloadHex || !Number.isFinite(record.rssi)) return;
  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(String(record.payloadHex).toUpperCase());
  } catch {
    return;
  }
  const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
  if (payloadType !== "Advert") return;
  const pathRaw = Array.isArray(decoded.path) ? decoded.path : [];
  const hopCount = pathRaw.length || (Number.isFinite(decoded.pathLength) ? decoded.pathLength : 0);
  if (hopCount > 1) return;
  const adv = decoded.payload?.decoded || decoded.decoded || decoded.payload || null;
  const pub = adv?.publicKey || adv?.pub || adv?.pubKey || null;
  if (!pub) return;
  const key = String(pub).toUpperCase();
  const rpt = byPub[key];
  if (!rpt || !rpt.gps || !Number.isFinite(rpt.gps.lat) || !Number.isFinite(rpt.gps.lon)) return;
  if (rpt.gps.lat === 0 && rpt.gps.lon === 0) return;

  if (!entry.locValidated) {
    entry.approxLatSum = (entry.approxLatSum || 0) + rpt.gps.lat;
    entry.approxLonSum = (entry.approxLonSum || 0) + rpt.gps.lon;
    entry.approxCount = (entry.approxCount || 0) + 1;
    entry.gpsApprox = {
      lat: entry.approxLatSum / entry.approxCount,
      lon: entry.approxLonSum / entry.approxCount
    };
    entry.locApprox = true;
    entry.locApproxAt = new Date().toISOString();
  }

  const best = Number.isFinite(entry.bestRssi) ? entry.bestRssi : -999;
  const bestHop = Number.isFinite(entry.bestHopCount) ? entry.bestHopCount : 99;
  const isBetterHop = hopCount < bestHop;
  const isBetterRssi = record.rssi > best;
  if (isBetterHop || (hopCount === bestHop && isBetterRssi)) {
    entry.bestRssi = record.rssi;
    entry.bestHopCount = hopCount;
    entry.bestRepeaterPub = key;
    entry.bestRepeaterName = rpt.name || key;
    entry.gps = rpt.gps;
    entry.locSource = rpt.name ? `${rpt.name} (direct)` : `${key} (direct)`;
  }

  const firstSeen = entry.firstSeen ? new Date(entry.firstSeen).getTime() : 0;
  const ageHours = firstSeen ? (Date.now() - firstSeen) / 3600000 : 0;
  if (entry.bestRssi >= ESTIMATE_MIN_RSSI && ageHours >= ESTIMATE_MIN_HOURS && entry.gps) {
    entry.locValidated = true;
    entry.locValidatedAt = new Date().toISOString();
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
