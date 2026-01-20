#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const mqtt = require("mqtt");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");

const projectRoot = path.resolve(__dirname, "..", "..");
const dataDir = path.join(projectRoot, "data");
const observerPath = path.join(dataDir, "observer.ndjson");
const observerStatusPath = path.join(dataDir, "observers.json");
const devicesPath = path.join(dataDir, "devices.json");
const ingestLogPath = path.join(dataDir, "ingest.log");
const GPS_WARN_KM = 50;
const ESTIMATE_MIN_HOURS = 4;
const ESTIMATE_MIN_RSSI = -75;

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
  updateEstimatedLocation(record, entry, byPub);
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
  logIngest(
    "MSG",
    `observer=${record.observerId} rssi=${record.rssi ?? "?"} len=${record.len ?? "?"}`
  );
});

client.on("error", (err) => {
  console.error("(mqtt-ingest) error", err.message);
  logIngest("ERROR", err.message);
});
