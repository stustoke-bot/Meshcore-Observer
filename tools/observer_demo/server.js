#!/usr/bin/env node
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const readline = require("readline");

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

const CHANNEL_TAIL_LINES = 10000;
const CHANNEL_HISTORY_LIMIT = 10;
const OBSERVER_DEBUG_TAIL_LINES = 5000;
const MESSAGE_DEBUG_TAIL_LINES = 20000;
const NODE_RANK_TAIL_LINES = 20000;

let observerHitsCache = { mtimeMs: null, size: null, map: new Map(), offset: 0, lastReadAt: 0 };
let channelMessagesCache = { mtimeMs: null, size: null, payload: null, builtAt: 0 };
let channelMessagesInFlight = null;
const CHANNEL_CACHE_MIN_MS = 500;
const CHANNEL_CACHE_STALE_MS = 1500;

async function getObserverHitsMap() {
  if (!fs.existsSync(observerPath)) return new Map();
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
  if (observerHitsCache.lastReadAt && (now - observerHitsCache.lastReadAt) < 500) {
    return observerHitsCache.map;
  }
  let map = observerHitsCache.map || new Map();
  let offset = observerHitsCache.offset || 0;
  if (stat.size < offset) {
    map = new Map();
    offset = 0;
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
    if (!observerKey) observerKey = rec.observerName || "";
    observerKey = String(observerKey).trim();
    const keys = [];
    const frameKey = rec.frameHash ? String(rec.frameHash).toUpperCase() : null;
    const hashKey = rec.hash ? String(rec.hash).toUpperCase() : null;
    const msgKey = rec.messageHash ? String(rec.messageHash).toUpperCase() : null;
    const payloadKey = rec.payloadHex ? sha256Hex(String(rec.payloadHex).toUpperCase()) : null;
    if (frameKey) keys.push(frameKey);
    if (hashKey) keys.push(hashKey);
    if (msgKey) keys.push(msgKey);
    if (payloadKey) keys.push(payloadKey);
    if (!msgKey && rec.payloadHex && keyStore) {
      try {
        const decoded = MeshCoreDecoder.decode(String(rec.payloadHex).toUpperCase(), { keyStore });
        const payloadType = Utils.getPayloadTypeName(decoded.payloadType);
        if (payloadType === "GroupText" && decoded.messageHash) {
          keys.push(String(decoded.messageHash).toUpperCase());
        }
      } catch {}
    }
    if (!observerKey || !keys.length) continue;
    keys.forEach((key) => {
      if (!map.has(key)) map.set(key, new Set());
      map.get(key).add(observerKey);
    });
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
const RANK_REFRESH_MS = 15 * 60 * 1000;
const NODE_RANK_REFRESH_MS = 5 * 60 * 1000;

let rankCache = { updatedAt: null, count: 0, items: [], cachedAt: null };
let rankRefreshInFlight = null;

let observerRankCache = { updatedAt: null, items: [] };
let observerRankRefresh = null;

let nodeRankCache = { updatedAt: null, count: 0, items: [], cachedAt: null };
let nodeRankRefreshInFlight = null;

let ChannelCrypto;
try {
  ({ ChannelCrypto } = require("@michaelhart/meshcore-decoder/dist/crypto/channel-crypto"));
} catch {
  ChannelCrypto = null;
}

const host = "0.0.0.0";
const port = 5199;

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
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
  const devices = readJsonSafe(devicesPath, { byPub: {} });
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
  const observers = readJsonSafe(observersPath, { byId: {} });
  const byId = observers.byId || {};
  const devices = readJsonSafe(devicesPath, { byPub: {} });
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
    const manualGps = entry.manualLocation ? (entry.gpsApprox || entry.gps) : null;
    stats.set(entry.id, {
      id: entry.id,
      name: entry.name || entry.id,
      firstSeen: entry.firstSeen || null,
      lastSeen: entry.lastSeen || null,
      gps: manualGps || entry.gps || null,
      locSource: entry.locSource || (manualGps ? "manual" : null),
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
          repeaters: new Set()
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
      if (ageHours > 24 * 7) continue;
      const uptimeHours = firstSeen ? (now - firstSeen) / 3600000 : 0;
    let gps = s.gps && Number.isFinite(s.gps.lat) && Number.isFinite(s.gps.lon) ? s.gps : null;
    let locSource = s.locSource;
    if (!gps && s.bestRepeaterPub && repeatersByPub.has(String(s.bestRepeaterPub).toUpperCase())) {
      gps = repeatersByPub.get(String(s.bestRepeaterPub).toUpperCase())?.gps || null;
      locSource = locSource || s.bestRepeaterPub;
    }
    let coverageKm = 0;
    let nearestRepeaterName = null;
    let nearestRepeaterKm = null;
    if (gps && s.repeaters.size) {
      let maxKm = 0;
      for (const pub of s.repeaters) {
        const rpt = repeatersByPub.get(pub);
        const rptGps = rpt?.gps;
        if (!rptGps) continue;
        const km = haversineKm(gps.lat, gps.lon, rptGps.lat, rptGps.lon);
        if (km > maxKm) maxKm = km;
      }
      coverageKm = Math.round(maxKm * 10) / 10;
    }
    if (gps) {
      let bestKm = Infinity;
      let bestName = null;
      for (const rpt of repeatersByPub.values()) {
        if (!rpt?.gps) continue;
        const km = haversineKm(gps.lat, gps.lon, rpt.gps.lat, rpt.gps.lon);
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
    const coverageCount = s.repeaters.size;
    const score = scoreFor({ uptimeHours, packetsToday: s.packetsToday });
    const scoreColor = colorForAge(ageHours);
    const isStale = ageHours > 48;
    items.push({
      id: s.id,
      name: s.name || s.id,
      gps,
      lastSeen: s.lastSeen,
      firstSeen: s.firstSeen,
      ageHours,
      stale: isStale,
      uptimeHours,
      packetsToday: s.packetsToday,
      coverageKm,
      coverageCount,
      nearestRepeaterName,
      nearestRepeaterKm,
      locSource,
      score,
      scoreColor
    });
  }

  items.sort((a, b) => (b.score - a.score) || (b.packetsToday - a.packetsToday));
  return { updatedAt: new Date().toISOString(), items };
}

async function buildObserverDebug(observerId) {
  const id = String(observerId || "").trim();
  if (!id) return { ok: false, error: "observerId required" };
  const devices = readJsonSafe(devicesPath, { byPub: {} });
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
        hasGps: !!(dev?.gps && Number.isFinite(dev.gps.lat) && Number.isFinite(dev.gps.lon)),
        gps: dev?.gps || null,
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

async function buildRepeaterRank() {
  const devices = readJsonSafe(devicesPath, { byPub: {} });
  const byPub = devices.byPub || {};
  const nodeMap = buildNodeHashMap();
  const isRepeaterPub = (pub) => {
    if (!pub) return false;
    const entry = byPub[String(pub).toUpperCase()];
    return !!entry?.isRepeater;
  };

  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;

  const stats = new Map(); // pub -> {total24h, msgCounts: Map, rssi: [], snr: [], bestRssi, bestSnr, zeroHopNeighbors}

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
      if (payloadTypeName !== "Advert") continue;
        const adv = decoded?.payload?.decoded || decoded?.decoded || decoded;
        if (!adv || typeof adv !== "object") continue;
        const pub = adv.publicKey || adv.pub || adv.pubKey;
        if (!pub) continue;
        if (!isRepeaterPub(pub)) continue;
        const ts = parseIso(rec.ts);
        if (!ts) continue;
        if (now - ts.getTime() > windowMs) continue;

      if (!stats.has(pub)) stats.set(pub, { total24h: 0, msgCounts: new Map(), lastSeenTs: null, zeroHopNeighbors: new Set() });
      const s = stats.get(pub);
      s.total24h += 1;
      if (!s.lastSeenTs || ts > s.lastSeenTs) s.lastSeenTs = ts;
      if (!s.rssi) s.rssi = [];
      if (!s.snr) s.snr = [];
      if (Number.isFinite(rec.rssi)) s.rssi.push(rec.rssi);
      if (Number.isFinite(rec.snr)) s.snr.push(rec.snr);
      if (!Number.isFinite(s.bestRssi) || (Number.isFinite(rec.rssi) && rec.rssi > s.bestRssi)) s.bestRssi = rec.rssi;
      if (!Number.isFinite(s.bestSnr) || (Number.isFinite(rec.snr) && rec.snr > s.bestSnr)) s.bestSnr = rec.snr;
      const path = Array.isArray(decoded?.path) ? decoded.path.map(normalizePathHash) : [];
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
      if (payloadTypeName !== "Advert") continue;
        const adv = decoded?.payload?.decoded || decoded?.decoded || decoded;
        if (!adv || typeof adv !== "object") continue;
        const pub = adv.publicKey || adv.pub || adv.pubKey;
        if (!pub) continue;
        if (!isRepeaterPub(pub)) continue;
        const ts = parseIso(rec.archivedAt);
        if (!ts) continue;
        if (now - ts.getTime() > windowMs) continue;

      if (!stats.has(pub)) stats.set(pub, { total24h: 0, msgCounts: new Map(), lastSeenTs: null, zeroHopNeighbors: new Set() });
      const s = stats.get(pub);
      s.total24h += 1;
      if (!s.lastSeenTs || ts > s.lastSeenTs) s.lastSeenTs = ts;
      if (!s.rssi) s.rssi = [];
      if (!s.snr) s.snr = [];
      if (Number.isFinite(rec.rssi)) s.rssi.push(rec.rssi);
      if (Number.isFinite(rec.snr)) s.snr.push(rec.snr);
      if (!Number.isFinite(s.bestRssi) || (Number.isFinite(rec.rssi) && rec.rssi > s.bestRssi)) s.bestRssi = rec.rssi;
      if (!Number.isFinite(s.bestSnr) || (Number.isFinite(rec.snr) && rec.snr > s.bestSnr)) s.bestSnr = rec.snr;
      const path = Array.isArray(decoded?.path) ? decoded.path.map(normalizePathHash) : [];
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

  const items = Object.entries(byPub)
    .filter(([, d]) => d && d.isRepeater)
    .map(([key, d]) => {
      const pub = d.pub || d.publicKey || d.pubKey || d.raw?.lastAdvert?.publicKey || key;
      const s = stats.get(pub) || { total24h: 0, msgCounts: new Map(), rssi: [], snr: [], zeroHopNeighbors: new Set() };
      const lastSeen = s.lastSeenTs ? s.lastSeenTs.toISOString() : (d.lastSeen || null);
      const lastSeenDate = parseIso(lastSeen);
      const ageHours = lastSeenDate ? (now - lastSeenDate.getTime()) / 3600000 : Infinity;
        const isStale = ageHours > 48;
        if (ageHours > 24 * 7) return null;
        const uniqueMsgs = s.msgCounts.size || 0;
        const avgRepeats = uniqueMsgs ? (s.total24h / uniqueMsgs) : 0;
        const zeroHopNeighbors24h = s.zeroHopNeighbors ? s.zeroHopNeighbors.size : 0;
        const zeroHopNeighborNames = s.zeroHopNeighbors
          ? Array.from(s.zeroHopNeighbors)
            .map((hash) => nodeMap.get(hash)?.name || hash)
            .filter(Boolean)
          : [];

      const avgRssi = trimmedMean(s.rssi, 0.1);
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

        return {
          pub,
          hashByte: nodeHashFromPub(pub),
          name: d.name || d.raw?.lastAdvert?.appData?.name || "Unknown",
          gps: d.gps || null,
        lastSeen,
        isObserver: !!d.isObserver,
        bestRssi,
        bestSnr,
        avgRssi: Number.isFinite(avgRssi) ? Number(avgRssi.toFixed(2)) : null,
        avgSnr: Number.isFinite(avgSnr) ? Number(avgSnr.toFixed(2)) : null,
          total24h: s.total24h,
          zeroHopNeighbors24h,
          zeroHopNeighborNames,
          uniqueMsgs,
          avgRepeats: Number(avgRepeats.toFixed(2)),
        score: Math.round(score),
        stale: isStale,
        color,
        hiddenOnMap: !!d.hiddenOnMap
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
  const devices = readJsonSafe(devicesPath, { byPub: {} });
  const byPub = devices.byPub || {};
  const all = Object.values(byPub).filter(Boolean);
  const companions = all.filter((d) => !d.isRepeater && d.role !== "room_server" && d.role !== "chat");
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
    return rankCache;
  })();
  try {
    return await rankRefreshInFlight;
  } finally {
    rankRefreshInFlight = null;
  }
}

async function buildChannelMessages() {
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
          repeats: hopCount,
          path,
          pathNames,
          pathPoints,
          observerHits,
          observerCount
        });
      } else {
        const m = messagesMap.get(msgKey);
        m.repeats += hopCount;
        if (ts && (!m.ts || new Date(ts) > new Date(m.ts))) m.ts = ts;
        if (path.length > (m.path?.length || 0)) {
          m.path = path;
          m.pathNames = pathNames;
          m.pathPoints = pathPoints;
        }
        if (observerCount && !m.observerCount) {
          m.observerCount = observerCount;
          m.observerHits = observerHits;
        }
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

    if (!messagesMap.has(msgKey)) {
      messagesMap.set(msgKey, {
        id: msgHash,
        frameHash,
        messageHash,
        channelName: chName,
        sender,
        body,
        ts,
        repeats: hopCount,
        path,
        pathNames,
        pathPoints,
        observerHits,
        observerCount
      });
    } else {
      const m = messagesMap.get(msgKey);
      m.repeats += hopCount;
      if (ts && (!m.ts || new Date(ts) > new Date(m.ts))) m.ts = ts;
      if (path.length > (m.path?.length || 0)) {
        m.path = path;
        m.pathNames = pathNames;
        m.pathPoints = pathPoints;
      }
      if (observerCount && !m.observerCount) {
        m.observerCount = observerCount;
        m.observerHits = observerHits;
      }
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
  const devices = readJsonSafe(devicesPath, { byPub: {} });
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

  const latestKey = series.length ? series[series.length - 1].date : new Date().toISOString().slice(0, 10);
  const latestIndex = series.findIndex((s) => s.date === latestKey);
  const today = series.find((s) => s.date === latestKey) || { score: 0, messages: 0, avgRepeats: 0 };
  const yesterday = latestIndex > 0 ? series[latestIndex - 1] : { score: 0, messages: 0, avgRepeats: 0 };

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
    series
  };
}

async function buildRfLatest(limit) {
  const sourcePath = getRfSourcePath();
  const lines = await tailLines(sourcePath, limit || 80);
  const keyCfg = loadKeys();
  const keyStore = buildKeyStore(keyCfg);
  const devices = readJsonSafe(devicesPath, { byPub: {} });
  const byPub = devices.byPub || {};
  const keyMap = {};
  for (const ch of keyCfg.channels || []) {
    if (ch?.hashByte && ch?.name) keyMap[String(ch.hashByte).toUpperCase()] = String(ch.name);
  }
  const nodeMap = buildNodeHashMap();
  const items = [];
  for (const line of lines) {
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
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
    const senderPublicKey = payloadDecoded?.senderPublicKey || payloadDecoded?.publicKey || payloadDecoded?.senderPub || null;
    const senderName = senderPublicKey ? (byPub[senderPublicKey]?.name || byPub[senderPublicKey]?.raw?.lastAdvert?.appData?.name || null) : null;
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
      if (observerLabel) clone.observerHits.add(observerLabel);
      grouped.set(key, clone);
      continue;
    }
    if (observerLabel) existing.observerHits.add(observerLabel);
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
    return {
      ...item,
      observerHits: hits,
      observerCount: hits.length
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

  if (u.pathname === "/api/channels") {
    if (req.method === "GET") {
      const payload = await buildChannelMessages();
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ channels: payload.channels }));
    }
    if (req.method === "DELETE") {
      try {
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

  if (u.pathname === "/api/messages") {
    const payload = await buildChannelMessages();
    const channel = u.searchParams.get("channel");
    const limitRaw = u.searchParams.get("limit");
    const beforeRaw = u.searchParams.get("before");
    const limit = limitRaw ? Number(limitRaw) : null;
    let beforeTs = null;
    if (beforeRaw) {
      const beforeDate = parseIso(beforeRaw);
      if (beforeDate) beforeTs = beforeDate.getTime();
    }

    let list = payload.messages;
    if (channel) {
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
      for (const bucket of perChannel.values()) {
        const keep = bucket.slice(Math.max(0, bucket.length - CHANNEL_HISTORY_LIMIT));
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
    const limit = limitRaw ? Number(limitRaw) : 0;
    if (force || !last) {
      await refreshRankCache(true);
    } else if (now - last >= RANK_REFRESH_MS) {
      refreshRankCache(false).catch(() => {});
    }
    const payload = limit > 0
      ? { ...rankCache, items: rankCache.items.slice(0, limit) }
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
    const last = rankCache.updatedAt ? new Date(rankCache.updatedAt).getTime() : 0;
    if (!last || now - last >= RANK_REFRESH_MS) {
      await refreshRankCache(true);
    }
    const summary = {
      updatedAt: rankCache.updatedAt,
      count: rankCache.count,
      totals: {
        active: rankCache.items.filter((r) => !r.stale).length,
        total24h: rankCache.items.reduce((sum, r) => sum + (r.total24h || 0), 0)
      }
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(summary));
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
      const devices = readJsonSafe(devicesPath, { byPub: {} });
      const byPub = devices.byPub || {};
      if (!byPub[pub]) {
        return send(res, 404, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: "pub not found" }));
      }
      byPub[pub].hiddenOnMap = hidden;
      devices.updatedAt = new Date().toISOString();
      const tmp = devicesPath + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(devices, null, 2));
      fs.renameSync(tmp, devicesPath);
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({ ok: true, hidden }));
    } catch (err) {
      return send(res, 400, "application/json; charset=utf-8", JSON.stringify({ ok: false, error: String(err?.message || err) }));
    }
  }

  if (u.pathname === "/api/meshscore") {
    const payload = await buildMeshScore();
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (u.pathname === "/api/observers") {
    const payload = readJsonSafe(observersPath, { byId: {}, updatedAt: null });
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
      const data = readJsonSafe(observersPath, { byId: {} });
      const byId = data.byId || {};
      const entry = byId[id] || { id, firstSeen: new Date().toISOString(), count: 0 };
      entry.gpsApprox = { lat, lon };
      entry.locApprox = true;
      entry.locApproxAt = new Date().toISOString();
      entry.locSource = "manual";
      entry.manualLocation = true;
      byId[id] = entry;
      data.byId = byId;
      data.updatedAt = new Date().toISOString();
      writeJsonSafe(observersPath, data);
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

  if (u.pathname === "/api/observer-rank") {
    const now = Date.now();
    const last = observerRankCache.updatedAt ? new Date(observerRankCache.updatedAt).getTime() : 0;
    const force = u.searchParams.get("refresh") === "1";
    const limitRaw = u.searchParams.get("_limit");
    const limit = limitRaw ? Number(limitRaw) : 0;
    if (force || !last || now - last >= 5 * 60 * 1000) {
      if (!observerRankRefresh) {
        observerRankRefresh = buildObserverRank()
          .then((data) => { observerRankCache = data; })
          .finally(() => { observerRankRefresh = null; });
      }
      await observerRankRefresh;
    }
    const payload = limit > 0
      ? { ...observerRankCache, items: observerRankCache.items.slice(0, limit) }
      : observerRankCache;
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }
  if (u.pathname === "/api/observer-rank-summary") {
    const now = Date.now();
    const last = observerRankCache.updatedAt ? new Date(observerRankCache.updatedAt).getTime() : 0;
    if (!last || now - last >= 5 * 60 * 1000) {
      if (!observerRankRefresh) {
        observerRankRefresh = buildObserverRank()
          .then((data) => { observerRankCache = data; })
          .finally(() => { observerRankRefresh = null; });
      }
      await observerRankRefresh;
    }
    const summary = {
      updatedAt: observerRankCache.updatedAt,
      count: observerRankCache.items.length,
      totals: {
        active: observerRankCache.items.filter((o) => o.ageHours < 1).length,
        packetsToday: observerRankCache.items.reduce((sum, o) => sum + (o.packetsToday || 0), 0)
      }
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(summary));
  }

  if (u.pathname === "/api/route-suggestions") {
    const suggestions = getRouteSuggestions();
    const devices = readJsonSafe(devicesPath, { byPub: {} });
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
      const devices = readJsonSafe(devicesPath, { byPub: {} });
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
    const limit = Number(u.searchParams.get("limit") || 400);
    const payload = await buildRfLatest(limit);
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  return send(res, 404, "text/plain; charset=utf-8", "Not found");
});

server.listen(port, host, () => {
  console.log(`(observer-demo) http://${host}:${port}`);
});
