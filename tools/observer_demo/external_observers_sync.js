#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const API_BASE = "https://api.letsmesh.net/api";
const PACKETS_URL = `${API_BASE}/packets/filtered?limit=500`;
const OBSERVERS_URL = `${API_BASE}/observers`;

const projectRoot = path.resolve(__dirname, "..", "..");
const dataDir = path.join(projectRoot, "data");
const outputPath = path.join(dataDir, "external_observers.json");
const messageStatsPath = path.join(dataDir, "external_message_stats.json");
const externalPacketsPath = path.join(dataDir, "external_packets.ndjson");
const keysPath = path.join(projectRoot, "tools", "meshcore_keys.json");
const localPacketsPath = path.join(dataDir, "letsmesh_packets.json");
const localObserversPath = path.join(dataDir, "letsmesh_observers.json");

const UK_REGIONS = new Set([
  "LHR", "LCY", "LON", "LTN", "OXF", "MAN", "UPV", "LPL", "BHX", "LBA", "MME", "CBG"
]);

function isUkRegion(regions) {
  if (!Array.isArray(regions) || !regions.length) return false;
  return regions.some((r) => UK_REGIONS.has(String(r).toUpperCase()));
}

let MeshCoreDecoder;
let Utils;
try {
  ({ MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder"));
} catch {}

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

function buildKeyStore(keys) {
  if (!keys || !Array.isArray(keys.channels)) return null;
  const store = {};
  keys.channels.forEach((ch) => {
    if (!ch?.hashByte || !ch?.secretHex) return;
    store[String(ch.hashByte).toUpperCase()] = String(ch.secretHex);
  });
  return Object.keys(store).length ? store : null;
}

const adjectives = [
  "Brisk", "Quiet", "Calm", "Swift", "Bold", "Bright", "Mellow", "Cool",
  "Keen", "Sunny", "Misty", "Clear", "Nimble", "Solid", "Steady", "Sharp",
  "Amber", "Ivory", "Crimson", "Azure", "Olive", "Slate", "Sable", "Cobalt"
];
const nouns = [
  "Falcon", "Otter", "Hawk", "Pine", "Ridge", "Stone", "River", "Harbor",
  "Cedar", "Wolf", "Oak", "Grove", "Heron", "Lynx", "Puma", "Fox",
  "Wren", "Bison", "Cairn", "Peak", "Dune", "Vale", "Fjord", "Cove"
];

function anonymizeName(id) {
  const hash = crypto.createHash("sha1").update(String(id)).digest();
  const adj = adjectives[hash[0] % adjectives.length];
  const noun = nouns[hash[1] % nouns.length];
  const suffix = String((hash[2] % 90) + 10);
  return `${adj} ${noun} ${suffix}`;
}

function parseIso(value) {
  const d = new Date(value || "");
  return Number.isFinite(d.getTime()) ? d.toISOString() : null;
}

async function fetchJson(url) {
  const headers = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "referer": "https://analyzer.letsmesh.net/",
    "origin": "https://analyzer.letsmesh.net"
  };
  if (process.env.LETSMESH_API_KEY) {
    headers["authorization"] = `Bearer ${process.env.LETSMESH_API_KEY}`;
  }
  if (process.env.LETSMESH_COOKIE) {
    headers["cookie"] = process.env.LETSMESH_COOKIE;
  }
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return await res.json();
}

function coerceId(obj) {
  const raw = obj?.public_key || obj?.id || obj?.observer_id || obj?.observerId || obj?.origin_id || obj?.originId;
  return raw ? String(raw).trim() : null;
}

function coerceName(obj) {
  return obj?.name || obj?.observer_name || obj?.observerName || obj?.origin || null;
}

function coerceGps(obj) {
  const lat = obj?.lat ?? obj?.latitude ?? obj?.gps?.lat;
  const lon = obj?.lon ?? obj?.longitude ?? obj?.gps?.lon;
  if (!Number.isFinite(Number(lat)) || !Number.isFinite(Number(lon))) return null;
  return { lat: Number(lat), lon: Number(lon) };
}

function coerceTime(obj, keys) {
  for (const k of keys) {
    const value = obj?.[k];
    const iso = parseIso(value);
    if (iso) return iso;
  }
  return null;
}

function upsert(map, id, patch) {
  if (!map[id]) map[id] = { id, count: 0 };
  Object.assign(map[id], patch);
}

async function main() {
  const byId = {};
  const byHash = {};
  let packets = [];
  let observers = [];
  const keyStore = buildKeyStore(loadKeys());

  if (fs.existsSync(localPacketsPath)) {
    packets = JSON.parse(fs.readFileSync(localPacketsPath, "utf8"));
  } else {
    try { packets = await fetchJson(PACKETS_URL); } catch (err) { console.error("packets fetch failed:", err.message); }
  }
  if (fs.existsSync(localObserversPath)) {
    observers = JSON.parse(fs.readFileSync(localObserversPath, "utf8"));
  } else {
    try { observers = await fetchJson(OBSERVERS_URL); } catch (err) { console.error("observers fetch failed:", err.message); }
  }

  if (Array.isArray(observers)) {
    for (const obj of observers) {
      if (!isUkRegion(obj?.region ? [obj.region] : obj?.regions)) continue;
      const id = coerceId(obj);
      if (!id) continue;
      const gps = coerceGps(obj);
      const lastSeen = coerceTime(obj, [
        "last_packet_ingested",
        "last_status_at",
        "last_seen",
        "lastSeen",
        "heard_at",
        "heardAt",
        "updated_at",
        "updatedAt"
      ]);
      const firstSeen = coerceTime(obj, ["first_seen", "firstSeen", "created_at", "createdAt"]);
      const name = anonymizeName(id);
      upsert(byId, id, {
        name,
        sourceName: coerceName(obj),
        gps,
        gpsApprox: gps || null,
        locApprox: !!gps,
        locSource: gps ? "external" : null,
        lastSeen: lastSeen || byId[id]?.lastSeen || null,
        firstSeen: firstSeen || byId[id]?.firstSeen || null,
        source: "external"
      });
    }
  }

  if (Array.isArray(packets)) {
    const externalLines = [];
    for (const pkt of packets) {
      if (!isUkRegion(pkt?.regions)) continue;
      const id = coerceId(pkt);
      if (!id) continue;
      const hash = pkt?.hash ? String(pkt.hash).toUpperCase() : null;
      const heardAt = parseIso(pkt?.heard_at || pkt?.heardAt || pkt?.created_at || pkt?.createdAt);
      const name = anonymizeName(id);
      const prev = byId[id]?.lastSeen ? new Date(byId[id].lastSeen).getTime() : 0;
      const next = heardAt ? new Date(heardAt).getTime() : 0;
      upsert(byId, id, {
        name,
        lastSeen: next > prev ? heardAt : (byId[id]?.lastSeen || heardAt || null),
        firstSeen: byId[id]?.firstSeen || heardAt || null,
        source: "external"
      });
      byId[id].count = Number(byId[id].count || 0) + 1;

      if (hash) {
        let msgHash = hash;
        if (MeshCoreDecoder && typeof pkt?.raw_data === "string") {
          try {
            const decoded = MeshCoreDecoder.decode(String(pkt.raw_data).toUpperCase(), keyStore ? { keyStore } : undefined);
            if (decoded?.messageHash) msgHash = String(decoded.messageHash).toUpperCase();
          } catch {}
        }
        if (!byHash[hash]) byHash[hash] = { observers: {}, maxHops: 0, lastSeen: null };
        byHash[hash].observers[id] = true;
        const hops = Array.isArray(pkt?.path) ? pkt.path.length : 0;
        if (hops > byHash[hash].maxHops) byHash[hash].maxHops = hops;
        if (heardAt) {
          const prev = byHash[hash].lastSeen ? new Date(byHash[hash].lastSeen).getTime() : 0;
          const next = new Date(heardAt).getTime();
          if (next > prev) byHash[hash].lastSeen = heardAt;
        }

        if (msgHash && msgHash !== hash) {
          if (!byHash[msgHash]) byHash[msgHash] = { observers: {}, maxHops: 0, lastSeen: null };
          byHash[msgHash].observers[id] = true;
          if (hops > byHash[msgHash].maxHops) byHash[msgHash].maxHops = hops;
          if (heardAt) {
            const prev = byHash[msgHash].lastSeen ? new Date(byHash[msgHash].lastSeen).getTime() : 0;
            const next = new Date(heardAt).getTime();
            if (next > prev) byHash[msgHash].lastSeen = heardAt;
          }
        }
      }

      if (typeof pkt?.raw_data === "string") {
        externalLines.push(JSON.stringify({
          archivedAt: heardAt || null,
          payloadHex: String(pkt.raw_data).toUpperCase(),
          hash: hash || null,
          observerId: id,
          observerName: name,
          source: "external"
        }));
      }
    }
    if (externalLines.length) {
      fs.writeFileSync(externalPacketsPath, externalLines.join("\n") + "\n");
    }
  }

  const payload = { byId, updatedAt: new Date().toISOString() };
  const messagePayload = {
    byHash: Object.fromEntries(
      Object.entries(byHash).map(([hash, entry]) => [
        hash,
        {
          observers: Object.keys(entry.observers),
          maxHops: entry.maxHops,
          lastSeen: entry.lastSeen || null
        }
      ])
    ),
    updatedAt: new Date().toISOString()
  };
  fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2));
  fs.writeFileSync(messageStatsPath, JSON.stringify(messagePayload, null, 2));
  console.log(`Wrote ${Object.keys(byId).length} external observers to ${outputPath}`);
  console.log(`Wrote ${Object.keys(messagePayload.byHash).length} external message stats to ${messageStatsPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
