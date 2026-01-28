#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

const devicesPath = path.resolve(__dirname, "..", "data", "devices.json");
if (!fs.existsSync(devicesPath)) {
  console.error("data/devices.json not found");
  process.exit(1);
}

let devicesPayload;
try {
  devicesPayload = JSON.parse(fs.readFileSync(devicesPath, "utf8"));
} catch (err) {
  console.error("failed to parse devices.json:", err.message);
  process.exit(1);
}

const hoursArg = Number.parseFloat(process.argv[2]);
const hours = Number.isFinite(hoursArg) && hoursArg > 0 ? hoursArg : 24;
const nowMs = Date.now();
const cutoffMs = nowMs - hours * 60 * 60 * 1000;

function toMsFromTimestamp(raw) {
  if (raw === undefined || raw === null) return null;
  const num = Number(raw);
  if (!Number.isFinite(num)) return null;
  return num > 1e12 ? num : num * 1000;
}

const liveRepeaters = new Map();
for (const [pubKey, entry] of Object.entries(devicesPayload.byPub || {})) {
  if (!entry) continue;
  const isRepeater = !!(entry.isRepeater || entry.appFlags?.isRepeater);
  if (!isRepeater) continue;

  const heardMs = entry.lastSeen ? Date.parse(entry.lastSeen) : null;
  const rawAdvert = entry.raw?.lastAdvert;
  const advertMs = toMsFromTimestamp(rawAdvert?.timestamp ?? rawAdvert?.ts);
  const effectiveMs = Number.isFinite(heardMs) ? heardMs : advertMs;
  if (!Number.isFinite(effectiveMs)) continue;
  if (effectiveMs < cutoffMs || effectiveMs > nowMs) continue;

  const name = entry.name || rawAdvert?.appData?.name || rawAdvert?.appData?.location?.name || "(unnamed)";
  const role = entry.appFlags?.roleName || rawAdvert?.appData?.roleName || rawAdvert?.appData?.deviceRole || "unknown";
  liveRepeaters.set(pubKey.toUpperCase(), {
    pub: pubKey.toUpperCase(),
    name,
    role,
    heardAt: new Date(effectiveMs).toISOString(),
    advertAt: Number.isFinite(advertMs) ? new Date(advertMs).toISOString() : ""
  });
}

const dbPath = path.resolve(__dirname, "..", "data", "meshrank.db");
if (!fs.existsSync(dbPath)) {
  console.error("data/meshrank.db not found");
  process.exit(1);
}

const db = new Database(dbPath, { readonly: true });
const cacheRow = db.prepare("SELECT payload FROM repeater_rank_cache WHERE id = 1").get();
if (!cacheRow || !cacheRow.payload) {
  console.error("repeater_rank_cache payload missing");
  process.exit(1);
}

const cachePayload = JSON.parse(cacheRow.payload);
const items = Array.isArray(cachePayload.items) ? cachePayload.items : [];

const zeroHopRows = [];
const liveSet = new Set(liveRepeaters.keys());

for (const item of items) {
  const pub = String(item.pub || "").toUpperCase();
  if (!pub) continue;
  if (!liveSet.has(pub)) continue;

  const neighbors = new Map();
  const details = Array.isArray(item.zeroHopNeighborDetails) ? item.zeroHopNeighborDetails : [];
  for (const detail of details) {
    const neighborPub = String(detail.pub || "").toUpperCase();
    if (!neighborPub) continue;
    if (!liveSet.has(neighborPub)) continue;
    const displayName = detail.name || neighborPub;
    neighbors.set(neighborPub, `${displayName} (${neighborPub})`);
  }

  zeroHopRows.push({
    pub,
    name: liveRepeaters.get(pub)?.name || item.name || "(unnamed)",
    role: liveRepeaters.get(pub)?.role || item.role || "unknown",
    heardAt: liveRepeaters.get(pub)?.heardAt || "",
    advertAt: liveRepeaters.get(pub)?.advertAt || "",
    count: neighbors.size,
    neighbors: Array.from(neighbors.values()).join("|")
  });
}

if (!zeroHopRows.length) {
  console.log(`No zero-hop neighbors for repeaters seen in the past ${hours}h.`);
  process.exit(0);
}

zeroHopRows.sort((a, b) => b.count - a.count || b.heardAt.localeCompare(a.heardAt));
console.log(`repeater zero-hop neighbors last ${hours}h (count=${zeroHopRows.length})`);
console.log("heardAt\tadvertAt\tpub\tname\trole\tzeroHopCount\tmatchedNeighbors");
for (const row of zeroHopRows) {
  console.log(`${row.heardAt}\t${row.advertAt}\t${row.pub}\t${row.name}\t${row.role}\t${row.count}\t${row.neighbors}`);
}
