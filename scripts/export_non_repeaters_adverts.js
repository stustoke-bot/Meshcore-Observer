#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const dataPath = path.resolve(__dirname, "..", "data", "devices.json");
if (!fs.existsSync(dataPath)) {
  console.error("data/devices.json not found");
  process.exit(1);
}

let payload;
try {
  payload = JSON.parse(fs.readFileSync(dataPath, "utf8"));
} catch (err) {
  console.error("failed to parse devices.json:", err.message);
  process.exit(1);
}

const byPub = payload?.byPub || {};
const nowMs = Date.now();
const hoursArg = Number.parseFloat(process.argv[2]);
const hours = Number.isFinite(hoursArg) && hoursArg > 0 ? hoursArg : 24;
const cutoffMs = nowMs - hours * 60 * 60 * 1000;
const rows = [];

for (const [pub, entry] of Object.entries(byPub)) {
  if (!entry) continue;
  if (entry.isRepeater) continue;

  const rawAdvert = entry.raw?.lastAdvert;
  const timestampSec = rawAdvert?.timestamp ?? rawAdvert?.ts;
  const ts = Number(timestampSec);
  const tsMs = Number.isFinite(ts) ? (ts > 1e12 ? ts : ts * 1000) : null;
  const heardMs = entry.lastSeen ? Date.parse(entry.lastSeen) : null;
  const effectiveMs = Number.isFinite(heardMs) ? heardMs : tsMs;
  if (!Number.isFinite(effectiveMs)) continue;
  if (effectiveMs < cutoffMs) continue;
  if (effectiveMs > nowMs) continue;

  const name = entry.name || rawAdvert?.appData?.name || rawAdvert?.appData?.location?.name || "(unnamed)";
  const role = entry.appFlags?.roleName || rawAdvert?.appData?.roleName || rawAdvert?.appData?.deviceRole || "unknown";
  const heardIso = new Date(effectiveMs).toISOString();
  const advertIso = Number.isFinite(tsMs) ? new Date(tsMs).toISOString() : "";
  rows.push({
    pub,
    name,
    role,
    heardAt: heardIso,
    advertAt: advertIso,
    gps: rawAdvert?.appData?.location || entry.gps || null
  });
}

if (!rows.length) {
  console.log(`No non-repeater adverts in the past ${hours}h.`);
  process.exit(0);
}

rows.sort((a, b) => b.heardAt.localeCompare(a.heardAt));
console.log(`non-repeater adverts in last ${hours}h (count=${rows.length})`);
for (const row of rows) {
  console.log(`${row.heardAt}\t${row.advertAt}\t${row.pub}\t${row.role}\t${row.name}`);
}
