#!/usr/bin/env node
"use strict";

/**
 * Build repeater signal heat map data
 * Reads data/devices.json
 * Outputs data/heatmap.json (map-ready)
 */

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA = path.join(ROOT, "data");
const DEV_DB = path.join(DATA, "devices.json");
const OUT = path.join(DATA, "heatmap.json");

function loadJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function classifySignal(rssi) {
  if (rssi === undefined || rssi === null) return "unknown";
  if (rssi > -70) return "strong";     // green
  if (rssi > -95) return "medium";     // amber
  return "weak";                       // red
}

const devices = loadJson(DEV_DB);
if (!devices || !devices.byPub) {
  console.error("No devices.json yet");
  process.exit(1);
}

const heat = [];

for (const pub of Object.keys(devices.byPub)) {
  const d = devices.byPub[pub];

  if (d.role !== "repeater") continue; // focus on repeaters for now

  const rssi = d.stats?.bestRssi;
  const snr = d.stats?.bestSnr;

  heat.push({
    pubKey: pub,
    name: d.name,
    role: d.role,
    signal: classifySignal(rssi),
    bestRssi: rssi,
    bestSnr: snr,
    gps: d.gps || null,
    firstSeen: d.firstSeen,
    lastSeen: d.lastSeen,
    note: d.gps ? "location known" : "location unknown"
  });
}

fs.writeFileSync(OUT, JSON.stringify({
  generatedAt: new Date().toISOString(),
  count: heat.length,
  heat
}, null, 2));

console.log(`heatmap written: ${OUT}`);
