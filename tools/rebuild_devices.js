#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { Utils } = require("@michaelhart/meshcore-decoder");

const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");
const decodedPath = path.join(dataDir, "decoded.ndjson");
const devicesPath = path.join(dataDir, "devices.json");

function nowIso() {
  return new Date().toISOString();
}

function saveJsonAtomic(p, obj) {
  const tmp = p + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
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

function updateDeviceFromAdvert(devices, advert, rssi, snr, channelName, ts) {
  const pub = advert.publicKey || advert.pub || advert.pubKey;
  if (!pub) return null;

  const prev = devices.byPub[pub] || {};

  const flagsByte =
    (typeof advert.appFlags === "number" ? advert.appFlags : null) ??
    (typeof advert.flags === "number" ? advert.flags : null) ??
    (typeof advert.appData?.flags === "number" ? advert.appData.flags : null);

  const flags = decodeAppFlags(flagsByte);

  let gps = prev.gps || null;
  const loc = advert.gps || advert.location || advert.appData?.location || null;
  if (flags?.hasLocation && loc) {
    const lat = Number(loc.lat ?? loc.latitude);
    const lon = Number(loc.lon ?? loc.lng ?? loc.longitude);
    if (Number.isFinite(lat) && Number.isFinite(lon)) gps = { lat, lon };
  }

  const isRepeater = !!(flags?.isRepeater);
  const name = advert.appData?.name || advert.name || prev.name || null;

  const updated = {
    ...prev,
    pub,
    channel: channelName || prev.channel || "#unknown",
    name,
    role: flags?.roleName || "unknown",
    isRepeater,
    appFlags: flags || null,
    gps,
    stats: {
      lastRssi: rssi,
      lastSnr: snr,
      bestRssi: Math.max(prev?.stats?.bestRssi ?? -999, rssi),
      bestSnr: Math.max(prev?.stats?.bestSnr ?? -999, snr)
    },
    firstSeen: prev.firstSeen || ts || nowIso(),
    lastSeen: ts || nowIso(),
    raw: {
      ...(prev.raw || {}),
      lastAdvert: advert
    }
  };

  devices.byPub[pub] = updated;
  return updated;
}

async function rebuild() {
  if (!fs.existsSync(decodedPath)) {
    console.error("(rebuild) missing " + decodedPath);
    process.exit(1);
  }

  if (fs.existsSync(devicesPath)) {
    const stamp = nowIso().replace(/[:.]/g, "-");
    const backup = devicesPath + ".bak-" + stamp;
    fs.copyFileSync(devicesPath, backup);
    console.log("(rebuild) backup=" + backup);
  }

  const devices = { version: 1, updatedAt: nowIso(), byPub: {} };

  const rl = readline.createInterface({
    input: fs.createReadStream(decodedPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let total = 0;
  let adverts = 0;

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    total++;

    let rec;
    try { rec = JSON.parse(t); } catch { continue; }
    const decoded = rec?.decoded || null;
    const payloadTypeName = rec.payloadType || (decoded ? Utils.getPayloadTypeName(decoded.payloadType) : null);
    if (payloadTypeName !== "Advert") continue;
    const adv = decoded?.payload?.decoded || decoded?.decoded || decoded;
    if (!adv || typeof adv !== "object") continue;

    adverts++;
    const rssi = Number(rec.rssi ?? -999);
    const snr = Number(rec.snr ?? -999);
    const channelName = rec.channel?.name || "#unknown";
    const ts = rec.ts || nowIso();

    updateDeviceFromAdvert(devices, adv, rssi, snr, channelName, ts);
  }

  devices.updatedAt = nowIso();
  saveJsonAtomic(devicesPath, devices);
  console.log("(rebuild) lines=" + total + " adverts=" + adverts);
  console.log("(rebuild) devices=" + Object.keys(devices.byPub).length);
}

rebuild().catch((err) => {
  console.error("(rebuild) error", err);
  process.exit(1);
});
