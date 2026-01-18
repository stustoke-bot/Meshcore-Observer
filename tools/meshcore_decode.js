"use strict";

/**
 * MeshCORE RF stream decoder + device registry builder (Windows safe)
 *
 * - Reads JSON lines from your ESP32 sniffer on STDIN
 * - Logs raw RF forever:        data/rf.ndjson
 * - Logs decoded summaries:     data/decoded.ndjson
 * - Maintains live devices DB:  data/devices.json   (for the live map)
 *
 * Role model (as you defined):
 * - Chat nodes are NOT repeaters
 * - Repeaters / Room servers ARE repeaters
 * - Anything else is a companion
 *
 * Keys file schema supported (yours):
 * {
 *   "channels": [
 *     { "hashByte": "11", "name": "#public", "secretHex": "32hex..." },
 *     { "hashByte": "D9", "name": "#test",   "secretHex": "32hex..." }
 *   ]
 * }
 *
 * Run (recommended):
 * cmd /c "cd /d C:\Users\USER\Documents\PlatformIO\Projects\heltec_v3_meshcore_sniffer && py -m platformio device monitor -p COM6 -b 115200 --filter direct --quiet | node tools\meshcore_decode.js"
 */

const fs = require("fs");
const path = require("path");
const readline = require("readline");

// -------------------- Require decoder library --------------------
let meshcore;
try {
  meshcore = require("@michaelhart/meshcore-decoder");
} catch (e) {
  console.error("(decoder) ERROR: missing @michaelhart/meshcore-decoder");
  console.error("(decoder) Run from project root: npm install @michaelhart/meshcore-decoder");
  process.exit(1);
}

// -------------------- Paths (NEVER depend on CWD) --------------------
const scriptDir = __dirname;                       // ...\tools
const projectRoot = path.resolve(scriptDir, ".."); // project root
const dataDir = path.join(projectRoot, "data");
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

const keysPath = path.join(scriptDir, "meshcore_keys.json");
const rfLogPath = path.join(dataDir, "rf.ndjson");
const decodedLogPath = path.join(dataDir, "decoded.ndjson");
const devicesDbPath = path.join(dataDir, "devices.json");

// -------------------- Helpers --------------------
function nowIso() { return new Date().toISOString(); }

function loadJsonSafe(p, def) {
  try {
    if (!fs.existsSync(p)) return def;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return def;
  }
}

function saveJsonAtomic(p, obj) {
  const tmp = p + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
}

function appendNdjson(p, obj) {
  fs.appendFileSync(p, JSON.stringify(obj) + "\n");
}

function isHex32(s) {
  return typeof s === "string" && /^[0-9a-fA-F]{32}$/.test(s);
}

function hexToBuf(hex) {
  const clean = String(hex || "").replace(/\s+/g, "");
  if (!clean) return Buffer.alloc(0);
  return Buffer.from(clean, "hex");
}

// -------------------- App Flags decoding (AUTHORITATIVE) --------------------
// Chat nodes are NOT repeaters. Repeaters/Room servers are repeaters. Anything else is companion.
function decodeAppFlags(flags) {
  if (!Number.isInteger(flags)) return null;

  const roleCode = flags & 0x0f;
  let roleName = "unknown";
  if (roleCode === 0x00) roleName = "sensor";
  else if (roleCode === 0x01) roleName = "chat";
  else if (roleCode === 0x02) roleName = "repeater";
  else if (roleCode === 0x03) roleName = "room_server";

  const isRepeater = roleCode === 0x02 || roleCode === 0x03;

  return {
    raw: flags,
    roleCode,
    roleName,
    isRepeater,
    hasLocation: !!(flags & 0x10), // bit 4
    hasName: !!(flags & 0x80)      // bit 7
  };
}

// -------------------- Keys loader (matches YOUR schema) --------------------
function loadKeys() {
  const cfg = loadJsonSafe(keysPath, null);

  const out = {
    channelNames: {},   // map "11" -> "#public"
    secrets: [],        // array of 32-hex strings
    rejectedSecrets: [],
  };

  if (!cfg) return out;

  if (Array.isArray(cfg.channels)) {
    for (const ch of cfg.channels) {
      if (!ch || typeof ch !== "object") continue;

      const hb = typeof ch.hashByte === "string" ? ch.hashByte.toUpperCase() : null;
      const nm = typeof ch.name === "string" ? ch.name : null;
      const sec = typeof ch.secretHex === "string" ? ch.secretHex : null;

      if (hb && nm && nm.startsWith("#")) out.channelNames[hb] = nm;

      if (sec) {
        if (isHex32(sec)) out.secrets.push(sec.toUpperCase());
        else out.rejectedSecrets.push(sec);
      }
    }
  }

  // de-dupe secrets
  out.secrets = Array.from(new Set(out.secrets));
  return out;
}

// -------------------- Load device DB --------------------
const devices = loadJsonSafe(devicesDbPath, { byPub: {} });

// -------------------- Load keys --------------------
const keys = loadKeys();
const keyStore = keys.secrets.length ? meshcore.MeshCoreDecoder.createKeyStore({ channelSecrets: keys.secrets }) : null;

// -------------------- Boot report --------------------
console.log("(decoder) scriptDir=" + scriptDir);
console.log("(decoder) projectRoot=" + projectRoot);
console.log("(decoder) config=" + keysPath);
console.log("(decoder) rfLog=" + rfLogPath);
console.log("(decoder) decodedLog=" + decodedLogPath);
console.log("(decoder) devicesDb=" + devicesDbPath);
console.log("(decoder) channels loaded=" + Object.keys(keys.channelNames).length);
console.log("(decoder) name map keys=" + (Object.keys(keys.channelNames).length ? Object.keys(keys.channelNames).join(",") : "(none)"));
console.log("(decoder) valid secrets=" + keys.secrets.length);
if (keys.rejectedSecrets.length) {
  console.log("(decoder) rejected secrets (must be 32 hex chars):");
  for (const s of keys.rejectedSecrets.slice(0, 10)) console.log("  - " + String(s));
  if (keys.rejectedSecrets.length > 10) console.log("  ... (" + (keys.rejectedSecrets.length - 10) + " more)");
}

// -------------------- Repeat collapsing (per fingerprint/message key) --------------------
const agg = new Map(); // key => {repeats,bestRssi,bestSnr,lastRssi,lastSnr,firstSeenMs,lastSeenMs,printed}
const AGG_TTL_MS = 5 * 60 * 1000;

function gcAgg() {
  const t = Date.now();
  for (const [k, v] of agg.entries()) {
    if (t - v.lastSeenMs > AGG_TTL_MS) agg.delete(k);
  }
}

function bumpAgg(key, rssi, snr) {
  const t = Date.now();
  const prev = agg.get(key) || {
    repeats: 0,
    bestRssi: -9999,
    bestSnr: -9999,
    lastRssi: null,
    lastSnr: null,
    firstSeenMs: t,
    lastSeenMs: t,
    printed: false
  };

  const next = {
    ...prev,
    repeats: prev.repeats + 1,
    bestRssi: Math.max(prev.bestRssi, Number.isFinite(rssi) ? rssi : -9999),
    bestSnr: Math.max(prev.bestSnr, Number.isFinite(snr) ? snr : -9999),
    lastRssi: rssi,
    lastSnr: snr,
    lastSeenMs: t
  };

  agg.set(key, next);
  return next;
}

// -------------------- Device update --------------------
function updateDeviceFromAdvert(advert, rssi, snr, channelName) {
  const pub = advert.publicKey || advert.pub || advert.pubKey;
  if (!pub) return null;

  const prev = devices.byPub[pub] || {};
  let hiddenOnMap = prev.hiddenOnMap;
  if (hiddenOnMap === undefined) {
    const disk = loadJsonSafe(devicesDbPath, null);
    const diskEntry = disk?.byPub?.[pub];
    if (diskEntry && diskEntry.hiddenOnMap !== undefined) {
      hiddenOnMap = diskEntry.hiddenOnMap;
    }
  }

  const flagsByte =
    (typeof advert.appFlags === "number" ? advert.appFlags : null) ??
    (typeof advert.flags === "number" ? advert.flags : null) ??
    (typeof advert.appData?.flags === "number" ? advert.appData.flags : null);

  const flags = decodeAppFlags(flagsByte);

  // Promote GPS ONLY if flags say location exists
  let gps = prev.gps || null;
  const loc = advert.gps || advert.location || advert.appData?.location || null;
  if (flags?.hasLocation && loc) {
    const lat = Number(loc.lat ?? loc.latitude);
    const lon = Number(loc.lon ?? loc.lng ?? loc.longitude);
    if (Number.isFinite(lat) && Number.isFinite(lon)) gps = { lat, lon };
  }
  const prevGps = prev.gps || null;
  const gpsChanged = !!(gps && (!prevGps || prevGps.lat !== gps.lat || prevGps.lon !== gps.lon));

  // Your rule: chat/repeater/room_server are repeaters; anything else companion
  const isRepeater = !!(flags?.isRepeater);

  const updated = {
    ...prev,
    pub,
    channel: channelName || prev.channel || "#unknown",
    role: flags?.roleName || prev.role || "unknown",
    isRepeater,
    appFlags: flags || prev.appFlags || null,
    gps,
    hiddenOnMap: gpsChanged ? false : hiddenOnMap,
    stats: {
      lastRssi: rssi,
      lastSnr: snr,
      bestRssi: Math.max(prev?.stats?.bestRssi ?? -999, rssi),
      bestSnr: Math.max(prev?.stats?.bestSnr ?? -999, snr)
    },
    firstSeen: prev.firstSeen || nowIso(),
    lastSeen: nowIso(),
    raw: {
      ...(prev.raw || {}),
      lastAdvert: advert
    }
  };

  devices.byPub[pub] = updated;
  saveJsonAtomic(devicesDbPath, devices);
  return updated;
}

// -------------------- Decode wrapper --------------------
// Different versions of the library expose different entrypoints.
// We try a few, safely.
function tryDecode(hex) {
  if (!hex) return null;
  try {
    if (meshcore.MeshCoreDecoder && typeof meshcore.MeshCoreDecoder.decode === "function") {
      return meshcore.MeshCoreDecoder.decode(String(hex).toUpperCase(), keyStore ? { keyStore } : undefined);
    }
  } catch {}

  return null;
}

// -------------------- STDIN: PlatformIO monitor stream --------------------
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });

rl.on("line", (line) => {
  const s = String(line || "").trim();
  if (!s.startsWith("{")) return;

  let pkt;
  try { pkt = JSON.parse(s); } catch { return; }
  if (pkt.type !== "rf") return;

  const rssi = Number(pkt.rssi ?? 0);
  const snr = Number(pkt.snr ?? 0);
  const crc = !!pkt.crc;
  const fp = pkt.fp ? String(pkt.fp) : null;

  // Always log raw RF for replay
  appendNdjson(rfLogPath, { archivedAt: nowIso(), ...pkt });

  const hex = String(pkt.hex || "").trim();
  const buf = hexToBuf(hex);
  if (!buf.length) return;

  const decoded = tryDecode(hex);

  // Log decoded summary even if null (for retro-processing)
  appendNdjson(decodedLogPath, {
    ts: nowIso(),
    fp,
    crc,
    rssi,
    snr,
    len: pkt.len ?? pkt.reported_len ?? null,
    hex: pkt.hex || null,
    decoded
  });

  // Aggregation key: prefer decoded message hash, else fp, else first bytes of hex
  const messageKey =
    (decoded && (decoded.messageHash || decoded.hash || decoded.id)) ? String(decoded.messageHash || decoded.hash || decoded.id) :
    (fp || (pkt.hex ? pkt.hex.slice(0, 16) : "unknown"));

  gcAgg();
  const a = bumpAgg(messageKey, rssi, snr);

  // Print header once, then updates
  if (!a.printed) {
    a.printed = true;
    console.log("");
    console.log("────────────────────────────────────────");
    console.log(`[${new Date().toLocaleTimeString()}] key:${String(messageKey).slice(0, 8)} repeats:${a.repeats} CRC:${crc} RSSI:${rssi} SNR:${snr} bestRSSI:${a.bestRssi} bestSNR:${a.bestSnr}`);
  } else {
    console.log(`(update) key:${String(messageKey).slice(0, 8)} repeats:${a.repeats} lastRSSI:${rssi} lastSNR:${snr} bestRSSI:${a.bestRssi} bestSNR:${a.bestSnr}`);
  }

  if (!decoded) {
    console.log("DECODE: (no decode yet)");
    return;
  }

  // Channel name (best effort)
  let chByte = null;

  if (typeof decoded.channelHash === "number") {
    chByte = decoded.channelHash;
  } else if (typeof decoded.channel === "object" && typeof decoded.channel.hashByte === "string") {
    chByte = parseInt(decoded.channel.hashByte, 16);
  } else if (typeof decoded.decoded?.channelHash === "string") {
    chByte = parseInt(decoded.decoded.channelHash, 16);
  }

  let chKey = null;
  if (Number.isInteger(chByte) && chByte >= 0 && chByte <= 255) {
    chKey = chByte.toString(16).toUpperCase().padStart(2, "0");
  }

  const chName = chKey ? (keys.channelNames[chKey] || `#ch_${chKey}`) : "#unknown";

  // Extract advert (best effort across decoder variants)
  const payloadTypeName = typeof meshcore.Utils?.getPayloadTypeName === "function"
    ? meshcore.Utils.getPayloadTypeName(decoded.payloadType)
    : decoded.payloadType;
  const adv =
    decoded.advert ||
    decoded.payload?.advert ||
    decoded.decoded?.advert ||
    (payloadTypeName === "Advert" ? (decoded.payload?.decoded || decoded.decoded || decoded) : null) ||
    (decoded.type === "Advert" ? decoded : null) ||
    (decoded.decoded && decoded.decoded.type === "Advert" ? decoded.decoded : null);

  if (adv) {
    const dev = updateDeviceFromAdvert(adv, a.bestRssi, a.bestSnr, chName);

    console.log(`CHANNEL: ${chName}`);
    if (dev) {
      const label = dev.isRepeater ? "REPEATER" : "COMPANION";
      console.log(`ADVERT : ${label} role=${dev.role} flags=${dev.appFlags ? "0x" + dev.appFlags.raw.toString(16).toUpperCase().padStart(2, "0") : "?"} pub=${String(dev.pub).slice(0, 8)}...`);
      if (dev.gps) console.log(`GPS   : ${dev.gps.lat}, ${dev.gps.lon}`);
    } else {
      console.log("ADVERT : (no pubkey found)");
    }
    return;
  }

  // Fallback: show payload type if present
  const payloadType = decoded.payloadType || decoded.type || decoded.decoded?.type || "unknown";
  console.log(`CHANNEL: ${chName}`);
  console.log(`PAYLOAD: ${payloadType}`);
});

// -------------------- Shutdown --------------------
process.on("SIGINT", () => {
  try { saveJsonAtomic(devicesDbPath, devices); } catch {}
  process.exit(0);
});


