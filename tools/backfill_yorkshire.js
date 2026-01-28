#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const Database = require("better-sqlite3");
const crypto = require("crypto");
const { MeshCoreDecoder, Utils } = require("@michaelhart/meshcore-decoder");
const { getDbPath } = require("./observer_demo/db_path");

const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");
const observerLogPath = path.join(dataDir, "observer.ndjson");
const keysPath = path.join(__dirname, "meshcore_keys.json");
const dbPath = getDbPath();

const args = process.argv.slice(2);
const limitArg = args.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Number(limitArg.split("=")[1]) || null : null;
const sinceArg = args.find((arg) => arg.startsWith("--since="));
const sinceMs = sinceArg ? parseTimestampValue(sinceArg.split("=")[1]) : null;
const channelArgs = args.filter((arg) => arg.startsWith("--channel="));
const hashArgs = args.filter((arg) => arg.startsWith("--hash="));
const dryRun = args.includes("--dry-run");

function parseTimestampValue(value) {
  if (!value) return null;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePathHash(value) {
  if (!value) return null;
  const clean = String(value).trim().toUpperCase();
  if (/^[0-9A-F]{2}$/.test(clean)) return clean;
  return null;
}

function parseRecordTimestamp(record) {
  if (!record) return null;
  if (typeof record.archivedAt === "string") {
    const parsed = Date.parse(record.archivedAt);
    if (Number.isFinite(parsed)) return { ms: parsed, iso: record.archivedAt };
  }
  if (typeof record.ts === "string") {
    const parsed = Date.parse(record.ts);
    if (Number.isFinite(parsed)) return { ms: parsed, iso: record.ts };
  }
  if (Number.isFinite(record.ts)) {
    return { ms: Number(record.ts), iso: new Date(record.ts).toISOString() };
  }
  return null;
}

function sha256Hex(hex) {
  try {
    const buf = Buffer.from(hex, "hex");
    return crypto.createHash("sha256").update(buf).digest("hex").toUpperCase();
  } catch {
    return null;
  }
}

const keysConfig = JSON.parse(fs.readFileSync(keysPath, "utf8"));
const keyMap = new Map();
const secrets = [];
for (const ch of keysConfig.channels || []) {
  if (ch?.hashByte && ch?.name) {
    keyMap.set(String(ch.hashByte).toUpperCase(), String(ch.name));
  }
  if (ch?.secretHex) {
    secrets.push(String(ch.secretHex));
  }
}
if (!keyMap.size) {
  console.error("No channels defined in meshcore_keys.json");
  process.exit(1);
}
const keyStore = secrets.length
  ? MeshCoreDecoder.createKeyStore({ channelSecrets: Array.from(new Set(secrets)) })
  : null;
if (!keyStore) {
  console.error("No secrets available to build key store");
  process.exit(1);
}

const nameToHash = new Map();
for (const [hash, name] of Array.from(keyMap.entries())) {
  nameToHash.set(String(name || "").toLowerCase(), hash);
}

const requestedHashes = new Set();
if (!channelArgs.length && !hashArgs.length) {
  requestedHashes.add("80");
} else {
  for (const arg of channelArgs) {
    const value = arg.split("=")[1];
    if (!value) continue;
    const normalized = value.startsWith("#") ? value : `#${value}`;
    const target = nameToHash.get(normalized.toLowerCase());
    if (target) requestedHashes.add(target);
  }
  for (const arg of hashArgs) {
    const value = arg.split("=")[1];
    if (!value) continue;
    const clean = value.trim().toUpperCase();
    if (/^[0-9A-F]{2}$/.test(clean)) {
      requestedHashes.add(clean);
    }
  }
}
if (!requestedHashes.size) {
  console.error("No target channel hashes resolved; check --channel or --hash");
  process.exit(1);
}

const db = new Database(dbPath);
const targetNames = Array.from(requestedHashes)
  .map((hash) => keyMap.get(hash))
  .filter(Boolean);
const existingHashes = new Set();
if (targetNames.length) {
  const placeholders = targetNames.map(() => "?").join(",");
  const rows = db
    .prepare(`SELECT message_hash FROM messages WHERE channel_name IN (${placeholders})`)
    .all(...targetNames);
  for (const row of rows) {
    const hash = String(row.message_hash || "").toUpperCase();
    if (hash) existingHashes.add(hash);
  }
}

const msgInsert = db.prepare(`
  INSERT INTO messages (
    message_hash, frame_hash, channel_name, channel_hash, sender, sender_pub,
    body, ts, path_json, path_text, path_length, repeats
  ) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
  )
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

const msgObserverInsert = db.prepare(`
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

function resolveObserverId(record) {
  if (!record) return null;
  let observerId = record.observerId || "";
  if (!observerId && record.topic) {
    const match = String(record.topic).match(/observers\/([^/]+)\//i);
    if (match) observerId = match[1];
  }
  return String(observerId || record.observerName || "").trim() || null;
}

async function run() {
  if (!fs.existsSync(observerLogPath)) {
    console.error("observer.ndjson missing:", observerLogPath);
    process.exit(1);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(observerLogPath, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  let processed = 0;
  let inserted = 0;
  let observerInserts = 0;
  let decoded = 0;
  let skippedExisting = 0;

  for await (const line of rl) {
    if (!line) continue;
    processed += 1;
    if (limit && processed > limit) break;

    let record;
    try {
      record = JSON.parse(line);
    } catch {
      continue;
    }

    const timestamp = parseRecordTimestamp(record);
    if (sinceMs && timestamp && timestamp.ms < sinceMs) {
      continue;
    }

    const hex = (record.payloadHex || record.hex || "").toString().toUpperCase();
    if (!hex) continue;

    let decodedPacket;
    try {
      decodedPacket = MeshCoreDecoder.decode(hex, { keyStore });
    } catch {
      continue;
    }
    decoded += 1;

    const payloadTypeName = Utils.getPayloadTypeName(decodedPacket.payloadType);
    if (payloadTypeName !== "GroupText") continue;
    const payload = decodedPacket.payload?.decoded;
    if (!payload?.decrypted) continue;

    const chHash = typeof payload.channelHash === "string" ? payload.channelHash.toUpperCase() : null;
    if (!chHash || !requestedHashes.has(chHash)) continue;
    const channelName = keyMap.get(chHash);
    if (!channelName) continue;

    const msgHash = String(
      decodedPacket.messageHash ||
      record.frameHash ||
      sha256Hex(hex) ||
      hex.slice(0, 16) ||
      "unknown"
    ).toUpperCase();
    if (existingHashes.has(msgHash)) {
      skippedExisting += 1;
      continue;
    }

    const frameHash = (record.frameHash ? String(record.frameHash) : sha256Hex(hex) || "").toUpperCase();
    const sender = String(payload.decrypted.sender || "unknown");
    const body = String(payload.decrypted.message || "");
    const pathRaw = Array.isArray(decodedPacket.path) ? decodedPacket.path : [];
    const path = pathRaw.map(normalizePathHash).filter(Boolean);
    const pathJson = path.length ? JSON.stringify(path) : null;
    const pathText = path.length ? path.join("|") : null;
    const pathLength = path.length || (Number.isFinite(decodedPacket.pathLength) ? decodedPacket.pathLength : 0);
    const hopCount = pathLength;
    const senderPub = payload.senderPublicKey || payload.publicKey || payload.senderPub || null;
    const tsIso = timestamp ? timestamp.iso : null;

    if (dryRun) {
      console.log("dry-run insert", msgHash, channelName, tsIso);
      continue;
    }

    msgInsert.run(
      msgHash,
      frameHash || null,
      channelName,
      chHash,
      sender || null,
      senderPub ? String(senderPub).toUpperCase() : null,
      body || null,
      tsIso,
      pathJson,
      pathText,
      pathLength,
      hopCount
    );
    existingHashes.add(msgHash);
    inserted += 1;

    const observerId = resolveObserverId(record);
    if (observerId) {
      const archivedMs = timestamp?.ms ?? null;
      msgObserverInsert.run(
        msgHash,
        observerId,
        record.observerName || observerId,
        tsIso,
        Number.isFinite(archivedMs) ? archivedMs : null,
        pathJson,
        pathText,
        pathLength
      );
      observerInserts += 1;
    }
  }

  console.log(`processed=${processed} decoded=${decoded} inserted=${inserted} observers=${observerInserts} skippedExisting=${skippedExisting}`);
  db.close();
}

run().catch((err) => {
  console.error("backfill failed", err);
  process.exit(1);
});
