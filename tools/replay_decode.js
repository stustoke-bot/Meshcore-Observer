#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { MeshCoreDecoder, PayloadType, Utils } = require("@michaelhart/meshcore-decoder");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(PROJECT_ROOT, "data");
const RF_LOG = path.join(DATA_DIR, "rf.ndjson");
const OUT = path.join(DATA_DIR, "decoded_replay.ndjson");
const CONFIG_PATH = path.join(__dirname, "meshcore_keys.json");

function loadConfig() {
  let channels = [];
  try {
    const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
    channels = Array.isArray(cfg.channels) ? cfg.channels : [];
  } catch {}
  const channelSecrets = [];
  for (const c of channels) {
    const sk = String(c?.secretHex || "").trim();
    if (/^[0-9a-fA-F]{32}$/.test(sk)) channelSecrets.push(sk);
  }
  const keyStore =
    channelSecrets.length > 0
      ? MeshCoreDecoder.createKeyStore({ channelSecrets })
      : null;
  return { keyStore, secretCount: channelSecrets.length };
}

function isDecryptedGroupText(decoded) {
  if (decoded.payloadType !== PayloadType.GroupText) return false;
  const gt = decoded.payload?.decoded;
  return !!gt?.decrypted;
}

(async () => {
  const { keyStore, secretCount } = loadConfig();
  console.log(`(replay) rf=${RF_LOG}`);
  console.log(`(replay) out=${OUT}`);
  console.log(`(replay) secrets=${secretCount}`);

  let total = 0, ok = 0, decrypted = 0, failed = 0;

  const rl = readline.createInterface({
    input: fs.createReadStream(RF_LOG, { encoding: "utf8" }),
    crlfDelay: Infinity
  });

  const out = fs.createWriteStream(OUT, { flags: "w" });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;

    total++;
    let pkt;
    try { pkt = JSON.parse(t); } catch { failed++; continue; }
    if (!pkt.hex) continue;

    const rfHex = String(pkt.hex).toUpperCase();
    try {
      const decodedPkt = MeshCoreDecoder.decode(rfHex, keyStore ? { keyStore } : undefined);
      ok++;

      const rec = {
        ts: pkt.archivedAt || new Date().toISOString(),
        messageHash: decodedPkt.messageHash || rfHex.slice(0, 16),
        rssi: pkt.rssi,
        snr: pkt.snr,
        crc: pkt.crc,
        payloadType: Utils.getPayloadTypeName(decodedPkt.payloadType),
        routeType: Utils.getRouteTypeName(decodedPkt.routeType),
        pathLength: decodedPkt.pathLength ?? 0,
        path: Array.isArray(decodedPkt.path) ? decodedPkt.path.map((h) => String(h).toUpperCase()) : [],
        decrypted: isDecryptedGroupText(decodedPkt),
        decoded: decodedPkt.payload?.decoded || null
      };

      if (rec.decrypted) decrypted++;
      out.write(JSON.stringify(rec) + "\n");
    } catch (e) {
      failed++;
      out.write(JSON.stringify({ ts: pkt.archivedAt || null, ok: false, error: String(e?.message || e), rf: pkt }) + "\n");
    }

    if (total % 5000 === 0) {
      console.log(`(replay) total=${total} ok=${ok} decrypted=${decrypted} failed=${failed}`);
    }
  }

  out.end();
  console.log(`(replay done) total=${total} ok=${ok} decrypted=${decrypted} failed=${failed}`);
})();
