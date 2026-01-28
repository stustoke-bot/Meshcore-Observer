#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const readline = require("readline");

let meshcore;
try {
  meshcore = require("@michaelhart/meshcore-decoder");
} catch (err) {
  console.error("missing @michaelhart/meshcore-decoder; run npm install");
  process.exit(1);
}

const { MeshCoreDecoder, getDeviceRoleName } = meshcore;
if (!MeshCoreDecoder || typeof MeshCoreDecoder.decode !== "function") {
  console.error("MeshCoreDecoder not available from @michaelhart/meshcore-decoder");
  process.exit(1);
}

const argv = process.argv.slice(2);
const rawFile = argv[0]
  ? path.resolve(argv[0])
  : path.resolve("data/external_packets.ndjson");

if (!fs.existsSync(rawFile)) {
  console.error(`raw file not found: ${rawFile}`);
  process.exit(1);
}

process.stdout.on("error", (err) => {
  if (err.code !== "EPIPE") throw err;
});

const stream = fs.createReadStream(rawFile, { encoding: "utf8" });
const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

let lineNo = 0;
let scanned = 0;
let repeaters = 0;

function normalizeHex(hex) {
  if (!hex) return "";
  return String(hex).replace(/[^0-9a-fA-F]/g, "").toUpperCase();
}

rl.on("line", (line) => {
  lineNo += 1;
  const trimmed = String(line || "").trim();
  if (!trimmed) return;
  let packet;
  try {
    packet = JSON.parse(trimmed);
  } catch {
    return;
  }

  const hexField = packet.payloadHex || packet.hex || packet.payload?.payloadHex || packet.payload?.hex;
  const normalizedHex = normalizeHex(hexField);
  if (!normalizedHex) return;

  scanned += 1;

  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(normalizedHex);
  } catch {
    return;
  }

  const advert = decoded?.payload?.decoded;
  const flags = advert?.appData?.flags;
  if (!Number.isInteger(flags)) return;

  const roleCode = flags & 0x0f;
  const isRepeater = roleCode === 0x02;
  if (!isRepeater) return;

  repeaters += 1;
  const flagsHex = flags.toString(16).padStart(2, "0").toUpperCase();
  const flagsBinary = flags.toString(2).padStart(8, "0");
  const hasLocation = Boolean(flags & 0x10);
  const hasName = Boolean(flags & 0x80);
  const roleName = getDeviceRoleName ? getDeviceRoleName(roleCode) : `roleCode ${roleCode}`;
  const pub = advert.publicKey || packet.publicKey || packet.pub || "(no pub)";
  const name = advert.appData?.name || packet.nodeName || packet.name || "";

  console.log(
    `line ${lineNo}: pub=${pub.padEnd(44).slice(0, 44)} role=${roleName} flags=0x${flagsHex} (${flagsBinary}) loc=${hasLocation} name=${hasName}` +
      (name ? ` gist="${name}"` : "")
  );
});

rl.on("close", () => {
  console.log(`scanned ${scanned} packets, repeaters matching appFlags: ${repeaters}`);
});

rl.on("error", (err) => {
  console.error(`failed to read ${rawFile}:`, err);
});
