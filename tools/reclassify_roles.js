#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const devicesPath = path.join(__dirname, "..", "data", "devices.json");

function loadJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.error("Failed to read:", p, err?.message || err);
    process.exit(1);
  }
}

function saveJson(p, obj) {
  const tmp = p + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
}

function classifyFromFlags(flags) {
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
    hasLocation: !!(flags & 0x10),
    hasName: !!(flags & 0x80)
  };
}

const devices = loadJson(devicesPath);
const byPub = devices.byPub || {};

let updated = 0;
for (const entry of Object.values(byPub)) {
  if (!entry || typeof entry !== "object") continue;
  const rawFlags = entry.appFlags?.raw ?? entry.raw?.lastAdvert?.appData?.flags;
  const flags = Number.isInteger(rawFlags) ? rawFlags : null;
  if (flags === null) continue;
  const classified = classifyFromFlags(flags);
  if (!classified) continue;
  entry.appFlags = { ...entry.appFlags, ...classified };
  entry.role = classified.roleName;
  entry.isRepeater = classified.isRepeater;
  updated += 1;
}

devices.updatedAt = new Date().toISOString();
saveJson(devicesPath, devices);

console.log(JSON.stringify({ updated }, null, 2));
