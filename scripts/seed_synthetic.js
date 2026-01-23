#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const Database = require("better-sqlite3");

const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");
const dbPath = path.join(dataDir, "meshrank.db");

const messageCount = Number(process.argv[2] || 500);
const channelCount = Number(process.argv[3] || 4);
const observerCount = Number(process.argv[4] || 25);

function ensureDataDir() {
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
}

function randHex(bytes) {
  return crypto.randomBytes(bytes).toString("hex").toUpperCase();
}

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

function buildChannels(count) {
  const channels = ["#public"];
  for (let i = 1; i < count; i += 1) {
    channels.push(`#chan${i}`);
  }
  return channels;
}

function buildObservers(count) {
  const out = [];
  for (let i = 0; i < count; i += 1) {
    out.push(`observer-${String(i + 1).padStart(3, "0")}`);
  }
  return out;
}

function buildPath() {
  const len = 1 + Math.floor(Math.random() * 4);
  const out = [];
  for (let i = 0; i < len; i += 1) {
    out.push(randHex(1));
  }
  return out;
}

function ensureColumn(db, tableName, columnName, columnType) {
  const cols = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (cols.some((c) => c.name === columnName)) return;
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`);
}

function main() {
  ensureDataDir();
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("temp_store = MEMORY");
  db.pragma("cache_size = -64000");
  db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      message_hash TEXT PRIMARY KEY,
      frame_hash TEXT,
      channel_name TEXT,
      channel_hash TEXT,
      sender TEXT,
      sender_pub TEXT,
      body TEXT,
      ts TEXT,
      path_json TEXT,
      path_text TEXT,
      path_length INTEGER,
      repeats INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_name, ts);
    CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
    CREATE INDEX IF NOT EXISTS idx_messages_sender_channel_ts ON messages(sender, channel_name, ts);
    CREATE TABLE IF NOT EXISTS message_observers (
      message_hash TEXT NOT NULL,
      observer_id TEXT NOT NULL,
      observer_name TEXT,
      ts TEXT,
      path_json TEXT,
      path_text TEXT,
      path_length INTEGER,
      PRIMARY KEY (message_hash, observer_id)
    );
    CREATE INDEX IF NOT EXISTS idx_message_observers_hash ON message_observers(message_hash);
  `);
  ensureColumn(db, "messages", "path_text", "TEXT");
  ensureColumn(db, "message_observers", "path_text", "TEXT");

  const insertMessage = db.prepare(`
    INSERT INTO messages (
      message_hash, frame_hash, channel_name, channel_hash, sender, sender_pub,
      body, ts, path_json, path_text, path_length, repeats
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertObserver = db.prepare(`
    INSERT OR IGNORE INTO message_observers (
      message_hash, observer_id, observer_name, ts, path_json, path_text, path_length
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const channels = buildChannels(channelCount);
  const observers = buildObservers(observerCount);
  const now = Date.now();

  const tx = db.transaction(() => {
    for (let i = 0; i < messageCount; i += 1) {
      const messageHash = randHex(16);
      const frameHash = randHex(12);
      const channel = pick(channels);
      const sender = `user${String(1 + Math.floor(Math.random() * 20)).padStart(2, "0")}`;
      const body = `Synthetic message ${i + 1}`;
      const ts = new Date(now - Math.floor(Math.random() * 24 * 60 * 60 * 1000)).toISOString();
      const path = buildPath();
      const pathText = path.join("|");
      const repeats = Math.max(path.length, 1 + Math.floor(Math.random() * 3));
      insertMessage.run(
        messageHash,
        frameHash,
        channel,
        null,
        sender,
        null,
        body,
        ts,
        JSON.stringify(path),
        pathText,
        path.length,
        repeats
      );

      const observerHits = 1 + Math.floor(Math.random() * 3);
      const picked = new Set();
      while (picked.size < observerHits) {
        picked.add(pick(observers));
      }
      for (const observerId of picked) {
        insertObserver.run(
          messageHash,
          observerId,
          observerId,
          ts,
          JSON.stringify(path),
          pathText,
          path.length
        );
      }
    }
  });

  tx();
  console.log(`Seeded ${messageCount} messages across ${channels.length} channels.`);
}

main();
