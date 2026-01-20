#!/usr/bin/env node
"use strict";

const http = require("http");
const https = require("https");
const { Constants, NodeJSSerialConnection } = require("@liamcottle/meshcore.js");
const yargs = require("yargs");
const { hideBin } = require("yargs/helpers");

const argv = yargs(hideBin(process.argv))
  .option("port", {
    alias: "p",
    type: "string",
    description: "Serial port to connect to",
    default: process.env.MESHCORE_PORT || (process.platform === "win32" ? "COM3" : "/dev/ttyUSB0")
  })
  .option("channel", {
    alias: "c",
    type: "string",
    description: "Channel name to listen on",
    default: process.env.BOT_CHANNEL || "#test"
  })
  .option("label", {
    alias: "l",
    type: "string",
    description: "Label used in responses",
    default: process.env.BOT_LABEL || "Stoke"
  })
  .option("cooldown", {
    alias: "d",
    type: "number",
    description: "Cooldown per sender (ms)",
    default: Number(process.env.BOT_COOLDOWN_MS || "60000")
  })
  .option("lookup-delay", {
    type: "number",
    description: "Delay before querying Meshrank (ms)",
    default: Number(process.env.BOT_LOOKUP_DELAY_MS || "500")
  })
  .option("ignore-sender", {
    type: "string",
    description: "Ignore messages from this sender name",
    default: process.env.BOT_IGNORE_SENDER || ""
  })
  .option("reconnect-ms", {
    type: "number",
    description: "Reconnect delay after disconnect (ms)",
    default: Number(process.env.BOT_RECONNECT_MS || "3000")
  })
  .option("meshrank-url", {
    type: "string",
    description: "Meshrank base URL",
    default: process.env.MESHRANK_URL || "https://meshrank.net"
  })
  .strict(false)
  .help()
  .argv;

const port = argv.port;
const channelName = argv.channel;
const meshrankUrl = String(argv["meshrank-url"] || "https://meshrank.net").replace(/\/+$/, "");
const cooldownMs = Number(argv.cooldown);
const lookupDelayMs = Number(argv["lookup-delay"]);
const botLabel = argv.label;
const ignoreSender = argv["ignore-sender"];
const reconnectMs = Number(argv["reconnect-ms"]);

const connection = new NodeJSSerialConnection(port);
const lastReplyBySender = new Map();
let channelIdxCache = null;
let isProcessing = false;
let lastReplyAt = null;
let lastReplyText = null;

console.log(`(observer-bot) port: ${port}`);
console.log(`(observer-bot) channel: ${channelName}`);
console.log(`(observer-bot) label: ${botLabel}`);
console.log(`(observer-bot) meshrank: ${meshrankUrl}`);
console.log(`(observer-bot) cooldown: ${cooldownMs}ms, lookup-delay: ${lookupDelayMs}ms`);

function shouldReply(sender) {
  const now = Date.now();
  const last = lastReplyBySender.get(sender) || 0;
  if (now - last < cooldownMs) return false;
  lastReplyBySender.set(sender, now);
  return true;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeText(val) {
  return String(val || "").trim();
}

function httpGetJson(urlString) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlString);
    const lib = url.protocol === "http:" ? http : https;
    const req = lib.get(url, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        try {
          resolve(JSON.parse(data));
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on("error", reject);
  });
}

async function fetchMeshrankMessage(message) {
  try {
    const url = `${meshrankUrl}/api/messages?channel=${encodeURIComponent(channelName)}`;
    const data = await httpGetJson(url);
    const text = normalizeText(message.text);
    const matches = (data.messages || []).filter((m) =>
      normalizeText(m.body) === text
    );
    if (!matches.length) return null;
    return matches[matches.length - 1];
  } catch {
    return null;
  }
}

async function fetchObserverCount(message, meshrankMessage) {
  if (meshrankMessage && Array.isArray(meshrankMessage.observerHits)) {
    return meshrankMessage.observerHits.length;
  }
  return 0;
}

function resolveHops(message) {
  const hops = message.pathLen ?? message.pathLength ?? message.hops ?? 0;
  if (!Number.isFinite(hops)) return 0;
  if (hops === 0xff) return 0;
  return hops;
}

async function resolveChannelIdx() {
  if (channelIdxCache !== null) return channelIdxCache;
  const channels = await connection.getChannels();
  const found = channels.find((c) => String(c.name || "").trim() === channelName);
  channelIdxCache = found ? found.channelIdx : null;
  return channelIdxCache;
}

async function handleChannelMessage(message, channelIdx) {
  const text = String(message.text || "");
  if (!text.toLowerCase().includes("test")) return;
  if (lookupDelayMs > 0) {
    await sleep(lookupDelayMs);
  }
  const meshrankMessage = await fetchMeshrankMessage(message);
  const sender = meshrankMessage?.sender || message.senderName || "there";
  if (ignoreSender && sender === ignoreSender) return;
  const senderKey = sender || `unknown:${text.slice(0, 20)}`;
  if (!shouldReply(senderKey)) return;

  const hops = Number.isFinite(meshrankMessage?.repeats) ? meshrankMessage.repeats : resolveHops(message);
  const observerCount = await fetchObserverCount(message, meshrankMessage);
  const reply = `Hi ${sender} You are received as ${hops} hops to ${botLabel}. Youve been picked up by ${observerCount} local observers. see more info at meshrank.net/test`;
  await connection.sendChannelTextMessage(channelIdx, reply);
  lastReplyAt = new Date().toISOString();
  lastReplyText = reply;
  console.log(`(observer-bot) replied ${lastReplyAt} -> ${reply}`);
}

connection.on("connected", async () => {
  console.log(`(observer-bot) connected on ${port}`);
  channelIdxCache = null;
  try {
    await connection.syncDeviceTime();
  } catch {}
});

connection.on("disconnected", () => {
  console.log("(observer-bot) disconnected");
  channelIdxCache = null;
  setTimeout(() => {
    connection.connect().catch(() => {});
  }, reconnectMs);
});

connection.on(Constants.PushCodes.MsgWaiting, async () => {
  if (isProcessing) return;
  isProcessing = true;
  try {
    const waiting = await connection.getWaitingMessages();
    const channelIdx = await resolveChannelIdx();
    if (channelIdx === null) return;
    for (const msg of waiting) {
      if (msg.channelMessage && msg.channelMessage.channelIdx === channelIdx) {
        await handleChannelMessage(msg.channelMessage, channelIdx);
      }
    }
  } catch (err) {
    console.error("(observer-bot) message error", err.message || err);
  } finally {
    isProcessing = false;
  }
});

connection.connect().catch((err) => {
  console.error("(observer-bot) connect failed", err.message || err);
  process.exit(1);
});
