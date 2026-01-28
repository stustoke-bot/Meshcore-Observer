#!/usr/bin/env node

import { Constants, NodeJSSerialConnection } from "@liamcottle/meshcore.js";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

const argv = yargs(hideBin(process.argv))
  .option("port", { alias: "s", description: "Serial port to use", type: "string", demandOption: true })
  .option("channel", {
    alias: "c",
    description: "Channel name for auto replies",
    type: "string",
    default: "#test"
  })
  .option("template", {
    alias: "t",
    description: "Reply template (placeholders: {sender},{channel},{hops},{observers},{shareLink})",
    type: "string",
    default: "@{sender} {hops} hops witnessed by {observers} observers {shareLink}"
  })
  .option("server", {
    alias: "u",
    description: "Meshrank server base URL",
    type: "string",
    default: "https://meshrank.net"
  })
  .option("token", {
    alias: "k",
    description: "Bearer token for Meshrank API",
    type: "string"
  })
  .option("auto", {
    alias: "a",
    description: "Auto-respond when test is mentioned",
    type: "boolean",
    default: true
  })
  .help()
  .alias("help", "h")
  .parseSync();

const portPath = argv.port;
const channelName = argv.channel;
const replyTemplate = argv.template;
const serverBase = argv.server.replace(/\/$/, "");
const bearerToken = argv.token || process.env.MESHRANK_TOKEN || "";
const autoRespondEnabled = argv.auto;

let channelIndex = 0;

const connection = new NodeJSSerialConnection(portPath);
const channelMap = new Map();

function formatTemplate(data = {}) {
  return replyTemplate
    .replace(/{sender}/g, data.sender || "Mesh")
    .replace(/{channel}/g, data.channel || channelName)
    .replace(/{hops}/g, String(data.hops ?? 0))
    .replace(/{observers}/g, String(data.observers ?? 0))
    .replace(/{shareLink}/g, data.shareLink || serverBase);
}

function buildHeaders() {
  const headers = { "Content-Type": "application/json" };
  if (bearerToken) headers.Authorization = `Bearer ${bearerToken}`;
  return headers;
}

async function fetchShareForMessage(message) {
  const identifier = message.messageHash || message.hash || message.frameHash;
  if (!identifier || !bearerToken) return null;
  try {
    const res = await fetch(`${serverBase}/api/routes/${encodeURIComponent(identifier)}/share`, {
      method: "POST",
      headers: buildHeaders()
    });
    if (!res.ok) return null;
    const payload = await res.json().catch(() => null);
    return payload?.url || (payload?.code ? `${serverBase}/msg/${payload.code}` : null);
  } catch {
    return null;
  }
}

function resolveChannelIndex(name) {
  const normalized = String(name || "").toLowerCase().replace(/^#/, "");
  for (const [idx, entry] of channelMap.entries()) {
    if (String(entry.name || "").toLowerCase().replace(/^#/, "") === normalized) {
      return entry.channelIdx ?? idx;
    }
  }
  return 0;
}

async function sendSmartReply(details) {
  const shareLink = await fetchShareForMessage(details.message);
  const reply = formatTemplate({
    sender: details.message.senderName || details.message.sender || "Mesh",
    hops: details.message.pathLength ?? 0,
    observers: details.observers ?? 0,
    channel: details.message.channelName || channelName,
    shareLink: shareLink || serverBase
  });
  const idx = resolveChannelIndex(channelName);
  console.log(`[bot] Sending reply to ${channelName} -> ${reply}`);
  await connection.sendChannelTextMessage(idx, reply);
}

const autoTracker = new Map();

function shouldAutoRespond(text, messageId) {
  if (!autoRespondEnabled || !messageId) return false;
  const normalized = (text || "").toLowerCase();
  if (!normalized.includes("test")) return false;
  if (autoTracker.has(messageId)) return false;
  autoTracker.set(messageId, true);
  return true;
}

connection.on("connected", async () => {
  console.log("Connected to Companion node.");
  try {
    const channels = await connection.getChannels();
    if (Array.isArray(channels)) {
      channels.forEach((channel) => {
        channelMap.set(channel.channelIdx, channel);
      });
      channelIndex = resolveChannelIndex(channelName);
      console.log(`Auto target channel index: ${channelIndex}`);
    }
  } catch (err) {
    console.error("Unable to read channels", err);
  }
});

connection.on(Constants.PushCodes.MsgWaiting, async () => {
  try {
    const waitingMessages = await connection.getWaitingMessages();
    for (const message of waitingMessages) {
      if (message.channelMessage) {
        await handleChannelMessage(message.channelMessage);
      }
    }
  } catch (err) {
    console.error("Message drain failed", err);
  }
});

async function handleChannelMessage(channelMessage) {
  const body = channelMessage.text || channelMessage.message || "";
  const obsCount = channelMessage.observerCount ?? 0;
  const msgId = channelMessage.messageHash || channelMessage.hash || channelMessage.frameHash;
  if (shouldAutoRespond(body, msgId)) {
    await sendSmartReply({
      message: channelMessage,
      observers: obsCount
    });
  }
}

async function main() {
  try {
    await connection.connect();
    console.log("Meshcore bot is online.");
  } catch (err) {
    console.error("Failed to connect to node", err);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("Unexpected error", err);
  process.exit(1);
});
