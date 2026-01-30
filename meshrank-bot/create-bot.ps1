# Run this from meshrank-bot folder to create bot.js (if copy from meshcore-bot.js failed)
$content = @'
#!/usr/bin/env node
"use strict";

const { Constants, NodeJSSerialConnection } = require("@liamcottle/meshcore.js");
const yargs = require("yargs");
const { hideBin } = require("yargs/helpers");

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
    description: "Reply template (placeholders: {sender},{channel},{hops},{observers},{path},{shareLink})",
    type: "string",
    default: "@[{sender}] Hi! I got {hops} Hops via {observers} Observers. see more here {shareLink}"
  })
  .option("server", {
    alias: "u",
    description: "Meshrank server base URL",
    type: "string",
    default: "https://meshrank.net"
  })
  .option("token", { alias: "k", description: "Bearer token for Meshrank API", type: "string" })
  .option("auto", { alias: "a", description: "Auto-respond when test is mentioned", type: "boolean", default: true })
  .option("delay", { alias: "d", description: "Delay in ms before sending reply", type: "number", default: 10000 })
  .help()
  .alias("help", "h")
  .parseSync();

const portPath = argv.port;
const channelName = argv.channel;
const replyTemplate = argv.template;
const serverBase = argv.server.replace(/\/$/, "");
const bearerToken = argv.token || process.env.MESHRANK_TOKEN || "";
const autoRespondEnabled = argv.auto;
const replyDelayMs = Math.max(0, Number(argv.delay) || 30000);

const connection = new NodeJSSerialConnection(portPath);
const channelMap = new Map();

function formatTemplate(data) {
  return replyTemplate
    .replace(/{sender}/g, data.sender || "Mesh")
    .replace(/{channel}/g, data.channel || channelName)
    .replace(/{hops}/g, String(data.hops ?? 0))
    .replace(/{observers}/g, String(data.observers ?? 0))
    .replace(/{path}/g, data.path || "-")
    .replace(/{shareLink}/g, data.shareLink || serverBase);
}

function buildHeaders() {
  const headers = { "Content-Type": "application/json" };
  if (bearerToken) headers.Authorization = "Bearer " + bearerToken;
  return headers;
}

async function fetchShareForMessage(messageHash) {
  if (!messageHash) return null;
  const hash = String(messageHash).trim().toUpperCase();
  try {
    const res = await fetch(serverBase + "/api/routes/" + encodeURIComponent(hash) + "/share", { method: "POST", headers: buildHeaders() });
    if (!res.ok) return null;
    const payload = await res.json().catch(function() { return null; });
    if (payload && payload.code) return serverBase + "/msg/" + payload.code;
    return payload && payload.url ? payload.url : null;
  } catch (e) { return null; }
}

async function fetchMessageRoutes(messageHash) {
  if (!messageHash) return null;
  try {
    const res = await fetch(serverBase + "/api/message-routes?hash=" + encodeURIComponent(messageHash) + "&hours=24", { headers: buildHeaders() });
    if (!res.ok) return null;
    const payload = await res.json().catch(function() { return null; });
    if (!payload || !payload.ok || !Array.isArray(payload.routes) || !payload.routes.length) return null;
    const first = payload.routes[0];
    const hops = Number.isFinite(first.hopCount) ? first.hopCount : (first.pathNames && first.pathNames.length) || 0;
    const path = Array.isArray(first.pathNames) && first.pathNames.length ? first.pathNames.join(" -> ") : "-";
    return { hops: hops, path: path };
  } catch (e) { return null; }
}

async function fetchMessageDebug(messageHash) {
  if (!messageHash) return 0;
  const hash = String(messageHash).trim().toUpperCase();
  try {
    const res = await fetch(serverBase + "/api/message-debug?hash=" + encodeURIComponent(hash), { headers: buildHeaders() });
    if (!res.ok) return 0;
    const payload = await res.json().catch(function() { return null; });
    if (!payload || !payload.ok) return 0;
    const fromIndex = Array.isArray(payload.fromIndex) ? payload.fromIndex : [];
    const fromLog = Array.isArray(payload.fromLog) ? payload.fromLog : [];
    const unique = new Set([].concat(fromIndex, fromLog).filter(Boolean));
    return unique.size;
  } catch (e) { return 0; }
}

function resolveChannelIndex(name) {
  const normalized = String(name || "").toLowerCase().replace(/^#/, "");
  for (const entry of channelMap.values()) {
    if (String(entry.name || "").toLowerCase().replace(/^#/, "") === normalized) return entry.channelIdx;
  }
  return 0;
}

async function sendSmartReply(details) {
  const msgHash = details.message && (details.message.messageHash || details.message.hash || details.message.frameHash);
  let hops = details.message && details.message.pathLength || 0;
  let observers = details.observers ?? 0;
  let path = "-";
  let shareLink = serverBase;
  if (msgHash) {
    const hash = String(msgHash).trim().toUpperCase();
    const routesResult = await fetchMessageRoutes(hash);
    const observerCount = await fetchMessageDebug(hash);
    const shareUrl = await fetchShareForMessage(hash);
    if (routesResult) { hops = routesResult.hops; path = routesResult.path; }
    if (Number.isFinite(observerCount)) observers = observerCount;
    if (shareUrl) shareLink = shareUrl;
  }
  const reply = formatTemplate({
    sender: details.message && (details.message.senderName || details.message.sender) || "Mesh",
    hops: hops,
    observers: observers,
    path: path,
    channel: details.message && details.message.channelName || channelName,
    shareLink: shareLink
  });
  const idx = resolveChannelIndex(channelName);
  console.log("[bot] Sending reply to " + channelName + " -> " + reply);
  await connection.sendChannelTextMessage(idx, reply);
}

const autoTracker = new Map();
function shouldAutoRespond(text, messageId) {
  if (!autoRespondEnabled || !messageId) return false;
  if (!(text || "").toLowerCase().includes("test")) return false;
  if (autoTracker.has(messageId)) return false;
  autoTracker.set(messageId, true);
  return true;
}

connection.on("connected", async function() {
  console.log("Connected to Companion node.");
  try {
    const channels = await connection.getChannels();
    if (Array.isArray(channels)) {
      channels.forEach(function(ch) { channelMap.set(ch.channelIdx, ch); });
      console.log("Auto target channel index: " + resolveChannelIndex(channelName));
    }
  } catch (err) { console.error("Unable to read channels", err); }
});

connection.on(Constants.PushCodes.MsgWaiting, async function() {
  try {
    const waitingMessages = await connection.getWaitingMessages();
    for (const message of waitingMessages) {
      if (message.channelMessage) await handleChannelMessage(message.channelMessage);
    }
  } catch (err) { console.error("Message drain failed", err); }
});

async function handleChannelMessage(channelMessage) {
  const body = channelMessage.text || channelMessage.message || "";
  const obsCount = channelMessage.observerCount ?? 0;
  const msgId = channelMessage.messageHash || channelMessage.hash || channelMessage.frameHash;
  if (!shouldAutoRespond(body, msgId)) return;
  console.log("[bot] Test seen in " + channelName + ", waiting " + replyDelayMs + "ms then querying meshrank...");
  setTimeout(async function() {
    try {
      await sendSmartReply({ message: channelMessage, observers: obsCount });
    } catch (err) { console.error("[bot] Delayed reply failed", err.message || err); }
  }, replyDelayMs);
}

async function main() {
  try {
    await connection.connect();
    console.log("Meshrank bot is online (using " + serverBase + ").");
  } catch (err) {
    console.error("Failed to connect to node", err);
    process.exit(1);
  }
}
main().catch(function(err) { console.error("Unexpected error", err); process.exit(1); });
'@
$outPath = Join-Path $PSScriptRoot "bot.js"
[System.IO.File]::WriteAllText($outPath, $content.Trim(), [System.Text.UTF8Encoding]::new($false))
Write-Host "Created: $outPath"
Write-Host "Run: node bot.js --port COM5"
