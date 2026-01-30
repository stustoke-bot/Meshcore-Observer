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
  .option("stream", {
    description: "Listen to Meshrank bot stream instead of mesh incoming messages",
    type: "boolean",
    default: false
  })
  .option("stream-url", {
    description: "Bot stream URL (default: /api/bot-stream on server)",
    type: "string"
  })
  .option("auto", { alias: "a", description: "Auto-respond when test is mentioned", type: "boolean", default: true })
  .option("delay", { alias: "d", description: "Delay in ms before sending reply", type: "number", default: 10000 })
  .option("dry-run", {
    alias: "n",
    description: "Do not send reply on mesh; print reply on screen only (for testing)",
    type: "boolean",
    default: false
  })
  .option("log-only", {
    description: "Log a reply for EVERY message (no send). Implies --echo-all and --dry-run.",
    type: "boolean",
    default: false
  })
  .option("echo-all", {
    alias: "e",
    description: "For EVERY channel message: run lookup and print would-be reply (no send, no 'test' required). Use to verify DB lookup.",
    type: "boolean",
    default: false
  })
  .help()
  .alias("help", "h")
  .parseSync();

const portPath = argv.port;
const channelName = argv.channel;
const replyTemplate = argv.template;
const serverBase = argv.server.replace(/\/$/, "");
const bearerToken = argv.token || process.env.MESHRANK_TOKEN || "";
const useStream = argv.stream === true;
const botStreamUrl = (argv["stream-url"] || `${serverBase}/api/bot-stream`).replace(/\/$/, "");
const autoRespondEnabled = argv.auto;
const replyDelayMs = useStream ? 0 : Math.max(0, Number(argv.delay) || 30000);
const logOnly = argv["log-only"] === true;
const dryRun = argv["dry-run"] === true || logOnly;
const echoAll = argv["echo-all"] === true || logOnly;

let channelIndex = 0;
const connection = new NodeJSSerialConnection(portPath);
const channelMap = new Map();

function log(...args) {
  const ts = new Date().toISOString();
  console.log(`[${ts}]`, ...args);
}

function formatTemplate(data = {}) {
  return replyTemplate
    .replace(/{sender}/g, data.sender || "Mesh")
    .replace(/{channel}/g, data.channel || channelName)
    .replace(/{hops}/g, String(data.hops ?? 0))
    .replace(/{observers}/g, String(data.observers ?? 0))
    .replace(/{path}/g, data.path || "—")
    .replace(/{shareLink}/g, data.shareLink || serverBase);
}

function normalizeSenderName(name) {
  const raw = String(name || "").trim();
  if (!raw) return "Mesh";
  const unwrapped = raw.replace(/^\[+/, "").replace(/\]+$/, "").trim();
  return unwrapped ? unwrapped.toUpperCase() : "Mesh";
}

function buildHeaders() {
  const headers = { "Content-Type": "application/json" };
  if (bearerToken) headers.Authorization = `Bearer ${bearerToken}`;
  return headers;
}

async function startBotStream() {
  if (!useStream) return;
  const url = botStreamUrl;
  log("[stream] Connecting to bot stream", url);
  try {
    const res = await fetch(url, { headers: buildHeaders() });
    if (!res.ok || !res.body) {
      log("[stream] Connection failed", res.status, res.statusText);
      setTimeout(startBotStream, 5000);
      return;
    }
    log("[stream] Connected");
    const reader = res.body.getReader();
    let buffer = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += Buffer.from(value).toString("utf8");
      let idx;
      while ((idx = buffer.indexOf("\n\n")) >= 0) {
        const chunk = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const lines = chunk.split("\n");
        let eventName = "message";
        let data = "";
        for (const line of lines) {
          if (line.startsWith("event:")) eventName = line.slice(6).trim();
          if (line.startsWith("data:")) data += line.slice(5).trim();
        }
        if (eventName === "ping" || !data) continue;
        let payload = null;
        try { payload = JSON.parse(data); } catch { payload = null; }
        if (eventName === "ready") {
          log("[stream] Ready", payload);
          continue;
        }
        if (eventName === "reply" && payload) {
          const pathText = Array.isArray(payload.path) ? payload.path.join(" → ") : "—";
          const reply = formatTemplate({
            sender: normalizeSenderName(payload.sender),
            hops: payload.hops,
            observers: payload.observers,
            path: pathText,
            channel: payload.channel || channelName,
            shareLink: payload.shareLink
          });
          const idx = resolveChannelIndex(payload.channel || channelName);
          log("[stream] Reply for", payload.channel || channelName, "index", idx);
          log("[stream] Text:", reply);
          if (dryRun) {
            log("[dry-run] >>> WOULD SEND (not sent – dry-run mode) <<<");
            log("[dry-run]", reply);
            log("[dry-run] >>> END <<<");
          } else {
            await connection.sendChannelTextMessage(idx, reply);
            log("[stream] Sent OK");
          }
        }
      }
    }
  } catch (err) {
    log("[stream] Error", err?.message || err);
  }
  log("[stream] Disconnected; retrying in 5s...");
  setTimeout(startBotStream, 5000);
}

async function fetchShareForMessage(messageHash) {
  if (!messageHash) {
    log("[api] share: no messageHash, skip");
    return null;
  }
  const hash = String(messageHash).trim().toUpperCase();
  const url = `${serverBase}/api/routes/${encodeURIComponent(hash)}/share`;
  log("[api] share POST", url);
  try {
    const res = await fetch(url, { method: "POST", headers: buildHeaders() });
    log("[api] share status", res.status, res.statusText);
    const payload = await res.json().catch(() => null);
    if (!res.ok) {
      log("[api] share body", JSON.stringify(payload));
      return null;
    }
    const link = payload?.code ? `${serverBase}/msg/${payload.code}` : (payload?.url || null);
    log("[api] share result: code=", payload?.code, "url=", link);
    return link;
  } catch (err) {
    log("[api] share error", err?.message || err);
    return null;
  }
}

async function fetchMessageRoutes(messageHash) {
  if (!messageHash) {
    log("[api] message-routes: no messageHash, skip");
    return null;
  }
  const hash = String(messageHash).trim().toUpperCase();
  const url = `${serverBase}/api/message-routes?hash=${encodeURIComponent(hash)}&hours=24`;
  log("[api] message-routes GET", url);
  try {
    const res = await fetch(url, { headers: buildHeaders() });
    log("[api] message-routes status", res.status);
    const payload = await res.json().catch(() => null);
    if (!res.ok) {
      log("[api] message-routes body", JSON.stringify(payload));
      return null;
    }
    if (!payload?.ok || !Array.isArray(payload.routes) || !payload.routes.length) {
      log("[api] message-routes: no routes, payload.ok=", payload?.ok, "routes.length=", payload?.routes?.length);
      return null;
    }
    const first = payload.routes[0];
    const hops = Number.isFinite(first.hopCount) ? first.hopCount : (first.pathNames?.length ?? 0);
    const path = Array.isArray(first.pathNames) && first.pathNames.length
      ? first.pathNames.join(" → ")
      : (first.path?.length ? "—" : "—");
    log("[api] message-routes result: hops=", hops, "path=", path);
    return { hops, path };
  } catch (err) {
    log("[api] message-routes error", err?.message || err);
    return null;
  }
}

async function fetchMessageDebug(messageHash) {
  if (!messageHash) {
    log("[api] message-debug: no messageHash, skip");
    return 0;
  }
  const hash = String(messageHash).trim().toUpperCase();
  const url = `${serverBase}/api/message-debug?hash=${encodeURIComponent(hash)}`;
  log("[api] message-debug GET", url);
  try {
    const res = await fetch(url, { headers: buildHeaders() });
    log("[api] message-debug status", res.status);
    const payload = await res.json().catch(() => null);
    if (!res.ok) return 0;
    if (!payload?.ok) {
      log("[api] message-debug: payload.ok=false");
      return 0;
    }
    const fromIndex = Array.isArray(payload.fromIndex) ? payload.fromIndex : [];
    const fromLog = Array.isArray(payload.fromLog) ? payload.fromLog : [];
    const unique = new Set([...fromIndex, ...fromLog].filter(Boolean));
    const count = unique.size;
    log("[api] message-debug result: observers=", count);
    return count;
  } catch (err) {
    log("[api] message-debug error", err?.message || err);
    return 0;
  }
}

/** Get sender (and other fields) from observer DB by message hash */
async function fetchMessageByHash(messageHash) {
  if (!messageHash) return null;
  const hash = String(messageHash).trim().toUpperCase();
  const url = `${serverBase}/api/message?hash=${encodeURIComponent(hash)}`;
  log("[api] message GET (sender from DB)", url);
  try {
    const res = await fetch(url, { headers: buildHeaders() });
    log("[api] message status", res.status);
    const payload = await res.json().catch(() => null);
    if (!res.ok || !payload?.ok) return null;
    log("[api] message result: sender=", payload.sender);
    return payload;
  } catch (err) {
    log("[api] message error", err?.message || err);
    return null;
  }
}

/** Find message in observer DB by channel + body (e.g. after 10s delay message may be ingested); returns sender + messageHash */
async function fetchMessageLookup(channelName, body, minutes = 3) {
  if (!body || typeof body !== "string") return null;
  const url = `${serverBase}/api/message-lookup?channel=${encodeURIComponent(channelName)}&body=${encodeURIComponent(body.trim())}&minutes=${Math.min(60, Math.max(1, minutes))}`;
  log("[api] message-lookup GET (find by channel+body)", url);
  try {
    const res = await fetch(url, { headers: buildHeaders() });
    log("[api] message-lookup status", res.status);
    const payload = await res.json().catch(() => null);
    if (!res.ok || !payload?.ok) return null;
    log("[api] message-lookup result: sender=", payload.sender, "messageHash=", payload.messageHash || "(none)");
    return payload;
  } catch (err) {
    log("[api] message-lookup error", err?.message || err);
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

/**
 * Derive sender from body – MeshCore protocol does NOT send sender name/id, only text.
 * "NAME: msg" or "NAME – msg". Do NOT use @[ID] – that is who they're replying to, not the sender.
 */
function deriveSenderFromBody(body) {
  if (!body || typeof body !== "string") return null;
  const s = body.trim();

  // "97180D7E: Test" or "Alice: hello" -> use part before first colon
  const colon = s.indexOf(":");
  if (colon > 0) {
    const before = s.slice(0, colon).trim();
    if (before) return before;
  }

  // "Alice – message" or "Alice - message" (em dash or hyphen)
  const dash = s.match(/^([^\s–\-]+)\s+[–\-]\s+/);
  if (dash && dash[1]) return dash[1].trim();

  // Do NOT use @[ID] – that is the person being replied to, not the sender
  return null;
}

async function sendSmartReply(details) {
  const msg = details.message || {};
  let msgHash = msg.messageHash || msg.hash || msg.frameHash;
  const body = msg.text || msg.message || "";
  let hops = msg.pathLength ?? 0;
  let observers = details.observers ?? 0;
  let path = "—";
  let shareLink = serverBase;
  let senderFromDb = null;

  log("[reply] Building reply for messageHash=", msgHash || "(none)");

  if (msgHash) {
    const hash = String(msgHash).trim().toUpperCase();
    const dbMessage = await fetchMessageByHash(hash);
    if (dbMessage && dbMessage.sender) senderFromDb = dbMessage.sender;
    log("[reply] Fetching routes, debug, share from meshrank...");
    const [routesResult, observerCount, shareUrl] = await Promise.all([
      fetchMessageRoutes(hash),
      fetchMessageDebug(hash),
      fetchShareForMessage(hash)
    ]);
    if (routesResult) {
      hops = routesResult.hops ?? hops;
      path = routesResult.path ?? path;
    }
    if (Number.isFinite(observerCount)) observers = observerCount;
    if (shareUrl) shareLink = shareUrl;
  } else {
    let lookup = await fetchMessageLookup(channelName, body, 3);
    if (!lookup && body && body.includes(":")) {
      const bodyAfterColon = body.slice(body.indexOf(":") + 1).trim();
      if (bodyAfterColon) lookup = await fetchMessageLookup(channelName, bodyAfterColon, 3);
    }
    if (lookup) {
      if (lookup.sender) senderFromDb = lookup.sender;
      if (lookup.messageHash) {
        msgHash = lookup.messageHash;
        log("[reply] Found message in observer DB, messageHash=", msgHash);
        const hash = String(msgHash).trim().toUpperCase();
        const [routesResult, observerCount, shareUrl] = await Promise.all([
          fetchMessageRoutes(hash),
          fetchMessageDebug(hash),
          fetchShareForMessage(hash)
        ]);
        if (routesResult) {
          hops = routesResult.hops ?? hops;
          path = routesResult.path ?? path;
        }
        if (Number.isFinite(observerCount)) observers = observerCount;
        if (shareUrl) shareLink = shareUrl;
      }
    }
  }

  const derived = deriveSenderFromBody(body);
  const sender = normalizeSenderName(msg.senderName || msg.sender || senderFromDb || derived || "Mesh");
  if (senderFromDb) log("[reply] Sender from observer DB:", senderFromDb);
  else if (!msg.senderName && !msg.sender && derived) log("[reply] Derived sender from body:", derived);
  else if (!msg.senderName && !msg.sender && !derived) log("[reply] No sender in message/DB/body – using 'Mesh'");
  const reply = formatTemplate({
    sender,
    hops,
    observers,
    path,
    channel: msg.channelName || channelName,
    shareLink
  });
  const idx = resolveChannelIndex(channelName);
  log("[reply] Channel index", idx, "(", channelName, ")");
  log("[reply] Text:", reply);
  if (dryRun || echoAll) {
    log("[dry-run] >>> WOULD SEND (not sent – dry-run/echo-all mode) <<<");
    log("[dry-run]", reply);
    log("[dry-run] >>> END <<<");
    return;
  }
  await connection.sendChannelTextMessage(idx, reply);
  log("[reply] Sent OK");
}

const autoTracker = new Map();

function shouldAutoRespond(text, messageId) {
  if (!autoRespondEnabled) {
    log("[filter] Skip: auto-respond disabled");
    return false;
  }
  const normalized = (text || "").toLowerCase();
  if (!messageId) {
    log("[filter] Skip: no messageId and could not build synthetic id");
    return false;
  }
  if (!normalized.includes("test")) {
    log("[filter] Skip: text does not contain 'test'");
    return false;
  }
  if (autoTracker.has(messageId)) {
    log("[filter] Skip: already replied to this messageId");
    return false;
  }
  autoTracker.set(messageId, true);
  log("[filter] Will reply: message contains 'test', messageId=", String(messageId).slice(0, 16) + "...");
  return true;
}

connection.on("connected", async () => {
  log("[event] Connected to Companion node.");
  try {
    const channels = await connection.getChannels();
    log("[event] getChannels returned", Array.isArray(channels) ? channels.length : 0, "channels");
    if (Array.isArray(channels)) {
      channels.forEach((ch) => {
        channelMap.set(ch.channelIdx, ch);
        log("[event]   channel index", ch.channelIdx, "name=", ch.name || ch.channelName || "(unnamed)");
      });
      channelIndex = resolveChannelIndex(channelName);
      log("[event] Target channel for replies: index=", channelIndex, "name=", channelName);
    }
  } catch (err) {
    log("[event] Unable to read channels", err?.message || err);
  }
});

if (!useStream) {
  connection.on(Constants.PushCodes.MsgWaiting, async () => {
  log("[event] MsgWaiting fired – checking for messages...");
  try {
    const waitingMessages = await connection.getWaitingMessages();
    log("[event] getWaitingMessages returned", Array.isArray(waitingMessages) ? waitingMessages.length : 0, "items");
    if (!Array.isArray(waitingMessages)) {
      log("[event] waitingMessages is not an array, skip");
      return;
    }
    for (let i = 0; i < waitingMessages.length; i++) {
      const message = waitingMessages[i];
      log("[event] Message", i + 1, "keys:", message ? Object.keys(message) : "null");
      if (message.channelMessage) {
        log("[event] Message", i + 1, "has channelMessage – processing");
        await handleChannelMessage(message.channelMessage);
      } else {
        log("[event] Message", i + 1, "no channelMessage (private/direct?), skip");
      }
    }
  } catch (err) {
    log("[event] Message drain failed", err?.message || err);
  }
  });
}

async function handleChannelMessage(channelMessage) {
  const body = channelMessage.text || channelMessage.message || "";
  const obsCount = channelMessage.observerCount ?? 0;
  let msgId = channelMessage.messageHash || channelMessage.hash || channelMessage.frameHash;
  if (!msgId && (channelMessage.senderTimestamp != null || body)) {
    msgId = "synthetic_" + String(channelMessage.senderTimestamp ?? "") + "_" + (body || "").slice(0, 80);
    log("[in] No messageHash from device – using synthetic id for dedupe");
  }
  const sender = channelMessage.senderName || channelMessage.sender || "?";
  const chName = channelMessage.channelName || "?";

  log("[in] >>> INCOMING CHANNEL MESSAGE <<<");
  log("[in]   channel:", chName, "channelIdx:", channelMessage.channelIdx);
  log("[in]   sender:", sender);
  log("[in]   body:", body || "(empty)");
  log("[in]   messageHash:", channelMessage.messageHash || channelMessage.hash || channelMessage.frameHash || "(none)");
  log("[in]   observerCount:", obsCount);
  log("[in]   raw keys:", Object.keys(channelMessage));

  const doEchoAll = echoAll;
  const willReply = shouldAutoRespond(body, msgId);
  if (!willReply && !doEchoAll) return;

  if (doEchoAll) {
    log("[echo-all] Message triggered echo-all – running lookup and printing reply (no send)...");
  } else {
    log("[bot] Will reply: scheduling in", replyDelayMs, "ms...");
  }

  const delayMs = doEchoAll ? 2000 : replyDelayMs;
  setTimeout(async () => {
    if (doEchoAll) log("[echo-all] Delay elapsed – querying observer DB and building reply...");
    else log("[bot] Delay elapsed – querying meshrank and sending reply...");
    try {
      await sendSmartReply({
        message: channelMessage,
        observers: obsCount
      });
    } catch (err) {
      log("[bot] Delayed reply failed", err?.message || err);
    }
  }, delayMs);
}

async function main() {
  log("[start] Meshrank bot starting, port=", portPath, "channel=", channelName, "server=", serverBase, "delay=", replyDelayMs, "ms");
  if (dryRun) log("[start] DRY-RUN: replies will NOT be sent on mesh (use without --dry-run to go live)");
  if (echoAll) log("[start] ECHO-ALL: every channel message will trigger lookup + print reply (no send, no 'test' required)");
  if (logOnly) log("[start] LOG-ONLY: printing replies for every message; no mesh send");
  if (useStream) log("[start] STREAM MODE: replies are driven by /api/bot-stream (delay disabled)");
  try {
    await connection.connect();
    log("[start] Meshrank bot is online.");
    if (useStream) startBotStream().catch(() => {});
  } catch (err) {
    log("[start] Failed to connect", err?.message || err);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("[fatal]", err?.message || err);
  process.exit(1);
});
