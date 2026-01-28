#!/usr/bin/env node
"use strict";

const { Client, GatewayIntentBits } = require("discord.js");

const DEFAULT_BASE_URL = "http://127.0.0.1:5199";
const DEFAULT_CHANNEL = "#midlands";
const DEFAULT_POLL_INTERVAL_MS = 10_000;
const DEFAULT_FETCH_LIMIT = 25;

const config = {
  baseUrl: String(process.env.MESHRANK_BASE_URL || DEFAULT_BASE_URL).replace(/\/+$/, ""),
  channel: normalizeChannelName(process.env.MESHRANK_CHANNEL || DEFAULT_CHANNEL),
  pollIntervalMs: Number.parseInt(process.env.MESHRANK_DISCORD_POLL_MS, 10) || DEFAULT_POLL_INTERVAL_MS,
  fetchLimit: Number.parseInt(process.env.MESHRANK_DISCORD_FETCH_LIMIT, 10) || DEFAULT_FETCH_LIMIT,
  webhookUrl: String(process.env.DISCORD_WEBHOOK_URL || "").trim(),
  botToken: String(process.env.DISCORD_BOT_TOKEN || "").trim(),
  botChannelId: String(process.env.DISCORD_CHANNEL_ID || "").trim(),
  maxMessageLength: Number.parseInt(process.env.MESHRANK_DISCORD_MAX_LENGTH, 10) || 1600
};

if (!config.webhookUrl && (!config.botToken || !config.botChannelId)) {
  console.error(
    "Discord bridge requires either DISCORD_WEBHOOK_URL or both DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID."
  );
  process.exit(1);
}

(async () => {
  const publisher = await createDiscordPublisher(config);
  await publisher.ready;
  console.log(`Discord bridge ready → ${await publisher.describeTarget()}`);
  const messageState = { initialized: false, ts: null, ids: new Set() };
  let running = true;
  process.on("SIGINT", () => stop("SIGINT"));
  process.on("SIGTERM", () => stop("SIGTERM"));

  async function stop(signal) {
    if (!running) return;
    running = false;
    console.log(`Shutting down (${signal})…`);
    if (publisher.shutdown) {
      await publisher.shutdown();
    }
    process.exit(0);
  }

  while (running) {
    try {
      const messages = await fetchLatestMessages(config);
      if (!messages.length) {
        if (!messageState.initialized) {
          messageState.initialized = true;
        }
        await wait(config.pollIntervalMs);
        continue;
      }
      const sorted = sortMessages(messages);
      if (!messageState.initialized) {
        primeState(sorted, messageState);
        messageState.initialized = true;
      } else {
        const fresh = filterNewMessages(sorted, messageState);
        for (const msg of fresh) {
          await publisher.send(formatDiscordMessage(msg, config));
        }
        if (fresh.length) {
          primeState(sorted, messageState);
        }
      }
    } catch (err) {
      console.error("Discord bridge error:", err?.message || err);
    }
    await wait(config.pollIntervalMs);
  }
})().catch((err) => {
  console.error("Failed to start Discord bridge:", err?.message || err);
  process.exit(1);
});

function normalizeChannelName(raw) {
  if (!raw) return DEFAULT_CHANNEL;
  const trimmed = String(raw).trim();
  return trimmed.startsWith("#") ? trimmed : `#${trimmed}`;
}

function resolveTimestamp(ts) {
  if (typeof ts === "number" && Number.isFinite(ts)) {
    return ts;
  }
  const parsed = Date.parse(ts);
  return Number.isFinite(parsed) ? parsed : null;
}

function sortMessages(messages) {
  return messages
    .map((msg) => ({
      ...msg,
      id: (String(msg.messageHash || msg.id || msg.frameHash || "") || "").toUpperCase(),
      ts: resolveTimestamp(msg.ts)
    }))
    .sort((a, b) => (a.ts || 0) - (b.ts || 0));
}

function filterNewMessages(sorted, state) {
  return sorted.filter((entry) => {
    if (!Number.isFinite(entry.ts)) return false;
    if (state.ts === null) return false;
    if (entry.ts > state.ts) return true;
    if (entry.ts === state.ts && entry.id && !state.ids.has(entry.id)) return true;
    return false;
  });
}

function primeState(sorted, state) {
  if (!sorted.length) return;
  const newest = sorted[sorted.length - 1];
  if (!Number.isFinite(newest.ts)) return;
  const ts = newest.ts;
  state.ts = ts;
  state.ids.clear();
  sorted
    .filter((entry) => entry.ts === ts && entry.id)
    .forEach((entry) => state.ids.add(entry.id));
}

function formatDiscordMessage(msg, cfg) {
  const ts = Number.isFinite(msg.ts) ? msg.ts : Date.now();
  const date = new Date(ts);
  const timeLabel = date.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
  const sender = msg.sender || "unknown";
  const body = truncate(String(msg.body || "(no content)"), cfg.maxMessageLength);
  const channelLabel = msg.channelName || cfg.channel;
  const linkNote = msg.messageHash ? ` • [${msg.messageHash.slice(0, 8)}]` : "";
  return {
    content: `**${channelLabel}** ${timeLabel} • **${sender}**${linkNote}\n${body}`
  };
}

function truncate(value, limit) {
  if (typeof value !== "string") return value;
  if (value.length <= limit) return value;
  return `${value.slice(0, limit - 3)}…`;
}

async function fetchLatestMessages(cfg) {
  const url = new URL("/api/messages", cfg.baseUrl);
  url.searchParams.set("channel", cfg.channel);
  url.searchParams.set("limit", String(cfg.fetchLimit));
  const res = await fetch(url.toString(), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} fetching messages`);
  }
  const payload = await res.json();
  if (!Array.isArray(payload.messages)) return [];
  return payload.messages;
}

async function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createDiscordPublisher(cfg) {
  if (cfg.webhookUrl) {
    const describeTarget = () => Promise.resolve(`webhook ${cfg.webhookUrl}`);
    const send = async (payload) => {
      const body = { content: payload.content };
      const res = await fetch(cfg.webhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        throw new Error(`Discord webhook error ${res.status}`);
      }
    };
    return { ready: Promise.resolve(), send, describeTarget, shutdown: async () => {} };
  }

  const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages] });
  const readyPromise = client.login(cfg.botToken).then(() => client);
  const channelPromise = readyPromise.then(() => client.channels.fetch(cfg.botChannelId));
  const describeTarget = async () => {
    const channel = await channelPromise;
    return `channel ${cfg.botChannelId} (${channel?.name || "unknown"})`;
  };
  const send = async (payload) => {
    const channel = await channelPromise;
    if (!channel || typeof channel.send !== "function") {
      throw new Error("Discord channel unavailable");
    }
    await channel.send(payload);
  };
  const shutdown = async () => {
    try {
      await readyPromise;
    } catch {}
    client.destroy();
  };
  return { ready: channelPromise, send, describeTarget, shutdown };
}
