#!/usr/bin/env node
"use strict";

/**
 * Live map server (no dependencies).
 * Reads: data/devices.json
 * Serves: http://localhost:5177
 * Refresh: every 120s (client side)
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");
const devicesPath = path.join(dataDir, "devices.json");

const PORT = 5177;
const REFRESH_MS = 120000;

function safeReadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); }
  catch { return fallback; }
}

function classifySignal(bestRssi) {
  if (bestRssi === undefined || bestRssi === null) return "unknown";
  const rssi = Number(bestRssi);
  if (!Number.isFinite(rssi)) return "unknown";
  if (rssi > -70) return "strong";
  if (rssi > -95) return "medium";
  return "weak";
}

function buildHeat() {
  const db = safeReadJson(devicesPath, { byPub: {} });
  const byPub = db.byPub || {};

  const counts = { strong: 0, medium: 0, weak: 0, unknown: 0 };
  const features = [];
  const unmapped = [];

  for (const pubKey of Object.keys(byPub)) {
    const d = byPub[pubKey];

    // repeaters only (role is protocol-based from decoder)
    if ((d.role || "").toLowerCase() !== "repeater") continue;

    const bestRssi = d?.stats?.bestRssi ?? null;
    const bestSnr = d?.stats?.bestSnr ?? null;
    const signal = classifySignal(bestRssi);
    counts[signal]++;

    const gps = d?.gps;
    if (gps && Number.isFinite(gps.lat) && Number.isFinite(gps.lon)) {
      features.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [gps.lon, gps.lat] },
        properties: {
          pubKey,
          role: d.role,
          signal,
          bestRssi,
          bestSnr,
          lastSeen: d.lastSeen || null
        }
      });
    } else {
      unmapped.push({ pubKey, bestRssi, bestSnr, lastSeen: d.lastSeen || null });
    }
  }

  unmapped.sort((a, b) => (Number(b.bestRssi ?? -9999) - Number(a.bestRssi ?? -9999)));

  return {
    generatedAt: new Date().toISOString(),
    counts,
    mapped: features.length,
    unmapped: unmapped.length,
    unmappedRepeaters: unmapped,
    geojson: { type: "FeatureCollection", features }
  };
}

function send(res, code, type, body) {
  res.writeHead(code, { "Content-Type": type, "Cache-Control": "no-store" });
  res.end(body);
}

const INDEX_HTML = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>MeshCORE Live Repeater Map</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
  <style>
    html, body { height: 100%; margin: 0; }
    #wrap { display: grid; grid-template-columns: 360px 1fr; height: 100%; }
    #sidebar { padding: 14px; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; border-right: 1px solid #ddd; }
    #map { height: 100%; }
    .kpi { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
    .card { border: 1px solid #eee; border-radius: 10px; padding: 10px; }
    .dot { display:inline-block; width:10px; height:10px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
    .green { background: #1bb34a; }
    .amber { background: #f0b429; }
    .red { background: #e02f2f; }
    .grey { background: #999; }
    .muted { color: #666; font-size: 12px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    button { padding: 8px 10px; border-radius: 8px; border: 1px solid #ddd; background: #fff; cursor: pointer; }
    button:hover { background: #f7f7f7; }
    .list { margin-top: 10px; max-height: 55vh; overflow:auto; }
    .row { padding: 8px 0; border-bottom: 1px solid #f0f0f0; }
  </style>
</head>
<body>
  <div id="wrap">
    <div id="sidebar">
      <h2 style="margin:0 0 6px 0;">MeshCORE Live Map</h2>
      <div class="muted">Auto-refresh: <b>120s</b>. Colour = best RSSI at your sniffer.</div>
      <div style="margin-top:10px;">
        <button id="refreshBtn">Refresh now</button>
      </div>

      <div class="kpi">
        <div class="card">
          <div><span class="dot green"></span><b id="kStrong">0</b> strong</div>
          <div><span class="dot amber"></span><b id="kMed">0</b> medium</div>
          <div><span class="dot red"></span><b id="kWeak">0</b> weak</div>
          <div><span class="dot grey"></span><b id="kUnknown">0</b> unknown</div>
        </div>
        <div class="card">
          <div><b id="kMapped">0</b> mapped</div>
          <div><b id="kUnmapped">0</b> no GPS yet</div>
          <div class="muted" style="margin-top:6px;">Last update:</div>
          <div class="mono" id="kUpdated">-</div>
        </div>
      </div>

      <div class="list" id="unmappedList"></div>
    </div>
    <div id="map"></div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
  <script>
    const REFRESH_MS = ${REFRESH_MS};

    const map = L.map('map').setView([54.5, -3.0], 6);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 18 }).addTo(map);

    const layer = L.layerGroup().addTo(map);
    const markers = new Map();

    function colour(signal) {
      if (signal === "strong") return "#1bb34a";
      if (signal === "medium") return "#f0b429";
      if (signal === "weak") return "#e02f2f";
      return "#666";
    }
    function setText(id, v) { document.getElementById(id).textContent = String(v); }

    async function refresh() {
      const res = await fetch("/api/heat", { cache: "no-store" });
      const data = await res.json();

      setText("kStrong", data.counts.strong || 0);
      setText("kMed", data.counts.medium || 0);
      setText("kWeak", data.counts.weak || 0);
      setText("kUnknown", data.counts.unknown || 0);
      setText("kMapped", data.mapped || 0);
      setText("kUnmapped", data.unmapped || 0);
      document.getElementById("kUpdated").textContent = data.generatedAt;

      const list = document.getElementById("unmappedList");
      list.innerHTML = "";
      const header = document.createElement("div");
      header.className = "muted";
      header.style.marginTop = "12px";
      header.textContent = "Repeaters with no GPS yet:";
      list.appendChild(header);

      (data.unmappedRepeaters || []).slice(0, 250).forEach(r => {
        const row = document.createElement("div");
        row.className = "row";
        row.innerHTML =
          "<div class='mono'><b>" + r.pubKey.slice(0,12) + "…</b></div>" +
          "<div class='muted'>bestRSSI " + r.bestRssi + " | bestSNR " + r.bestSnr + "</div>" +
          "<div class='muted'>lastSeen " + (r.lastSeen || "-") + "</div>";
        list.appendChild(row);
      });

      const seen = new Set();
      (data.geojson.features || []).forEach(f => {
        const pub = f.properties.pubKey;
        seen.add(pub);

        const c = colour(f.properties.signal);
        const lat = f.geometry.coordinates[1];
        const lon = f.geometry.coordinates[0];

        const popup =
          "<b>Repeater</b><br/>" +
          "<span class='mono'>" + pub + "</span><br/>" +
          "signal: <b>" + f.properties.signal + "</b><br/>" +
          "bestRSSI: " + f.properties.bestRssi + "<br/>" +
          "bestSNR: " + f.properties.bestSnr + "<br/>" +
          "lastSeen: " + (f.properties.lastSeen || "-");

        if (markers.has(pub)) {
          const m = markers.get(pub);
          m.setLatLng([lat, lon]);
          m.setStyle({ color: c, fillColor: c, fillOpacity: 0.85 });
          m.setPopupContent(popup);
        } else {
          const m = L.circleMarker([lat, lon], { radius: 8, color: c, fillColor: c, fillOpacity: 0.85, weight: 2 }).addTo(layer);
          m.bindPopup(popup);
          markers.set(pub, m);
        }
      });

      for (const [pub, m] of markers.entries()) {
        if (!seen.has(pub)) {
          layer.removeLayer(m);
          markers.delete(pub);
        }
      }
    }

    document.getElementById("refreshBtn").addEventListener("click", () => refresh().catch(console.error));
    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), REFRESH_MS);
  </script>
</body>
</html>`;

const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);

  if (u.pathname === "/") {
    return send(res, 200, "text/html; charset=utf-8", INDEX_HTML);
  }

  if (u.pathname === "/api/heat") {
    if (!fs.existsSync(devicesPath)) {
      return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
        generatedAt: new Date().toISOString(),
        error: "devices.json not found",
        counts: { strong: 0, medium: 0, weak: 0, unknown: 0 },
        mapped: 0,
        unmapped: 0,
        unmappedRepeaters: [],
        geojson: { type: "FeatureCollection", features: [] }
      }));
    }
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(buildHeat()));
  }

  if (u.pathname === "/api/health") {
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify({
      ok: true,
      devicesPath,
      devicesExists: fs.existsSync(devicesPath),
      refreshMs: REFRESH_MS
    }));
  }

  return send(res, 404, "text/plain; charset=utf-8", "Not found");
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`(live-map) running: http://localhost:${PORT}`);
  console.log(`(live-map) health:  http://localhost:${PORT}/api/health`);
  console.log(`(live-map) reading: ${devicesPath}`);
});

