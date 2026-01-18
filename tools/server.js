"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const url = require("url");

const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");
const devicesPath = path.join(dataDir, "devices.json");

const host = "127.0.0.1";
const port = 5177;

function readDevicesFresh() {
  try {
    if (!fs.existsSync(devicesPath)) {
      return { byPub: {} };
    }
    const txt = fs.readFileSync(devicesPath, "utf8");
    const obj = JSON.parse(txt);
    if (!obj || typeof obj !== "object") return { byPub: {} };
    if (!obj.byPub || typeof obj.byPub !== "object") obj.byPub = {};
    return obj;
  } catch {
    return { byPub: {} };
  }
}

function send(res, status, contentType, body) {
  res.writeHead(status, {
    "Content-Type": contentType,
    "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
  });
  res.end(body);
}

function htmlPage() {
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>MeshCORE UK Heat Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    html, body { height: 100%; margin: 0; background:#0b0f14; color:#e6edf3; font-family: system-ui, Segoe UI, Arial; }
    #map { height: 100%; width: 100%; }
    .hud {
      position: absolute; z-index: 9999; top: 10px; left: 10px;
      background: rgba(0,0,0,0.65); border: 1px solid rgba(255,255,255,0.15);
      padding: 10px 12px; border-radius: 10px; min-width: 280px;
      backdrop-filter: blur(6px);
    }
    .hud h1 { margin:0 0 8px 0; font-size: 14px; font-weight: 700; }
    .hud .row { display:flex; justify-content: space-between; gap: 10px; font-size: 13px; margin: 3px 0; }
    .hud .muted { opacity: 0.75; }
    .legend { margin-top: 8px; font-size: 12px; opacity: 0.9; }
    .dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:6px; vertical-align: -1px; }
    .btn {
      margin-top: 8px; width: 100%; padding: 8px 10px; border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.2); background: rgba(255,255,255,0.06);
      color: #e6edf3; cursor: pointer;
    }
    .btn:hover { background: rgba(255,255,255,0.10); }
  </style>
</head>
<body>
  <div id="map"></div>

  <div class="hud">
    <h1>MeshCORE Repeater Heat Map (UK)</h1>
    <div class="row"><div class="muted">Last refresh</div><div id="lastRefresh">—</div></div>
    <div class="row"><div class="muted">Total devices</div><div id="total">—</div></div>
    <div class="row"><div class="muted">Repeaters (incl chat/room)</div><div id="repeaters">—</div></div>
    <div class="row"><div class="muted">Repeaters with GPS</div><div id="withGps">—</div></div>
    <div class="row"><div class="muted">Plotted markers</div><div id="plotted">—</div></div>

    <div class="legend">
      <div><span class="dot" style="background:#22c55e"></span>Strong (best RSSI ≥ -75)</div>
      <div><span class="dot" style="background:#eab308"></span>Medium (-95 .. -76)</div>
      <div><span class="dot" style="background:#ef4444"></span>Weak (≤ -96)</div>
    </div>

    <button class="btn" onclick="refreshNow()">Refresh now</button>
  </div>

  <script>
    const REFRESH_MS = 120000;

    const map = L.map('map', { zoomControl: true }).setView([54.5, -3.0], 6);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);

    const markersLayer = L.layerGroup().addTo(map);

    function colourForBestRssi(bestRssi) {
      if (bestRssi === null || bestRssi === undefined || !Number.isFinite(bestRssi)) return "#64748b";
      if (bestRssi >= -75) return "#22c55e";
      if (bestRssi >= -95) return "#eab308";
      return "#ef4444";
    }

    function fmt(n) {
      if (n === null || n === undefined) return "—";
      return String(n);
    }

    async function fetchJson(path) {
      const res = await fetch(path, { cache: "no-store" });
      if (!res.ok) throw new Error("HTTP " + res.status);
      return await res.json();
    }

    function updateHud(stats) {
      document.getElementById("lastRefresh").textContent = new Date().toLocaleTimeString();
      document.getElementById("total").textContent = fmt(stats.total);
      document.getElementById("repeaters").textContent = fmt(stats.repeaters);
      document.getElementById("withGps").textContent = fmt(stats.repeatersWithGps);
      document.getElementById("plotted").textContent = fmt(stats.plotted);
    }

    async function refreshNow() {
      try {
        const data = await fetchJson("/api/devices");
        markersLayer.clearLayers();

        let plotted = 0;

        for (const d of data.devices) {
          if (!d.isRepeater) continue;
          if (!d.gps || !Number.isFinite(d.gps.lat) || !Number.isFinite(d.gps.lon)) continue;

          plotted++;

          const bestRssi = d.stats?.bestRssi;
          const c = colourForBestRssi(bestRssi);

          const marker = L.circleMarker([d.gps.lat, d.gps.lon], {
            radius: 7,
            color: c,
            fillColor: c,
            fillOpacity: 0.85,
            weight: 2
          });

          const title = (d.raw?.lastAdvert?.appData?.name) ? d.raw.lastAdvert.appData.name : (d.name || "(no name)");
          const flags = d.appFlags?.raw !== undefined ? ("0x" + d.appFlags.raw.toString(16).toUpperCase().padStart(2,"0")) : "—";

          marker.bindPopup(
            "<b>" + title + "</b><br/>" +
            "role: " + (d.role || "unknown") + "<br/>" +
            "flags: " + flags + "<br/>" +
            "bestRSSI: " + (bestRssi ?? "—") + "<br/>" +
            "bestSNR: " + (d.stats?.bestSnr ?? "—") + "<br/>" +
            "pub: " + (d.pub ? d.pub.slice(0,8) + "…" : "—")
          );

          marker.addTo(markersLayer);
        }

        updateHud({
          total: data.stats.total,
          repeaters: data.stats.repeaters,
          repeatersWithGps: data.stats.repeatersWithGps,
          plotted
        });
      } catch (e) {
        console.error(e);
      }
    }

    refreshNow();
    setInterval(refreshNow, REFRESH_MS);
  </script>
</body>
</html>`;
}

// -------------------- HTTP server --------------------
const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);

  if (parsed.pathname === "/") {
    return send(res, 200, "text/html; charset=utf-8", htmlPage());
  }

  if (parsed.pathname === "/api/devices") {
    const db = readDevicesFresh();
    const by = db.byPub || {};
    const all = Object.values(by);

    const repeaters = all.filter(d => d && d.isRepeater);
    const repeatersWithGps = repeaters.filter(d => d.gps && Number.isFinite(d.gps.lat) && Number.isFinite(d.gps.lon));

    const payload = {
      stats: {
        total: all.length,
        repeaters: repeaters.length,
        repeatersWithGps: repeatersWithGps.length
      },
      devices: all
    };

    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  if (parsed.pathname === "/api/health") {
    const exists = fs.existsSync(devicesPath);
    const payload = {
      devicesPath,
      exists,
      mtime: exists ? fs.statSync(devicesPath).mtime.toISOString() : null
    };
    return send(res, 200, "application/json; charset=utf-8", JSON.stringify(payload));
  }

  return send(res, 404, "text/plain; charset=utf-8", "Not found");
});

server.listen(port, host, () => {
  console.log(`(map) projectRoot=${projectRoot}`);
  console.log(`(map) devicesPath=${devicesPath}`);
  console.log(`(map) listening http://${host}:${port}`);
});
