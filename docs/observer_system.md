# Observer Repeaters: Data Model, API, and Scoring

This design keeps raw RF intact, ingests observer uploads, and provides a message view that merges mesh and observer sightings.

## Data Model (authoritative vs derived)

Authoritative (append-only):
- data/rf.ndjson
  - Raw RF from local sniffer (already in place)
- data/observer.ndjson
  - Raw observer uploads (encrypted payload as received)
  - One JSON object per packet

Derived (rebuildable):
- data/decoded.ndjson
  - Local decode summaries (already in place)
- data/observations.ndjson
  - One "hearing" per packet per observer or mesh path
- data/messages.ndjson
  - Aggregated message rows for UI (one row per message key)

## Observer Upload Record (data/observer.ndjson)
Each line is a single encrypted packet as received by a remote observer repeater:
{
  "ts": "2026-01-17T10:20:00.000Z",
  "observerId": "OBS_LTN",
  "observerPub": "optional public key",
  "rssi": -98,
  "snr": 3.5,
  "crc": true,
  "freq": 868.1,
  "sf": 7,
  "bw": 125,
  "cr": "4/5",
  "payloadHex": "....",          // encrypted packet bytes
  "frameHash": "SHA256(payloadHex)"
}

Notes:
- Store payloadHex exactly as received. Do not mutate.
- frameHash is computed once by the uploader or server to match across sources.

## Observation Record (data/observations.ndjson)
One line per "hearing" (mesh path or observer):
{
  "ts": "2026-01-17T10:20:01.000Z",
  "messageKey": "E6552EC7",   // messageHash if decoded else frameHash
  "frameHash": "....",
  "source": "mesh|observer",
  "observerId": "OBS_LTN",
  "rssi": -98,
  "snr": 3.5,
  "path": ["A6","95","CE","5B","25","2B"],  // mesh only
  "hopCount": 6
}

## Message Record (data/messages.ndjson)
Aggregated message view used by UI:
{
  "messageKey": "E6552EC7",
  "channel": "#public",
  "payloadType": "GroupText",
  "body": "hello",
  "sender": "callsign",
  "firstSeen": "2026-01-17T10:20:01.000Z",
  "lastSeen": "2026-01-17T10:20:12.000Z",
  "meshHeardCount": 8,
  "observerHeardCount": 3,
  "bestRssi": -74,
  "bestSnr": 9.5,
  "pathMaxHops": 12,
  "observerIds": ["OBS_LTN","OBS_LON"],
  "source": "mesh|observer|mixed",
  "confidence": 82
}

## Message Identity
Use messageHash from decoded packets when available.
If not decoded, use frameHash = SHA256(raw packet bytes).
This lets observer uploads merge with mesh sightings without decrypting.

## API Endpoints (proposed)
POST /api/observer/upload
- Accepts NDJSON stream or JSON array of observer packets.
- Server appends to data/observer.ndjson and returns count accepted.

GET /api/messages?channel=&since=&limit=
- Returns aggregated message rows (data/messages.ndjson).

GET /api/messages/:messageKey/route
- Returns path nodes, observers, and hop list for route map.

GET /api/observers
- Returns observer registry with activity score and lastSeen.

GET /api/observations?messageKey=
- Raw hearing list for diagnostics.

## Scoring Algorithm (confidence)
Normalize RSSI/SNR and blend with mesh and observer repeat counts.

Definitions:
- rssiScore = clamp((bestRssi + 120) / 70, 0, 1)  // -120..-50
- snrScore = clamp((bestSnr + 20) / 30, 0, 1)      // -20..+10
- meshRepeatScore = 1 - exp(-meshHeardCount / 6)
- observerRepeatScore = 1 - exp(-observerHeardCount / 4)
- observerQuality = average(activityScore of observers hearing this msg)

Confidence:
confidence = 100 * clamp(
  0.50 * rssiScore +
  0.20 * snrScore +
  0.20 * meshRepeatScore +
  0.10 * observerRepeatScore * observerQuality,
  0, 1
)

Observer activity score (0..1):
activityScore = clamp(
  0.5 * (uniqueMessages24h / 200) +
  0.5 * (uptimeHours24h / 24),
  0, 1
)

## UI Coloring Rules
- Mesh-only: blue bubbles (iOS-style)
- Observer-only: green bubbles
- Mixed: blue bubble with a green outline or "mixed" badge

