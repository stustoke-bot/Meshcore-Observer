# SSE Events

Endpoint: `GET /api/message-stream`

## Events
- `ready`: `{ ok, lastRowId }`
- `packet`: `{ updates, lastRowId }` (observer/path updates keyed by message hash)
- `counters`: `{ ts, channels, rotm, stats }`
- `ranks`: `{ ts, repeater, node, observer, meshscore }`
- `health`: `{ ts, uptime }`
- `ping`: `{}` (keepalive)
- `error`: `{ error }`

## Bot Stream
Endpoint: `GET /api/bot-stream` (requires `Authorization: Bearer <token>` or a valid session cookie)

### Events
- `ready`: `{ ok, ts }`
- `reply`: `{ channel, sender, messageHash, body, hops, observers, path, shareLink }`
- `ping`: `{}`

## Payload notes
- `channels` matches `/api/channels` output (id, name, snippet, time).
- `rotm` matches `/api/rotm` output (feed, leaderboard, updatedAt).
- `stats` comes from the `stats_5m` rollup table and includes totals + per-channel rows.
- `repeater`, `node`, and `observer` are summary payloads matching their respective summary endpoints.
- `meshscore` matches `/api/meshscore` output.
