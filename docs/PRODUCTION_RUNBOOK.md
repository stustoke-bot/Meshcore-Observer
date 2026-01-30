# MeshRank Production Runbook

## Current Architecture
- **Caddy** terminates TLS on ports 80/443 and reverse-proxies **meshrank.net** to `127.0.0.1:5199`.
- **meshrank.service** (working tree `/root/Meshcore-Observer`) is the **only** Node process that should listen on port `5199`.
- **meshrank-server.service** must remain disabled; it is the legacy service that previously fought for the port.
- Repository path: `/root/Meshcore-Observer` (this is where updates are pulled/built).
- **Data/DB**: `data` is a symlink: `/root/Meshcore-Observer/data` → `/root/meshrank/data`. The production DB is therefore **`/root/meshrank/data/meshrank.db`**. To query it from the repo: `node tools/observer_demo/query_rf_packets_today.js` (uses same path via symlink).
- **Bot stream auth**: `/api/bot-stream` accepts `Authorization: Bearer <token>`; set `MESHRANK_BOT_TOKEN` to a shared secret for the desktop bot.
- **Cache / startup**: **Site load is never blocked**: if the messages cache is cold, `/api/messages` returns empty immediately so the site loads; the cache is built in the background after a short delay (10s) to avoid blocking the event loop right after start. New messages then arrive in realtime via `/api/message-stream`. Observer rank and other heavy caches return current (or empty) data immediately; refresh is scheduled in background only after a **5-minute warmup** (`CACHE_WARMUP_MS`). Persisted caches (repeater rank, observer rank) are loaded from DB at 30s; no rebuild on load. Auto-refresh loop starts after **15 minutes** (`RANK_DEFER_MS`).

## If site returns 502 or does not load
**If you only kill the process**, systemd may restart **meshrank-server** (if it is enabled) and it will grab 5199 again. Always **stop and disable meshrank-server** as part of recovery so it does not restart.

1. **Stop the legacy service first** (so it doesn’t restart and steal 5199):  
   `sudo systemctl stop meshrank-server; sudo systemctl disable meshrank-server`
2. **Kill any process on 5199**, then start meshrank:  
   `sudo kill -9 $(ss -ltnp | grep ':5199' | grep -oP 'pid=\K[0-9]+' | head -1) 2>/dev/null; sleep 2; sudo systemctl start meshrank`
3. **Clear connection backlog**:  
   `sudo systemctl reload caddy`
4. **Nuclear restart** (one command: stop legacy, kill port, start meshrank, reload Caddy):  
   `sudo systemctl stop meshrank-server; sudo systemctl disable meshrank-server; sudo kill -9 $(ss -ltnp | grep ':5199' | grep -oP 'pid=\K[0-9]+' | head -1) 2>/dev/null; sleep 2; sudo systemctl start meshrank; sleep 3; sudo systemctl reload caddy`
5. Wait 30–60s for the server to warm; then try https://meshrank.net again.

## Website recovery (post-restart)
**Minimal recovery** (often enough without killing by port): stop the legacy service so it cannot grab 5199, then restart meshrank:
```sh
sudo systemctl stop meshrank-server && sudo systemctl disable meshrank-server
sudo systemctl restart meshrank
```
**Verify**:
- `systemctl status meshrank` → Active: active (running), note PID.
- Local health: `curl -i http://127.0.0.1:5199/api/health` → HTTP 200 (nowIso, DB path).
- Ports: `ss -ltnp | egrep ':80|:443|:5199'` → node/Caddy on expected ports.
- Public: https://meshrank.net → HTTP 200.

**Warm-up behaviour**: After a restart the backend spends 5–10 minutes building caches (Node can sit at high CPU). During that window:
- Caddy may log connection-reset, 502/504, dial/connect-refused, or timeout (e.g. “no recent network activity” while streaming).
- Heavy endpoints can be slow or drop: `curl -i "http://127.0.0.1:5199/api/dashboard?channel=public"` (up to 120 s timeout) and `curl -N http://127.0.0.1:5199/api/message-stream` may time out or disconnect until warm-up finishes.

**Next steps**:
1. Give the service **5–10 minutes** to finish rank/cache warm-up (runbook: `CACHE_WARMUP_MS` 5 min; auto-refresh after 15 min) before hitting the front end repeatedly.
2. Once warm, verify from outside (meshrank.net or a browser) and repeat the API checks above.
3. If 502/504 or errors persist after ~15 min, collect logs for the post-restart window and trace:
   ```sh
   journalctl -u meshrank --since "15 minutes ago" --no-pager
   journalctl -u caddy --since "15 minutes ago" --no-pager
   ```

## Quick Health Checks
1. **Port ownership**:
   ```sh
   sudo ss -ltnp | egrep ':80|:443|:5199'
   ```
2. **Service enablement & status**:
   ```sh
   sudo systemctl is-enabled meshrank meshrank-server
   sudo systemctl status meshrank --no-pager -l | sed -n '1,40p'
   sudo systemctl status meshrank-server --no-pager -l | sed -n '1,40p' || true
   ```
3. **Caddy block sanity**:
   ```sh
   sudo sed -n '1,80p' /etc/caddy/Caddyfile
   ```
4. **HTTP sanity**:
   ```sh
   curl -I https://meshrank.net
   curl -i https://meshrank.net/api/share/00000 | head -n 40
   ```

## Common failure: `EADDRINUSE` on 5199
This often happens because **meshrank-server.service** is enabled and binds to 5199; when you kill the process, systemd restarts it and it grabs the port again. Fix: stop and disable meshrank-server, then start meshrank.

1. **Stop and disable the legacy service** (so it does not restart):
   ```sh
   sudo systemctl stop meshrank-server
   sudo systemctl disable meshrank-server
   ```
2. Kill whatever is on 5199, then start meshrank:
   ```sh
   sudo kill -9 $(ss -ltnp | grep ':5199' | grep -oP 'pid=\K[0-9]+' | head -1) 2>/dev/null
   sleep 2
   sudo systemctl start meshrank
   ```
3. Confirm only meshrank is on 5199: `sudo ss -ltnp | egrep ':5199'` → PID should be meshrank.service.

## Rollback (toggle services)
- **Rollback to legacy runner**:
  ```sh
  sudo systemctl stop meshrank
  sudo systemctl disable meshrank
  sudo systemctl enable meshrank-server
  sudo systemctl start meshrank-server
  sudo ss -ltnp | egrep ':5199'
  ```
- **Restore desired state**:
  ```sh
  sudo systemctl stop meshrank-server
  sudo systemctl disable meshrank-server
  sudo systemctl enable meshrank
  sudo systemctl start meshrank
  sudo ss -ltnp | egrep ':5199'
  ```
