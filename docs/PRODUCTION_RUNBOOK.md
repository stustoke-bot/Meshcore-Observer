# MeshRank Production Runbook

## Current Architecture
- **Caddy** terminates TLS on ports 80/443 and reverse-proxies **meshrank.net** to `127.0.0.1:5199`.
- **meshrank.service** (working tree `/root/Meshcore-Observer`) is the **only** Node process that should listen on port `5199`.
- **meshrank-server.service** must remain disabled; it is the legacy service that previously fought for the port.
- Repository path: `/root/Meshcore-Observer` (this is where updates are pulled/built).

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
1. `sudo ss -ltnp | egrep ':5199'` → see PID and owning process.
2. `ps -p <PID> -o pid,ppid,cmd` → find how it was started.
3. If the owner is **meshrank-server.service**, that is the wrong unit; stop + disable it:
   ```sh
   sudo systemctl stop meshrank-server
   sudo systemctl disable meshrank-server
   ```
4. Start + enable the intended service:
   ```sh
   sudo systemctl enable meshrank
   sudo systemctl start meshrank
   ```
5. Confirm `ss` now reports PID belonging to `meshrank.service`.

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
