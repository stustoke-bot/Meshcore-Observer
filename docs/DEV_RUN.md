# Dev Run

## Observer demo server
```bash
node tools/observer_demo/server.js
```

Optional debug logging:
```bash
DEBUG_PERF=1 DEBUG_SQL=1 node tools/observer_demo/server.js
```

## Runbook (live site)
**Use `meshrank.service`** (port 5199). The legacy `meshrank-server.service` must stay **disabled** or it will conflict on 5199.
```bash
. /root/.nvm/nvm.sh && nvm use 20
cd /root/Meshcore-Observer
sudo systemctl restart meshrank
```

Logs:
- `journalctl -u meshrank -f` or `/root/observer_demo.log` if configured

Status checks:
```bash
systemctl status meshrank --no-pager -l | head -n 10
ss -lntp | grep 5199
curl -i http://127.0.0.1:5199/api/health
curl -i "http://127.0.0.1:5199/api/dashboard?channel=public"
curl -N http://127.0.0.1:5199/api/message-stream | head -n 20
```

## Runbook (test site)
```bash
. /root/.nvm/nvm.sh && nvm use 20
cd /root/Meshcore-Observer
sudo systemctl restart meshrank-test.service
```

Logs:
- `/root/observer_demo_test.log`

Status checks:
```bash
systemctl status meshrank-test.service --no-pager -l | head -n 10
ss -lntp | grep 5200
curl -i http://127.0.0.1:5200/api/admin/status
curl -i "http://127.0.0.1:5200/api/dashboard?channel=public"
curl -N http://127.0.0.1:5200/api/message-stream | head -n 20
```

> **Note:** `meshrank.net` proxies to port `5199` (`meshrank.service`). **Disable `meshrank-server.service`** so it doesn't fight for 5199. `test.meshrank.net` uses port `5200` (`meshrank-test.service`).

## MQTT ingest (optional)
```bash
MESHRANK_MQTT_URL="mqtts://meshrank.net:8883" \
MESHRANK_MQTT_TOPIC="meshrank/observers/+/packets" \
MESHRANK_MQTT_USER="" \
MESHRANK_MQTT_PASS="" \
node tools/observer_demo/mqtt_ingest.js
```

## Synthetic seed data
```bash
node scripts/seed_synthetic.js 1000 6 40
```

## Perf verification
```bash
scripts/verify_perf.sh
```
