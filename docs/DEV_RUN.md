# Dev Run

## Observer demo server
```bash
node tools/observer_demo/server.js
```

Optional debug logging:
```bash
DEBUG_PERF=1 DEBUG_SQL=1 node tools/observer_demo/server.js
```

## Runbook (restart + status)
```bash
. /root/.nvm/nvm.sh && nvm use 20
cd /root/Meshcore-Observer
sudo systemctl restart meshrank-server.service
```

Logs:
- `/root/observer_demo.log`

Status checks:
```bash
systemctl status meshrank-server.service --no-pager -l | head -n 10
ss -lntp | grep 5199
curl -i http://127.0.0.1:5199/api/admin/status
```

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
