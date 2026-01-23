# Dev Run

## Observer demo server
```bash
node tools/observer_demo/server.js
```

Optional debug logging:
```bash
DEBUG_PERF=1 DEBUG_SQL=1 node tools/observer_demo/server.js
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
