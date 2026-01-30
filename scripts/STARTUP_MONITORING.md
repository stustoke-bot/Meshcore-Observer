# Server Startup Monitoring Guide

## Overview
This monitoring setup tracks detailed timing and performance metrics during server startup to identify blocking operations and bottlenecks.

## What Gets Monitored

### 1. Server Code Instrumentation (Built-in)
The server.js file now includes timing logs for:
- Module load time
- Database initialization time
- Server listen callback time
- Cache hydration time
- Individual request processing time

All timing logs appear in the journal/systemd logs with `[TIMING]` prefix.

### 2. External Monitoring Script
The `monitor_startup.sh` script monitors:
- System resources (CPU, memory, load)
- Node.js process details
- Port 5199 listening status
- Systemd service status
- Journal logs in real-time
- API endpoint response times
- Database file locks and sizes

## How to Use

### Step 1: Start the Monitoring Script
```bash
cd /root/Meshcore-Observer
./scripts/monitor_startup.sh
```

The script will:
- Create a log file in `logs/startup_monitor_TIMESTAMP.log`
- Start monitoring all resources
- Display "NOW RESTART THE SERVER" message when ready

### Step 2: Restart the Server
In a **separate terminal**, restart the server:
```bash
sudo systemctl restart meshrank-server.service
```

### Step 3: Watch the Monitoring Output
The monitoring script will show:
- Real-time resource usage
- Request processing times
- Database operations
- Systemd journal entries

### Step 4: Stop Monitoring
Press `Ctrl+C` in the monitoring terminal to stop.

## Analyzing the Results

### Key Metrics to Look For

1. **Module Load Time**
   - Look for `[TIMING] Module load END`
   - Should be < 1 second typically

2. **Database Initialization**
   - Look for `[TIMING] DB init START` and `DB init END`
   - Check the elapsed time - this can be slow if tables need creation

3. **Server Listen Callback**
   - Look for `[TIMING] Server listen END (callback)`
   - Time from `server.listen()` call to callback execution

4. **Cache Hydration**
   - Look for `[TIMING] Cache hydration START` and `END`
   - This runs synchronously in the listen callback
   - If this is slow, it blocks request handling

5. **Request Processing**
   - Look for `[TIMING] Request START` and `FINISH`
   - Compare times for different endpoints
   - Identify which endpoints are slow

### Common Issues

**Server accepts connections but doesn't respond:**
- Check if cache hydration is blocking
- Look for long gaps between "Server listen END" and "Server ready"
- Check if database queries are taking too long

**High CPU during startup:**
- Check database initialization time
- Look for synchronous operations in module load
- Check if cache hydration is processing too much data

**504/502 errors:**
- Check request processing times
- Look for requests that never finish
- Check for database locks or slow queries

## Log Locations

- **Monitoring log**: `logs/startup_monitor_TIMESTAMP.log`
- **Server logs**: `journalctl -u meshrank-server.service`
- **System logs**: `journalctl -xe`

## Example Output

```
[TIMING] Module load END: 234.56ms (total: 234.56ms)
[TIMING] Server listen START: START (total: 234.78ms)
[TIMING] Server listen END (callback): 12.34ms (total: 247.12ms)
(cache) Starting cache hydration...
[TIMING] Cache hydration START: START (total: 247.45ms)
[TIMING] Cache hydration - getDb() call: 5.67ms (total: 253.12ms)
[TIMING] Cache hydration END: 1234.56ms (total: 1482.01ms)
[TIMING] Server ready to accept requests: 1234.78ms (total: 1482.23ms)
(server) GET /api/test
[TIMING] Request START: GET /api/test: START (total: 1483.45ms)
[TIMING] Request FINISH: GET /api/test (200): 2.34ms (total: 1485.79ms)
```

## Notes

- The monitoring script runs until stopped (Ctrl+C)
- All logs are appended to the log file
- The script monitors multiple aspects simultaneously
- Server timing logs appear in both the monitoring log and journalctl
