#!/bin/bash
# Deep monitoring script for server startup diagnostics
# Run this BEFORE restarting the server, then restart the server

set -e

LOG_DIR="/root/Meshcore-Observer/logs"
MONITOR_LOG="$LOG_DIR/startup_monitor_$(date +%Y%m%d_%H%M%S).log"
PID_FILE="/tmp/meshrank_monitor.pid"

mkdir -p "$LOG_DIR"

echo "=== MeshRank Server Startup Monitor ===" | tee "$MONITOR_LOG"
echo "Started at: $(date)" | tee -a "$MONITOR_LOG"
echo "Log file: $MONITOR_LOG" | tee -a "$MONITOR_LOG"
echo "" | tee -a "$MONITOR_LOG"

# Function to log with timestamp
log() {
    echo "[$(date +%H:%M:%S.%3N)] $1" | tee -a "$MONITOR_LOG"
}

# Function to get process info
get_proc_info() {
    local pid=$1
    if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
        ps -p "$pid" -o pid,pcpu,pmem,rss,vsz,etime,state,cmd --no-headers | tee -a "$MONITOR_LOG"
    else
        echo "Process not found" | tee -a "$MONITOR_LOG"
    fi
}

# Function to monitor system resources
monitor_resources() {
    while true; do
        log "=== System Resources ==="
        echo "CPU: $(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1"%"}')" | tee -a "$MONITOR_LOG"
        echo "Memory: $(free -h | awk '/^Mem:/ {print $3 "/" $2}')" | tee -a "$MONITOR_LOG"
        echo "Load: $(uptime | awk -F'load average:' '{print $2}')" | tee -a "$MONITOR_LOG"
        
        # Check for node processes
        local node_pids=$(pgrep -f "node.*server.js" || echo "")
        if [ -n "$node_pids" ]; then
            log "=== Node Process Info ==="
            for pid in $node_pids; do
                get_proc_info "$pid"
            done
        fi
        
        # Check port 5199
        local port_info=$(ss -lntp | grep 5199 || echo "Port 5199 not listening")
        log "Port 5199: $port_info"
        
        # Check systemd service status
        log "=== Service Status ==="
        systemctl status meshrank-server.service --no-pager -l | head -10 | tee -a "$MONITOR_LOG"
        
        echo "" | tee -a "$MONITOR_LOG"
        sleep 2
    done
}

# Function to monitor journal logs in real-time
monitor_journal() {
    log "=== Starting Journal Monitor ==="
    journalctl -u meshrank-server.service --since "now" --follow --no-pager | while IFS= read -r line; do
        echo "[JOURNAL] $line" | tee -a "$MONITOR_LOG"
    done
}

# Function to test API endpoints
test_endpoints() {
    local base_url="http://127.0.0.1:5199"
    local endpoints=("/api/test" "/api/messages" "/api/repeater-rank-summary" "/api/rotm")
    
    while true; do
        log "=== Testing Endpoints ==="
        for endpoint in "${endpoints[@]}"; do
            local start=$(date +%s%N)
            local result=$(timeout 3 curl -s -w "\nHTTP_CODE:%{http_code}\nTIME:%{time_total}" "$base_url$endpoint" 2>&1 || echo "TIMEOUT")
            local end=$(date +%s%N)
            local duration=$(( (end - start) / 1000000 ))
            
            if echo "$result" | grep -q "TIMEOUT"; then
                log "$endpoint: TIMEOUT (>3s)"
            else
                local http_code=$(echo "$result" | grep "HTTP_CODE" | cut -d: -f2)
                local time_total=$(echo "$result" | grep "TIME" | cut -d: -f2)
                log "$endpoint: HTTP $http_code in ${time_total}s (curl measured: ${duration}ms)"
            fi
        done
        echo "" | tee -a "$MONITOR_LOG"
        sleep 5
    done
}

# Function to monitor database locks
monitor_db() {
    local db_path="/root/Meshcore-Observer/data/meshrank.db"
    if [ -f "$db_path" ]; then
        while true; do
            log "=== Database Status ==="
            # Check for locks
            if command -v fuser > /dev/null 2>&1; then
                local locks=$(fuser "$db_path" 2>&1 || echo "No locks")
                log "DB locks: $locks"
            fi
            
            # Check DB size
            local size=$(du -h "$db_path" | cut -f1)
            log "DB size: $size"
            
            # Check WAL files
            if [ -f "${db_path}-wal" ]; then
                local wal_size=$(du -h "${db_path}-wal" | cut -f1)
                log "WAL size: $wal_size"
            fi
            
            echo "" | tee -a "$MONITOR_LOG"
            sleep 5
        done
    fi
}

# Trap to cleanup on exit
cleanup() {
    log "=== Monitor Stopping ==="
    [ -f "$PID_FILE" ] && rm -f "$PID_FILE"
    pkill -P $$ 2>/dev/null || true
    exit 0
}

trap cleanup EXIT INT TERM

# Start monitoring processes in background
log "Starting resource monitor..."
monitor_resources &
RESOURCE_PID=$!

log "Starting journal monitor..."
monitor_journal &
JOURNAL_PID=$!

log "Starting endpoint tester..."
test_endpoints &
ENDPOINT_PID=$!

log "Starting database monitor..."
monitor_db &
DB_PID=$!

# Save PIDs
echo "$RESOURCE_PID" > "$PID_FILE"
echo "$JOURNAL_PID" >> "$PID_FILE"
echo "$ENDPOINT_PID" >> "$PID_FILE"
echo "$DB_PID" >> "$PID_FILE"

log "=== All Monitors Started ==="
log "Resource Monitor PID: $RESOURCE_PID"
log "Journal Monitor PID: $JOURNAL_PID"
log "Endpoint Tester PID: $ENDPOINT_PID"
log "Database Monitor PID: $DB_PID"
log ""
log "NOW RESTART THE SERVER: sudo systemctl restart meshrank-server.service"
log "Monitor will continue running. Press Ctrl+C to stop."
log ""

# Wait for all background processes
wait
