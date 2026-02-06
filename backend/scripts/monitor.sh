#!/bin/bash
# System monitoring script - runs for 30 minutes

LOG_FILE="/tmp/monitor_results.log"
DURATION=1800  # 30 minutes in seconds
INTERVAL=60    # Check every 60 seconds
START_TIME=$(date +%s)

echo "========================================" > $LOG_FILE
echo "System Monitoring Started: $(date)" >> $LOG_FILE
echo "Duration: 30 minutes" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

check_count=0
error_count=0
warning_count=0

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [ $ELAPSED -ge $DURATION ]; then
        break
    fi

    check_count=$((check_count + 1))
    REMAINING=$((DURATION - ELAPSED))

    echo "" >> $LOG_FILE
    echo "--- Check #$check_count at $(date '+%H:%M:%S') (${REMAINING}s remaining) ---" >> $LOG_FILE

    # 1. Check backend health
    HEALTH=$(curl -s -w "%{http_code}" -o /tmp/health_response.txt http://localhost:8000/health 2>/dev/null)
    if [ "$HEALTH" = "200" ]; then
        echo "[OK] Backend health: $(cat /tmp/health_response.txt)" >> $LOG_FILE
    else
        echo "[ERROR] Backend health check failed: HTTP $HEALTH" >> $LOG_FILE
        error_count=$((error_count + 1))
    fi

    # 2. Check API status
    STATUS=$(curl -s http://localhost:8000/api/status 2>/dev/null)
    if [ -n "$STATUS" ]; then
        ACTIVE=$(echo $STATUS | grep -o '"active_signals":[0-9]*' | cut -d: -f2)
        echo "[OK] API status: active_signals=$ACTIVE" >> $LOG_FILE
    else
        echo "[ERROR] API status failed" >> $LOG_FILE
        error_count=$((error_count + 1))
    fi

    # 3. Check API stats
    STATS=$(curl -s http://localhost:8000/api/stats 2>/dev/null)
    if [ -n "$STATS" ]; then
        TOTAL=$(echo $STATS | grep -o '"total_signals":[0-9]*' | cut -d: -f2)
        WINS=$(echo $STATS | grep -o '"wins":[0-9]*' | cut -d: -f2)
        LOSSES=$(echo $STATS | grep -o '"losses":[0-9]*' | cut -d: -f2)
        WINRATE=$(echo $STATS | grep -o '"win_rate":[0-9.]*' | cut -d: -f2)
        echo "[OK] Stats: total=$TOTAL wins=$WINS losses=$LOSSES win_rate=$WINRATE%" >> $LOG_FILE
    else
        echo "[ERROR] API stats failed" >> $LOG_FILE
        error_count=$((error_count + 1))
    fi

    # 4. Check frontend
    FRONTEND=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5173 2>/dev/null)
    if [ "$FRONTEND" = "200" ]; then
        echo "[OK] Frontend accessible" >> $LOG_FILE
    else
        echo "[ERROR] Frontend not accessible: HTTP $FRONTEND" >> $LOG_FILE
        error_count=$((error_count + 1))
    fi

    # 5. Check for errors in backend log (last minute)
    if [ -f /tmp/backend.log ]; then
        REDIS_ERRORS=$(tail -100 /tmp/backend.log | grep -c "Redis.*error\|Too many connections" 2>/dev/null || echo "0")
        DB_ERRORS=$(tail -100 /tmp/backend.log | grep -c "sqlalchemy.*Error\|database.*error" 2>/dev/null || echo "0")
        WS_ERRORS=$(tail -100 /tmp/backend.log | grep -c "WebSocket.*error\|picows.*error" 2>/dev/null || echo "0")

        if [ "$REDIS_ERRORS" -gt 0 ]; then
            echo "[WARNING] Redis errors in log: $REDIS_ERRORS" >> $LOG_FILE
            warning_count=$((warning_count + 1))
        fi
        if [ "$DB_ERRORS" -gt 0 ]; then
            echo "[WARNING] Database errors in log: $DB_ERRORS" >> $LOG_FILE
            warning_count=$((warning_count + 1))
        fi
        if [ "$WS_ERRORS" -gt 0 ]; then
            echo "[WARNING] WebSocket errors in log: $WS_ERRORS" >> $LOG_FILE
            warning_count=$((warning_count + 1))
        fi

        # Check for new signals
        NEW_SIGNALS=$(tail -100 /tmp/backend.log | grep -c "signal:" 2>/dev/null || echo "0")
        TP_HITS=$(tail -100 /tmp/backend.log | grep -c "hit TP" 2>/dev/null || echo "0")
        SL_HITS=$(tail -100 /tmp/backend.log | grep -c "hit SL" 2>/dev/null || echo "0")

        if [ "$NEW_SIGNALS" -gt 0 ] || [ "$TP_HITS" -gt 0 ] || [ "$SL_HITS" -gt 0 ]; then
            echo "[INFO] Recent activity: signals=$NEW_SIGNALS TP=$TP_HITS SL=$SL_HITS" >> $LOG_FILE
        fi
    fi

    # 6. Check processes are still running
    BACKEND_PID=$(pgrep -f "uvicorn app.main:app" 2>/dev/null)
    FRONTEND_PID=$(pgrep -f "node.*vite" 2>/dev/null)

    if [ -z "$BACKEND_PID" ]; then
        echo "[ERROR] Backend process not running!" >> $LOG_FILE
        error_count=$((error_count + 1))
    fi
    if [ -z "$FRONTEND_PID" ]; then
        echo "[ERROR] Frontend process not running!" >> $LOG_FILE
        error_count=$((error_count + 1))
    fi

    sleep $INTERVAL
done

echo "" >> $LOG_FILE
echo "========================================" >> $LOG_FILE
echo "Monitoring Complete: $(date)" >> $LOG_FILE
echo "Total checks: $check_count" >> $LOG_FILE
echo "Errors: $error_count" >> $LOG_FILE
echo "Warnings: $warning_count" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

# Final status check
echo "" >> $LOG_FILE
echo "=== FINAL STATUS ===" >> $LOG_FILE
curl -s http://localhost:8000/api/status >> $LOG_FILE 2>/dev/null
echo "" >> $LOG_FILE
curl -s http://localhost:8000/api/stats >> $LOG_FILE 2>/dev/null
echo "" >> $LOG_FILE
