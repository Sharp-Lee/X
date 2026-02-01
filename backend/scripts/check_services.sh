#!/bin/bash
#
# MSR Retest Capture 系统服务状态检查脚本
# 用法: ./scripts/check_services.sh [--kill]
#

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "=============================================="
echo "   MSR Retest Capture 服务状态检查"
echo "=============================================="
echo ""

# 定义要检查的进程模式
PATTERNS=(
    "uvicorn.*app"
    "python.*start.py"
    "python.*main.py"
    "node.*vite"
    "npm.*dev"
)

# 检查函数
check_service() {
    local pattern=$1
    local name=$2
    local pids=$(pgrep -f "$pattern" 2>/dev/null || true)

    if [ -n "$pids" ]; then
        echo -e "${GREEN}[运行中]${NC} $name"
        ps aux | grep -E "$pattern" | grep -v grep | while read line; do
            pid=$(echo "$line" | awk '{print $2}')
            cpu=$(echo "$line" | awk '{print $3}')
            mem=$(echo "$line" | awk '{print $4}')
            cmd=$(echo "$line" | awk '{for(i=11;i<=NF;i++) printf $i" "; print ""}')
            echo "         PID: $pid | CPU: ${cpu}% | MEM: ${mem}%"
            echo "         CMD: ${cmd:0:60}..."
        done
        return 0
    else
        echo -e "${YELLOW}[未运行]${NC} $name"
        return 1
    fi
}

# 检查各服务
echo "[后端服务]"
echo "-------------------------------------------"
check_service "uvicorn.*app" "Uvicorn (FastAPI)" || true
check_service "python.*start.py" "Start Script" || true
check_service "python.*main.py" "Main Script" || true

echo ""
echo "[前端服务]"
echo "-------------------------------------------"
check_service "node.*vite" "Vite Dev Server" || true
check_service "npm.*dev" "NPM Dev Server" || true

echo ""
echo "[数据库服务]"
echo "-------------------------------------------"

# 检查 PostgreSQL
if pgrep -x "postgres" > /dev/null 2>&1; then
    echo -e "${GREEN}[运行中]${NC} PostgreSQL"
else
    # macOS Homebrew 可能使用不同的进程名
    if brew services list 2>/dev/null | grep -q "postgresql.*started"; then
        echo -e "${GREEN}[运行中]${NC} PostgreSQL (Homebrew)"
    else
        echo -e "${YELLOW}[未运行]${NC} PostgreSQL"
    fi
fi

# 检查 Redis
if pgrep -x "redis-server" > /dev/null 2>&1; then
    echo -e "${GREEN}[运行中]${NC} Redis"
else
    if brew services list 2>/dev/null | grep -q "redis.*started"; then
        echo -e "${GREEN}[运行中]${NC} Redis (Homebrew)"
    else
        echo -e "${YELLOW}[未运行]${NC} Redis"
    fi
fi

echo ""

# 如果传入 --kill 参数，停止所有服务
if [ "$1" == "--kill" ]; then
    echo "=============================================="
    echo "   停止所有服务..."
    echo "=============================================="
    echo ""

    for pattern in "${PATTERNS[@]}"; do
        pids=$(pgrep -f "$pattern" 2>/dev/null || true)
        if [ -n "$pids" ]; then
            for pid in $pids; do
                echo -e "${RED}[停止]${NC} PID $pid ($pattern)"
                kill -9 $pid 2>/dev/null || true
            done
        fi
    done

    echo ""
    echo -e "${GREEN}所有应用服务已停止${NC}"
    echo "(数据库服务需手动停止: brew services stop postgresql redis)"
    echo ""
fi

echo "=============================================="
echo "用法: ./scripts/check_services.sh [--kill]"
echo "=============================================="
echo ""
