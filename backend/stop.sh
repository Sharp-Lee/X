#!/bin/bash
#
# MSR Retest Capture 系统停止脚本
#

echo "停止 MSR Retest Capture 系统..."

# 停止 uvicorn 进程
pkill -f "uvicorn app.main" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ 服务已停止"
else
    echo "! 服务未在运行"
fi
