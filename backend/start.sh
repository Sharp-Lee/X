#!/bin/bash
#
# MSR Retest Capture 系统启动脚本
#
# 使用方式:
#   ./start.sh              # 正常启动
#   ./start.sh --clean      # 清空数据后启动
#   ./start.sh --check      # 只检查环境
#   ./start.sh --port 8080  # 指定端口
#

# 切换到脚本所在目录
cd "$(dirname "$0")"

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "错误: 虚拟环境不存在"
    echo "请先运行: python -m venv .venv && source .venv/bin/activate && pip install -e ."
    exit 1
fi

# 激活虚拟环境
source .venv/bin/activate

# 运行启动脚本
python scripts/start.py "$@"
