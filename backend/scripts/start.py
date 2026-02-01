#!/usr/bin/env python3
"""
MSR Retest Capture 系统启动脚本
==============================

功能:
  - 检查环境依赖
  - 清空数据（可选）
  - 启动后端服务

使用方式:
    python scripts/start.py              # 正常启动
    python scripts/start.py --clean      # 清空数据后启动
    python scripts/start.py --check      # 只检查环境，不启动
    python scripts/start.py --port 8080  # 指定端口
"""

import argparse
import asyncio
import os
import sys
import subprocess
import signal
import warnings

# 忽略第三方库的 deprecation 警告
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 确保能导入 app 模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def print_banner():
    """打印启动横幅"""
    print()
    print("=" * 60)
    print("   MSR Retest Capture Trading System")
    print("   再测捕捉交易信号系统")
    print("=" * 60)
    print()


def check_python_version():
    """检查 Python 版本"""
    print("[检查] Python 版本...", end=" ")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 11:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"✗ Python {version.major}.{version.minor} (需要 >= 3.11)")
        return False


def check_virtual_env():
    """检查是否在虚拟环境中"""
    print("[检查] 虚拟环境...", end=" ")
    if sys.prefix != sys.base_prefix:
        print(f"✓ {sys.prefix}")
        return True
    else:
        print("✗ 未激活虚拟环境")
        print("  请运行: source .venv/bin/activate")
        return False


def check_dependencies():
    """检查关键依赖"""
    print("[检查] 核心依赖...")

    deps = [
        ("fastapi", "FastAPI"),
        ("uvicorn", "Uvicorn"),
        ("sqlalchemy", "SQLAlchemy"),
        ("redis", "Redis"),
        ("picows", "PicoWS"),
        ("orjson", "orjson"),
    ]

    all_ok = True
    for module, name in deps:
        try:
            __import__(module)
            print(f"  ✓ {name}")
        except ImportError:
            print(f"  ✗ {name} (pip install {module})")
            all_ok = False

    # 检查 uvloop (可选)
    try:
        import uvloop
        print(f"  ✓ uvloop (高性能事件循环)")
    except ImportError:
        print(f"  ! uvloop (可选，使用默认事件循环)")

    # 检查 TA-Lib (可选但推荐)
    try:
        import talib
        print(f"  ✓ TA-Lib (高性能指标计算)")
    except ImportError:
        print(f"  ! TA-Lib (可选，使用手写指标)")

    return all_ok


async def check_database():
    """检查数据库连接"""
    print("[检查] 数据库连接...", end=" ")
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy import text
        from app.config import get_settings

        settings = get_settings()
        db_url = settings.database_url.replace('postgresql://', 'postgresql+asyncpg://')
        engine = create_async_engine(db_url)

        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            result.fetchone()

        await engine.dispose()
        print("✓ PostgreSQL 连接正常")
        return True
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        return False


async def check_redis():
    """检查 Redis 连接"""
    print("[检查] Redis 连接...", end=" ")
    try:
        import redis.asyncio as redis
        from app.config import get_settings

        settings = get_settings()
        client = redis.from_url(settings.redis_url)
        await client.ping()
        await client.close()
        print("✓ Redis 连接正常")
        return True
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        print("  Redis 是可选的，系统会使用内存缓存")
        return True  # Redis 是可选的


async def check_binance():
    """检查 Binance API 连接"""
    print("[检查] Binance API...", end=" ")
    try:
        from app.clients import BinanceRestClient

        client = BinanceRestClient()
        # 获取服务器时间来测试连接
        klines = await client.get_klines(symbol="BTCUSDT", interval="1m", limit=1)
        await client.close()

        if klines:
            print(f"✓ 连接正常 (BTCUSDT: {klines[0].close})")
            return True
        else:
            print("✗ 无法获取数据")
            return False
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        return False


async def clear_database():
    """清空数据库"""
    print()
    print("[清空数据]")

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    from app.config import get_settings

    settings = get_settings()
    db_url = settings.database_url.replace('postgresql://', 'postgresql+asyncpg://')
    engine = create_async_engine(db_url)

    async with engine.begin() as conn:
        # 清空所有表
        result = await conn.execute(text('DELETE FROM signals'))
        print(f"  删除了 {result.rowcount} 条信号记录")

        result = await conn.execute(text('DELETE FROM klines'))
        print(f"  删除了 {result.rowcount} 条 K线记录")

        result = await conn.execute(text('DELETE FROM aggtrades'))
        print(f"  删除了 {result.rowcount} 条交易记录")

    await engine.dispose()
    print("  数据已清空!")


async def run_checks():
    """运行所有检查"""
    print()
    print("-" * 60)
    print("环境检查")
    print("-" * 60)

    results = []

    results.append(check_python_version())
    results.append(check_virtual_env())
    results.append(check_dependencies())
    results.append(await check_database())
    results.append(await check_redis())
    results.append(await check_binance())

    print()
    print("-" * 60)

    if all(results):
        print("✓ 所有检查通过!")
        return True
    else:
        print("✗ 部分检查失败，请修复后重试")
        return False


def start_server(host: str, port: int):
    """启动服务器"""
    from app.config import get_settings
    settings = get_settings()

    print()
    print("-" * 60)
    print("  启动服务")
    print("-" * 60)
    print(f"  交易对: {', '.join(settings.symbols)}")
    print(f"  周期:   {', '.join(settings.timeframes)}")
    print(f"  地址:   http://{host}:{port}")
    print(f"  API:    http://{host}:{port}/docs")
    print("-" * 60)
    print()

    # 使用 uvicorn 启动
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        log_level="warning",  # 只显示警告和错误
        reload=False,
    )


def main():
    parser = argparse.ArgumentParser(
        description="MSR Retest Capture 系统启动脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/start.py              # 正常启动
  python scripts/start.py --clean      # 清空数据后启动
  python scripts/start.py --check      # 只检查环境
  python scripts/start.py --port 8080  # 指定端口
        """
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="只检查环境，不启动服务"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="启动前清空所有数据"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="监听地址 (默认: 0.0.0.0)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="监听端口 (默认: 8000)"
    )

    args = parser.parse_args()

    print_banner()

    # 运行环境检查
    if not asyncio.run(run_checks()):
        sys.exit(1)

    # 只检查模式
    if args.check:
        print()
        print("检查完成，退出。")
        return

    # 清空数据
    if args.clean:
        asyncio.run(clear_database())

    # 启动服务
    start_server(args.host, args.port)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n服务已停止。")
        sys.exit(0)
