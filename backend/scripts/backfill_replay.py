"""Backfill missing klines and replay through signal generator.

Usage: .venv/bin/python -m scripts.backfill_replay
"""

import asyncio
import logging
from datetime import datetime, timezone

from app.config import get_settings
from app.storage import init_database
from app.storage.kline_repo import KlineRepository
from app.storage.signal_repo import SignalRepository
from app.storage import cache
from app.clients.binance_rest import BinanceRestClient
from app.trading_config import load_trading_config
from core.kline_aggregator import KlineAggregator
from core.signal_generator import SignalGenerator
from core.models import Kline, KlineBuffer, StrategyConfig, kline_to_fast, fast_to_kline
from core.atr_tracker import AtrPercentileTracker
from core.indicators import atr as compute_atr
from app.storage import streak_cache
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    settings = get_settings()
    symbols = settings.symbols
    timeframes = settings.timeframes

    # Gap boundaries (from DB analysis)
    gap_start = datetime(2026, 2, 11, 16, 9, tzinfo=timezone.utc)
    gap_end = datetime(2026, 2, 12, 8, 53, tzinfo=timezone.utc)

    logger.info(f"Backfilling gap: {gap_start} to {gap_end}")
    logger.info(f"Duration: {gap_end - gap_start}")

    # Initialize
    await init_database()
    await cache.init_cache()

    kline_repo = KlineRepository()
    signal_repo = SignalRepository()
    rest_client = BinanceRestClient(
        api_key=settings.binance_api_key,
        api_secret=settings.binance_api_secret,
    )

    # Step 1: Download missing 1m klines
    logger.info("=" * 60)
    logger.info("STEP 1: Download missing 1m klines from Binance")
    logger.info("=" * 60)

    for symbol in symbols:
        klines = await rest_client.get_all_klines(
            symbol=symbol,
            interval="1m",
            start_time=gap_start,
            end_time=gap_end,
        )
        if klines:
            await kline_repo.save_batch(klines)
            logger.info(f"  {symbol}: downloaded {len(klines)} 1m klines")
        else:
            logger.warning(f"  {symbol}: no klines returned!")

    # Step 2: Set up signal generator for replay
    logger.info("=" * 60)
    logger.info("STEP 2: Setup signal generator")
    logger.info("=" * 60)

    trading_config = load_trading_config()
    portfolio = trading_config.get_signal_filters()
    signal_filters = {f.key: f for f in portfolio}

    # ATR tracker - warm up from DB klines (same as main.py warmup)
    atr_tracker = AtrPercentileTracker()
    portfolio_keys = [(f.symbol, f.timeframe) for f in portfolio]
    for sym, tf in portfolio_keys:
        klines = await kline_repo.get_latest(sym, tf, limit=1500)
        if len(klines) < settings.atr_period + 1:
            logger.warning(f"  ATR warmup: {sym} {tf} only {len(klines)} klines, skipping")
            continue
        highs = [k.high for k in klines]
        lows = [k.low for k in klines]
        closes = [k.close for k in klines]
        atr_series = compute_atr(highs, lows, closes, period=settings.atr_period)
        atr_values = [float(v) for v in atr_series if not v.is_nan() and v > 0]
        if atr_values:
            atr_tracker.bulk_load(sym, tf, atr_values)

    new_signals = []

    async def on_signal_save(signal):
        await signal_repo.save(signal)
        new_signals.append(signal)
        logger.info(f"  NEW SIGNAL: {signal.symbol} {signal.timeframe} {signal.direction.name} @ {signal.entry_price}")
        return signal

    config = StrategyConfig(
        ema_period=settings.ema_period,
        fib_period=settings.fib_period,
        atr_period=settings.atr_period,
        tp_atr_mult=Decimal(str(settings.tp_atr_mult)),
        sl_atr_mult=Decimal(str(settings.sl_atr_mult)),
        max_risk_percent=Decimal(str(settings.max_risk_percent)),
    )

    signal_generator = SignalGenerator(
        config=config,
        save_signal=on_signal_save,
        save_streak=streak_cache.save_streak,
        load_streaks=streak_cache.load_all_streaks,
        load_active_signals=signal_repo.get_active,
        filters=signal_filters,
        atr_tracker=atr_tracker,
    )
    await signal_generator.init()

    # Step 3: Set up aggregator and buffers
    logger.info("=" * 60)
    logger.info("STEP 3: Prepare buffers and aggregator")
    logger.info("=" * 60)

    aggregated_timeframes = [tf for tf in timeframes if tf != "1m"]
    aggregator = KlineAggregator(target_timeframes=aggregated_timeframes)

    kline_buffers: dict[str, KlineBuffer] = {}

    # Restore buffers from before the gap
    for symbol in symbols:
        for tf in timeframes:
            key = f"{symbol}_{tf}"
            klines = await kline_repo.get_before(
                symbol=symbol,
                timeframe=tf,
                before_time=gap_start,
                limit=200,
            )
            if klines:
                buf = KlineBuffer(symbol=symbol, timeframe=tf, max_size=200)
                for k in klines:
                    buf.add(k)
                kline_buffers[key] = buf
                logger.info(f"  {key}: restored {len(klines)} klines")

    # Prefill aggregator with 1m klines before gap
    for symbol in symbols:
        key = f"{symbol}_1m"
        buf = kline_buffers.get(key)
        if buf:
            fast_klines = [kline_to_fast(k) for k in buf.klines]
            aggregator.prefill_from_history(symbol, fast_klines)

    # Step 4: Replay 1m klines through the gap
    logger.info("=" * 60)
    logger.info("STEP 4: Replay klines through signal generator")
    logger.info("=" * 60)

    total_replayed = 0

    for symbol in symbols:
        # Get 1m klines in the gap period
        klines_1m = await kline_repo.get_range(
            symbol=symbol,
            timeframe="1m",
            start=gap_start,
            end=gap_end,
        )

        if not klines_1m:
            logger.info(f"  {symbol}: no klines to replay")
            continue

        logger.info(f"  {symbol}: replaying {len(klines_1m)} 1m klines...")

        for kline in klines_1m:
            # Update 1m buffer
            key_1m = f"{symbol}_1m"
            if key_1m not in kline_buffers:
                kline_buffers[key_1m] = KlineBuffer(symbol=symbol, timeframe="1m", max_size=200)
            kline_buffers[key_1m].add(kline)

            # Process through signal generator (1m)
            if kline.is_closed:
                await signal_generator.process_kline(kline, kline_buffers[key_1m])

            # Aggregate to higher timeframes
            fast_kline = kline_to_fast(kline)
            aggregated = await aggregator.add_1m_kline(fast_kline)

            for agg_fk in aggregated:
                # Convert back to Kline for signal processing
                agg_kline = fast_to_kline(agg_fk)

                # Save aggregated kline to DB
                await kline_repo.save(agg_kline)

                # Update buffer
                agg_key = f"{agg_kline.symbol}_{agg_kline.timeframe}"
                if agg_key not in kline_buffers:
                    kline_buffers[agg_key] = KlineBuffer(symbol=agg_kline.symbol, timeframe=agg_kline.timeframe, max_size=200)
                kline_buffers[agg_key].add(agg_kline)

                # Process through signal generator
                if agg_kline.is_closed:
                    await signal_generator.process_kline(agg_kline, kline_buffers[agg_key])

        total_replayed += len(klines_1m)
        logger.info(f"  {symbol}: done")

    # Summary
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total 1m klines replayed: {total_replayed}")
    logger.info(f"New signals generated: {len(new_signals)}")
    for sig in new_signals:
        logger.info(f"  {sig.symbol} {sig.timeframe} {sig.direction.name} @ {sig.entry_price} ({sig.signal_time})")

    await cache.close_cache()


if __name__ == "__main__":
    asyncio.run(main())
