"""Tests for backtest OutcomeTracker (kline-based outcome determination)."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal

from core.models.kline import Kline
from core.models.signal import Direction, Outcome, SignalRecord
from backtest.outcome import OutcomeTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_kline(
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    timestamp: datetime | None = None,
    open: str = "50000",
    high: str = "50100",
    low: str = "49900",
    close: str = "50050",
    volume: str = "100",
) -> Kline:
    """Build a Kline with sensible defaults."""
    return Kline(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=timestamp or datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        open=Decimal(open),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
    )


def make_long_signal(
    symbol: str = "BTCUSDT",
    entry: str = "50000",
    tp: str = "50500",
    sl: str = "49000",
    timeframe: str = "5m",
    signal_time: datetime | None = None,
) -> SignalRecord:
    """Build a LONG SignalRecord with sensible defaults."""
    return SignalRecord(
        symbol=symbol,
        timeframe=timeframe,
        signal_time=signal_time or datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        direction=Direction.LONG,
        entry_price=Decimal(entry),
        tp_price=Decimal(tp),
        sl_price=Decimal(sl),
    )


def make_short_signal(
    symbol: str = "BTCUSDT",
    entry: str = "50000",
    tp: str = "49500",
    sl: str = "51000",
    timeframe: str = "5m",
    signal_time: datetime | None = None,
) -> SignalRecord:
    """Build a SHORT SignalRecord with sensible defaults."""
    return SignalRecord(
        symbol=symbol,
        timeframe=timeframe,
        signal_time=signal_time or datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        direction=Direction.SHORT,
        entry_price=Decimal(entry),
        tp_price=Decimal(tp),
        sl_price=Decimal(sl),
    )


# ---------------------------------------------------------------------------
# TIER 1 -- Pessimistic rule: both TP and SL hit on same kline => SL
# ---------------------------------------------------------------------------

class TestPessimisticRule:
    """When a single kline touches both TP and SL, outcome must be SL."""

    async def test_long_both_hit_yields_sl(self):
        """LONG: kline high >= tp AND low <= sl => SL (pessimistic)."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        # Kline spans both targets
        kline = make_kline(high="50600", low="48900")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_price == Decimal("49000")
        assert tracker.active_count == 0
        assert tracker.resolved_count == 1

    async def test_short_both_hit_yields_sl(self):
        """SHORT: kline low <= tp AND high >= sl => SL (pessimistic)."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        # Kline spans both targets
        kline = make_kline(high="51100", low="49400")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_price == Decimal("51000")
        assert tracker.active_count == 0
        assert tracker.resolved_count == 1

    async def test_long_both_hit_exact_boundary(self):
        """LONG: kline high == tp AND low == sl (exact) => SL."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50500", low="49000")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL

    async def test_short_both_hit_exact_boundary(self):
        """SHORT: kline low == tp AND high == sl (exact) => SL."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        kline = make_kline(high="51000", low="49500")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL


# ---------------------------------------------------------------------------
# TIER 1 -- LONG outcome determination
# ---------------------------------------------------------------------------

class TestLongOutcomes:
    """Outcome checks for LONG signals."""

    async def test_tp_hit(self):
        """LONG: kline.high >= tp_price => TP."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.TP
        assert signal.outcome_price == Decimal("50500")
        assert tracker.active_count == 0

    async def test_sl_hit(self):
        """LONG: kline.low <= sl_price => SL."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="48900")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_price == Decimal("49000")
        assert tracker.active_count == 0

    async def test_no_hit(self):
        """LONG: kline stays within range => no outcome, stays active."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="49200")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.ACTIVE
        assert tracker.active_count == 1
        assert tracker.resolved_count == 0

    async def test_tp_hit_exact_boundary(self):
        """LONG: kline.high == tp_price (exact) => TP."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50500", low="49500")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.TP
        assert signal.outcome_price == Decimal("50500")

    async def test_sl_hit_exact_boundary(self):
        """LONG: kline.low == sl_price (exact) => SL."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="49000")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_price == Decimal("49000")


# ---------------------------------------------------------------------------
# TIER 1 -- SHORT outcome determination
# ---------------------------------------------------------------------------

class TestShortOutcomes:
    """Outcome checks for SHORT signals."""

    async def test_tp_hit(self):
        """SHORT: kline.low <= tp_price => TP."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="49400")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.TP
        assert signal.outcome_price == Decimal("49500")
        assert tracker.active_count == 0

    async def test_sl_hit(self):
        """SHORT: kline.high >= sl_price => SL."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        kline = make_kline(high="51100", low="49800")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_price == Decimal("51000")
        assert tracker.active_count == 0

    async def test_no_hit(self):
        """SHORT: kline stays within range => no outcome, stays active."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        kline = make_kline(high="50800", low="49600")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.ACTIVE
        assert tracker.active_count == 1
        assert tracker.resolved_count == 0

    async def test_tp_hit_exact_boundary(self):
        """SHORT: kline.low == tp_price (exact) => TP."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="49500")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.TP
        assert signal.outcome_price == Decimal("49500")

    async def test_sl_hit_exact_boundary(self):
        """SHORT: kline.high == sl_price (exact) => SL."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        kline = make_kline(high="51000", low="49800")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.SL
        assert signal.outcome_price == Decimal("51000")


# ---------------------------------------------------------------------------
# MAE / MFE tracking
# ---------------------------------------------------------------------------

class TestMAEMFE:
    """MAE and MFE ratio updates through kline high/low."""

    async def test_long_mae_mfe(self):
        """LONG: low tracks adverse excursion, high tracks favorable."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="51000", sl="49000")
        # risk = 50000 - 49000 = 1000
        tracker.add_signal(signal)

        # Kline: adverse move to 49500, favorable move to 50300
        kline = make_kline(high="50300", low="49500")
        await tracker.check_kline(kline)

        # adverse = 50000 - 49500 = 500 => mae_ratio = 500 / 1000 = 0.5
        assert signal.mae_ratio == Decimal("0.5")
        # favorable = 50300 - 50000 = 300 => mfe_ratio = 300 / 1000 = 0.3
        assert signal.mfe_ratio == Decimal("0.3")

    async def test_short_mae_mfe(self):
        """SHORT: high tracks adverse excursion, low tracks favorable."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49000", sl="51000")
        # risk = 51000 - 50000 = 1000
        tracker.add_signal(signal)

        # Kline: adverse move to 50400, favorable move to 49700
        kline = make_kline(high="50400", low="49700")
        await tracker.check_kline(kline)

        # adverse = 50400 - 50000 = 400 => mae_ratio = 400 / 1000 = 0.4
        assert signal.mae_ratio == Decimal("0.4")
        # favorable = 50000 - 49700 = 300 => mfe_ratio = 300 / 1000 = 0.3
        assert signal.mfe_ratio == Decimal("0.3")

    async def test_mae_mfe_accumulates_across_klines(self):
        """MAE/MFE should accumulate the worst/best across multiple klines."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="52000", sl="48000")
        # risk = 50000 - 48000 = 2000
        tracker.add_signal(signal)

        # First kline: small move
        k1 = make_kline(
            high="50200", low="49800",
            timestamp=datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        )
        await tracker.check_kline(k1)
        assert signal.mae_ratio == Decimal("0.1")   # 200/2000
        assert signal.mfe_ratio == Decimal("0.1")   # 200/2000

        # Second kline: bigger adverse move
        k2 = make_kline(
            high="50100", low="49500",
            timestamp=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        await tracker.check_kline(k2)
        assert signal.mae_ratio == Decimal("0.25")  # 500/2000
        assert signal.mfe_ratio == Decimal("0.1")   # unchanged

        # Third kline: bigger favorable move
        k3 = make_kline(
            high="50800", low="49900",
            timestamp=datetime(2025, 6, 1, 0, 2, tzinfo=timezone.utc),
        )
        await tracker.check_kline(k3)
        assert signal.mae_ratio == Decimal("0.25")  # unchanged
        assert signal.mfe_ratio == Decimal("0.4")   # 800/2000

    async def test_mae_mfe_not_updated_after_resolution(self):
        """Once a signal is resolved (TP/SL), MAE/MFE should not change.

        This is implicitly tested because resolved signals are removed from
        the active list, so check_kline won't process them at all.
        """
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        # Resolve: TP hit
        tp_kline = make_kline(
            high="50600", low="49500",
            timestamp=datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        )
        await tracker.check_kline(tp_kline)
        assert signal.outcome == Outcome.TP

        mae_after_tp = signal.mae_ratio
        mfe_after_tp = signal.mfe_ratio

        # Feed another kline - signal should not be checked
        further_kline = make_kline(
            high="51000", low="48000",
            timestamp=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        await tracker.check_kline(further_kline)

        assert signal.mae_ratio == mae_after_tp
        assert signal.mfe_ratio == mfe_after_tp


# ---------------------------------------------------------------------------
# Outcome immutability / removal from active list
# ---------------------------------------------------------------------------

class TestOutcomeImmutability:
    """Once resolved, a signal is removed and never checked again."""

    async def test_resolved_signal_removed_from_active(self):
        """Signal is removed from _active_signals after TP."""
        tracker = OutcomeTracker()
        signal = make_long_signal()
        tracker.add_signal(signal)
        assert tracker.active_count == 1

        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)

        assert tracker.active_count == 0
        assert tracker.resolved_count == 1

    async def test_resolved_signal_not_double_counted(self):
        """Feeding more klines after resolution does not increment resolved_count."""
        tracker = OutcomeTracker()
        signal = make_long_signal()
        tracker.add_signal(signal)

        kline1 = make_kline(
            high="50600", low="49500",
            timestamp=datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        )
        await tracker.check_kline(kline1)
        assert tracker.resolved_count == 1

        kline2 = make_kline(
            high="50700", low="49400",
            timestamp=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        await tracker.check_kline(kline2)
        assert tracker.resolved_count == 1  # unchanged

    async def test_multiple_signals_independent_resolution(self):
        """Two signals: one resolves, the other stays active."""
        tracker = OutcomeTracker()
        # sig_a: entry=50000, tp=50500, sl=49000
        sig_a = make_long_signal(entry="50000", tp="50500", sl="49000")
        # sig_b: different symbol so the kline for BTCUSDT doesn't affect it
        sig_b = make_long_signal(
            symbol="ETHUSDT",
            entry="3000", tp="3100", sl="2900",
            signal_time=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        tracker.add_signal(sig_a)
        tracker.add_signal(sig_b)
        assert tracker.active_count == 2

        # Kline hits TP for sig_a only; sig_b is ETHUSDT, unaffected
        kline = make_kline(symbol="BTCUSDT", high="50600", low="49500")
        await tracker.check_kline(kline)

        assert sig_a.outcome == Outcome.TP
        assert sig_b.outcome == Outcome.ACTIVE
        assert tracker.active_count == 1
        assert tracker.resolved_count == 1


# ---------------------------------------------------------------------------
# Symbol filtering
# ---------------------------------------------------------------------------

class TestSymbolFiltering:
    """Klines only affect signals for the same symbol."""

    async def test_different_symbol_ignored(self):
        """BTCUSDT signal is not affected by ETHUSDT kline."""
        tracker = OutcomeTracker()
        signal = make_long_signal(symbol="BTCUSDT", entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        eth_kline = make_kline(symbol="ETHUSDT", high="99999", low="1")
        await tracker.check_kline(eth_kline)

        assert signal.outcome == Outcome.ACTIVE
        assert tracker.active_count == 1
        assert tracker.resolved_count == 0

    async def test_same_symbol_affected(self):
        """BTCUSDT signal IS affected by BTCUSDT kline."""
        tracker = OutcomeTracker()
        signal = make_long_signal(symbol="BTCUSDT", entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        btc_kline = make_kline(symbol="BTCUSDT", high="50600", low="49500")
        await tracker.check_kline(btc_kline)

        assert signal.outcome == Outcome.TP
        assert tracker.active_count == 0

    async def test_multi_symbol_routing(self):
        """Multiple symbols: each kline only resolves matching signal."""
        tracker = OutcomeTracker()
        btc_signal = make_long_signal(symbol="BTCUSDT", entry="50000", tp="50500", sl="49000")
        eth_signal = make_short_signal(symbol="ETHUSDT", entry="3000", tp="2900", sl="3100")
        tracker.add_signal(btc_signal)
        tracker.add_signal(eth_signal)

        # BTC kline resolves BTC signal only
        btc_kline = make_kline(symbol="BTCUSDT", high="50600", low="49500")
        await tracker.check_kline(btc_kline)

        assert btc_signal.outcome == Outcome.TP
        assert eth_signal.outcome == Outcome.ACTIVE
        assert tracker.active_count == 1

        # ETH kline resolves ETH signal
        eth_kline = make_kline(symbol="ETHUSDT", high="3050", low="2890")
        await tracker.check_kline(eth_kline)

        assert eth_signal.outcome == Outcome.TP
        assert tracker.active_count == 0
        assert tracker.resolved_count == 2


# ---------------------------------------------------------------------------
# Finalize
# ---------------------------------------------------------------------------

class TestFinalize:
    """Finalize clears active signals, leaving their outcome as ACTIVE."""

    async def test_finalize_clears_active(self):
        """Unresolved signals stay ACTIVE, active list is cleared."""
        tracker = OutcomeTracker()
        sig1 = make_long_signal(
            signal_time=datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        )
        sig2 = make_short_signal(
            signal_time=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        tracker.add_signal(sig1)
        tracker.add_signal(sig2)

        tracker.finalize()

        assert tracker.active_count == 0
        assert sig1.outcome == Outcome.ACTIVE  # not resolved
        assert sig2.outcome == Outcome.ACTIVE

    async def test_finalize_after_partial_resolution(self):
        """Some resolved, some not -- finalize clears only remaining active."""
        tracker = OutcomeTracker()
        sig_resolved = make_long_signal(entry="50000", tp="50500", sl="49000")
        # sig_active: different symbol so BTC kline doesn't affect it
        sig_active = make_long_signal(
            symbol="ETHUSDT",
            entry="3000", tp="3100", sl="2900",
            signal_time=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        tracker.add_signal(sig_resolved)
        tracker.add_signal(sig_active)

        # Resolve one (BTC kline only affects sig_resolved)
        kline = make_kline(symbol="BTCUSDT", high="50600", low="49500")
        await tracker.check_kline(kline)
        assert tracker.active_count == 1
        assert tracker.resolved_count == 1

        tracker.finalize()

        assert tracker.active_count == 0
        assert sig_resolved.outcome == Outcome.TP
        assert sig_active.outcome == Outcome.ACTIVE

    def test_finalize_empty_tracker(self):
        """Finalize on empty tracker is a no-op."""
        tracker = OutcomeTracker()
        tracker.finalize()
        assert tracker.active_count == 0
        assert tracker.resolved_count == 0


# ---------------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------------

class TestCallback:
    """on_outcome callback is invoked correctly."""

    async def test_callback_invoked_on_tp(self):
        """Callback fires with (signal, Outcome.TP) on take profit."""
        results = []

        async def on_outcome(signal: SignalRecord, outcome: Outcome):
            results.append((signal, outcome))

        tracker = OutcomeTracker(on_outcome=on_outcome)
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)

        assert len(results) == 1
        assert results[0][0] is signal
        assert results[0][1] == Outcome.TP

    async def test_callback_invoked_on_sl(self):
        """Callback fires with (signal, Outcome.SL) on stop loss."""
        results = []

        async def on_outcome(signal: SignalRecord, outcome: Outcome):
            results.append((signal, outcome))

        tracker = OutcomeTracker(on_outcome=on_outcome)
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="48900")
        await tracker.check_kline(kline)

        assert len(results) == 1
        assert results[0][1] == Outcome.SL

    async def test_callback_not_invoked_when_no_hit(self):
        """Callback does not fire when kline stays in range."""
        results = []

        async def on_outcome(signal: SignalRecord, outcome: Outcome):
            results.append((signal, outcome))

        tracker = OutcomeTracker(on_outcome=on_outcome)
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="49200")
        await tracker.check_kline(kline)

        assert len(results) == 0

    async def test_no_callback_configured(self):
        """Tracker works fine with on_outcome=None (default)."""
        tracker = OutcomeTracker()
        signal = make_long_signal()
        tracker.add_signal(signal)

        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.TP
        assert tracker.resolved_count == 1


# ---------------------------------------------------------------------------
# Outcome time / price assignment
# ---------------------------------------------------------------------------

class TestOutcomeMetadata:
    """Verify outcome_time and outcome_price are set correctly."""

    async def test_outcome_time_set_to_kline_timestamp(self):
        """outcome_time is the kline's timestamp."""
        tracker = OutcomeTracker()
        signal = make_long_signal()
        tracker.add_signal(signal)

        ts = datetime(2025, 7, 15, 12, 30, tzinfo=timezone.utc)
        kline = make_kline(high="50600", low="49500", timestamp=ts)
        await tracker.check_kline(kline)

        assert signal.outcome_time == ts

    async def test_tp_outcome_price_is_tp_price(self):
        """On TP, outcome_price is signal.tp_price."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)

        assert signal.outcome_price == Decimal("50500")

    async def test_sl_outcome_price_is_sl_price(self):
        """On SL, outcome_price is signal.sl_price."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        kline = make_kline(high="50200", low="48900")
        await tracker.check_kline(kline)

        assert signal.outcome_price == Decimal("49000")


# ---------------------------------------------------------------------------
# update_atr
# ---------------------------------------------------------------------------

class TestUpdateATR:
    """Test the update_atr helper method."""

    def test_max_atr_updated_when_larger(self):
        """max_atr increases when current_atr exceeds it."""
        tracker = OutcomeTracker()
        signal = make_long_signal(timeframe="5m")
        signal.atr_at_signal = Decimal("100")
        signal.max_atr = Decimal("100")
        tracker.add_signal(signal)

        tracker.update_atr("BTCUSDT", "5m", 150.0)
        assert signal.max_atr == Decimal("150.0")

    def test_max_atr_not_decreased(self):
        """max_atr does not decrease."""
        tracker = OutcomeTracker()
        signal = make_long_signal(timeframe="5m")
        signal.max_atr = Decimal("200")
        tracker.add_signal(signal)

        tracker.update_atr("BTCUSDT", "5m", 150.0)
        assert signal.max_atr == Decimal("200")

    def test_update_atr_filters_by_symbol_and_timeframe(self):
        """update_atr only affects matching symbol + timeframe."""
        tracker = OutcomeTracker()
        btc_5m = make_long_signal(symbol="BTCUSDT", timeframe="5m")
        btc_5m.max_atr = Decimal("100")
        btc_15m = make_long_signal(
            symbol="BTCUSDT", timeframe="15m",
            signal_time=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        btc_15m.max_atr = Decimal("100")
        eth_5m = make_short_signal(
            symbol="ETHUSDT", timeframe="5m",
            signal_time=datetime(2025, 6, 1, 0, 2, tzinfo=timezone.utc),
        )
        eth_5m.max_atr = Decimal("100")

        tracker.add_signal(btc_5m)
        tracker.add_signal(btc_15m)
        tracker.add_signal(eth_5m)

        tracker.update_atr("BTCUSDT", "5m", 200.0)

        assert btc_5m.max_atr == Decimal("200.0")  # updated
        assert btc_15m.max_atr == Decimal("100")    # not updated (wrong timeframe)
        assert eth_5m.max_atr == Decimal("100")     # not updated (wrong symbol)

    async def test_update_atr_skips_resolved_signals(self):
        """Resolved signals are not updated."""
        tracker = OutcomeTracker()
        signal = make_long_signal(timeframe="5m")
        signal.max_atr = Decimal("100")
        tracker.add_signal(signal)

        # Resolve the signal
        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)
        assert signal.outcome == Outcome.TP

        # Signal is removed from active, so update_atr has no effect
        tracker.update_atr("BTCUSDT", "5m", 999.0)
        assert signal.max_atr == Decimal("100")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Miscellaneous edge cases."""

    async def test_empty_active_list(self):
        """check_kline with no active signals is a no-op."""
        tracker = OutcomeTracker()
        kline = make_kline(high="99999", low="1")
        await tracker.check_kline(kline)  # should not raise
        assert tracker.resolved_count == 0

    async def test_many_signals_resolved_in_one_kline(self):
        """Multiple signals for the same symbol can be resolved by one kline."""
        tracker = OutcomeTracker()
        signals = []
        for i in range(5):
            sig = make_long_signal(
                entry="50000", tp="50500", sl="49000",
                signal_time=datetime(2025, 6, 1, 0, i, tzinfo=timezone.utc),
            )
            tracker.add_signal(sig)
            signals.append(sig)

        assert tracker.active_count == 5

        # One kline resolves all
        kline = make_kline(high="50600", low="49500")
        await tracker.check_kline(kline)

        assert tracker.active_count == 0
        assert tracker.resolved_count == 5
        for sig in signals:
            assert sig.outcome == Outcome.TP

    async def test_kline_just_misses_tp_and_sl_long(self):
        """LONG: kline just barely misses both TP and SL."""
        tracker = OutcomeTracker()
        signal = make_long_signal(entry="50000", tp="50500", sl="49000")
        tracker.add_signal(signal)

        # high is 1 tick below tp, low is 1 tick above sl
        kline = make_kline(high="50499.99", low="49000.01")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.ACTIVE

    async def test_kline_just_misses_tp_and_sl_short(self):
        """SHORT: kline just barely misses both TP and SL."""
        tracker = OutcomeTracker()
        signal = make_short_signal(entry="50000", tp="49500", sl="51000")
        tracker.add_signal(signal)

        # low is 1 tick above tp, high is 1 tick below sl
        kline = make_kline(high="50999.99", low="49500.01")
        await tracker.check_kline(kline)

        assert signal.outcome == Outcome.ACTIVE

    async def test_sequential_resolution_across_klines(self):
        """Two signals for the same symbol resolve on different klines."""
        tracker = OutcomeTracker()
        sig1 = make_long_signal(
            entry="50000", tp="50500", sl="49000",
            signal_time=datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
        )
        sig2 = make_long_signal(
            entry="50000", tp="51000", sl="49000",
            signal_time=datetime(2025, 6, 1, 0, 1, tzinfo=timezone.utc),
        )
        tracker.add_signal(sig1)
        tracker.add_signal(sig2)

        # First kline: hits sig1 TP (50500) but not sig2 TP (51000)
        k1 = make_kline(
            high="50600", low="49500",
            timestamp=datetime(2025, 6, 1, 0, 5, tzinfo=timezone.utc),
        )
        await tracker.check_kline(k1)
        assert sig1.outcome == Outcome.TP
        assert sig2.outcome == Outcome.ACTIVE
        assert tracker.active_count == 1

        # Second kline: hits sig2 TP
        k2 = make_kline(
            high="51100", low="50000",
            timestamp=datetime(2025, 6, 1, 0, 6, tzinfo=timezone.utc),
        )
        await tracker.check_kline(k2)
        assert sig2.outcome == Outcome.TP
        assert tracker.active_count == 0
        assert tracker.resolved_count == 2
