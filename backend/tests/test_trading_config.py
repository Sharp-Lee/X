"""Tests for trading_config.py and account_manager.py."""

import os
import textwrap
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import yaml

from app.trading_config import (
    AccountConfig,
    StrategyEntry,
    TradingConfig,
    load_trading_config,
)
from core.models.config import PORTFOLIO_A, PORTFOLIO_B, SignalFilterConfig


# ── TradingConfig model tests ─────────────────────────────────────────────


class TestTradingConfig:
    def test_default_portfolio_is_b(self):
        config = TradingConfig()
        assert config.portfolio == "B"

    def test_portfolio_a_returns_portfolio_a_filters(self):
        config = TradingConfig(portfolio="A")
        filters = config.get_signal_filters()
        assert len(filters) == len(PORTFOLIO_A)
        assert filters[0].symbol == PORTFOLIO_A[0].symbol

    def test_portfolio_b_returns_portfolio_b_filters(self):
        config = TradingConfig(portfolio="B")
        filters = config.get_signal_filters()
        assert len(filters) == len(PORTFOLIO_B)
        assert filters[0].symbol == PORTFOLIO_B[0].symbol

    def test_custom_portfolio_returns_custom_filters(self):
        config = TradingConfig(
            portfolio="custom",
            strategies=[
                StrategyEntry(
                    symbol="ETHUSDT",
                    timeframe="5m",
                    streak_lo=-2,
                    streak_hi=5,
                    atr_pct_threshold=0.50,
                    position_qty=10,
                ),
            ],
        )
        filters = config.get_signal_filters()
        assert len(filters) == 1
        assert filters[0].symbol == "ETHUSDT"
        assert filters[0].timeframe == "5m"
        assert filters[0].streak_lo == -2
        assert filters[0].streak_hi == 5
        assert filters[0].atr_pct_threshold == 0.50
        assert filters[0].position_qty == 10

    def test_custom_without_strategies_raises(self):
        with pytest.raises(ValueError, match="at least one entry"):
            TradingConfig(portfolio="custom", strategies=[])

    def test_invalid_portfolio_raises(self):
        with pytest.raises(ValueError, match="must be one of"):
            TradingConfig(portfolio="C")

    def test_strategy_entry_to_filter_config(self):
        entry = StrategyEntry(
            symbol="BTCUSDT",
            timeframe="15m",
            streak_lo=0,
            streak_hi=7,
            atr_pct_threshold=0.90,
            position_qty=1,
        )
        fc = entry.to_filter_config()
        assert isinstance(fc, SignalFilterConfig)
        assert fc.symbol == "BTCUSDT"
        assert fc.timeframe == "15m"
        assert fc.key == "BTCUSDT_15m"

    def test_disabled_strategy_entry(self):
        entry = StrategyEntry(symbol="BTCUSDT", timeframe="5m", enabled=False)
        fc = entry.to_filter_config()
        assert fc.enabled is False


# ── AccountConfig tests ───────────────────────────────────────────────────


class TestAccountConfig:
    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "secret123")
        acct = AccountConfig(name="test", api_key_env="MY_KEY")
        assert acct.api_key == "secret123"

    def test_api_secret_from_env(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "supersecret")
        acct = AccountConfig(name="test", api_secret_env="MY_SECRET")
        assert acct.api_secret == "supersecret"

    def test_missing_env_returns_empty(self):
        acct = AccountConfig(name="test", api_key_env="NONEXISTENT_VAR_XYZ")
        assert acct.api_key == ""

    def test_empty_env_name_returns_empty(self):
        acct = AccountConfig(name="test", api_key_env="")
        assert acct.api_key == ""

    def test_default_values(self):
        acct = AccountConfig(name="test")
        assert acct.testnet is True
        assert acct.enabled is True
        assert acct.auto_trade is False
        assert acct.leverage == 5
        assert acct.risk_pct == 0.015
        assert acct.strategies == []

    def test_risk_pct_default(self):
        acct = AccountConfig(name="test")
        assert acct.risk_pct == 0.015

    def test_risk_pct_custom(self):
        acct = AccountConfig(name="test", risk_pct=0.02)
        assert acct.risk_pct == 0.02

    def test_get_enabled_accounts(self):
        config = TradingConfig(
            accounts=[
                AccountConfig(name="a1", enabled=True, auto_trade=True),
                AccountConfig(name="a2", enabled=True, auto_trade=False),
                AccountConfig(name="a3", enabled=False, auto_trade=True),
                AccountConfig(name="a4", enabled=True, auto_trade=True),
            ]
        )
        enabled = config.get_enabled_accounts()
        assert len(enabled) == 2
        assert {a.name for a in enabled} == {"a1", "a4"}

    def test_no_accounts_returns_empty(self):
        config = TradingConfig()
        assert config.get_enabled_accounts() == []


# ── load_trading_config tests ─────────────────────────────────────────────


class TestLoadTradingConfig:
    def test_missing_file_returns_defaults(self, tmp_path):
        config = load_trading_config(tmp_path / "nonexistent.yaml")
        assert config.portfolio == "B"
        assert config.accounts == []

    def test_load_portfolio_a(self, tmp_path):
        yaml_path = tmp_path / "trading.yaml"
        yaml_path.write_text("portfolio: A\n")
        config = load_trading_config(yaml_path)
        assert config.portfolio == "A"
        assert len(config.get_signal_filters()) == len(PORTFOLIO_A)

    def test_load_custom_portfolio(self, tmp_path):
        yaml_content = textwrap.dedent("""\
            portfolio: custom
            strategies:
              - symbol: ETHUSDT
                timeframe: 30m
                streak_lo: 0
                streak_hi: 4
                atr_pct_threshold: 0.90
                position_qty: 10
        """)
        yaml_path = tmp_path / "trading.yaml"
        yaml_path.write_text(yaml_content)
        config = load_trading_config(yaml_path)
        assert config.portfolio == "custom"
        filters = config.get_signal_filters()
        assert len(filters) == 1
        assert filters[0].symbol == "ETHUSDT"

    def test_load_with_accounts(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_KEY", "k1")
        monkeypatch.setenv("TEST_SECRET", "s1")
        yaml_content = textwrap.dedent("""\
            portfolio: B
            accounts:
              - name: test-acct
                api_key_env: TEST_KEY
                api_secret_env: TEST_SECRET
                testnet: true
                auto_trade: true
                leverage: 10
                strategies: ["BTCUSDT_15m"]
        """)
        yaml_path = tmp_path / "trading.yaml"
        yaml_path.write_text(yaml_content)
        config = load_trading_config(yaml_path)
        assert len(config.accounts) == 1
        acct = config.accounts[0]
        assert acct.name == "test-acct"
        assert acct.api_key == "k1"
        assert acct.api_secret == "s1"
        assert acct.leverage == 10
        assert acct.strategies == ["BTCUSDT_15m"]

    def test_empty_yaml_returns_defaults(self, tmp_path):
        yaml_path = tmp_path / "trading.yaml"
        yaml_path.write_text("")
        config = load_trading_config(yaml_path)
        assert config.portfolio == "B"


# ── AccountManager tests ─────────────────────────────────────────────────


class TestAccountManager:
    """Test AccountManager signal routing logic (no real API calls)."""

    def _make_signal(self, symbol="XRPUSDT", timeframe="30m",
                     entry_price=Decimal("2.50"), risk_amount=Decimal("0.265")):
        """Create a minimal mock signal."""
        sig = type("Signal", (), {
            "id": "test-123",
            "symbol": symbol,
            "timeframe": timeframe,
            "direction": type("Dir", (), {"name": "LONG"})(),
            "entry_price": entry_price,
            "tp_price": entry_price + Decimal("0.06"),
            "sl_price": entry_price - risk_amount,
            "risk_amount": risk_amount,
        })()
        return sig

    def test_get_accounts_for_signal_all_strategies(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[
                AccountConfig(name="all", enabled=True, auto_trade=True, strategies=[]),
            ]
        )
        mgr = AccountManager(config)
        # Manually insert a fake account entry (skip connect)
        mock_svc = AsyncMock()
        mgr._accounts["all"] = (config.accounts[0], mock_svc)

        signal = self._make_signal("XRPUSDT", "30m")
        matches = mgr.get_accounts_for_signal(signal)
        assert len(matches) == 1
        assert matches[0][0].name == "all"

    def test_get_accounts_for_signal_filtered(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[
                AccountConfig(
                    name="xrp-only",
                    enabled=True,
                    auto_trade=True,
                    strategies=["XRPUSDT_30m"],
                ),
            ]
        )
        mgr = AccountManager(config)
        mock_svc = AsyncMock()
        mgr._accounts["xrp-only"] = (config.accounts[0], mock_svc)

        # Matching signal
        signal_match = self._make_signal("XRPUSDT", "30m")
        assert len(mgr.get_accounts_for_signal(signal_match)) == 1

        # Non-matching signal
        signal_no = self._make_signal("BTCUSDT", "15m")
        assert len(mgr.get_accounts_for_signal(signal_no)) == 0

    def test_get_accounts_multiple_accounts(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[
                AccountConfig(name="a", enabled=True, auto_trade=True, strategies=["XRPUSDT_30m"]),
                AccountConfig(name="b", enabled=True, auto_trade=True, strategies=["BTCUSDT_15m"]),
                AccountConfig(name="c", enabled=True, auto_trade=True, strategies=[]),
            ]
        )
        mgr = AccountManager(config)
        for acct in config.accounts:
            mgr._accounts[acct.name] = (acct, AsyncMock())

        signal = self._make_signal("XRPUSDT", "30m")
        matches = mgr.get_accounts_for_signal(signal)
        names = {m[0].name for m in matches}
        assert names == {"a", "c"}  # "b" doesn't match

    @pytest.mark.asyncio
    async def test_execute_signal_no_accounts(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig()
        mgr = AccountManager(config)
        # Should not raise
        await mgr.execute_signal(self._make_signal())

    @pytest.mark.asyncio
    async def test_execute_signal_zero_risk_amount(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[AccountConfig(name="a", enabled=True, auto_trade=True)]
        )
        mgr = AccountManager(config)
        mock_svc = AsyncMock()
        mgr._accounts["a"] = (config.accounts[0], mock_svc)

        # risk_amount = 0 should skip
        signal = self._make_signal(risk_amount=Decimal("0"))
        await mgr.execute_signal(signal)
        mock_svc.execute_signal.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_signal_dynamic_sizing(self):
        """Verify dynamic position size = (equity × risk_pct) / risk_amount."""
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[AccountConfig(
                name="a", enabled=True, auto_trade=True,
                leverage=10, risk_pct=0.015,
            )]
        )
        mgr = AccountManager(config)
        mock_svc = AsyncMock()
        mock_svc.get_balance.return_value = {"total": 2000, "free": 2000, "used": 0}
        mock_svc.execute_signal.return_value = {"orders": [{"id": 1}]}
        mgr._accounts["a"] = (config.accounts[0], mock_svc)

        # XRP signal: entry=2.50, risk_amount=0.265
        signal = self._make_signal("XRPUSDT", "30m",
                                   entry_price=Decimal("2.50"),
                                   risk_amount=Decimal("0.265"))
        await mgr.execute_signal(signal)

        mock_svc.get_balance.assert_awaited_once()
        mock_svc.set_leverage.assert_awaited_once_with("XRPUSDT", 10)
        mock_svc.execute_signal.assert_awaited_once()

        # Verify calculated quantity: 2000 * 0.015 / 0.265 ≈ 113.2075
        call_args = mock_svc.execute_signal.call_args
        actual_qty = call_args[0][1]  # second positional arg
        expected_qty = Decimal("2000") * Decimal("0.015") / Decimal("0.265")
        assert abs(actual_qty - expected_qty) < Decimal("0.0001")

    @pytest.mark.asyncio
    async def test_execute_signal_notional_too_small(self):
        """Skip trade when notional value < $5."""
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[AccountConfig(
                name="a", enabled=True, auto_trade=True, risk_pct=0.001,
            )]
        )
        mgr = AccountManager(config)
        mock_svc = AsyncMock()
        # Very small account
        mock_svc.get_balance.return_value = {"total": 10, "free": 10, "used": 0}
        mgr._accounts["a"] = (config.accounts[0], mock_svc)

        # BTC signal: entry=$96000, risk_amount=$2652
        # qty = (10 * 0.001) / 2652 = 0.0000037 BTC, notional = $0.36
        signal = self._make_signal("BTCUSDT", "15m",
                                   entry_price=Decimal("96000"),
                                   risk_amount=Decimal("2652"))
        await mgr.execute_signal(signal)
        mock_svc.execute_signal.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_execute_signal_custom_risk_pct(self):
        """Account with risk_pct=0.02 should size larger."""
        from app.services.account_manager import AccountManager

        config = TradingConfig(
            accounts=[AccountConfig(
                name="a", enabled=True, auto_trade=True, risk_pct=0.02,
            )]
        )
        mgr = AccountManager(config)
        mock_svc = AsyncMock()
        mock_svc.get_balance.return_value = {"total": 5000, "free": 5000, "used": 0}
        mock_svc.execute_signal.return_value = {"orders": [{"id": 1}]}
        mgr._accounts["a"] = (config.accounts[0], mock_svc)

        signal = self._make_signal("SOLUSDT", "5m",
                                   entry_price=Decimal("150"),
                                   risk_amount=Decimal("13.26"))
        await mgr.execute_signal(signal)

        call_args = mock_svc.execute_signal.call_args
        actual_qty = call_args[0][1]
        expected_qty = Decimal("5000") * Decimal("0.02") / Decimal("13.26")
        assert abs(actual_qty - expected_qty) < Decimal("0.0001")

    @pytest.mark.asyncio
    async def test_stop_closes_all(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig()
        mgr = AccountManager(config)
        mock1 = AsyncMock()
        mock2 = AsyncMock()
        mgr._accounts["a"] = (AccountConfig(name="a"), mock1)
        mgr._accounts["b"] = (AccountConfig(name="b"), mock2)

        await mgr.stop()
        mock1.close.assert_awaited_once()
        mock2.close.assert_awaited_once()
        assert len(mgr._accounts) == 0

    def test_active_count(self):
        from app.services.account_manager import AccountManager

        config = TradingConfig()
        mgr = AccountManager(config)
        assert mgr.active_count == 0
        mgr._accounts["a"] = (AccountConfig(name="a"), AsyncMock())
        assert mgr.active_count == 1
