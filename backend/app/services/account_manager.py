"""Account manager: per-account OrderService instances and signal routing."""

import logging
from decimal import Decimal

from app.models import SignalRecord
from app.services.order_service import OrderService
from app.trading_config import AccountConfig, TradingConfig

logger = logging.getLogger(__name__)


class AccountManager:
    """Manages multiple trading accounts, each with its own OrderService."""

    def __init__(self, config: TradingConfig):
        self._config = config
        self._accounts: dict[str, tuple[AccountConfig, OrderService]] = {}
        self._filters: dict[str, object] | None = None

    def set_filters(self, filters: dict) -> None:
        """Set signal filter configs for position_qty lookup."""
        self._filters = filters

    async def start(self) -> None:
        """Initialize OrderService for each enabled auto-trade account."""
        for acct in self._config.get_enabled_accounts():
            if not acct.api_key or not acct.api_secret:
                logger.warning(
                    "Account '%s': env vars not set (%s, %s), skipping",
                    acct.name,
                    acct.api_key_env,
                    acct.api_secret_env,
                )
                continue

            order_svc = OrderService(
                api_key=acct.api_key,
                api_secret=acct.api_secret,
                testnet=acct.testnet,
            )
            try:
                await order_svc.connect()
                self._accounts[acct.name] = (acct, order_svc)
                logger.info(
                    "Account '%s' connected (testnet=%s, strategies=%s)",
                    acct.name,
                    acct.testnet,
                    acct.strategies or "ALL",
                )
            except Exception as e:
                logger.error("Account '%s' failed to connect: %s", acct.name, e)

    async def stop(self) -> None:
        """Close all OrderService connections."""
        for name, (_, order_svc) in self._accounts.items():
            try:
                await order_svc.close()
                logger.info("Account '%s' disconnected", name)
            except Exception as e:
                logger.warning("Error closing account '%s': %s", name, e)
        self._accounts.clear()

    def get_accounts_for_signal(
        self, signal: SignalRecord
    ) -> list[tuple[AccountConfig, OrderService]]:
        """Return (config, order_service) pairs that should trade this signal."""
        signal_key = f"{signal.symbol}_{signal.timeframe}"
        result = []
        for _, (acct, order_svc) in self._accounts.items():
            if not acct.strategies or signal_key in acct.strategies:
                result.append((acct, order_svc))
        return result

    async def execute_signal(self, signal: SignalRecord) -> None:
        """Route a signal to all matching accounts for execution."""
        if not self._accounts:
            return

        # Look up position_qty from filter config
        signal_key = f"{signal.symbol}_{signal.timeframe}"
        filter_config = self._filters.get(signal_key) if self._filters else None
        if not filter_config or filter_config.position_qty <= 0:
            logger.warning(
                "No position_qty for %s, skipping auto-trade", signal_key
            )
            return

        quantity = Decimal(str(filter_config.position_qty))

        for acct_config, order_svc in self.get_accounts_for_signal(signal):
            try:
                await order_svc.set_leverage(signal.symbol, acct_config.leverage)
                result = await order_svc.execute_signal(signal, quantity)
                logger.info(
                    "Account '%s': executed %s %s (%d orders)",
                    acct_config.name,
                    signal.symbol,
                    signal.direction.name,
                    len(result.get("orders", [])),
                )
            except Exception as e:
                logger.error(
                    "Account '%s': failed to execute %s: %s",
                    acct_config.name,
                    signal.id,
                    e,
                )

    def _get_traded_symbols(self, acct: AccountConfig) -> list[str]:
        """Extract unique symbols this account trades."""
        if acct.strategies:
            # "XRPUSDT_30m" → "XRPUSDT"
            symbols = {s.rsplit("_", 1)[0] for s in acct.strategies}
            return sorted(symbols)
        # No strategy filter → all symbols from portfolio filters
        if self._filters:
            return sorted({k.rsplit("_", 1)[0] for k in self._filters})
        return []

    async def get_overview(self) -> list[dict]:
        """Get balance and positions for all active accounts."""
        results = []
        for name, (acct, order_svc) in self._accounts.items():
            entry: dict = {
                "name": name,
                "testnet": acct.testnet,
                "leverage": acct.leverage,
                "strategies": acct.strategies if acct.strategies else "ALL",
            }
            try:
                balance = await order_svc.get_balance()
                entry["balance"] = balance

                positions = []
                for symbol in self._get_traded_symbols(acct):
                    try:
                        pos = await order_svc.get_position(symbol)
                        if pos and pos.get("contracts", 0) != 0:
                            positions.append(pos)
                    except Exception:
                        pass
                entry["positions"] = positions
            except Exception as e:
                entry["error"] = str(e)
                entry["balance"] = {"total": 0, "free": 0, "used": 0}
                entry["positions"] = []

            results.append(entry)
        return results

    @property
    def active_count(self) -> int:
        return len(self._accounts)
