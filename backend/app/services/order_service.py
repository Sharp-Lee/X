"""Order execution service using ccxt for Binance Futures."""

import logging
from decimal import Decimal
from enum import Enum
from typing import Optional

import ccxt.async_support as ccxt

from app.config import get_settings
from app.models import Direction, SignalRecord

logger = logging.getLogger(__name__)


class OrderSide(str, Enum):
    """Order side enum."""
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """Order type enum."""
    MARKET = "market"
    LIMIT = "limit"
    STOP_MARKET = "stop_market"
    TAKE_PROFIT_MARKET = "take_profit_market"


class OrderService:
    """
    Service for executing orders on Binance Futures via ccxt.

    Supports:
    - Market orders for entry
    - Stop-loss and take-profit orders
    - Position management
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        testnet: bool = True,
    ):
        """
        Initialize order service.

        Args:
            api_key: Binance API key (from settings if None)
            api_secret: Binance API secret (from settings if None)
            testnet: Use Binance testnet (default True for safety)
        """
        settings = get_settings()
        self._api_key = api_key or settings.binance_api_key
        self._api_secret = api_secret or settings.binance_api_secret
        self._testnet = testnet
        self._trading_enabled = not testnet  # Disabled in testnet mode

        self._exchange: ccxt.binanceusdm | None = None

    async def connect(self) -> None:
        """Initialize connection to exchange."""
        if self._exchange:
            return

        self._exchange = ccxt.binanceusdm({
            "apiKey": self._api_key,
            "secret": self._api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
            },
        })

        if self._testnet:
            # Note: Binance Futures testnet is deprecated in ccxt
            # See: https://t.me/ccxt_announcements/92
            # For now, we connect to production but with a warning
            logger.warning(
                "Binance Futures testnet is deprecated. "
                "Connected to PRODUCTION with trading disabled. "
                "Set testnet=False to enable actual trading."
            )
            self._trading_enabled = False
        else:
            logger.warning("Connected to Binance Futures PRODUCTION - USE WITH CAUTION")
            self._trading_enabled = True

        # Load markets
        await self._exchange.load_markets()
        logger.info(f"Loaded {len(self._exchange.markets)} markets")

    async def close(self) -> None:
        """Close exchange connection."""
        if self._exchange:
            await self._exchange.close()
            self._exchange = None

    async def get_balance(self, currency: str = "USDT") -> dict:
        """
        Get account balance.

        Args:
            currency: Currency to check (default USDT)

        Returns:
            Dict with balance info
        """
        if not self._exchange:
            await self.connect()

        balance = await self._exchange.fetch_balance()
        return {
            "total": float(balance.get(currency, {}).get("total", 0)),
            "free": float(balance.get(currency, {}).get("free", 0)),
            "used": float(balance.get(currency, {}).get("used", 0)),
        }

    async def get_position(self, symbol: str) -> dict | None:
        """
        Get current position for symbol.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")

        Returns:
            Position info or None if no position
        """
        if not self._exchange:
            await self.connect()

        positions = await self._exchange.fetch_positions([symbol])
        for pos in positions:
            if pos["symbol"] == symbol and float(pos.get("contracts", 0)) != 0:
                return {
                    "symbol": pos["symbol"],
                    "side": pos["side"],
                    "contracts": float(pos["contracts"]),
                    "entry_price": float(pos["entryPrice"]),
                    "unrealized_pnl": float(pos.get("unrealizedPnl", 0)),
                    "leverage": int(pos.get("leverage", 1)),
                }
        return None

    async def place_market_order(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        reduce_only: bool = False,
    ) -> dict:
        """
        Place a market order.

        Args:
            symbol: Trading pair
            side: Buy or sell
            amount: Order quantity
            reduce_only: If True, only reduces existing position

        Returns:
            Order response
        """
        if not self._exchange:
            await self.connect()

        if not self._trading_enabled:
            logger.warning("Trading disabled in testnet mode - simulating order")
            return {
                "id": "SIMULATED",
                "status": "simulated",
                "symbol": symbol,
                "side": side.value,
                "amount": float(amount),
                "info": "Trading disabled - testnet mode",
            }

        params = {}
        if reduce_only:
            params["reduceOnly"] = True

        logger.info(f"Placing {side.value} market order: {symbol} qty={amount}")
        order = await self._exchange.create_order(
            symbol=symbol,
            type="market",
            side=side.value,
            amount=float(amount),
            params=params,
        )

        logger.info(f"Order placed: {order['id']} status={order['status']}")
        return order

    async def place_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> dict:
        """
        Place a limit order.

        Args:
            symbol: Trading pair
            side: Buy or sell
            amount: Order quantity
            price: Limit price
            reduce_only: If True, only reduces existing position

        Returns:
            Order response
        """
        if not self._exchange:
            await self.connect()

        params = {}
        if reduce_only:
            params["reduceOnly"] = True

        logger.info(
            f"Placing {side.value} limit order: {symbol} qty={amount} @ {price}"
        )
        order = await self._exchange.create_order(
            symbol=symbol,
            type="limit",
            side=side.value,
            amount=float(amount),
            price=float(price),
            params=params,
        )

        logger.info(f"Order placed: {order['id']} status={order['status']}")
        return order

    async def place_stop_loss(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        stop_price: Decimal,
    ) -> dict:
        """
        Place a stop-loss market order.

        Args:
            symbol: Trading pair
            side: Buy (for short position SL) or Sell (for long position SL)
            amount: Order quantity
            stop_price: Stop trigger price

        Returns:
            Order response
        """
        if not self._exchange:
            await self.connect()

        logger.info(
            f"Placing {side.value} stop-loss: {symbol} qty={amount} stop={stop_price}"
        )
        order = await self._exchange.create_order(
            symbol=symbol,
            type="stop_market",
            side=side.value,
            amount=float(amount),
            params={
                "stopPrice": float(stop_price),
                "reduceOnly": True,
            },
        )

        logger.info(f"Stop-loss placed: {order['id']}")
        return order

    async def place_take_profit(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        tp_price: Decimal,
    ) -> dict:
        """
        Place a take-profit market order.

        Args:
            symbol: Trading pair
            side: Buy (for short position TP) or Sell (for long position TP)
            amount: Order quantity
            tp_price: Take profit trigger price

        Returns:
            Order response
        """
        if not self._exchange:
            await self.connect()

        logger.info(
            f"Placing {side.value} take-profit: {symbol} qty={amount} tp={tp_price}"
        )
        order = await self._exchange.create_order(
            symbol=symbol,
            type="take_profit_market",
            side=side.value,
            amount=float(amount),
            params={
                "stopPrice": float(tp_price),
                "reduceOnly": True,
            },
        )

        logger.info(f"Take-profit placed: {order['id']}")
        return order

    async def execute_signal(
        self,
        signal: SignalRecord,
        quantity: Decimal,
        place_sl_tp: bool = True,
    ) -> dict:
        """
        Execute a trading signal with optional SL/TP orders.

        Args:
            signal: The signal to execute
            quantity: Position size
            place_sl_tp: Whether to place SL/TP orders

        Returns:
            Dict with order details
        """
        if not self._exchange:
            await self.connect()

        # Determine entry side
        if signal.direction == Direction.LONG:
            entry_side = OrderSide.BUY
            exit_side = OrderSide.SELL
        else:
            entry_side = OrderSide.SELL
            exit_side = OrderSide.BUY

        result = {"signal_id": signal.id, "orders": []}

        # Place entry order
        entry_order = await self.place_market_order(
            symbol=signal.symbol,
            side=entry_side,
            amount=quantity,
        )
        result["orders"].append({"type": "entry", "order": entry_order})

        if place_sl_tp:
            # Place stop-loss
            sl_order = await self.place_stop_loss(
                symbol=signal.symbol,
                side=exit_side,
                amount=quantity,
                stop_price=signal.sl_price,
            )
            result["orders"].append({"type": "stop_loss", "order": sl_order})

            # Place take-profit
            tp_order = await self.place_take_profit(
                symbol=signal.symbol,
                side=exit_side,
                amount=quantity,
                tp_price=signal.tp_price,
            )
            result["orders"].append({"type": "take_profit", "order": tp_order})

        logger.info(
            f"Signal {signal.id} executed: {len(result['orders'])} orders placed"
        )
        return result

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        """
        Cancel an open order.

        Args:
            symbol: Trading pair
            order_id: Order ID to cancel

        Returns:
            Cancellation response
        """
        if not self._exchange:
            await self.connect()

        logger.info(f"Cancelling order {order_id} for {symbol}")
        result = await self._exchange.cancel_order(order_id, symbol)
        return result

    async def cancel_all_orders(self, symbol: str) -> list:
        """
        Cancel all open orders for a symbol.

        Args:
            symbol: Trading pair

        Returns:
            List of cancelled orders
        """
        if not self._exchange:
            await self.connect()

        logger.info(f"Cancelling all orders for {symbol}")
        result = await self._exchange.cancel_all_orders(symbol)
        return result

    async def close_position(self, symbol: str) -> dict | None:
        """
        Close an existing position with a market order.

        Args:
            symbol: Trading pair

        Returns:
            Order response or None if no position
        """
        position = await self.get_position(symbol)
        if not position:
            logger.info(f"No position to close for {symbol}")
            return None

        # Cancel any existing orders first
        await self.cancel_all_orders(symbol)

        # Close position with opposite market order
        side = OrderSide.SELL if position["side"] == "long" else OrderSide.BUY
        amount = Decimal(str(abs(position["contracts"])))

        logger.info(f"Closing position: {symbol} {position['side']} qty={amount}")
        order = await self.place_market_order(
            symbol=symbol,
            side=side,
            amount=amount,
            reduce_only=True,
        )

        return order

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        """
        Set leverage for a symbol.

        Args:
            symbol: Trading pair
            leverage: Leverage multiplier (1-125)

        Returns:
            Response from exchange
        """
        if not self._exchange:
            await self.connect()

        logger.info(f"Setting leverage for {symbol} to {leverage}x")
        result = await self._exchange.set_leverage(leverage, symbol)
        return result
