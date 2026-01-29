"""REST API routes."""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.config import get_settings
from app.models import Direction, Outcome
from app.storage import SignalRepository
from app.services import OrderService, OrderSide

logger = logging.getLogger(__name__)

router = APIRouter()


# Response models
class SignalResponse(BaseModel):
    """Signal response model."""

    id: str
    symbol: str
    timeframe: str
    signal_time: datetime
    direction: str
    entry_price: float
    tp_price: float
    sl_price: float
    streak_at_signal: int
    mae_ratio: float
    mfe_ratio: float
    outcome: str
    outcome_time: Optional[datetime] = None
    outcome_price: Optional[float] = None


class SystemStatus(BaseModel):
    """System status response."""

    status: str
    version: str
    symbols: list[str]
    timeframe: str
    active_signals: int


class OrderRequest(BaseModel):
    """Order request model."""

    symbol: str
    side: str  # "buy" or "sell"
    quantity: float
    price: Optional[float] = None  # None for market order


class OrderResponse(BaseModel):
    """Order response model."""

    success: bool
    order_id: Optional[str] = None
    message: str


# Dependency for signal repository
def get_signal_repo() -> SignalRepository:
    return SignalRepository()


@router.get("/status", response_model=SystemStatus)
async def get_status():
    """Get system status."""
    settings = get_settings()
    repo = get_signal_repo()
    active = await repo.get_active()

    return SystemStatus(
        status="running",
        version="0.1.0",
        symbols=settings.symbols,
        timeframe=settings.timeframe,
        active_signals=len(active),
    )


@router.get("/signals", response_model=list[SignalResponse])
async def get_signals(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum signals to return"),
    outcome: Optional[str] = Query(None, description="Filter by outcome (active, tp, sl)"),
):
    """Get recent signals."""
    repo = get_signal_repo()
    signals = await repo.get_recent(limit=limit, symbol=symbol)

    # Filter by outcome if specified
    if outcome:
        signals = [s for s in signals if s.outcome.value == outcome]

    return [
        SignalResponse(
            id=s.id,
            symbol=s.symbol,
            timeframe=s.timeframe,
            signal_time=s.signal_time,
            direction=s.direction.name,
            entry_price=float(s.entry_price),
            tp_price=float(s.tp_price),
            sl_price=float(s.sl_price),
            streak_at_signal=s.streak_at_signal,
            mae_ratio=float(s.mae_ratio),
            mfe_ratio=float(s.mfe_ratio),
            outcome=s.outcome.value,
            outcome_time=s.outcome_time,
            outcome_price=float(s.outcome_price) if s.outcome_price else None,
        )
        for s in signals
    ]


@router.get("/signals/active", response_model=list[SignalResponse])
async def get_active_signals(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
):
    """Get active (open) signals."""
    repo = get_signal_repo()
    signals = await repo.get_active(symbol=symbol)

    return [
        SignalResponse(
            id=s.id,
            symbol=s.symbol,
            timeframe=s.timeframe,
            signal_time=s.signal_time,
            direction=s.direction.name,
            entry_price=float(s.entry_price),
            tp_price=float(s.tp_price),
            sl_price=float(s.sl_price),
            streak_at_signal=s.streak_at_signal,
            mae_ratio=float(s.mae_ratio),
            mfe_ratio=float(s.mfe_ratio),
            outcome=s.outcome.value,
            outcome_time=s.outcome_time,
            outcome_price=float(s.outcome_price) if s.outcome_price else None,
        )
        for s in signals
    ]


@router.get("/signals/{signal_id}", response_model=SignalResponse)
async def get_signal(signal_id: str):
    """Get a specific signal by ID."""
    repo = get_signal_repo()
    signal = await repo.get_by_id(signal_id)

    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found")

    return SignalResponse(
        id=signal.id,
        symbol=signal.symbol,
        timeframe=signal.timeframe,
        signal_time=signal.signal_time,
        direction=signal.direction.name,
        entry_price=float(signal.entry_price),
        tp_price=float(signal.tp_price),
        sl_price=float(signal.sl_price),
        streak_at_signal=signal.streak_at_signal,
        mae_ratio=float(signal.mae_ratio),
        mfe_ratio=float(signal.mfe_ratio),
        outcome=signal.outcome.value,
        outcome_time=signal.outcome_time,
        outcome_price=float(signal.outcome_price) if signal.outcome_price else None,
    )


@router.post("/order", response_model=OrderResponse)
async def place_order(order: OrderRequest):
    """
    Place a manual order on the exchange.

    This endpoint is for manual trading triggered from the frontend.
    """
    order_service = OrderService(testnet=True)  # Default to testnet for safety
    try:
        await order_service.connect()

        side = OrderSide.BUY if order.side.lower() == "buy" else OrderSide.SELL

        if order.price is None:
            # Market order
            result = await order_service.place_market_order(
                symbol=order.symbol,
                side=side,
                amount=Decimal(str(order.quantity)),
            )
        else:
            # Limit order
            result = await order_service.place_limit_order(
                symbol=order.symbol,
                side=side,
                amount=Decimal(str(order.quantity)),
                price=Decimal(str(order.price)),
            )

        return OrderResponse(
            success=True,
            order_id=result.get("id"),
            message=f"Order placed: {result.get('status')}",
        )

    except Exception as e:
        logger.error(f"Order placement failed: {e}")
        return OrderResponse(
            success=False,
            order_id=None,
            message=f"Order failed: {str(e)}",
        )
    finally:
        await order_service.close()


@router.get("/stats")
async def get_stats(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    days: int = Query(30, ge=1, le=365, description="Days to analyze"),
):
    """Get trading statistics."""
    repo = get_signal_repo()
    signals = await repo.get_recent(limit=10000, symbol=symbol)

    # Filter to recent days
    cutoff = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # Simple cutoff - in production, use proper date range query

    total = len(signals)
    wins = sum(1 for s in signals if s.outcome == Outcome.TP)
    losses = sum(1 for s in signals if s.outcome == Outcome.SL)
    active = sum(1 for s in signals if s.outcome == Outcome.ACTIVE)

    win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

    return {
        "total_signals": total,
        "wins": wins,
        "losses": losses,
        "active": active,
        "win_rate": round(win_rate, 2),
        "breakeven_win_rate": 81.5,  # Required win rate for 1:4.42 R:R
    }


# Signal execution request model
class ExecuteSignalRequest(BaseModel):
    """Request to execute a signal."""
    signal_id: str
    quantity: float
    place_sl_tp: bool = True


class ExecuteSignalResponse(BaseModel):
    """Response from signal execution."""
    success: bool
    message: str
    orders: list[dict] = []


class PositionResponse(BaseModel):
    """Current position response."""
    symbol: str
    side: str
    contracts: float
    entry_price: float
    unrealized_pnl: float
    leverage: int


class BalanceResponse(BaseModel):
    """Account balance response."""
    total: float
    free: float
    used: float


@router.post("/execute-signal", response_model=ExecuteSignalResponse)
async def execute_signal(request: ExecuteSignalRequest):
    """
    Execute a trading signal with optional SL/TP orders.

    This places:
    1. Entry market order
    2. Stop-loss order (if place_sl_tp=True)
    3. Take-profit order (if place_sl_tp=True)
    """
    repo = get_signal_repo()
    signal = await repo.get_by_id(request.signal_id)

    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found")

    if signal.outcome != Outcome.ACTIVE:
        raise HTTPException(status_code=400, detail="Signal is no longer active")

    order_service = OrderService(testnet=True)
    try:
        await order_service.connect()

        result = await order_service.execute_signal(
            signal=signal,
            quantity=Decimal(str(request.quantity)),
            place_sl_tp=request.place_sl_tp,
        )

        return ExecuteSignalResponse(
            success=True,
            message=f"Signal executed with {len(result['orders'])} orders",
            orders=result["orders"],
        )

    except Exception as e:
        logger.error(f"Signal execution failed: {e}")
        return ExecuteSignalResponse(
            success=False,
            message=f"Execution failed: {str(e)}",
            orders=[],
        )
    finally:
        await order_service.close()


@router.get("/position/{symbol}", response_model=Optional[PositionResponse])
async def get_position(symbol: str):
    """Get current position for a symbol."""
    order_service = OrderService(testnet=True)
    try:
        await order_service.connect()
        position = await order_service.get_position(symbol)

        if position is None:
            return None

        return PositionResponse(
            symbol=position["symbol"],
            side=position["side"],
            contracts=position["contracts"],
            entry_price=position["entry_price"],
            unrealized_pnl=position["unrealized_pnl"],
            leverage=position["leverage"],
        )
    finally:
        await order_service.close()


@router.get("/balance", response_model=BalanceResponse)
async def get_balance(currency: str = "USDT"):
    """Get account balance."""
    order_service = OrderService(testnet=True)
    try:
        await order_service.connect()
        balance = await order_service.get_balance(currency)

        return BalanceResponse(
            total=balance["total"],
            free=balance["free"],
            used=balance["used"],
        )
    finally:
        await order_service.close()


@router.post("/close-position/{symbol}")
async def close_position(symbol: str):
    """Close an existing position."""
    order_service = OrderService(testnet=True)
    try:
        await order_service.connect()
        result = await order_service.close_position(symbol)

        if result is None:
            return {"success": True, "message": "No position to close"}

        return {
            "success": True,
            "message": "Position closed",
            "order_id": result.get("id"),
        }
    except Exception as e:
        logger.error(f"Failed to close position: {e}")
        return {
            "success": False,
            "message": f"Failed: {str(e)}",
        }
    finally:
        await order_service.close()


@router.post("/set-leverage/{symbol}")
async def set_leverage(symbol: str, leverage: int = Query(10, ge=1, le=125)):
    """Set leverage for a symbol."""
    order_service = OrderService(testnet=True)
    try:
        await order_service.connect()
        result = await order_service.set_leverage(symbol, leverage)

        return {
            "success": True,
            "message": f"Leverage set to {leverage}x",
            "result": result,
        }
    except Exception as e:
        logger.error(f"Failed to set leverage: {e}")
        return {
            "success": False,
            "message": f"Failed: {str(e)}",
        }
    finally:
        await order_service.close()
