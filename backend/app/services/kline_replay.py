"""K-line replay service for restoring system state after restart.

This service replays historical K-lines to ensure signal determinism:
- Only replays 1m K-lines (higher timeframes derived via aggregator)
- Restores buffer state before replay
- Checkpoints progress for crash recovery
"""

import logging
from datetime import datetime, timezone

from app.models import Kline, KlineBuffer, ProcessingState, kline_to_fast, fast_to_kline
from app.storage import KlineRepository, ProcessingStateRepository

logger = logging.getLogger(__name__)

# Checkpoint interval during replay (every N klines)
CHECKPOINT_INTERVAL = 100


class KlineReplayService:
    """Replays historical K-lines to restore system state.

    Used during startup to replay missed K-lines from last checkpoint to now,
    ensuring signal determinism - the system behaves as if it never stopped.
    """

    def __init__(
        self,
        kline_repo: KlineRepository,
        state_repo: ProcessingStateRepository,
    ):
        self.kline_repo = kline_repo
        self.state_repo = state_repo
        self._replay_mode = False

        # Callbacks set by DataCollector
        self._on_kline_callback = None
        self._on_aggregated_callback = None

    @property
    def is_replaying(self) -> bool:
        """Check if replay is in progress."""
        return self._replay_mode

    def set_callbacks(
        self,
        on_kline,
        on_aggregated,
    ) -> None:
        """Set callbacks for processing klines during replay.

        Args:
            on_kline: Callback for 1m kline processing (updates buffer, signals)
            on_aggregated: Callback for aggregated kline processing
        """
        self._on_kline_callback = on_kline
        self._on_aggregated_callback = on_aggregated

    async def get_checkpoint_time(self, symbol: str) -> datetime | None:
        """Get the last processed timestamp for a symbol.

        Args:
            symbol: Trading pair

        Returns:
            Last processed timestamp, or None if first run
        """
        state = await self.state_repo.get_state(symbol, "1m")
        if state:
            return state.last_processed_time
        return None

    async def restore_buffer_state(
        self,
        symbol: str,
        timeframe: str,
        checkpoint_time: datetime,
        limit: int = 200,
    ) -> KlineBuffer:
        """Restore buffer to state at checkpoint time.

        Loads K-lines UP TO AND INCLUDING the checkpoint time.
        The checkpoint_time kline was already processed, so it should be
        in the buffer for indicator context when processing the next kline.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            checkpoint_time: Last processed kline timestamp
            limit: Number of historical klines to load

        Returns:
            Restored KlineBuffer
        """
        # Get klines up to and including checkpoint_time
        # We need checkpoint_time in the buffer because:
        # 1. It was already processed (that's why it's the checkpoint)
        # 2. Next kline's indicators depend on it being in the buffer
        klines = await self.kline_repo.get_latest_until(
            symbol=symbol,
            timeframe=timeframe,
            until_time=checkpoint_time,
            limit=limit,
        )

        buffer = KlineBuffer(symbol=symbol, timeframe=timeframe)
        for kline in klines:
            buffer.add(kline)

        logger.info(
            f"Restored buffer for {symbol} {timeframe} with {len(buffer)} klines "
            f"(up to {checkpoint_time})"
        )
        return buffer

    async def replay_from_checkpoint(
        self,
        symbol: str,
        checkpoint_time: datetime,
        aggregator,
        buffers: dict[str, KlineBuffer],
        signal_generator,
        timeframes: list[str],
    ) -> int:
        """Replay 1m K-lines from checkpoint to now.

        This method:
        1. Loads 1m klines from checkpoint to current time
        2. Feeds each kline through the aggregator (generates higher timeframes)
        3. Updates buffers for each timeframe
        4. Runs signal detection for each completed kline
        5. Checkpoints progress periodically

        Args:
            symbol: Trading pair
            checkpoint_time: Start replay from this time
            aggregator: KlineAggregator instance
            buffers: Dict of {symbol_timeframe: KlineBuffer}
            signal_generator: SignalGenerator instance
            timeframes: List of all timeframes to process

        Returns:
            Number of klines replayed
        """
        self._replay_mode = True
        replayed = 0

        try:
            # Load 1m klines AFTER checkpoint to now
            # checkpoint_time is the last PROCESSED kline, so we skip it
            klines_1m = await self.kline_repo.get_after(
                symbol=symbol,
                timeframe="1m",
                after_time=checkpoint_time,
                end_time=datetime.now(timezone.utc),
            )

            if not klines_1m:
                logger.info(f"No klines to replay for {symbol}")
                return 0

            logger.info(
                f"Replaying {len(klines_1m)} 1m klines for {symbol} "
                f"from {checkpoint_time} to {klines_1m[-1].timestamp}"
            )

            # Mark state as pending (crash recovery)
            await self.state_repo.mark_pending(symbol, "1m")

            for kline in klines_1m:
                if not kline.is_closed:
                    continue

                # 1. Update 1m buffer
                buffer_key = f"{symbol}_1m"
                if buffer_key in buffers:
                    buffers[buffer_key].add(kline)

                # 2. Run signal detection on 1m
                if signal_generator and buffer_key in buffers:
                    await signal_generator.process_kline(kline, buffers[buffer_key])

                # 3. Feed to aggregator (triggers higher timeframe callbacks)
                fast_kline = kline_to_fast(kline)
                aggregated_list = await aggregator.add_1m_kline(fast_kline)

                # 4. Process aggregated klines
                for agg_fast in aggregated_list:
                    agg_kline = fast_to_kline(agg_fast)
                    agg_buffer_key = f"{symbol}_{agg_kline.timeframe}"

                    # Update buffer for this timeframe
                    if agg_buffer_key in buffers:
                        buffers[agg_buffer_key].add(agg_kline)

                    # Run signal detection for this timeframe
                    if signal_generator and agg_buffer_key in buffers:
                        await signal_generator.process_kline(
                            agg_kline, buffers[agg_buffer_key]
                        )

                replayed += 1

                # Checkpoint every N klines
                if replayed % CHECKPOINT_INTERVAL == 0:
                    state = ProcessingState(
                        symbol=symbol,
                        timeframe="1m",
                        system_start_time=checkpoint_time,  # Preserved from original
                        last_processed_time=kline.timestamp,
                        state_status="pending",
                    )
                    await self.state_repo.upsert_state(state)
                    logger.debug(
                        f"Checkpoint at {replayed}/{len(klines_1m)}: {kline.timestamp}"
                    )

            # Final confirmed checkpoint
            if klines_1m:
                # Preserve original system_start_time
                existing_state = await self.state_repo.get_state(symbol, "1m")
                system_start = (
                    existing_state.system_start_time
                    if existing_state
                    else klines_1m[0].timestamp
                )

                state = ProcessingState(
                    symbol=symbol,
                    timeframe="1m",
                    system_start_time=system_start,
                    last_processed_time=klines_1m[-1].timestamp,
                    state_status="confirmed",
                )
                await self.state_repo.upsert_state(state)

            logger.info(f"Replay complete for {symbol}: {replayed} klines processed")

        finally:
            self._replay_mode = False

        return replayed

    async def initialize_state(
        self,
        symbol: str,
        system_start_time: datetime,
        initial_kline_time: datetime,
    ) -> None:
        """Initialize processing state for first run.

        Args:
            symbol: Trading pair
            system_start_time: System first startup time
            initial_kline_time: First kline timestamp in database
        """
        state = ProcessingState(
            symbol=symbol,
            timeframe="1m",
            system_start_time=system_start_time,
            last_processed_time=initial_kline_time,
            state_status="confirmed",
        )
        await self.state_repo.upsert_state(state)
        logger.info(
            f"Initialized state for {symbol}: start={system_start_time}, "
            f"last_processed={initial_kline_time}"
        )

    async def check_pending_recovery(self) -> list[ProcessingState]:
        """Check for crashed replays that need recovery.

        Returns:
            List of pending states that were interrupted mid-replay
        """
        return await self.state_repo.get_pending_states()
