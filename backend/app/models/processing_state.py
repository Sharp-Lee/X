"""Processing state model for tracking K-line replay progress."""

from datetime import datetime
from pydantic import BaseModel, ConfigDict


class ProcessingState(BaseModel):
    """Processing state for tracking K-line replay progress.

    Used to ensure signal determinism across restarts:
    - system_start_time: First-ever startup time (never changes after first run)
    - last_processed_time: Last successfully processed kline timestamp
    - state_status: 'pending' during replay, 'confirmed' after commit
    """

    model_config = ConfigDict(frozen=False)

    symbol: str
    timeframe: str
    system_start_time: datetime
    last_processed_time: datetime
    state_status: str = "confirmed"  # 'pending' | 'confirmed'
