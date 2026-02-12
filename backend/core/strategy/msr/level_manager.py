"""Level management for the MSR Retest Capture strategy.

Handles support/resistance level classification, nearest level detection,
level scoring, and touch detection.
"""

import math
from decimal import Decimal


def _is_nan(value) -> bool:
    """Check if a value is NaN (handles Decimal and float).

    Args:
        value: Value to check (Decimal, float, or None)

    Returns:
        True if value is NaN or None
    """
    if value is None:
        return True
    if isinstance(value, Decimal):
        return value.is_nan()
    if isinstance(value, float):
        return math.isnan(value)
    # For string comparison (legacy support)
    return str(value) == "NaN"


class LevelManager:
    """Manages support and resistance levels based on indicators."""

    def __init__(self, touch_tolerance: Decimal = Decimal("0.001")):
        """
        Args:
            touch_tolerance: Tolerance for price touching levels (as ratio)
        """
        self.touch_tolerance = touch_tolerance

    def get_levels(
        self,
        close: Decimal,
        fib_382: Decimal,
        fib_500: Decimal,
        fib_618: Decimal,
        vwap_value: Decimal,
    ) -> tuple[list[Decimal], list[Decimal]]:
        """
        Classify levels as support or resistance based on current price.

        Returns:
            Tuple of (support_levels, resistance_levels)
        """
        support_levels = []
        resistance_levels = []

        for level in [fib_382, fib_500, fib_618, vwap_value]:
            if close < level:
                resistance_levels.append(level)
            else:
                support_levels.append(level)

        return support_levels, resistance_levels

    def get_nearest_levels(
        self,
        close: Decimal,
        support_levels: list[Decimal],
        resistance_levels: list[Decimal],
    ) -> tuple[Decimal | None, Decimal | None]:
        """
        Get the nearest support and resistance levels.

        Returns:
            Tuple of (nearest_support, nearest_resistance)
        """
        nearest_support = None
        nearest_resistance = None

        for level in support_levels:
            if level < close:
                if nearest_support is None or level > nearest_support:
                    nearest_support = level

        for level in resistance_levels:
            if level > close:
                if nearest_resistance is None or level < nearest_resistance:
                    nearest_resistance = level

        return nearest_support, nearest_resistance

    def calculate_level_score(
        self,
        price: Decimal,
        levels: list[Decimal],
        is_support: bool,
    ) -> tuple[Decimal, int]:
        """
        Calculate score based on proximity to levels.

        Returns:
            Tuple of (score, count)
        """
        score = Decimal("0")
        count = 0

        for level in levels:
            if (is_support and level < price) or (not is_support and level > price):
                dist = abs(price - level) / price * 100
                score += Decimal("1") / (Decimal("1") + dist)
                count += 1

        return score, count

    def is_touching_level(
        self,
        price: Decimal,
        level: Decimal,
    ) -> bool:
        """Check if price is touching a level within tolerance.

        Note: This method is not used in the current MSR Retest Capture strategy,
        which uses exact price comparison (low <= support) matching the Pine Script.
        This method is available for alternative strategies that need tolerance-based
        level detection.
        """
        tolerance = level * self.touch_tolerance
        return abs(price - level) <= tolerance
