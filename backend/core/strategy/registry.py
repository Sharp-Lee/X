"""Strategy registry for discovering and instantiating strategies.

Usage:
    @register_strategy("my_strategy")
    class MyStrategy:
        ...

    strategy = create_strategy("my_strategy", config=config)
    strategies = list_strategies()
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Global registry: strategy_name -> strategy_class
_REGISTRY: dict[str, type] = {}


def register_strategy(name: str):
    """Decorator to register a strategy class under a given name.

    Args:
        name: Unique strategy name (e.g., 'msr_retest_capture').

    Returns:
        Decorator that registers the class and returns it unchanged.

    Raises:
        ValueError: If a strategy with the same name is already registered.
    """

    def decorator(cls):
        if name in _REGISTRY:
            raise ValueError(
                f"Strategy '{name}' is already registered by {_REGISTRY[name].__name__}"
            )
        _REGISTRY[name] = cls
        logger.debug("Registered strategy: %s -> %s", name, cls.__name__)
        return cls

    return decorator


def create_strategy(name: str, **kwargs: Any):
    """Create a strategy instance by name.

    Args:
        name: Registered strategy name.
        **kwargs: Arguments passed to the strategy constructor.

    Returns:
        An instance of the registered strategy class.

    Raises:
        KeyError: If no strategy is registered under the given name.
    """
    cls = _REGISTRY.get(name)
    if cls is None:
        available = ", ".join(sorted(_REGISTRY.keys())) or "(none)"
        raise KeyError(
            f"Unknown strategy '{name}'. Available: {available}"
        )
    return cls(**kwargs)


def get_strategy_class(name: str) -> type:
    """Get the strategy class by name (without instantiating).

    Args:
        name: Registered strategy name.

    Returns:
        The strategy class.

    Raises:
        KeyError: If no strategy is registered under the given name.
    """
    cls = _REGISTRY.get(name)
    if cls is None:
        available = ", ".join(sorted(_REGISTRY.keys())) or "(none)"
        raise KeyError(
            f"Unknown strategy '{name}'. Available: {available}"
        )
    return cls


def list_strategies() -> list[str]:
    """Return a sorted list of registered strategy names."""
    return sorted(_REGISTRY.keys())
