from __future__ import annotations


def dynamic_cooldown_resume_ok(
    *,
    symbol: str,
    max_abs_ret: float,
    cusum: float,
    velocity: float,
    imbalance: float,
    hurst: float,
    p_loss: float | None,
    cusum_limit: float,
    velocity_limit: float,
    imbalance_limit: float,
    ensemble_loss_threshold: float,
    spike_threshold: float = 0.00015,
    min_hurst: float = 0.4,
) -> bool:
    """Return whether a session cooldown can end early on live market data."""
    if max_abs_ret >= spike_threshold or hurst <= min_hurst:
        return False
    if p_loss is not None and p_loss >= ensemble_loss_threshold:
        return False

    symbol_upper = symbol.upper()
    if "CRASH" in symbol_upper:
        return (
            cusum >= -abs(cusum_limit)
            and velocity >= -abs(velocity_limit)
            and imbalance >= -abs(imbalance_limit)
        )
    if "BOOM" in symbol_upper:
        return (
            cusum <= abs(cusum_limit)
            and velocity <= abs(velocity_limit)
            and imbalance <= abs(imbalance_limit)
        )
    return abs(cusum) <= abs(cusum_limit)
