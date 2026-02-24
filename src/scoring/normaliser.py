from __future__ import annotations

"""Normalise raw metric values to 0-100 scores using segment-specific thresholds.

Linear interpolation between threshold boundaries:
- At or beyond green boundary → 100
- At or beyond red boundary → 0
- Between green and yellow → 50-100 (linear)
- Between yellow and red → 0-50 (linear)
"""


def normalise_metric(
    raw_value: float | None,
    green: float,
    yellow: float,
    red: float,
    lower_is_better: bool,
) -> float | None:
    """Convert a raw metric value to a 0-100 score.

    Args:
        raw_value: The raw metric value. None if data is missing.
        green: Threshold for a "green" (best) score.
        yellow: Threshold for a "yellow" (mid) score.
        red: Threshold for a "red" (worst) score.
        lower_is_better: If True, lower values get higher scores
                         (e.g. response time, ticket volume).

    Returns:
        Float 0-100, or None if raw_value is None.
    """
    if raw_value is None:
        return None

    if lower_is_better:
        # Lower values = better. green < yellow < red.
        if raw_value <= green:
            return 100.0
        elif raw_value >= red:
            return 0.0
        elif raw_value <= yellow:
            # Between green and yellow → score 50-100
            range_size = yellow - green
            if range_size == 0:
                return 75.0
            position = (raw_value - green) / range_size
            return round(100.0 - (position * 50.0), 1)
        else:
            # Between yellow and red → score 0-50
            range_size = red - yellow
            if range_size == 0:
                return 25.0
            position = (raw_value - yellow) / range_size
            return round(50.0 - (position * 50.0), 1)
    else:
        # Higher values = better. green > yellow > red.
        if raw_value >= green:
            return 100.0
        elif raw_value <= red:
            return 0.0
        elif raw_value >= yellow:
            # Between yellow and green → score 50-100
            range_size = green - yellow
            if range_size == 0:
                return 75.0
            position = (raw_value - yellow) / range_size
            return round(50.0 + (position * 50.0), 1)
        else:
            # Between red and yellow → score 0-50
            range_size = yellow - red
            if range_size == 0:
                return 25.0
            position = (raw_value - red) / range_size
            return round(position * 50.0, 1)
