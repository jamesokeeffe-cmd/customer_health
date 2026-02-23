"""Qualitative signal modifier logic.

Applies score caps based on active Churn_Signal__c records:
- Critical + Confirmed → set to 50
- 2+ Critical → cap at 55
- 1 Critical → cap at 65
- 1+ Moderate → cap at 75
- Watch only or none → no cap

Caps can only pull a score DOWN, never up.
"""


def apply_qualitative_modifier(
    quantitative_score: float,
    critical_count: int,
    moderate_count: int,
    watch_count: int,
    has_critical_confirmed: bool,
) -> dict:
    """Apply qualitative signal modifier to a quantitative health score.

    Args:
        quantitative_score: The calculated quantitative score (0-100).
        critical_count: Number of active Critical signals.
        moderate_count: Number of active Moderate signals.
        watch_count: Number of active Watch signals.
        has_critical_confirmed: True if any Critical signal has "Confirmed" confidence.

    Returns:
        dict with:
            final_score: Score after modifier applied.
            modifier_applied: Description of which rule was applied, or None.
            cap_value: The cap value applied, or None.
            override_active: True if the modifier changed the score.
    """
    cap_value = None
    modifier_applied = None

    # Evaluate rules in order of severity (most severe first)
    if has_critical_confirmed and critical_count >= 1:
        cap_value = 50
        modifier_applied = "Critical + Confirmed → set to 50"
    elif critical_count >= 2:
        cap_value = 55
        modifier_applied = f"{critical_count} Critical signals → cap at 55"
    elif critical_count >= 1:
        cap_value = 65
        modifier_applied = "1 Critical signal → cap at 65"
    elif moderate_count >= 1:
        cap_value = 75
        modifier_applied = f"{moderate_count} Moderate signal(s) → cap at 75"

    if cap_value is not None:
        final_score = min(quantitative_score, cap_value)
        override_active = final_score < quantitative_score
    else:
        final_score = quantitative_score
        override_active = False

    return {
        "final_score": round(final_score, 1),
        "modifier_applied": modifier_applied,
        "cap_value": cap_value,
        "override_active": override_active,
    }
