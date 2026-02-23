"""Tests for the normaliser: raw metric → 0-100 score."""

import pytest

from src.scoring.normaliser import normalise_metric


class TestLowerIsBetter:
    """Metrics where lower raw values = better scores (e.g. response time, ticket volume)."""

    def test_at_green_boundary(self):
        # At green threshold → 100
        assert normalise_metric(2, green=2, yellow=4, red=8, lower_is_better=True) == 100.0

    def test_below_green_boundary(self):
        # Below green → 100
        assert normalise_metric(0, green=2, yellow=4, red=8, lower_is_better=True) == 100.0

    def test_at_red_boundary(self):
        # At red → 0
        assert normalise_metric(8, green=2, yellow=4, red=8, lower_is_better=True) == 0.0

    def test_above_red_boundary(self):
        # Above red → 0
        assert normalise_metric(15, green=2, yellow=4, red=8, lower_is_better=True) == 0.0

    def test_at_yellow_boundary(self):
        # At yellow → 50
        assert normalise_metric(4, green=2, yellow=4, red=8, lower_is_better=True) == 50.0

    def test_between_green_and_yellow(self):
        # Midpoint between green (2) and yellow (4) → 75
        result = normalise_metric(3, green=2, yellow=4, red=8, lower_is_better=True)
        assert result == 75.0

    def test_between_yellow_and_red(self):
        # Midpoint between yellow (4) and red (8) → 25
        result = normalise_metric(6, green=2, yellow=4, red=8, lower_is_better=True)
        assert result == 25.0

    def test_none_returns_none(self):
        assert normalise_metric(None, green=2, yellow=4, red=8, lower_is_better=True) is None


class TestHigherIsBetter:
    """Metrics where higher raw values = better scores (e.g. adoption %, days to renewal)."""

    def test_at_green_boundary(self):
        assert normalise_metric(180, green=180, yellow=90, red=30, lower_is_better=False) == 100.0

    def test_above_green_boundary(self):
        assert normalise_metric(365, green=180, yellow=90, red=30, lower_is_better=False) == 100.0

    def test_at_red_boundary(self):
        assert normalise_metric(30, green=180, yellow=90, red=30, lower_is_better=False) == 0.0

    def test_below_red_boundary(self):
        assert normalise_metric(10, green=180, yellow=90, red=30, lower_is_better=False) == 0.0

    def test_at_yellow_boundary(self):
        assert normalise_metric(90, green=180, yellow=90, red=30, lower_is_better=False) == 50.0

    def test_between_yellow_and_green(self):
        # Midpoint: 135 between 90 and 180 → 75
        result = normalise_metric(135, green=180, yellow=90, red=30, lower_is_better=False)
        assert result == 75.0

    def test_between_red_and_yellow(self):
        # Midpoint: 60 between 30 and 90 → 25
        result = normalise_metric(60, green=180, yellow=90, red=30, lower_is_better=False)
        assert result == 25.0


class TestFirstResponseThresholds:
    """Test with actual first response time thresholds from the model."""

    def test_paid_green(self):
        # <60 min → 100 for Paid Success
        assert normalise_metric(30, green=60, yellow=120, red=240, lower_is_better=True) == 100.0

    def test_paid_at_yellow(self):
        assert normalise_metric(120, green=60, yellow=120, red=240, lower_is_better=True) == 50.0

    def test_standard_green(self):
        # <240 min → 100 for Standard Success
        assert normalise_metric(120, green=240, yellow=480, red=1440, lower_is_better=True) == 100.0

    def test_standard_slow(self):
        # 1440+ min → 0 for Standard
        assert normalise_metric(1440, green=240, yellow=480, red=1440, lower_is_better=True) == 0.0
