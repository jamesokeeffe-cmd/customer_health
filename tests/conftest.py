from __future__ import annotations

"""Conftest: mock external SDK modules that may not be installed locally."""

import sys
from unittest.mock import MagicMock

# Mock simple_salesforce before any src module imports it
if "simple_salesforce" not in sys.modules:
    mock_sf_module = MagicMock()
    sys.modules["simple_salesforce"] = mock_sf_module

# Mock looker_sdk and its sub-modules before any src module imports it
if "looker_sdk" not in sys.modules:
    mock_looker = MagicMock()
    sys.modules["looker_sdk"] = mock_looker
    sys.modules["looker_sdk.models40"] = mock_looker.models40
