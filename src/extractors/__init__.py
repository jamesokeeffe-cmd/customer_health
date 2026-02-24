from src.extractors.intercom import IntercomExtractor
from src.extractors.jira import JiraExtractor

# Looker and Salesforce have heavy dependencies (looker_sdk, simple_salesforce)
# that may not be installed in all environments. Import them lazily.
try:
    from src.extractors.looker import LookerExtractor
except ImportError:
    LookerExtractor = None  # type: ignore[assignment,misc]

try:
    from src.extractors.salesforce import SalesforceExtractor
except ImportError:
    SalesforceExtractor = None  # type: ignore[assignment,misc]

__all__ = [
    "IntercomExtractor",
    "JiraExtractor",
    "LookerExtractor",
    "SalesforceExtractor",
]
