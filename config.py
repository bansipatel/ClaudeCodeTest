"""
Global configuration for SEC filing extraction.

Edit USER_AGENT and CONTACT_EMAIL before heavy use to comply with SEC guidelines.
"""

import time
from dataclasses import dataclass


SEC_BASE = "https://data.sec.gov"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"


@dataclass
class SecApiConfig:
    user_agent: str = "YourAppName/1.0 (your_email@example.com)"
    contact_email: str = "your_email@example.com"
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5
    # Be conservative even though SEC allows higher burst rates
    min_request_interval_seconds: float = 0.2


DEFAULT_CONFIG = SecApiConfig()


class RateLimiter:
    """
    Very small, process-local rate limiter.
    """

    def __init__(self, min_interval_seconds: float):
        self.min_interval_seconds = min_interval_seconds
        self._last_call_ts: float | None = None

    def wait(self) -> None:
        now = time.time()
        if self._last_call_ts is None:
            self._last_call_ts = now
            return
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_call_ts = time.time()


# Keywords used to classify statement roles from presentation linkbases
INCOME_STATEMENT_KEYWORDS = [
    "statement of income",
    "statement of operations",
    "statement of earnings",
    "profit and loss",
]

BALANCE_SHEET_KEYWORDS = [
    "balance sheet",
    "financial position",
    "statement of financial position",
]

CASH_FLOW_STATEMENT_KEYWORDS = [
    "cash flows",
    "cash flow",
    "cashflow",
]


def build_sec_headers(config: SecApiConfig | None = None) -> dict:
    cfg = config or DEFAULT_CONFIG
    return {
        "User-Agent": cfg.user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }

