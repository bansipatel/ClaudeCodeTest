"""
Utilities for mapping stock tickers to CIKs using SEC's public ticker list.

We deliberately avoid third-party APIs and rely on:
https://www.sec.gov/files/company_tickers.json
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Optional

import requests

from config import DEFAULT_CONFIG, RateLimiter


TICKER_LIST_URL = "https://www.sec.gov/files/company_tickers.json"


@dataclass
class TickerInfo:
    cik_str: str
    ticker: str
    title: str


class TickerToCikResolver:
    """
    Lazily loads the SEC ticker mapping JSON and resolves tickers to CIK.
    """

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        # Use www.sec.gov host for this endpoint
        self._headers = {
            "User-Agent": DEFAULT_CONFIG.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        }
        self._rate_limiter = RateLimiter(DEFAULT_CONFIG.min_request_interval_seconds)
        self._cache: Dict[str, TickerInfo] = {}
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        self._rate_limiter.wait()
        resp = self.session.get(TICKER_LIST_URL, headers=self._headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # SEC uses an object keyed by integer-like strings
        cache: Dict[str, TickerInfo] = {}
        for entry in data.values():
            ticker = entry["ticker"].upper()
            cik_str = str(entry["cik_str"]).zfill(10)
            cache[ticker] = TickerInfo(
                cik_str=cik_str,
                ticker=ticker,
                title=entry.get("title", ""),
            )
        self._cache = cache
        self._loaded = True

    def get_cik(self, ticker: str) -> str:
        """
        Return 10-digit zero-padded CIK for the given ticker.

        Raises KeyError if ticker is unknown.
        """
        self._load()
        t = ticker.upper()
        if t not in self._cache:
            raise KeyError(f"Ticker {ticker} not found in SEC ticker list.")
        return self._cache[t].cik_str

    def get_ticker_info(self, ticker: str) -> TickerInfo:
        self._load()
        t = ticker.upper()
        if t not in self._cache:
            raise KeyError(f"Ticker {ticker} not found in SEC ticker list.")
        return self._cache[t]

