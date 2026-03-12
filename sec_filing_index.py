"""
Build a filing index for a given CIK using SEC submissions JSON.

We focus on 10-K and 10-Q filings and track enough metadata to later
download XBRL files and reconstruct statements.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import requests

from config import SEC_BASE, build_sec_headers, DEFAULT_CONFIG, RateLimiter


SUBMISSIONS_URL_TEMPLATE = SEC_BASE + "/submissions/CIK{cik}.json"


@dataclass
class FilingMetadata:
    cik: str
    ticker: str
    accession_number: str
    filing_date: str
    report_period: str
    form: str
    primary_document: str

    @property
    def accession_no_nodashes(self) -> str:
        return self.accession_number.replace("-", "")


class SecFilingIndexBuilder:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self._headers = build_sec_headers(DEFAULT_CONFIG)
        self._rate_limiter = RateLimiter(DEFAULT_CONFIG.min_request_interval_seconds)

    def _fetch_submissions(self, cik: str) -> dict:
        url = SUBMISSIONS_URL_TEMPLATE.format(cik=cik)
        self._rate_limiter.wait()
        resp = self.session.get(url, headers=self._headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def build_index(
        self,
        cik: str,
        ticker: str,
        max_10k: int = 5,
    ) -> Tuple[pd.DataFrame, List[FilingMetadata]]:
        """
        Returns (filing_index_df, filings_list) where the list contains the
        selected 10-K and 10-Q filings.
        """
        submissions = self._fetch_submissions(cik)
        recent = submissions.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        accession_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        report_periods = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])

        records: List[FilingMetadata] = []
        for form, acc, fdate, rdate, primary_doc in zip(
            forms, accession_numbers, filing_dates, report_periods, primary_docs
        ):
            if form not in ("10-K", "10-Q"):
                continue
            if not acc:
                continue
            records.append(
                FilingMetadata(
                    cik=cik,
                    ticker=ticker.upper(),
                    accession_number=acc,
                    filing_date=fdate,
                    report_period=rdate or "",
                    form=form,
                    primary_document=primary_doc,
                )
            )

        # Sort by filing date descending
        records.sort(key=lambda r: r.filing_date, reverse=True)

        # Select last max_10k 10-K filings
        ten_ks = [r for r in records if r.form == "10-K"]
        selected_10k = ten_ks[:max_10k]

        if selected_10k:
            earliest_10k_period = min(r.report_period for r in selected_10k if r.report_period)
        else:
            earliest_10k_period = None

        # Select 10-Qs that are "accompanying" the selected 10-Ks:
        # heuristic: 10-Qs with report_period >= earliest selected 10-K period.
        ten_qs = [r for r in records if r.form == "10-Q"]
        if earliest_10k_period:
            selected_10q = [
                r for r in ten_qs if r.report_period and r.report_period >= earliest_10k_period
            ]
        else:
            selected_10q = ten_qs

        selected = selected_10k + selected_10q
        # Keep deterministic ordering: by report period then form then filing date
        selected.sort(key=lambda r: (r.report_period, r.form, r.filing_date))

        index_rows = [
            {
                "cik": r.cik,
                "ticker": r.ticker,
                "accession_number": r.accession_number,
                "filing_date": r.filing_date,
                "report_period": r.report_period,
                "form": r.form,
                "primary_document": r.primary_document,
                "filing_url": f"{SEC_BASE}/Archives/edgar/data/{int(r.cik)}/{r.accession_no_nodashes}/{r.primary_document}",
                "xbrl_parsed": False,
                "statement_extraction_succeeded": False,
                "error": "",
            }
            for r in selected
        ]

        df = pd.DataFrame(index_rows)
        return df, selected

