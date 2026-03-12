from __future__ import annotations

"""
Small helper to audit where a reported statement value came from.

Given:
- ticker (for workbook name)
- concept_qname (e.g. 'us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax')
- period_end date (e.g. '2023-09-30')

it will:
- open {ticker}_sec_reported_statements.xlsx
- search the 'Raw Facts / Debug' sheet for matching facts
- print the matching rows (including accession_number, form, context, value, dates)

This is read-only; it does NOT modify the workbook.
"""

import argparse
from pathlib import Path

import pandas as pd


def audit_fact(
    ticker: str,
    concept_qname: str,
    period_end: str,
    workbook_dir: str = ".",
) -> None:
    ticker = ticker.upper().strip()
    wb_path = Path(workbook_dir) / f"{ticker}_sec_reported_statements.xlsx"
    if not wb_path.exists():
        raise FileNotFoundError(f"Workbook not found: {wb_path}")

    print(f"Loading workbook: {wb_path}")
    # Load only the Raw Facts / Debug sheet
    raw_df = pd.read_excel(wb_path, sheet_name="Raw Facts / Debug")

    # Match either duration end_date or instant date to the requested period_end
    mask = (
        (raw_df["concept_qname"] == concept_qname)
        & (
            (raw_df["end_date"] == period_end)
            | (raw_df["instant"] == period_end)
        )
    )
    matches = raw_df.loc[mask].copy()

    if matches.empty:
        print("No matching facts found for that concept and period_end.")
        return

    cols = [
        "accession_number",
        "form",
        "report_period",
        "concept_qname",
        "value",
        "unit_ref",
        "decimals",
        "context_id",
        "period_type",
        "instant",
        "start_date",
        "end_date",
        "is_nil",
    ]
    cols = [c for c in cols if c in matches.columns]

    print("\nMatching facts:")
    print(matches[cols].to_string(index=False))


def main():
    ap = argparse.ArgumentParser(
        description="Audit helper: trace a reported statement cell back to its XBRL facts."
    )
    ap.add_argument("ticker", help="Ticker used to build the workbook (e.g., AAPL)")
    ap.add_argument("concept_qname", help="XBRL concept QName (e.g., us-gaap_RevenueFromContractWithCustomer)")
    ap.add_argument("period_end", help="Fiscal period end date (YYYY-MM-DD, e.g., 2023-09-30)")
    ap.add_argument(
        "--workbook-dir",
        default=".",
        help="Directory containing {ticker}_sec_reported_statements.xlsx",
    )
    args = ap.parse_args()

    audit_fact(
        ticker=args.ticker,
        concept_qname=args.concept_qname,
        period_end=args.period_end,
        workbook_dir=args.workbook_dir,
    )


if __name__ == "__main__":
    main()

