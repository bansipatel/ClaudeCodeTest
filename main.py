from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from tqdm import tqdm

from ticker_to_cik import TickerToCikResolver
from sec_filing_index import SecFilingIndexBuilder, FilingMetadata
from filing_downloader import FilingDownloader
from xbrl_parser import XbrlParser
from presentation_resolver import PresentationResolver
from statement_extractor import StatementExtractor
from excel_writer import ExcelWriter, META_COLUMNS
from config import DEFAULT_CONFIG


STATEMENT_TYPES = ["income", "balance", "cashflow"]


def _pick_best_role(role_trees, role_classification, statement_type: str):
    candidates = [
        rt for uri, rt in role_trees.items() if role_classification.get(uri) == statement_type
    ]
    if not candidates:
        return None
    # Prefer the role with the most presented nodes (best chance it is the primary statement)
    candidates.sort(key=lambda r: len(r.nodes_in_order), reverse=True)
    return candidates[0]


def _merge_statements_historical(filing_statements: List[pd.DataFrame]) -> pd.DataFrame:
    """
    filing_statements: list of dataframes shaped:
      META_COLUMNS + [period_end_column]
    The first element is treated as the base ordering (usually most recent filing).
    """
    if not filing_statements:
        return pd.DataFrame(columns=META_COLUMNS)

    base = filing_statements[0].copy()
    # Ensure unique by concept_qname
    base = base.drop_duplicates(subset=["concept_qname"], keep="first").reset_index(drop=True)

    for df in filing_statements[1:]:
        df = df.drop_duplicates(subset=["concept_qname"], keep="first").copy()
        value_cols = [c for c in df.columns if c not in META_COLUMNS]
        if not value_cols:
            continue
        value_col = value_cols[0]

        # Add missing rows (concepts not in base)
        missing = df.loc[~df["concept_qname"].isin(base["concept_qname"])].copy()
        if not missing.empty:
            # Append at end to avoid destructive reordering
            missing_meta = missing[META_COLUMNS].copy()
            base = pd.concat([base, missing_meta], ignore_index=True)

        # Merge value column
        base = base.merge(
            df[["concept_qname", value_col]],
            on="concept_qname",
            how="left",
            suffixes=("", ""),
        )

        # Update label/depth from the first (most recent) filing; keep base as-is for fidelity.
        # If base label is missing, fill from df.
        for col in ["label", "depth", "preferred_label_role"]:
            if col in base.columns and col in df.columns:
                base[col] = base[col].where(base[col].notna() & (base[col] != ""), None)
                fill_map = df.set_index("concept_qname")[col].to_dict()
                base[col] = base.apply(
                    lambda r: r[col] if (r[col] is not None and r[col] is not pd.NA and r[col] != "") else fill_map.get(r["concept_qname"], r[col]),
                    axis=1,
                )

    return base


def build_readme_text() -> str:
    return "\n".join(
        [
            "SEC Reported Statements Export",
            "",
            "What this workbook is:",
            "- A filing-driven extraction of reported statement line items (not a normalized template).",
            "- Rows are driven by each filing’s XBRL presentation ordering when available.",
            "",
            "How it works (high level):",
            "- Maps ticker -> CIK using SEC’s public ticker list.",
            "- Pulls filing metadata from SEC submissions JSON.",
            "- For each 10-K / 10-Q filing, downloads the filing’s XBRL files from SEC Archives.",
            "- Parses XBRL instance facts + label linkbase + presentation linkbase.",
            "- Reconstructs the Income Statement, Balance Sheet, and Cash Flow Statement from presentation roles.",
            "",
            "Annual vs Quarterly:",
            "- Annual sheets are built from 10-K filings; quarterly sheets from 10-Q filings.",
            "- For income/cash flow statements, values in 10-Q filings are often year-to-date (YTD) as filed.",
            "  This tool preserves the filing-reported values and does not automatically derive true-quarter values.",
            "",
            "Fidelity notes / limitations:",
            "- Some filings have incomplete or unusual XBRL presentation structures; role classification is heuristic.",
            "- Labels are taken from the filing label linkbase when available; otherwise the concept name is used.",
            "- Rendered HTML/PDF statements may differ slightly from XBRL presentation ordering in some cases.",
            "- Dimensional facts (segments, products, geographies) are currently not expanded into separate rows.",
            "",
            "Auditability:",
            "- See 'Filing Index' for the accession numbers, dates, and URLs.",
            "- See 'Raw Facts / Debug' for extracted facts across filings.",
        ]
    )


def main():
    ap = argparse.ArgumentParser(description="Export SEC reported statements to Excel (filing-driven).")
    ap.add_argument("ticker", help="Stock ticker (e.g., AAPL)")
    ap.add_argument("--out-dir", default=".", help="Output directory")
    ap.add_argument("--cache-dir", default="sec_cache", help="Cache directory for SEC downloads")
    ap.add_argument("--max-10k", type=int, default=5, help="Number of annual 10-K filings to include")
    ap.add_argument(
        "--user-agent",
        default=None,
        help="SEC-compliant User-Agent string (recommended: 'YourApp/1.0 (email@example.com)')",
    )
    args = ap.parse_args()

    if args.user_agent:
        DEFAULT_CONFIG.user_agent = args.user_agent

    ticker = args.ticker.upper().strip()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ticker}_sec_reported_statements.xlsx"

    session = requests.Session()

    cik_resolver = TickerToCikResolver(session=session)
    cik_10 = cik_resolver.get_cik(ticker)

    index_builder = SecFilingIndexBuilder(session=session)
    filing_index_df, selected_filings = index_builder.build_index(cik_10, ticker, max_10k=args.max_10k)

    downloader = FilingDownloader(cache_root=args.cache_dir, session=session)
    xparser = XbrlParser()
    pres = PresentationResolver()
    extractor = StatementExtractor()

    annual_by_type: Dict[str, List[pd.DataFrame]] = {t: [] for t in STATEMENT_TYPES}
    quarterly_by_type: Dict[str, List[pd.DataFrame]] = {t: [] for t in STATEMENT_TYPES}

    raw_facts_all: List[pd.DataFrame] = []

    # Prefer ordering based on most recent filing of each category (10-K for annual, 10-Q for quarterly)
    tenk_filings = [f for f in selected_filings if f.form == "10-K"]
    tenq_filings = [f for f in selected_filings if f.form == "10-Q"]
    tenk_filings.sort(key=lambda f: f.filing_date, reverse=True)
    tenq_filings.sort(key=lambda f: f.filing_date, reverse=True)

    for filing in tqdm(tenk_filings + tenq_filings, desc="Processing filings"):
        idx_mask = filing_index_df["accession_number"] == filing.accession_number
        try:
            paths = downloader.download_filing_xbrl(cik_10, filing.accession_number, ticker)
            if not paths.instance_xml or not paths.presentation_xml:
                raise RuntimeError("Missing instance.xml or presentation.xml in filing directory.")

            facts_df, _ = xparser.parse_instance(paths.instance_xml)
            facts_df.insert(0, "accession_number", filing.accession_number)
            facts_df.insert(1, "form", filing.form)
            facts_df.insert(2, "report_period", filing.report_period)
            raw_facts_all.append(facts_df)

            label_map = {}
            if paths.labels_xml:
                label_map = xparser.parse_labels(paths.labels_xml)

            role_defs = {}
            if paths.schema_xsd:
                role_defs = xparser.parse_role_definitions_from_xsd(paths.schema_xsd)

            role_trees = pres.parse_presentation(paths.presentation_xml, role_defs=role_defs)
            role_classification = pres.classify_statement_roles(role_trees)

            extracted_any = False
            for stype in STATEMENT_TYPES:
                role_tree = _pick_best_role(role_trees, role_classification, stype)
                if not role_tree:
                    continue
                stmt = extractor.extract_statement_for_period(
                    statement_type=stype,
                    role_tree=role_tree,
                    label_map=label_map,
                    facts_df=facts_df,
                    period_end=filing.report_period,
                    form_type=filing.form,
                    filing_date=filing.filing_date,
                    accession_number=filing.accession_number,
                )

                period_col = filing.report_period
                out_df = stmt.rows_df[META_COLUMNS + ["value_raw"]].copy()
                out_df[period_col] = out_df["value_raw"].apply(lambda v: v)
                out_df = out_df.drop(columns=["value_raw"])

                if filing.form == "10-K":
                    annual_by_type[stype].append(out_df)
                else:
                    quarterly_by_type[stype].append(out_df)

                extracted_any = True

            filing_index_df.loc[idx_mask, "xbrl_parsed"] = True
            filing_index_df.loc[idx_mask, "statement_extraction_succeeded"] = extracted_any
        except Exception as e:
            filing_index_df.loc[idx_mask, "error"] = str(e)
            filing_index_df.loc[idx_mask, "statement_extraction_succeeded"] = False

    # Merge into historical views; base ordering from most recent filing per category
    # Ensure most recent filing first
    for stype in STATEMENT_TYPES:
        # Sort dfs by column (period) descending for consistent output
        def sort_key(df):
            cols = [c for c in df.columns if c not in META_COLUMNS]
            return cols[0] if cols else ""

        annual_by_type[stype].sort(key=sort_key, reverse=True)
        quarterly_by_type[stype].sort(key=sort_key, reverse=True)

    annual_combined = {t: _merge_statements_historical(annual_by_type[t]) for t in STATEMENT_TYPES}
    quarterly_combined = {t: _merge_statements_historical(quarterly_by_type[t]) for t in STATEMENT_TYPES}

    # Convert value columns to numeric where possible (keep blanks for non-reported)
    def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            if col in META_COLUMNS:
                continue
            df[col] = df[col].apply(lambda v: None if (v is None or v is pd.NA or v == "") else v)
        return df

    for t in STATEMENT_TYPES:
        annual_combined[t] = coerce_numeric(annual_combined[t])
        quarterly_combined[t] = coerce_numeric(quarterly_combined[t])

    raw_facts_df = pd.concat(raw_facts_all, ignore_index=True) if raw_facts_all else pd.DataFrame()

    writer = ExcelWriter()
    writer.write_workbook(
        out_path=str(out_path),
        annual_statements=annual_combined,
        quarterly_statements=quarterly_combined,
        filing_index_df=filing_index_df,
        raw_facts_df=raw_facts_df,
        readme_text=build_readme_text(),
    )

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()

