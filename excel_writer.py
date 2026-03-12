"""
Write reported statements and supporting tabs to an Excel workbook.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd


META_COLUMNS = ["concept_qname", "label", "depth", "preferred_label_role"]


def _to_number_or_nan(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return x
    s = str(x).strip().replace(",", "")
    if s in ("", "—", "-", "–"):
        return None
    # Parentheses for negatives
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None


class ExcelWriter:
    def __init__(self):
        pass

    def write_workbook(
        self,
        out_path: str,
        annual_statements: Dict[str, pd.DataFrame],
        quarterly_statements: Dict[str, pd.DataFrame],
        filing_index_df: pd.DataFrame,
        raw_facts_df: pd.DataFrame,
        readme_text: str,
    ) -> None:
        """
        annual_statements keys: income/balance/cashflow
        quarterly_statements keys: income/balance/cashflow
        """
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            workbook = writer.book
            header_fmt = workbook.add_format({"bold": True, "bg_color": "#F2F2F2"})
            indent_fmts = [workbook.add_format({"indent": i}) for i in range(0, 9)]
            num_fmt = workbook.add_format({"num_format": "#,##0"})
            num_fmt_dec = workbook.add_format({"num_format": "#,##0.00"})

            def write_df(sheet_name: str, df: pd.DataFrame, freeze_row: int = 1, freeze_col: int = 4):
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                ws = writer.sheets[sheet_name]
                ws.freeze_panes(freeze_row, freeze_col)

                # Bold header row
                for col_idx, col_name in enumerate(df.columns):
                    ws.write(0, col_idx, col_name, header_fmt)

                # column widths
                for col_idx, col_name in enumerate(df.columns):
                    width = 14
                    if col_name == "label":
                        width = 55
                    elif col_name == "concept_qname":
                        width = 38
                    elif col_name in ("depth",):
                        width = 8
                    elif col_name in ("preferred_label_role",):
                        width = 26
                    ws.set_column(col_idx, col_idx, width)

                # Apply indentation on label
                if "depth" in df.columns and "label" in df.columns:
                    label_col = list(df.columns).index("label")
                    depth_col = list(df.columns).index("depth")
                    for row_i in range(1, len(df) + 1):
                        depth = df.iloc[row_i - 1, depth_col]
                        try:
                            d = int(depth)
                        except Exception:
                            d = 0
                        d = max(0, min(d, 8))
                        ws.write(row_i, label_col, df.iloc[row_i - 1, label_col], indent_fmts[d])

                # Numeric formatting for period columns
                for col_idx, col_name in enumerate(df.columns):
                    if col_name in META_COLUMNS:
                        continue
                    ws.set_column(col_idx, col_idx, 18, num_fmt)

            # Annual sheets
            write_df("Annual Income Statement", annual_statements["income"])
            write_df("Annual Balance Sheet", annual_statements["balance"])
            write_df("Annual Cash Flow", annual_statements["cashflow"])

            # Quarterly sheets
            write_df("Quarterly Income Statement", quarterly_statements["income"])
            write_df("Quarterly Balance Sheet", quarterly_statements["balance"])
            write_df("Quarterly Cash Flow", quarterly_statements["cashflow"])

            # Filing index
            filing_index_df.to_excel(writer, sheet_name="Filing Index", index=False)
            ws = writer.sheets["Filing Index"]
            ws.freeze_panes(1, 0)
            for col_idx, col_name in enumerate(filing_index_df.columns):
                ws.write(0, col_idx, col_name, header_fmt)
                ws.set_column(col_idx, col_idx, 20)

            # Raw facts
            # Excel sheet names cannot contain "/" so we approximate the requested
            # "Raw Facts / Debug" name with a dash instead.
            raw_facts_df.to_excel(writer, sheet_name="Raw Facts - Debug", index=False)
            ws = writer.sheets["Raw Facts - Debug"]
            ws.freeze_panes(1, 0)
            for col_idx, col_name in enumerate(raw_facts_df.columns):
                ws.write(0, col_idx, col_name, header_fmt)
                ws.set_column(col_idx, col_idx, 22)

            # README tab
            readme_df = pd.DataFrame({"README": readme_text.splitlines()})
            readme_df.to_excel(writer, sheet_name="README", index=False)
            ws = writer.sheets["README"]
            ws.set_column(0, 0, 120)
            ws.freeze_panes(1, 0)
            ws.write(0, 0, "README", header_fmt)

