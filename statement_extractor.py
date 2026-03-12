"""
Extract statement views from a single filing using its presentation tree.

We intentionally avoid heavy normalization. Rows are driven by the filing's
presentation ordering and labels when available.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from presentation_resolver import PresentationRoleTree, PresentationNode


DEFAULT_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
TERSE_LABEL_ROLE = "http://www.xbrl.org/2003/role/terseLabel"
TOTAL_LABEL_ROLE = "http://www.xbrl.org/2003/role/totalLabel"
PERIOD_START_ROLE = "http://www.xbrl.org/2003/role/periodStartLabel"
PERIOD_END_ROLE = "http://www.xbrl.org/2003/role/periodEndLabel"


@dataclass
class FilingStatement:
    statement_type: str  # income/balance/cashflow
    role_uri: str
    role_name: str
    period_end: str
    form_type: str
    filing_date: str
    accession_number: str
    rows_df: pd.DataFrame  # ordered rows with metadata + a 'value' column


class StatementExtractor:
    def __init__(self):
        pass

    def _best_label(
        self,
        concept_qname: str,
        label_map: Dict[Tuple[str, str], str],
        preferred_role: Optional[str],
    ) -> str:
        for role in [preferred_role, DEFAULT_LABEL_ROLE, TERSE_LABEL_ROLE, TOTAL_LABEL_ROLE]:
            if not role:
                continue
            v = label_map.get((concept_qname, role))
            if v:
                return v
        # fallback to concept qname
        return concept_qname

    def extract_statement_for_period(
        self,
        statement_type: str,
        role_tree: PresentationRoleTree,
        label_map: Dict[Tuple[str, str], str],
        facts_df: pd.DataFrame,
        period_end: str,
        form_type: str,
        filing_date: str,
        accession_number: str,
    ) -> FilingStatement:
        """
        For a filing, extract a single statement (one role tree) for the given period_end.

        Selection of facts:
        - balance: use instant == period_end
        - income/cashflow: use duration end_date == period_end
        If multiple facts match a concept for that period, we keep the first non-null.
        """
        if statement_type == "balance":
            period_mask = (facts_df["period_type"] == "instant") & (facts_df["instant"] == period_end)
        else:
            period_mask = (facts_df["period_type"] == "duration") & (facts_df["end_date"] == period_end)

        period_facts = facts_df.loc[period_mask].copy()

        # Fallback: if no facts match the exact report_period, fall back to the
        # latest available period of the appropriate type within this filing.
        if period_facts.empty:
            if statement_type == "balance":
                instants = facts_df.loc[facts_df["period_type"] == "instant", "instant"].dropna()
                if not instants.empty:
                    latest = instants.max()
                    period_facts = facts_df.loc[
                        (facts_df["period_type"] == "instant") & (facts_df["instant"] == latest)
                    ].copy()
            else:
                ends = facts_df.loc[facts_df["period_type"] == "duration", "end_date"].dropna()
                if not ends.empty:
                    latest = ends.max()
                    period_facts = facts_df.loc[
                        (facts_df["period_type"] == "duration") & (facts_df["end_date"] == latest)
                    ].copy()

        # build concept->value selection
        concept_to_value: Dict[str, Optional[str]] = {}
        if not period_facts.empty:
            # Prefer non-nil and numeric-looking first, but keep raw strings
            for _, row in period_facts.iterrows():
                cq = row["concept_qname"]
                if cq in concept_to_value and concept_to_value[cq] not in (None, ""):
                    continue
                val = row["value"]
                if val is None or val == "":
                    continue
                concept_to_value[cq] = val

        rows: List[dict] = []
        seen = set()
        for node in role_tree.nodes_in_order:
            cq = node.concept_qname
            if cq in seen:
                continue
            seen.add(cq)
            label = self._best_label(cq, label_map, node.preferred_label_role)
            raw_val = concept_to_value.get(cq)
            if raw_val is not None:
                try:
                    raw_val = float(raw_val)
                except (ValueError, TypeError):
                    pass  # keep as string for non-numeric facts

            rows.append(
                {
                    "concept_qname": cq,
                    "label": label,
                    "depth": node.depth,
                    "preferred_label_role": node.preferred_label_role or "",
                    "value_raw": raw_val,
                }
            )

        df = pd.DataFrame(rows)
        return FilingStatement(
            statement_type=statement_type,
            role_uri=role_tree.role_uri,
            role_name=role_tree.role_name,
            period_end=period_end,
            form_type=form_type,
            filing_date=filing_date,
            accession_number=accession_number,
            rows_df=df,
        )

