"""
Parse and resolve XBRL presentation linkbase to an ordered tree per role.

We aim for filing fidelity: keep the filing's arc ordering and indentation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from lxml import etree

from config import (
    INCOME_STATEMENT_KEYWORDS,
    BALANCE_SHEET_KEYWORDS,
    CASH_FLOW_STATEMENT_KEYWORDS,
)


NS = {
    "link": "http://www.xbrl.org/2003/linkbase",
    "xlink": "http://www.w3.org/1999/xlink",
}


@dataclass
class PresentationNode:
    concept_qname: str  # like us-gaap_Assets or dei_EntityRegistrantName
    parent_qname: Optional[str]
    order: float
    preferred_label_role: Optional[str]
    depth: int


@dataclass
class PresentationRoleTree:
    role_uri: str
    role_name: str
    nodes_in_order: List[PresentationNode]


class PresentationResolver:
    def parse_presentation(
        self, pre_path: Path, role_defs: Optional[Dict[str, str]] = None
    ) -> Dict[str, PresentationRoleTree]:
        """
        Returns role_uri -> PresentationRoleTree.
        """
        tree = etree.parse(str(pre_path))
        root = tree.getroot()

        role_defs = role_defs or {}
        result: Dict[str, PresentationRoleTree] = {}

        for plink in root.findall(".//link:presentationLink", namespaces=NS):
            role_uri = plink.get(f"{{{NS['xlink']}}}role")
            if not role_uri:
                continue

            # role display name: prefer schema definitions; fallback to xlink:title
            role_name = role_defs.get(role_uri) or plink.get(f"{{{NS['xlink']}}}title") or role_uri

            # Map loc label -> concept key (from href fragment, reduced to local name)
            loc_label_to_concept: Dict[str, str] = {}
            for loc in plink.findall("./link:loc", namespaces=NS):
                loc_label = loc.get(f"{{{NS['xlink']}}}label")
                href = loc.get(f"{{{NS['xlink']}}}href")
                if not loc_label or not href:
                    continue
                fragment = href.split("#", 1)[-1]
                if "_" in fragment:
                    concept_key = fragment.split("_", 1)[-1]
                else:
                    concept_key = fragment
                loc_label_to_concept[loc_label] = concept_key

            # Build adjacency list: parent -> [(child, order, preferredLabelRole)]
            children: Dict[str, List[Tuple[str, float, Optional[str]]]] = {}
            parents: Dict[str, str] = {}

            for arc in plink.findall("./link:presentationArc", namespaces=NS):
                frm = arc.get(f"{{{NS['xlink']}}}from")
                to = arc.get(f"{{{NS['xlink']}}}to")
                if not frm or not to:
                    continue
                parent = loc_label_to_concept.get(frm)
                child = loc_label_to_concept.get(to)
                if not parent or not child:
                    continue
                order_str = arc.get("order") or "0"
                try:
                    order = float(order_str)
                except ValueError:
                    order = 0.0
                preferred = arc.get("preferredLabel")
                children.setdefault(parent, []).append((child, order, preferred))
                parents[child] = parent

            for p, lst in children.items():
                lst.sort(key=lambda t: (t[1], t[0]))

            # Roots = nodes that appear as parents but not as children
            all_parents = set(children.keys())
            all_children = set(parents.keys())
            roots = sorted(list(all_parents - all_children))
            if not roots:
                # fallback: choose any parent as root
                roots = sorted(list(all_parents))[:1]

            nodes_in_order: List[PresentationNode] = []

            def walk(node: str, depth: int) -> None:
                for (child, ordv, pref) in children.get(node, []):
                    nodes_in_order.append(
                        PresentationNode(
                            concept_qname=child,
                            parent_qname=node,
                            order=ordv,
                            preferred_label_role=pref,
                            depth=depth,
                        )
                    )
                    walk(child, depth + 1)

            for r in roots:
                # include root's children; root itself is often an abstract line like "Statement - ..."
                walk(r, 0)

            result[role_uri] = PresentationRoleTree(
                role_uri=role_uri,
                role_name=role_name,
                nodes_in_order=nodes_in_order,
            )

        return result

    # URI suffixes that indicate note/detail/table roles — exclude from primary classification
    _DETAIL_SUFFIXES = (
        "details", "detail", "tables", "table", "policies", "policy",
        "parenthetical", "narrative", "rollforward", "rollforward",
    )

    def classify_statement_roles(self, role_trees: Dict[str, PresentationRoleTree]) -> Dict[str, str]:
        """
        Returns role_uri -> statement_type ("income"|"balance"|"cashflow"|"" for unknown).
        Only roles whose URI or definition clearly identifies them as primary financial statements
        are classified; note/detail/table roles are excluded.
        """
        out: Dict[str, str] = {}
        for role_uri, tree in role_trees.items():
            name = (tree.role_name or "").lower()
            uri_lower = role_uri.lower()
            uri_last = uri_lower.rstrip("/").split("/")[-1]

            # Skip note / detail / table roles
            if any(uri_last.endswith(sfx) for sfx in self._DETAIL_SUFFIXES):
                out[role_uri] = ""
                continue

            # Roles that are clearly NOT the primary income statement
            _is_equity = any(x in uri_last for x in ("equity", "stockholders", "shareholders", "comprehensiveincome", "comprehensiveloss"))
            _is_cashflow_uri = any(x in uri_last for x in ("cashflow", "cashflows", "cash_flow", "cashactivities"))

            # Normalise "statements" -> "statement" for keyword matching
            text = f"{name} {uri_lower}".replace("statements", "statement")
            stype = ""
            if _is_cashflow_uri:
                stype = "cashflow"
            elif not _is_equity and any(k in text for k in INCOME_STATEMENT_KEYWORDS):
                stype = "income"
            elif any(k in text for k in BALANCE_SHEET_KEYWORDS):
                stype = "balance"
            elif any(k in text for k in CASH_FLOW_STATEMENT_KEYWORDS):
                stype = "cashflow"

            # URI token heuristics for camelCase role names (no spaces)
            if not stype and not _is_equity:
                if any(x in uri_last for x in (
                    "statementsofoperations", "statementofoperations",
                    "statementsofearnings", "statementofearnings",
                    "statementsofincome", "statementofincome",
                )):
                    stype = "income"
                elif any(x in uri_last for x in ("balancesheets", "balancesheet", "financialposition")):
                    stype = "balance"

            out[role_uri] = stype
        return out

