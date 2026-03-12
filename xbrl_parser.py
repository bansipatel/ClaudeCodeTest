"""
Parse filing XBRL instance, label linkbase, and schema role definitions.

Goal: capture enough structure to reconstruct statement presentation and
extract values for the right periods, with high auditability.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from lxml import etree


NS = {
    "xbrli": "http://www.xbrl.org/2003/instance",
    "link": "http://www.xbrl.org/2003/linkbase",
    "xlink": "http://www.w3.org/1999/xlink",
    "xl": "http://www.xbrl.org/2003/XLink",
}


@dataclass
class ContextPeriod:
    context_id: str
    period_type: str  # "instant" or "duration"
    instant: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class FactRow:
    concept: str
    concept_qname: str
    namespace: str
    local_name: str
    value: Optional[str]
    unit_ref: Optional[str]
    decimals: Optional[str]
    context_id: str
    period_type: str
    instant: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    is_nil: bool


class XbrlParser:
    def __init__(self):
        pass

    def parse_instance(self, instance_path: Path) -> Tuple[pd.DataFrame, Dict[str, ContextPeriod]]:
        """
        Returns (facts_df, contexts) for audit + downstream extraction.
        """
        tree = etree.parse(str(instance_path))
        root = tree.getroot()

        contexts: Dict[str, ContextPeriod] = {}
        for ctx in root.findall(".//xbrli:context", namespaces=NS):
            ctx_id = ctx.get("id")
            if not ctx_id:
                continue
            period_el = ctx.find("./xbrli:period", namespaces=NS)
            if period_el is None:
                continue
            instant_el = period_el.find("./xbrli:instant", namespaces=NS)
            if instant_el is not None and instant_el.text:
                contexts[ctx_id] = ContextPeriod(
                    context_id=ctx_id,
                    period_type="instant",
                    instant=instant_el.text.strip(),
                )
                continue
            start_el = period_el.find("./xbrli:startDate", namespaces=NS)
            end_el = period_el.find("./xbrli:endDate", namespaces=NS)
            if start_el is not None and end_el is not None and start_el.text and end_el.text:
                contexts[ctx_id] = ContextPeriod(
                    context_id=ctx_id,
                    period_type="duration",
                    start_date=start_el.text.strip(),
                    end_date=end_el.text.strip(),
                )

        fact_rows: List[FactRow] = []
        for el in root.iterchildren():
            # Skip non-facts
            if not isinstance(el.tag, str):
                continue
            if el.tag in (
                f"{{{NS['xbrli']}}}context",
                f"{{{NS['xbrli']}}}unit",
                f"{{{NS['xbrli']}}}schemaRef",
            ):
                continue

            context_id = el.get("contextRef")
            if not context_id:
                continue

            qname = el.tag  # {namespace}local
            if qname.startswith("{") and "}" in qname:
                namespace, local = qname[1:].split("}", 1)
            else:
                namespace, local = "", qname

            period = contexts.get(context_id)
            period_type = period.period_type if period else ""

            is_nil = el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true"
            value = None if is_nil else (el.text.strip() if el.text is not None else None)

            # Use the local element name as the common concept key. This is a practical
            # compromise that matches how presentation href fragments are reduced and
            # works well for statement rows, while still preserving full namespace info
            # separately for debugging.
            concept_key = local

            fact_rows.append(
                FactRow(
                    concept=f"{namespace}:{local}",
                    concept_qname=concept_key,
                    namespace=namespace,
                    local_name=local,
                    value=value,
                    unit_ref=el.get("unitRef"),
                    decimals=el.get("decimals"),
                    context_id=context_id,
                    period_type=period_type,
                    instant=period.instant if period else None,
                    start_date=period.start_date if period else None,
                    end_date=period.end_date if period else None,
                    is_nil=is_nil,
                )
            )

        facts_df = pd.DataFrame([fr.__dict__ for fr in fact_rows])
        return facts_df, contexts

    def parse_labels(self, labels_path: Path) -> Dict[Tuple[str, str], str]:
        """
        Map (concept_qname, label_role_uri) -> label text.
        """
        tree = etree.parse(str(labels_path))
        root = tree.getroot()

        # Locate label resources and locators inside labelLink(s)
        # Build locator label -> concept href
        loc_to_href: Dict[str, str] = {}
        for loc in root.findall(".//link:labelLink/link:loc", namespaces=NS):
            loc_label = loc.get(f"{{{NS['xlink']}}}label")
            href = loc.get(f"{{{NS['xlink']}}}href")
            if loc_label and href:
                loc_to_href[loc_label] = href

        # Build label resources: label id -> (role, text)
        label_res: Dict[str, Tuple[str, str]] = {}
        for lab in root.findall(".//link:labelLink/link:label", namespaces=NS):
            lab_label = lab.get(f"{{{NS['xlink']}}}label")
            role = lab.get(f"{{{NS['xlink']}}}role") or "http://www.xbrl.org/2003/role/label"
            text = "".join(lab.itertext()).strip()
            if lab_label and text:
                label_res[lab_label] = (role, text)

        # Arcs connect loc -> label resource
        mapping: Dict[Tuple[str, str], str] = {}
        for arc in root.findall(".//link:labelLink/link:labelArc", namespaces=NS):
            from_loc = arc.get(f"{{{NS['xlink']}}}from")
            to_lab = arc.get(f"{{{NS['xlink']}}}to")
            if not from_loc or not to_lab:
                continue
            href = loc_to_href.get(from_loc)
            if not href:
                continue
            # href like "schema.xsd#us-gaap_Assets"
            fragment = href.split("#", 1)[-1]
            # Reduce to local name (after first underscore) so it matches the
            # concept key used in facts and presentation.
            if "_" in fragment:
                concept_key = fragment.split("_", 1)[-1]
            else:
                concept_key = fragment
            role, text = label_res.get(to_lab, (None, None))
            if role and text:
                mapping[(concept_key, role)] = text

        return mapping

    def parse_role_definitions_from_xsd(self, schema_xsd_path: Path) -> Dict[str, str]:
        """
        Map roleURI -> definition text (when present).
        """
        tree = etree.parse(str(schema_xsd_path))
        root = tree.getroot()
        role_defs: Dict[str, str] = {}

        # Role types live under link:roleType; schema often includes link namespace
        for role_type in root.findall(".//{http://www.xbrl.org/2003/linkbase}roleType"):
            role_uri = role_type.get("roleURI")
            def_el = role_type.find("./{http://www.xbrl.org/2003/linkbase}definition")
            if role_uri and def_el is not None and def_el.text:
                role_defs[role_uri] = def_el.text.strip()
        return role_defs

