"""
Download filing-specific XBRL files from SEC Archives for a given accession.

We rely on the filing directory index JSON:
https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_nodashes}/index.json
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

from config import DEFAULT_CONFIG, RateLimiter


SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"


@dataclass
class FilingXbrlPaths:
    directory: Path
    index_json_path: Path
    instance_xml: Optional[Path]
    presentation_xml: Optional[Path]
    labels_xml: Optional[Path]
    schema_xsd: Optional[Path]
    calculation_xml: Optional[Path]


class FilingDownloader:
    def __init__(self, cache_root: str = "sec_cache", session: Optional[requests.Session] = None):
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.session = session or requests.Session()
        self._rate_limiter = RateLimiter(DEFAULT_CONFIG.min_request_interval_seconds)

    def _sec_headers(self) -> dict:
        return {
            "User-Agent": DEFAULT_CONFIG.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        }

    def _get_json(self, url: str) -> dict:
        self._rate_limiter.wait()
        resp = self.session.get(url, headers=self._sec_headers(), timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _download(self, url: str, out_path: Path) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.exists() and out_path.stat().st_size > 0:
            return out_path
        self._rate_limiter.wait()
        with self.session.get(url, headers=self._sec_headers(), stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
        return out_path

    def download_filing_xbrl(
        self, cik_10: str, accession_number: str, ticker: str
    ) -> FilingXbrlPaths:
        cik_int = str(int(cik_10))
        acc_nodash = accession_number.replace("-", "")

        filing_dir = self.cache_root / ticker.upper() / acc_nodash
        filing_dir.mkdir(parents=True, exist_ok=True)

        index_url = f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{acc_nodash}/index.json"
        index_json_path = filing_dir / "index.json"
        if not index_json_path.exists():
            data = self._get_json(index_url)
            index_json_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        else:
            data = json.loads(index_json_path.read_text(encoding="utf-8"))

        items = data.get("directory", {}).get("item", [])
        filenames = [it.get("name", "") for it in items]

        def pick_one(predicate):
            for fn in filenames:
                if predicate(fn):
                    return fn
            return None

        # Prefer the extracted XBRL instance (_htm.xml) produced from inline XBRL filings
        instance_fn = pick_one(lambda f: f.lower().endswith("_htm.xml"))
        # Fallback: any XML that isn't a linkbase, schema, or known non-instance file
        if not instance_fn:
            instance_fn = pick_one(
                lambda f: f.lower().endswith(".xml")
                and not f.lower().endswith(("_pre.xml", "_cal.xml", "_lab.xml", "_def.xml", "_ref.xml"))
                and "pre" not in f.lower()
                and "cal" not in f.lower()
                and "lab" not in f.lower()
                and "def" not in f.lower()
                and "ref" not in f.lower()
                and "summary" not in f.lower()
                and "filing" not in f.lower()
            )
        pre_fn = pick_one(lambda f: f.lower().endswith(("_pre.xml", "pre.xml")))
        lab_fn = pick_one(lambda f: f.lower().endswith(("_lab.xml", "lab.xml")))
        cal_fn = pick_one(lambda f: f.lower().endswith(("_cal.xml", "cal.xml")))
        xsd_fn = pick_one(lambda f: f.lower().endswith(".xsd"))

        def url_for(fn: str) -> str:
            return f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{acc_nodash}/{fn}"

        instance_path = (
            self._download(url_for(instance_fn), filing_dir / instance_fn) if instance_fn else None
        )
        pre_path = self._download(url_for(pre_fn), filing_dir / pre_fn) if pre_fn else None
        lab_path = self._download(url_for(lab_fn), filing_dir / lab_fn) if lab_fn else None
        xsd_path = self._download(url_for(xsd_fn), filing_dir / xsd_fn) if xsd_fn else None
        cal_path = self._download(url_for(cal_fn), filing_dir / cal_fn) if cal_fn else None

        return FilingXbrlPaths(
            directory=filing_dir,
            index_json_path=index_json_path,
            instance_xml=instance_path,
            presentation_xml=pre_path,
            labels_xml=lab_path,
            schema_xsd=xsd_path,
            calculation_xml=cal_path,
        )

