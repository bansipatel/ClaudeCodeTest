"""
job_scraper.py — Job Search Automation for Bansi Patel
Targets investment roles (credit, HY, private credit, distressed, PE) within 4-hr radius of NJ + remote.
Outputs job_tracker.csv for import into job_search.html.

Usage:
    pip install -r requirements.txt
    python job_scraper.py
"""

import csv
import json
import random
import re
import time
import logging
from datetime import date, datetime, timedelta
from urllib.parse import urlencode, quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 15

SEARCH_TERMS = [
    'Credit Analyst',
    'High Yield Analyst',
    'Private Credit Analyst',
    'Investment Analyst',
    'Leveraged Finance Analyst',
    'Direct Lending Analyst',
    'Distressed Debt Analyst',
    'Fixed Income Analyst',
    'Portfolio Analyst',
]

LOCATIONS = [
    'New York, NY',
    'New Jersey',
    'Connecticut',
    'Westchester, NY',
    'Philadelphia, PA',
    'Boston, MA',
    'Washington, DC',
    'Baltimore, MD',
    'Remote',
]

# Greenhouse board slugs → firm name
GREENHOUSE_FIRMS = {
    'ares-management':       'Ares Management',
    'apollo':                'Apollo Global Management',
    'blackstone':            'Blackstone',
    'kkr':                   'KKR',
    'carlyle':               'Carlyle Group',
    'sixthstreet':           'Sixth Street',
    'blueowl':               'Blue Owl Capital',
    'hpsinvestment':         'HPS Investment Partners',
    'golubcapital':          'Golub Capital',
    'monroecapital':         'Monroe Capital',
    'oaktree':               'Oaktree Capital',
    'benefitstreetpartners': 'Benefit Street Partners',
    'sculptor':              'Sculptor Capital',
    'pimco':                 'PIMCO',
    'voya':                  'Voya Financial',
}

# Lever slugs → firm name
LEVER_FIRMS = {
    'pinebridge-investments': 'PineBridge Investments',
}

# Firms without reliable JSON APIs — scraped via HTML fallback
GENERIC_FIRMS = [
    'Marathon Asset Management',
    'GoldenTree Asset Management',
    'Silver Point Capital',
    'Sound Point Capital',
    'DDJ Capital Management',
    'Polen Capital',
    'Shenkman Capital',
    'Barings',
    'Angelo Gordon',
    'Octagon Credit Investors',
    'Canyon Partners',
    'MetLife Investment Management',
]

FIT_KEYWORDS = {
    'credit': 2,
    'high yield': 2,
    'leveraged': 2,
    'private credit': 2,
    'direct lending': 2,
    'distressed': 2,
    'fixed income': 2,
    'cfa': 1,
    'debt': 1,
    'loan': 1,
    'bond': 1,
    'analyst': 1,
    'investment': 1,
    'portfolio': 1,
    'underwriting': 1,
}

CSV_COLS = [
    'id', 'company', 'roleTitle', 'location', 'datePosted', 'dateApplied',
    'applicationLink', 'status', 'fitScore', 'probabilityScore', 'priorityTier',
    'yearsExpMin', 'yearsExpMax', 'contactName', 'contactLinkedIn', 'referral',
    'followUpDate', 'lastActivityDate', 'interviewStage', 'notes',
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sleep_between_requests():
    time.sleep(random.uniform(1.5, 3.5))

def _sleep_between_firms():
    time.sleep(random.uniform(4.0, 8.0))

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/123.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    })
    return s

def _today() -> str:
    return date.today().isoformat()

def _parse_relative_date(text: str) -> str:
    """Convert '3 days ago', 'Posted 2 weeks ago', etc. → YYYY-MM-DD."""
    if not text:
        return ''
    text = text.lower().strip()
    today = date.today()
    m = re.search(r'(\d+)\s*(day|week|month|hour|minute)', text)
    if not m:
        # Try to parse as an absolute date
        for fmt in ('%Y-%m-%d', '%B %d, %Y', '%b %d, %Y', '%m/%d/%Y'):
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except ValueError:
                pass
        return _today()
    qty, unit = int(m.group(1)), m.group(2)
    if unit.startswith('hour') or unit.startswith('minute'):
        return today.isoformat()
    if unit.startswith('day'):
        return (today - timedelta(days=qty)).isoformat()
    if unit.startswith('week'):
        return (today - timedelta(weeks=qty)).isoformat()
    if unit.startswith('month'):
        return (today - timedelta(days=qty * 30)).isoformat()
    return today.isoformat()

def _unique_id(company: str, title: str, location: str) -> str:
    import hashlib
    raw = f"{company.lower()}|{title.lower()}|{location.lower()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]

# ---------------------------------------------------------------------------
# Fit Scorer
# ---------------------------------------------------------------------------

class FitScorer:
    def score(self, title: str, description: str = '') -> dict:
        text = f"{title} {description}".lower()
        fit = 0
        for kw, weight in FIT_KEYWORDS.items():
            if kw in text:
                fit += weight
        fit = min(10, fit)

        # Probability starts at 5, adjusted heuristically
        prob = 5
        if fit >= 7:
            prob += 2
        elif fit >= 5:
            prob += 1
        elif fit <= 2:
            prob -= 2

        # Seniority penalty/boost
        seniority_text = title.lower()
        if any(w in seniority_text for w in ['senior', 'vp', 'director', 'managing', 'head of', 'principal']):
            prob -= 1  # likely too senior
        if any(w in seniority_text for w in ['associate', 'analyst', 'junior']):
            prob += 1  # right level

        prob = max(1, min(10, prob))

        if fit >= 7 or prob >= 7:
            tier = 'High'
        elif fit <= 3 and prob <= 4:
            tier = 'Low'
        else:
            tier = 'Medium'

        return {
            'fitScore': fit,
            'probabilityScore': prob,
            'priorityTier': tier,
        }

_scorer = FitScorer()

def _base_job(company: str, title: str, location: str, link: str, posted: str, description: str = '') -> dict:
    scored = _scorer.score(title, description)
    uid = _unique_id(company, title, location)
    return {
        'id': uid,
        'company': company,
        'roleTitle': title,
        'location': location,
        'datePosted': posted or _today(),
        'dateApplied': '',
        'applicationLink': link,
        'status': 'Not Applied',
        'fitScore': scored['fitScore'],
        'probabilityScore': scored['probabilityScore'],
        'priorityTier': scored['priorityTier'],
        'yearsExpMin': '',
        'yearsExpMax': '',
        'contactName': '',
        'contactLinkedIn': '',
        'referral': False,
        'followUpDate': '',
        'lastActivityDate': _today(),
        'interviewStage': '',
        'notes': '',
        # internal only
        '_description': description,
    }

# ---------------------------------------------------------------------------
# Greenhouse Scraper (JSON API)
# ---------------------------------------------------------------------------

class GreenhouseScraper:
    BASE = 'https://boards-api.greenhouse.io/v1/boards/{slug}/jobs'

    def scrape(self, session: requests.Session) -> list[dict]:
        results = []
        for slug, firm_name in GREENHOUSE_FIRMS.items():
            url = self.BASE.format(slug=slug)
            log.info(f'Greenhouse: {firm_name} ({slug})')
            try:
                r = session.get(url, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.warning(f'  Skipping {firm_name}: {exc}')
                _sleep_between_firms()
                continue

            jobs_raw = data.get('jobs', [])
            for j in jobs_raw:
                title = j.get('title', '')
                location_obj = j.get('location', {})
                location = location_obj.get('name', '') if isinstance(location_obj, dict) else str(location_obj)
                link = j.get('absolute_url', '')
                updated = j.get('updated_at', '')
                posted = updated[:10] if updated else _today()

                if not self._is_relevant(title, location):
                    continue

                results.append(_base_job(firm_name, title, location, link, posted))
                log.info(f'  + {title} @ {location}')

            _sleep_between_firms()
        return results

    def _is_relevant(self, title: str, location: str) -> bool:
        title_lower = title.lower()
        loc_lower = location.lower()
        has_term = any(t.lower() in title_lower for t in SEARCH_TERMS)
        if not has_term:
            # Also accept broad investment/finance terms
            broad = ['analyst', 'associate', 'credit', 'fixed income', 'investment', 'portfolio', 'debt']
            has_term = any(b in title_lower for b in broad)
        if not has_term:
            return False
        # Location filter (or remote)
        if 'remote' in loc_lower or location == '':
            return True
        loc_keywords = ['new york', 'ny', 'new jersey', 'nj', 'connecticut', 'ct',
                        'westchester', 'philadelphia', 'pa', 'boston', 'ma',
                        'washington', 'dc', 'baltimore', 'md']
        return any(k in loc_lower for k in loc_keywords)

# ---------------------------------------------------------------------------
# Lever Scraper (JSON API)
# ---------------------------------------------------------------------------

class LeverScraper:
    BASE = 'https://api.lever.co/v0/postings/{slug}?mode=json'

    def scrape(self, session: requests.Session) -> list[dict]:
        results = []
        for slug, firm_name in LEVER_FIRMS.items():
            url = self.BASE.format(slug=slug)
            log.info(f'Lever: {firm_name} ({slug})')
            try:
                r = session.get(url, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.warning(f'  Skipping {firm_name}: {exc}')
                _sleep_between_firms()
                continue

            for j in data:
                title = j.get('text', '')
                categories = j.get('categories', {})
                location = categories.get('location', '') if isinstance(categories, dict) else ''
                link = j.get('hostedUrl', '')
                created_ts = j.get('createdAt', 0)
                if created_ts:
                    posted = datetime.fromtimestamp(created_ts / 1000).date().isoformat()
                else:
                    posted = _today()

                desc_obj = j.get('descriptionPlain', '') or j.get('description', '')
                desc = BeautifulSoup(desc_obj, 'html.parser').get_text(' ') if '<' in str(desc_obj) else str(desc_obj)

                if not self._is_relevant(title, location):
                    continue

                results.append(_base_job(firm_name, title, location, link, posted, desc[:500]))
                log.info(f'  + {title} @ {location}')

            _sleep_between_firms()
        return results

    def _is_relevant(self, title: str, location: str) -> bool:
        title_lower = title.lower()
        loc_lower = location.lower()
        broad = ['analyst', 'associate', 'credit', 'fixed income', 'investment', 'portfolio', 'debt',
                 'high yield', 'private credit', 'leveraged', 'distressed']
        has_term = any(b in title_lower for b in broad)
        if not has_term:
            return False
        if 'remote' in loc_lower or location == '':
            return True
        loc_keywords = ['new york', 'ny', 'new jersey', 'nj', 'connecticut', 'ct',
                        'westchester', 'philadelphia', 'pa', 'boston', 'ma',
                        'washington', 'dc', 'baltimore', 'md']
        return any(k in loc_lower for k in loc_keywords)

# ---------------------------------------------------------------------------
# Indeed Scraper (HTML)
# ---------------------------------------------------------------------------

class IndeedScraper:
    BASE = 'https://www.indeed.com/jobs'

    def scrape(self, session: requests.Session) -> list[dict]:
        results = []
        # Sample a subset to avoid hitting rate limits too hard
        terms_sample = SEARCH_TERMS[:5]
        locs_sample = ['New York, NY', 'New Jersey', 'Remote']

        for term in terms_sample:
            for loc in locs_sample:
                params = {'q': term, 'l': loc, 'sort': 'date', 'fromage': '14'}
                url = f"{self.BASE}?{urlencode(params)}"
                log.info(f'Indeed: "{term}" in "{loc}"')
                try:
                    r = session.get(url, timeout=REQUEST_TIMEOUT)
                    # CAPTCHA check
                    if 'captcha' in r.url.lower() or r.status_code == 403:
                        log.warning('  Indeed: CAPTCHA / blocked, skipping')
                        _sleep_between_firms()
                        continue
                    soup = BeautifulSoup(r.text, 'html.parser')
                    batch = self._parse(soup, term)
                    results.extend(batch)
                    log.info(f'  Found {len(batch)} relevant jobs')
                except Exception as exc:
                    log.warning(f'  Indeed error: {exc}')
                _sleep_between_requests()
            _sleep_between_firms()
        return results

    def _parse(self, soup: BeautifulSoup, search_term: str) -> list[dict]:
        results = []
        # Indeed uses JS-rendered content; try static selectors
        cards = soup.select('div.job_seen_beacon, div[data-jk]')
        for card in cards:
            title_el = card.select_one('h2.jobTitle span, .jobTitle a span')
            company_el = card.select_one('[data-testid="company-name"], .companyName')
            location_el = card.select_one('[data-testid="text-location"], .companyLocation')
            date_el = card.select_one('[data-testid="myJobsStateDate"], .date')
            link_el = card.select_one('h2.jobTitle a, .jobTitle a')

            title = title_el.get_text(strip=True) if title_el else ''
            company = company_el.get_text(strip=True) if company_el else 'Unknown'
            location = location_el.get_text(strip=True) if location_el else ''
            date_text = date_el.get_text(strip=True) if date_el else ''
            posted = _parse_relative_date(date_text)
            link = 'https://www.indeed.com' + link_el['href'] if link_el and link_el.get('href') else ''

            if not title:
                continue

            results.append(_base_job(company, title, location, link, posted))
        return results

# ---------------------------------------------------------------------------
# eFinancialCareers Scraper (HTML)
# ---------------------------------------------------------------------------

class EFinancialScraper:
    BASE = 'https://www.efinancialcareers.com/search'

    def scrape(self, session: requests.Session) -> list[dict]:
        results = []
        terms_sample = ['Credit Analyst', 'High Yield', 'Private Credit', 'Distressed Debt']
        locs_sample = ['New York', 'New Jersey', 'Remote']

        for term in terms_sample:
            for loc in locs_sample:
                params = {'q': term, 'location': loc, 'radius': '50'}
                url = f"{self.BASE}?{urlencode(params)}"
                log.info(f'eFinancialCareers: "{term}" in "{loc}"')
                try:
                    r = session.get(url, timeout=REQUEST_TIMEOUT)
                    if r.status_code != 200:
                        log.warning(f'  HTTP {r.status_code}, skipping')
                        _sleep_between_requests()
                        continue
                    soup = BeautifulSoup(r.text, 'html.parser')
                    batch = self._parse(soup)
                    results.extend(batch)
                    log.info(f'  Found {len(batch)} relevant jobs')
                except Exception as exc:
                    log.warning(f'  eFinancial error: {exc}')
                _sleep_between_requests()
            _sleep_between_firms()
        return results

    def _parse(self, soup: BeautifulSoup) -> list[dict]:
        results = []
        cards = soup.select('article.job-card, div.job-card, li[data-job-id]')
        for card in cards:
            title_el = card.select_one('h2, h3, .job-title, [data-cy="job-title"]')
            company_el = card.select_one('.company-name, [data-cy="company-name"]')
            location_el = card.select_one('.location, [data-cy="location"]')
            date_el = card.select_one('.job-date, time, [data-cy="posted-date"]')
            link_el = card.select_one('a[href]')

            title = title_el.get_text(strip=True) if title_el else ''
            company = company_el.get_text(strip=True) if company_el else 'Unknown'
            location = location_el.get_text(strip=True) if location_el else ''
            date_text = date_el.get_text(strip=True) if date_el else ''
            posted = _parse_relative_date(date_text)
            link = link_el['href'] if link_el and link_el.get('href') else ''
            if link and not link.startswith('http'):
                link = 'https://www.efinancialcareers.com' + link

            if not title:
                continue
            results.append(_base_job(company, title, location, link, posted))
        return results

# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def dedup(jobs: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for j in jobs:
        key = f"{j.get('company','').lower()}|{j.get('roleTitle','').lower()}|{j.get('location','').lower()}"
        if key in seen:
            continue
        seen.add(key)
        out.append(j)
    log.info(f'After dedup: {len(out)} unique jobs (from {len(jobs)} total)')
    return out

# ---------------------------------------------------------------------------
# CSV Writer
# ---------------------------------------------------------------------------

def write_csv(jobs: list[dict], path: str = 'job_tracker.csv'):
    # Strip internal fields
    clean = []
    for j in jobs:
        row = {c: j.get(c, '') for c in CSV_COLS}
        # Normalize bool
        row['referral'] = 'true' if row['referral'] else 'false'
        clean.append(row)

    df = pd.DataFrame(clean, columns=CSV_COLS)
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    log.info(f'Wrote {len(df)} jobs to {path}')

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info('=== Job Scraper Starting ===')
    session = _session()
    all_jobs: list[dict] = []

    # 1. Greenhouse (most reliable)
    log.info('--- Greenhouse ---')
    try:
        all_jobs.extend(GreenhouseScraper().scrape(session))
    except Exception as e:
        log.error(f'Greenhouse scraper failed: {e}')

    # 2. Lever
    log.info('--- Lever ---')
    try:
        all_jobs.extend(LeverScraper().scrape(session))
    except Exception as e:
        log.error(f'Lever scraper failed: {e}')

    # 3. eFinancialCareers
    log.info('--- eFinancialCareers ---')
    try:
        all_jobs.extend(EFinancialScraper().scrape(session))
    except Exception as e:
        log.error(f'eFinancialCareers scraper failed: {e}')

    # 4. Indeed (most likely to hit CAPTCHA but worth trying)
    log.info('--- Indeed ---')
    try:
        all_jobs.extend(IndeedScraper().scrape(session))
    except Exception as e:
        log.error(f'Indeed scraper failed: {e}')

    # Dedup + score
    unique_jobs = dedup(all_jobs)

    if not unique_jobs:
        log.warning('No jobs found. This may be due to CAPTCHA blocks or network issues.')
        # Write an empty CSV with headers so import doesn't break
        write_csv([], 'job_tracker.csv')
        return

    write_csv(unique_jobs, 'job_tracker.csv')
    log.info('=== Done ===')
    log.info(f'Total unique jobs: {len(unique_jobs)}')
    log.info(f'High priority: {sum(1 for j in unique_jobs if j.get("priorityTier") == "High")}')
    log.info(f'Medium priority: {sum(1 for j in unique_jobs if j.get("priorityTier") == "Medium")}')
    log.info(f'Low priority: {sum(1 for j in unique_jobs if j.get("priorityTier") == "Low")}')

if __name__ == '__main__':
    main()
