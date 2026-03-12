# SEC Filing-Driven Reported Statements Export

This project exports **filing-driven “reported” financial statements** (Income Statement, Balance Sheet, Cash Flow) from SEC EDGAR **10‑K** and **10‑Q** filings into Excel, prioritized for **fidelity to the company’s filed presentation** rather than forcing a universal template.

## Design philosophy (most important)

- **Filing-driven**: We do not start from `companyfacts` aggregation.
- **Presentation-first**: Rows and ordering are reconstructed from the filing’s **XBRL presentation linkbase** when available.
- **Company-specific**: Extension concepts and custom line items are preserved.
- **Auditability**: A Filing Index tab and a Raw Facts / Debug tab are included.

## What you get

An Excel workbook named:

`{TICKER}_sec_reported_statements.xlsx`

With tabs:

1. Annual Income Statement  
2. Annual Balance Sheet  
3. Annual Cash Flow  
4. Quarterly Income Statement  
5. Quarterly Balance Sheet  
6. Quarterly Cash Flow  
7. Filing Index  
8. Raw Facts / Debug  
9. README  

## How it works (high level)

1. Map ticker → CIK from SEC’s public ticker list.
2. Pull filing list from SEC “submissions” JSON for the company.
3. Select:
   - last 5 annual **10‑K**
   - “accompanying” **10‑Q** filings (heuristic based on report period range)
4. For each filing:
   - download filing directory `index.json`
   - download filing XBRL instance (`.xml`) + presentation (`*_pre.xml`) and labels (`*_lab.xml`) when available
   - parse:
     - instance facts (with contexts and periods)
     - filing labels
     - filing presentation trees by role
   - classify roles into income / balance / cash flow by role definition keywords
   - extract ordered line items from the best matching role per statement type
5. Merge filings into annual and quarterly historical sheets:
   - **row order is anchored to the most recent filing’s presentation**
   - missing rows from other periods are appended (no destructive normalization)

## Setup

Create a virtual environment and install requirements:

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

Run:

```bash
python main.py AAPL --out-dir . --cache-dir sec_cache --max-10k 5 --user-agent "YourApp/1.0 (you@example.com)"
```

Output:

- `AAPL_sec_reported_statements.xlsx`

## Fidelity notes / limitations (important)

- **Role classification is heuristic**: some filings have multiple statement roles (parent-only, parenthetical, etc.). The tool currently picks the role with the most presented nodes among roles whose definition matches the statement keywords.
- **Rendered HTML/PDF may differ**: the SEC HTML statement renderer can differ slightly from XBRL presentation ordering/labels in edge cases.
- **10‑Q values may be YTD**: income and cash flow statements in 10‑Q filings are often **year‑to‑date** as filed. This tool preserves values **as reported in the filing** and does not automatically compute “true quarter-only” values.
- **Dimensions**: dimensional facts (segments/products) are not expanded into separate rows in this version.

## Future enhancements

- optional normalized mapping layer (separate from reported extraction)
- quarter-only derivation logic with transparency + audit columns
- better statement role selection (including parenthetical detection)
- dimensional expansion (segments) with user controls
- multi-ticker batch mode
- fallback to rendered statement scraping when XBRL presentation is incomplete

# ClaudeCodeTest
