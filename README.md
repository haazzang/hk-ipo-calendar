# Hong Kong IPO Calendar (Streamlit)

A Streamlit app that shows a Hong Kong IPO calendar with bookbuilding and trade dates. Clicking a date surfaces IPO details, term sheet links, and best-effort summaries from HKEX filings.

## Setup

```bash
cd /Users/hyejinha/Desktop/Workspace/hk-ipo-calendar
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Notes

- Live data fetch uses `https://www.hkex.com.hk` and `https://www1.hkexnews.hk`.
- Filing search and PDF term extraction are best-effort; if no matches are found, the app falls back to sample data.
- Update the FX assumption in `hkex_client.py` if you want a different HKD/USD conversion.

## Manual Overrides

You can provide manual IPO details in `data/overrides.json` using a normalized company name key.

Example:

```json
{
  "alphabiotechholdings": {
    "term_sheet_url": "https://www1.hkexnews.hk/...",
    "ipo_value_usd": 1200000000,
    "raise_amount_usd": 250000000,
    "valuation_multiple": "18x",
    "business_model": "Focused on oncology biologics with a hospital distribution network.",
    "financial_trend": "Revenue grew from 2022 to 2024 while gross margin expanded.",
    "filings": [
      {
        "title": "Application Proof",
        "url": "https://www1.hkexnews.hk/...",
        "published_date": "2024-09-01",
        "source": "manual"
      }
    ]
  }
}
```
