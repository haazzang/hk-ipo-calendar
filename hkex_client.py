from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin
import calendar
import json
import os
import re

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

try:
    from pypdf import PdfReader
except ModuleNotFoundError:
    PdfReader = None

DATA_DIR = Path(__file__).resolve().parent / "data"
SAMPLE_CALENDAR_PATH = DATA_DIR / "sample_ipo_calendar.json"
OVERRIDES_PATH = DATA_DIR / "overrides.json"

HKEX_IPO_CALENDAR_URLS = [
    os.getenv("HKEX_IPO_CALENDAR_URL", "").strip(),
    "https://www.hkex.com.hk/Market-Data/IPO-Activity/IPO-Calendar?sc_lang=en",
]
HKEX_IPO_CALENDAR_URLS = [url for url in HKEX_IPO_CALENDAR_URLS if url]
HKEX_NEWS_HOST = "https://www1.hkexnews.hk"
HKEX_NEWS_BASE = "https://www2.hkexnews.hk"
HKEX_NEW_LISTING_MAIN_URL = (
    f"{HKEX_NEWS_BASE}/New-Listings/New-Listing-Information/Main-Board?sc_lang=en"
)
HKEX_NEW_LISTING_REPORT_SEGMENT = "/New-Listing-Report/Main/"
HKEX_APPLICATION_PROOF_URL = "https://www1.hkexnews.hk/app/appindex.html"
HKEX_APPLICATION_JSON_URLS = [
    ("Main Board", "https://www1.hkexnews.hk/ncms/json/eds/appactive_app_sehk_e.json"),
    ("GEM", "https://www1.hkexnews.hk/ncms/json/eds/appactive_app_gem_e.json"),
]
HKEX_SEARCH_ENDPOINTS = [
    ("servlet", f"{HKEX_NEWS_HOST}/search/titleSearchServlet.do", "post"),
    ("xhtml", f"{HKEX_NEWS_HOST}/search/titlesearch.xhtml", "get"),
]

DEFAULT_FX_USDHKD = 7.80
DEFAULT_TIMEOUT = 25
MAX_PDF_BYTES = 12_000_000
AASTOCKS_BASE = "https://www.aastocks.com"
AASTOCKS_UPCOMING_IPO_URL = f"{AASTOCKS_BASE}/en/stocks/market/ipo/mainpage.aspx"
TERM_SHEET_KEYWORDS = [
    "application proof",
    "phip",
    "term sheet",
    "hearing information pack",
    "offering circular",
    "prospectus",
    "announcement",
    "allotment results",
]
TERM_SHEET_MAX_PDFS = 3

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class Filing:
    title: str
    url: str
    published_date: Optional[date]
    source: str


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def safe_parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    try:
        return date_parser.parse(text, dayfirst=True).date()
    except (ValueError, TypeError):
        return None


def parse_ymd_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", text)
    if match:
        year, month, day = (int(part) for part in match.groups())
        try:
            return date(year, month, day)
        except ValueError:
            return None
    return safe_parse_date(text)


def _parse_compact_range(text: str) -> Optional[Tuple[date, date]]:
    match = re.search(r"(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", text)
    if not match:
        return None
    start_day, end_day, month, year = match.groups()
    start = safe_parse_date(f"{start_day} {month} {year}")
    end = safe_parse_date(f"{end_day} {month} {year}")
    if start and end:
        return start, end
    return None


def extract_date_range(text: str) -> Tuple[Optional[date], Optional[date]]:
    if not text:
        return None, None
    compact = _parse_compact_range(text)
    if compact:
        return compact
    matches = re.findall(r"\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}", text)
    dates = [safe_parse_date(m) for m in matches if safe_parse_date(m)]
    if len(dates) >= 2:
        return dates[0], dates[1]
    if len(dates) == 1:
        return dates[0], dates[0]
    return None, None


def extract_first_date(text: str) -> Optional[date]:
    if not text:
        return None
    match = re.search(r"\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}", text)
    if match:
        return safe_parse_date(match.group(0))
    return safe_parse_date(text)


def load_sample_calendar() -> List[Dict[str, Any]]:
    if not SAMPLE_CALENDAR_PATH.exists():
        return []
    with SAMPLE_CALENDAR_PATH.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    items = [normalize_calendar_item(item) for item in raw]
    return _shift_sample_to_recent(items)


def load_overrides() -> Dict[str, Dict[str, Any]]:
    if not OVERRIDES_PATH.exists():
        return {}
    with OVERRIDES_PATH.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def normalize_company_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


def normalize_stock_code(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    text = str(value).strip()
    if not text or text in {"\"", "-"}:
        return ""
    text = text.replace(".HK", "").replace("HK", "")
    text = text.strip()
    if text.isdigit():
        return f"{int(text):05d}"
    return text


def normalize_calendar_item(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(item)
    normalized["bookbuilding_start"] = safe_parse_date(item.get("bookbuilding_start"))
    normalized["bookbuilding_end"] = safe_parse_date(item.get("bookbuilding_end"))
    normalized["trade_date"] = safe_parse_date(item.get("trade_date"))
    return normalized


def _month_index(value: date) -> int:
    return value.year * 12 + value.month


def _add_months(value: date, months: int) -> date:
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _shift_item_months(item: Dict[str, Any], months: int) -> Dict[str, Any]:
    updated = dict(item)
    for key in ("bookbuilding_start", "bookbuilding_end", "trade_date"):
        value = item.get(key)
        if value:
            updated[key] = _add_months(value, months)
    return updated


def _shift_sample_to_recent(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return items
    dates = [
        value
        for item in items
        for value in (
            item.get("bookbuilding_start"),
            item.get("bookbuilding_end"),
            item.get("trade_date"),
        )
        if value
    ]
    if not dates:
        return items
    latest = max(dates)
    if latest >= date.today() - timedelta(days=60):
        return items
    earliest = min(dates)
    delta_months = _month_index(date.today()) - _month_index(earliest)
    if delta_months == 0:
        return items
    return [_shift_item_months(item, delta_months) for item in items]


def fetch_ipo_calendar(use_live: bool = True) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    errors: List[str] = []
    if use_live:
        items: List[Dict[str, Any]] = []
        try:
            items.extend(_fetch_new_listing_report_calendar())
        except Exception as exc:  # noqa: BLE001
            errors.append(f"HKEX new listing report fetch failed: {exc}")
        try:
            items.extend(_fetch_aastocks_upcoming_calendar())
        except Exception as exc:  # noqa: BLE001
            errors.append(f"AASTOCKS upcoming IPO fetch failed: {exc}")
        try:
            items.extend(_fetch_application_proof_items())
        except Exception as exc:  # noqa: BLE001
            errors.append(f"HKEX application proof fetch failed: {exc}")
        if items:
            try:
                documents = _fetch_new_listing_documents()
                _attach_new_listing_documents(items, documents)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"HKEX new listing document fetch failed: {exc}")
            items = _dedupe_calendar_items(items)
        if items:
            return items, {"source": "hkex-news", "errors": errors}
        try:
            items = _fetch_ipo_calendar_hkex()
            if items:
                return items, {"source": "hkex", "errors": errors}
            errors.append("HKEX calendar returned empty data")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"HKEX calendar fetch failed: {exc}")
    return load_sample_calendar(), {"source": "sample", "errors": errors}


def _fetch_ipo_calendar_hkex() -> List[Dict[str, Any]]:
    session = _session()
    last_error: Optional[str] = None
    for url in HKEX_IPO_CALENDAR_URLS:
        response = session.get(url, timeout=DEFAULT_TIMEOUT)
        if response.status_code == 404:
            last_error = f"HKEX IPO calendar endpoint not found (404): {url}"
            continue
        response.raise_for_status()
        items = _extract_calendar_from_html(response.text)
        if items:
            return items
        last_error = f"HKEX calendar returned empty data from {url}"
    if last_error:
        raise RuntimeError(last_error)
    raise RuntimeError("HKEX IPO calendar endpoint list is empty.")


def _fetch_new_listing_report_calendar() -> List[Dict[str, Any]]:
    if pd is None:
        raise RuntimeError("pandas/openpyxl not available for HKEX listing report")
    session = _session()
    response = session.get(HKEX_NEW_LISTING_MAIN_URL, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    html = response.text

    report_links = _extract_listing_report_links(html)
    items: List[Dict[str, Any]] = []

    for link in report_links:
        items.extend(_parse_listing_report(link))

    items = [normalize_calendar_item(item) for item in items]
    return items


def _fetch_application_proof_items() -> List[Dict[str, Any]]:
    session = _session()
    items: List[Dict[str, Any]] = []
    for board, url in HKEX_APPLICATION_JSON_URLS:
        response = session.get(url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        for record in payload.get("app", []):
            posting_date = safe_parse_date(record.get("d") or record.get("postingDate"))
            applicant = str(record.get("a") or "").strip()
            if not applicant or not posting_date:
                continue
            status = _application_status_label(record.get("s"))
            documents = _parse_application_documents(record)
            items.append(
                {
                    "company": applicant,
                    "stock_code": "",
                    "industry": "",
                    "application_status": status,
                    "application_board": board,
                    "application_proof_date": posting_date,
                    "application_documents": documents,
                    "bookbuilding_start": posting_date,
                    "bookbuilding_end": posting_date,
                    "bookbuilding_label": "Application proof",
                    "bookbuilding_type": "application",
                    "trade_date": None,
                    "trade_label": "Listing date",
                    "company_page_url": HKEX_APPLICATION_PROOF_URL,
                    "source": "application-proof",
                }
            )
    return items


def _application_status_label(value: Optional[str]) -> str:
    mapping = {
        "A": "Active",
        "I": "Inactive",
        "L": "Listed",
        "R": "Returned",
    }
    if not value:
        return "Unknown"
    return mapping.get(str(value).strip().upper(), str(value))


def _parse_application_documents(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    documents: List[Dict[str, Any]] = []
    warning_path = record.get("w")
    if warning_path:
        documents.append(
            {
                "title": "Warning statement",
                "url": _normalize_app_url(warning_path),
                "published_date": safe_parse_date(record.get("d")),
                "source": "hkex-application-proof",
            }
        )
    for group in (record.get("ls") or []) + (record.get("ps") or []):
        documents.extend(_parse_application_doc_group(group))
    return documents


def _parse_application_doc_group(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    base_title = str(group.get("nF") or "").strip()
    published = safe_parse_date(group.get("d"))
    url_keys = sorted(key for key in group.keys() if key.startswith("u"))
    for key in url_keys:
        path = group.get(key)
        if not path:
            continue
        suffix_key = f"nS{key[1:]}"
        suffix = str(group.get(suffix_key) or "").strip()
        title_parts = [part for part in (base_title, suffix) if part]
        title = " - ".join(title_parts) if title_parts else "Application proof"
        docs.append(
            {
                "title": title,
                "url": _normalize_app_url(path),
                "published_date": published,
                "source": "hkex-application-proof",
            }
        )
    return docs


def _normalize_app_url(path: str) -> str:
    if path.startswith("http"):
        return path
    return urljoin("https://www1.hkexnews.hk/app/", path.lstrip("/"))


def _fetch_aastocks_upcoming_calendar() -> List[Dict[str, Any]]:
    session = _session()
    response = session.get(AASTOCKS_UPCOMING_IPO_URL, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = _find_aastocks_upcoming_table(soup)
    if table is None:
        return []
    items: List[Dict[str, Any]] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 7:
            continue
        name_cell = cells[0]
        link = name_cell.find("a", href=True)
        company = _clean_text(link.get_text(" ", strip=True)) if link else ""
        summary_url = urljoin(AASTOCKS_BASE, link["href"]) if link else None
        stock_code = normalize_stock_code(_extract_stock_code(name_cell))
        if not company or company.lower() == "name/code":
            continue
        if stock_code and stock_code.lower() == "stock":
            continue
        industry = _clean_text(cells[1].get_text(" ", strip=True))
        offer_price_text = _clean_text(cells[2].get_text(" ", strip=True))
        lot_size = _clean_text(cells[3].get_text(" ", strip=True))
        entry_fee_text = _clean_text(cells[4].get_text(" ", strip=True))
        closing_date = parse_ymd_date(cells[5].get_text(" ", strip=True))
        listing_date = parse_ymd_date(cells[6].get_text(" ", strip=True))
        offer_period_text = _fetch_aastocks_offer_period(session, summary_url) if summary_url else None
        book_start, book_end = _parse_aastocks_offer_period(offer_period_text, closing_date)
        book_label = "Offer period" if offer_period_text else "Offer close"
        item = {
            "company": company,
            "stock_code": stock_code,
            "industry": industry,
            "bookbuilding_start": book_start,
            "bookbuilding_end": book_end,
            "bookbuilding_label": book_label,
            "bookbuilding_type": "bookbuilding",
            "trade_date": listing_date,
            "trade_label": "Listing date",
            "offer_price_text": offer_price_text,
            "lot_size": lot_size,
            "entry_fee_text": entry_fee_text,
            "schedule_source_url": summary_url,
            "company_page_url": HKEX_NEW_LISTING_MAIN_URL,
            "source": "aastocks",
        }
        if offer_price_text:
            price_value = _parse_float(offer_price_text)
            if price_value is not None:
                item["subscription_price_hkd"] = price_value
        items.append(item)
    return items


def _find_aastocks_upcoming_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    for table in soup.find_all("table"):
        header = table.find("thead")
        if not header:
            continue
        cells = header.find_all("td")
        headers = [_clean_text(cell.get_text(" ", strip=True)) for cell in cells]
        if "Closing Date" in headers and "Listing Date" in headers:
            return table
    return None


def _fetch_aastocks_offer_period(session: requests.Session, url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    response = session.get(url, timeout=DEFAULT_TIMEOUT)
    if not response.ok:
        return None
    match = re.search(r"Offer Period</td>\s*<td[^>]*>(.*?)</td>", response.text, re.S)
    if not match:
        return None
    return _clean_text(re.sub(r"<.*?>", " ", match.group(1)))


def _parse_aastocks_offer_period(
    offer_period_text: Optional[str], closing_date: Optional[date]
) -> Tuple[Optional[date], Optional[date]]:
    if offer_period_text:
        matches = re.findall(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}", offer_period_text)
        if len(matches) >= 2:
            start = parse_ymd_date(matches[0])
            end = parse_ymd_date(matches[1])
            if start or end:
                return start, end
        if len(matches) == 1:
            single = parse_ymd_date(matches[0])
            if single:
                return single, single
    if closing_date:
        return closing_date, closing_date
    return None, None


def _extract_stock_code(cell: BeautifulSoup) -> str:
    span = cell.find("span", class_="cls")
    if span:
        return span.get_text(strip=True)
    text = cell.get_text(" ", strip=True)
    match = re.search(r"(\d{4,5})\.HK", text)
    if match:
        return match.group(1)
    return ""


def _clean_text(value: str) -> str:
    return " ".join(value.split())


def _fetch_new_listing_documents() -> Dict[str, Dict[str, Any]]:
    session = _session()
    response = session.get(HKEX_NEW_LISTING_MAIN_URL, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return _extract_new_listing_documents(response.text)


def _attach_new_listing_documents(
    items: Iterable[Dict[str, Any]], documents: Dict[str, Dict[str, Any]]
) -> None:
    if not documents:
        return
    for item in items:
        code = normalize_stock_code(item.get("stock_code"))
        if not code:
            continue
        doc = documents.get(code)
        if not doc:
            continue
        item.setdefault("announcement_url", doc.get("announcement_url"))
        item.setdefault("prospectus_url", doc.get("prospectus_url"))
        item.setdefault("allotment_url", doc.get("allotment_url"))
        if not item.get("company"):
            item["company"] = doc.get("company")
        if not item.get("company_page_url"):
            item["company_page_url"] = (
                doc.get("prospectus_url")
                or doc.get("announcement_url")
                or HKEX_NEW_LISTING_MAIN_URL
            )


def _dedupe_calendar_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique: List[Dict[str, Any]] = []
    for item in items:
        key = (
            normalize_stock_code(item.get("stock_code")),
            item.get("trade_date"),
            item.get("bookbuilding_start"),
            normalize_company_key(item.get("company", "")),
            item.get("bookbuilding_type"),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _extract_listing_report_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if not href.lower().endswith(".xlsx"):
            continue
        if HKEX_NEW_LISTING_REPORT_SEGMENT.lower() not in href.lower():
            continue
        links.append(urljoin(HKEX_NEWS_BASE, href))

    def sort_key(url: str) -> int:
        match = re.search(r"(\d{4})", url)
        return int(match.group(1)) if match else 0

    return sorted(set(links), key=sort_key, reverse=True)


def _extract_new_listing_documents(html: str) -> Dict[str, Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    listing_table = None
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if headers and "Stock Code" in headers and "Stock Name" in headers:
            listing_table = table
            break
    if not listing_table:
        return {}
    documents: Dict[str, Dict[str, Any]] = {}
    for row in listing_table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        stock_code = normalize_stock_code(cells[0].get_text(strip=True))
        if not stock_code:
            continue
        company = cells[1].get_text(" ", strip=True)
        announcement_url = _first_link(cells[2])
        prospectus_url = _first_link(cells[3])
        allotment_url = _first_link(cells[4])
        documents[stock_code] = {
            "company": company,
            "announcement_url": announcement_url,
            "prospectus_url": prospectus_url,
            "allotment_url": allotment_url,
        }
    return documents


def _first_link(cell: BeautifulSoup) -> Optional[str]:
    anchor = cell.find("a", href=True)
    if not anchor:
        return None
    return urljoin(HKEX_NEWS_BASE, anchor["href"])


def _parse_listing_report(url: str) -> List[Dict[str, Any]]:
    if pd is None:
        return []
    try:
        raw = pd.read_excel(url, header=None)
    except Exception:  # noqa: BLE001
        return []
    header_row = _find_listing_report_header(raw)
    if header_row is None:
        return []
    rows = raw.iloc[header_row + 1 :]
    items: List[Dict[str, Any]] = []
    for _, row in rows.iterrows():
        if pd.isna(row[0]):
            continue
        stock_code = normalize_stock_code(row[1])
        if not stock_code:
            continue
        company = str(row[2]).replace("\n", " ").strip()
        prospectus_date = safe_parse_date(row[3])
        listing_date = safe_parse_date(row[4])
        funds_raised_hkd = _parse_float(row[8])
        subscription_price_hkd = _parse_float(row[9])
        item = {
            "company": company,
            "stock_code": stock_code,
            "industry": "",
            "prospectus_date": prospectus_date,
            "listing_date": listing_date,
            "funds_raised_hkd": funds_raised_hkd,
            "subscription_price_hkd": subscription_price_hkd,
            "bookbuilding_start": prospectus_date,
            "bookbuilding_end": prospectus_date,
            "bookbuilding_label": "Prospectus",
            "trade_date": listing_date,
            "trade_label": "Listing date",
            "company_page_url": HKEX_NEW_LISTING_MAIN_URL,
        }
        items.append(item)
    return items


def _find_listing_report_header(raw: Any) -> Optional[int]:
    for idx, row in raw.iterrows():
        values = [str(value) for value in row.tolist() if value is not None]
        if any("Stock Code" in value for value in values) and any(
            "Company Name" in value for value in values
        ):
            return idx
    return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "").strip()
    if not text or text == "-":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_calendar_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    json_items = _extract_calendar_from_scripts(soup)
    if json_items:
        return [normalize_calendar_item(item) for item in json_items]

    tables = soup.find_all("table")
    if not tables:
        return []

    header_map = {
        "company": ["company", "issuer", "applicant", "company name"],
        "stock_code": ["stock code", "code"],
        "bookbuilding": ["bookbuilding", "offer period", "book building"],
        "trade_date": ["listing date", "trade date", "listing"],
        "industry": ["industry", "sector"],
    }

    items: List[Dict[str, Any]] = []
    for table in tables:
        header_cells = table.find_all("th")
        if not header_cells:
            continue
        headers = [" ".join(cell.stripped_strings).lower() for cell in header_cells]
        if not any("listing" in h or "trade" in h for h in headers):
            continue

        column_map: Dict[str, int] = {}
        for field, candidates in header_map.items():
            for idx, header in enumerate(headers):
                if any(candidate in header for candidate in candidates):
                    column_map[field] = idx
                    break

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue
            text_cells = [" ".join(cell.stripped_strings) for cell in cells]
            company_idx = column_map.get("company", 0)
            company = text_cells[company_idx] if company_idx < len(text_cells) else ""
            if not company:
                continue
            company_url = None
            link = cells[company_idx].find("a", href=True)
            if link:
                company_url = link["href"]

            bookbuilding_start, bookbuilding_end = None, None
            trade_date = None
            if "bookbuilding" in column_map:
                bookbuilding_start, bookbuilding_end = extract_date_range(
                    text_cells[column_map["bookbuilding"]]
                )
            if "trade_date" in column_map:
                trade_date = extract_first_date(text_cells[column_map["trade_date"]])

            item = {
                "company": company,
                "stock_code": text_cells[column_map["stock_code"]]
                if "stock_code" in column_map and column_map["stock_code"] < len(text_cells)
                else "",
                "industry": text_cells[column_map["industry"]]
                if "industry" in column_map and column_map["industry"] < len(text_cells)
                else "",
                "bookbuilding_start": bookbuilding_start,
                "bookbuilding_end": bookbuilding_end,
                "trade_date": trade_date,
                "company_page_url": company_url,
            }
            items.append(normalize_calendar_item(item))

    return items


def _extract_calendar_from_scripts(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    for script in soup.find_all("script"):
        if not script.string:
            continue
        text = script.string
        if "ipo" not in text.lower() or "calendar" not in text.lower():
            continue
        state_match = re.search(r"__INITIAL_STATE__\s*=\s*(\{.*\});", text, re.S)
        if state_match:
            try:
                payload = json.loads(state_match.group(1))
                items = _find_calendar_items_in_state(payload)
                if items:
                    return items
            except json.JSONDecodeError:
                pass
        for match in re.finditer(r"\[\{.*?\}\]", text, re.S):
            try:
                candidate = json.loads(match.group(0))
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, list) and candidate and isinstance(candidate[0], dict):
                if "company" in candidate[0] or "issuer" in candidate[0]:
                    return candidate
    return []


def _find_calendar_items_in_state(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                keys = {k.lower() for k in value[0].keys()}
                if {"company", "listingdate"} & keys:
                    return value
            nested = _find_calendar_items_in_state(value)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = _find_calendar_items_in_state(item)
            if nested:
                return nested
    return []


def build_event_index(items: Iterable[Dict[str, Any]]) -> Dict[date, List[Dict[str, Any]]]:
    events: Dict[date, List[Dict[str, Any]]] = {}
    for item in items:
        book_start = item.get("bookbuilding_start")
        book_end = item.get("bookbuilding_end")
        trade_date = item.get("trade_date")
        book_label = item.get("bookbuilding_label", "Bookbuilding")
        book_type = item.get("bookbuilding_type", "bookbuilding")
        trade_label = item.get("trade_label", "Trade")

        if book_start and book_end:
            current = book_start
            while current <= book_end:
                events.setdefault(current, []).append(
                    {
                        "type": book_type,
                        "label": book_label,
                        "item": item,
                    }
                )
                current += timedelta(days=1)
        if trade_date:
            events.setdefault(trade_date, []).append(
                {"type": "trade", "label": trade_label, "item": item}
            )
    return events


def fetch_ipo_details(item: Dict[str, Any]) -> Dict[str, Any]:
    overrides = load_overrides()
    company = item.get("company", "")
    stock_code = normalize_stock_code(item.get("stock_code"))
    for key in (normalize_company_key(company), stock_code):
        if key and key in overrides:
            return overrides[key]

    filings: List[Filing] = []
    prospectus_date = item.get("prospectus_date")
    listing_date = item.get("listing_date")
    announcement_url = item.get("announcement_url")
    prospectus_url = item.get("prospectus_url")
    allotment_url = item.get("allotment_url")
    application_documents = item.get("application_documents") or []

    for document in application_documents:
        url = document.get("url")
        title = document.get("title")
        if not url or not title:
            continue
        filings.append(
            Filing(
                title=title,
                url=url,
                published_date=safe_parse_date(document.get("published_date")),
                source=document.get("source") or "hkex-application-proof",
            )
        )
    if announcement_url:
        filings.append(
            Filing(
                title="New listing announcement",
                url=announcement_url,
                published_date=safe_parse_date(prospectus_date),
                source="hkexnews",
            )
        )
    if prospectus_url:
        filings.append(
            Filing(
                title="Prospectus",
                url=prospectus_url,
                published_date=safe_parse_date(prospectus_date),
                source="hkexnews",
            )
        )
    if allotment_url:
        filings.append(
            Filing(
                title="Allotment results",
                url=allotment_url,
                published_date=safe_parse_date(listing_date),
                source="hkexnews",
            )
        )

    if company:
        filings.extend(search_hkex_filings(company))
    filings = _dedupe_filings(filings)
    term_sheet = select_term_sheet(filings)
    extracted_terms = extract_terms_from_filings(filings) if filings else {}

    raise_amount_usd = extracted_terms.get("raise_amount_usd")
    if raise_amount_usd is None:
        funds_raised_hkd = _parse_float(item.get("funds_raised_hkd"))
        if funds_raised_hkd is not None:
            raise_amount_usd = convert_to_usd(funds_raised_hkd, "HKD")

    return {
        "term_sheet_url": term_sheet.url if term_sheet else None,
        "filings": [filing.__dict__ for filing in filings[:6]],
        "ipo_value_usd": extracted_terms.get("ipo_value_usd"),
        "raise_amount_usd": raise_amount_usd,
        "valuation_multiple": extracted_terms.get("valuation_multiple"),
        "business_model": extracted_terms.get("business_model"),
        "financial_trend": extracted_terms.get("financial_trend"),
    }


def search_hkex_filings(company_name: str) -> List[Filing]:
    if not company_name:
        return []
    session = _session()
    params = _build_search_params(company_name)
    for source, url, method in HKEX_SEARCH_ENDPOINTS:
        try:
            if method == "post":
                response = session.post(url, data=params, timeout=DEFAULT_TIMEOUT)
            else:
                response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            if not response.ok:
                continue
            json_payload = _try_parse_json(response.text)
            if json_payload:
                filings = _extract_filings_from_json(json_payload, source)
                if filings:
                    return filings
            filings = _extract_filings_from_html(response.text, source)
            if filings:
                return filings
        except Exception:  # noqa: BLE001
            continue
    return []


def _build_search_params(company_name: str) -> Dict[str, str]:
    today = date.today()
    start = today - timedelta(days=365 * 2)
    return {
        "lang": "EN",
        "searchType": "SEHK",
        "searchMethod": "TITLE",
        "market": "SEHK",
        "title": company_name,
        "searchFromDate": start.strftime("%Y%m%d"),
        "searchToDate": today.strftime("%Y%m%d"),
        "sortDir": "0",
        "sortByOptions": "DateTime",
    }


def _try_parse_json(text: str) -> Optional[Any]:
    text = text.strip()
    if not text:
        return None
    if not (text.startswith("{") or text.startswith("[")):
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _pick_first(node: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        value = node.get(key)
        if value:
            return str(value)
    return None



def _extract_filings_from_json(payload: Any, source: str) -> List[Filing]:
    filings: List[Filing] = []
    stack = [payload]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            title = _pick_first(node, ["title", "docTitle", "headline", "documentTitle"])
            url = _pick_first(node, ["url", "docUrl", "fileLink", "documentUrl"])
            published = _pick_first(node, ["publishedDate", "date", "publishDate"])
            if title and url:
                filings.append(
                    Filing(
                        title=title,
                        url=_normalize_hkex_url(url),
                        published_date=safe_parse_date(published),
                        source=source,
                    )
                )
            for value in node.values():
                stack.append(value)
        elif isinstance(node, list):
            stack.extend(node)
    return _dedupe_filings(filings)


def _extract_filings_from_html(html: str, source: str) -> List[Filing]:
    soup = BeautifulSoup(html, "html.parser")
    filings: List[Filing] = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not _is_filing_link(href):
            continue
        title = " ".join(link.stripped_strings)
        parent_text = " ".join(link.parent.stripped_strings)
        published_date = extract_first_date(parent_text)
        filings.append(
            Filing(
                title=title or parent_text,
                url=_normalize_hkex_url(href),
                published_date=published_date,
                source=source,
            )
        )
    return _dedupe_filings(filings)


def _is_filing_link(href: str) -> bool:
    href_lower = href.lower()
    return any(href_lower.endswith(ext) for ext in (".pdf", ".htm", ".html"))


def _normalize_hkex_url(url: str) -> str:
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"{HKEX_NEWS_HOST}{url}"
    return f"{HKEX_NEWS_HOST}/{url}"


def _dedupe_filings(filings: List[Filing]) -> List[Filing]:
    seen = set()
    unique: List[Filing] = []
    for filing in filings:
        if filing.url in seen:
            continue
        seen.add(filing.url)
        unique.append(filing)
    unique.sort(key=lambda item: item.published_date or date.min, reverse=True)
    return unique


def select_term_sheet(filings: List[Filing]) -> Optional[Filing]:
    if not filings:
        return None
    return min(filings, key=_term_sheet_rank)


def _term_sheet_rank(filing: Filing) -> Tuple[int, int, int]:
    title = filing.title.lower()
    keyword_rank = next(
        (idx for idx, keyword in enumerate(TERM_SHEET_KEYWORDS) if keyword in title),
        len(TERM_SHEET_KEYWORDS),
    )
    source_rank = 0 if filing.source == "hkex-application-proof" else 1
    date_rank = -(filing.published_date or date.min).toordinal()
    return keyword_rank, source_rank, date_rank


def extract_terms_from_filings(filings: List[Filing]) -> Dict[str, Any]:
    if not filings:
        return {}
    extracted: Dict[str, Any] = {}
    checked = set()
    for filing in sorted(filings, key=_term_sheet_rank):
        if len(checked) >= TERM_SHEET_MAX_PDFS:
            break
        url = filing.url
        if not url or not url.lower().endswith(".pdf"):
            continue
        if url in checked:
            continue
        checked.add(url)
        terms = extract_terms_from_pdf(url)
        if not terms:
            continue
        for key, value in terms.items():
            if value is None:
                continue
            extracted.setdefault(key, value)
        if _has_enough_term_fields(extracted):
            break
    return extracted


def _has_enough_term_fields(terms: Dict[str, Any]) -> bool:
    keys = (
        "ipo_value_usd",
        "raise_amount_usd",
        "valuation_multiple",
        "business_model",
        "financial_trend",
    )
    return sum(1 for key in keys if terms.get(key)) >= 3


def extract_terms_from_pdf(url: str) -> Dict[str, Any]:
    if PdfReader is None:
        return {}
    try:
        data = _download_pdf(url)
    except Exception:  # noqa: BLE001
        return {}
    if not data:
        return {}
    text = _extract_text_from_pdf(data)
    if not text:
        return {}
    text = re.sub(r"\s+", " ", text)

    offer_price = _extract_offer_price(text)
    gross_proceeds = _extract_gross_proceeds(text)
    market_cap = _extract_market_cap(text)
    valuation_multiple = _extract_valuation_multiple(text)

    business_model = extract_summary(
        text,
        keywords=["our business", "business model", "principal activities", "we are", "we provide"],
        max_sentences=2,
    )
    financial_trend = extract_summary(
        text,
        keywords=[
            "revenue",
            "profit",
            "loss",
            "gross",
            "financial information",
            "operating results",
        ],
        max_sentences=2,
    )

    ipo_value_usd = None
    if market_cap:
        ipo_value_usd = convert_to_usd(market_cap[0], market_cap[1])

    raise_amount_usd = None
    if gross_proceeds:
        raise_amount_usd = convert_to_usd(gross_proceeds[0], gross_proceeds[1])

    return {
        "offer_price": offer_price,
        "gross_proceeds": gross_proceeds,
        "market_cap": market_cap,
        "valuation_multiple": valuation_multiple,
        "business_model": business_model,
        "financial_trend": financial_trend,
        "ipo_value_usd": ipo_value_usd,
        "raise_amount_usd": raise_amount_usd,
    }


def _download_pdf(url: str) -> bytes:
    session = _session()
    response = session.get(url, stream=True, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    content = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if chunk:
            content.extend(chunk)
        if len(content) > MAX_PDF_BYTES:
            break
    return bytes(content)


def _extract_text_from_pdf(data: bytes) -> str:
    if PdfReader is None:
        return ""
    reader = PdfReader(BytesIO(data))
    texts: List[str] = []
    for page in reader.pages[:10]:
        try:
            texts.append(page.extract_text() or "")
        except Exception:  # noqa: BLE001
            continue
    return " ".join(texts)


def extract_summary(text: str, keywords: List[str], max_sentences: int = 2) -> Optional[str]:
    lowered = text.lower()
    sentences = re.split(r"(?<=[.!?])\s+", text)
    selected: List[str] = []
    for sentence in sentences:
        if len(selected) >= max_sentences:
            break
        sentence_lower = sentence.lower()
        if any(keyword in sentence_lower for keyword in keywords):
            selected.append(sentence.strip())
    if selected:
        return " ".join(selected)
    if any(keyword in lowered for keyword in keywords):
        chunk = text[:400].strip()
        return chunk
    return None


def _extract_offer_price(text: str) -> Optional[Tuple[float, str]]:
    pattern = r"offer price(?: range)?[^\d]*(HK\$|US\$|USD|HKD)?\s*([0-9.,]+)"
    match = re.search(pattern, text, re.I)
    if match:
        return _parse_money(match.group(2), match.group(1))
    return None


def _extract_gross_proceeds(text: str) -> Optional[Tuple[float, str]]:
    pattern = r"gross proceeds[^\d]*(HK\$|US\$|USD|HKD)?\s*([0-9.,]+)\s*(million|billion|mn|bn)?"
    match = re.search(pattern, text, re.I)
    if match:
        return _parse_money(match.group(2), match.group(1), match.group(3))
    return None


def _extract_market_cap(text: str) -> Optional[Tuple[float, str]]:
    pattern = r"market capitali[sz]ation[^\d]*(HK\$|US\$|USD|HKD)?\s*([0-9.,]+)\s*(million|billion|mn|bn)?"
    match = re.search(pattern, text, re.I)
    if match:
        return _parse_money(match.group(2), match.group(1), match.group(3))
    return None


def _extract_valuation_multiple(text: str) -> Optional[str]:
    pattern = r"(P/E|price[- ]to[- ]earnings)[^\d]*([0-9]+(?:\.[0-9]+)?)\s*(x|times)?"
    match = re.search(pattern, text, re.I)
    if match:
        return f"{match.group(2)}x"
    return None


def _parse_money(value: str, currency: Optional[str], unit: Optional[str] = None) -> Tuple[float, str]:
    clean_value = float(value.replace(",", ""))
    multiplier = _unit_multiplier(unit)
    amount = clean_value * multiplier
    return amount, normalize_currency(currency)


def normalize_currency(currency: Optional[str]) -> str:
    if not currency:
        return "USD"
    currency = currency.upper().replace(" ", "")
    if currency in {"US$", "USD"}:
        return "USD"
    if currency in {"HK$", "HKD"}:
        return "HKD"
    return currency


def _unit_multiplier(unit: Optional[str]) -> float:
    if not unit:
        return 1.0
    unit = unit.lower()
    if unit in {"b", "bn", "billion"}:
        return 1_000_000_000.0
    if unit in {"m", "mn", "million"}:
        return 1_000_000.0
    return 1.0


def convert_to_usd(amount: float, currency: str) -> float:
    if currency == "USD":
        return amount
    if currency == "HKD":
        return amount / DEFAULT_FX_USDHKD
    return amount


def format_money(amount: Optional[float]) -> str:
    if amount is None:
        return "N/A"
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.2f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    return f"${amount:,.0f}"
