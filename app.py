import calendar as pycal
from datetime import date
from typing import Dict, List

import streamlit as st

from hkex_client import (
    build_event_index,
    convert_to_usd,
    fetch_ipo_calendar,
    fetch_ipo_details,
    format_money,
)

CALENDAR_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

CALENDAR_CSS = """
<style>
.calendar-header {
  font-weight: 600;
  text-align: center;
  padding: 0.25rem 0;
}
.event {
  font-size: 0.72rem;
  padding: 0.15rem 0.35rem;
  margin: 0.1rem 0;
  border-left: 3px solid #555;
  background: #f7f7f7;
  border-radius: 4px;
}
.event.bookbuilding { border-color: #1f77b4; }
.event.trade { border-color: #2ca02c; }
.out-of-month {
  color: #bbb;
  font-size: 0.8rem;
  text-align: center;
  padding: 0.55rem 0;
}
.legend {
  display: inline-block;
  padding: 0.15rem 0.4rem;
  margin-right: 0.4rem;
  border-radius: 12px;
  font-size: 0.75rem;
  background: #f4f4f4;
}
.legend.bookbuilding { border: 1px solid #1f77b4; }
.legend.trade { border: 1px solid #2ca02c; }
</style>
"""


@st.cache_data(ttl=1800)
def load_calendar(use_live: bool):
    return fetch_ipo_calendar(use_live=use_live)


@st.cache_data(ttl=1800)
def load_details(
    company: str,
    stock_code: str,
    prospectus_url: str,
    announcement_url: str,
    allotment_url: str,
    funds_raised_hkd: float,
    prospectus_date: date,
    listing_date: date,
) -> Dict:
    return fetch_ipo_details(
        {
            "company": company,
            "stock_code": stock_code,
            "prospectus_url": prospectus_url,
            "announcement_url": announcement_url,
            "allotment_url": allotment_url,
            "funds_raised_hkd": funds_raised_hkd,
            "prospectus_date": prospectus_date,
            "listing_date": listing_date,
        }
    )


def truncate_text(value: str, max_len: int = 16) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def render_calendar(year: int, month: int, events: Dict[date, List[Dict]]):
    st.markdown(CALENDAR_CSS, unsafe_allow_html=True)
    header_cols = st.columns(7)
    for idx, day_name in enumerate(CALENDAR_DAYS):
        header_cols[idx].markdown(f"<div class='calendar-header'>{day_name}</div>", unsafe_allow_html=True)

    calendar = pycal.Calendar(firstweekday=0)
    for week in calendar.monthdatescalendar(year, month):
        cols = st.columns(7)
        for idx, day in enumerate(week):
            with cols[idx]:
                if day.month != month:
                    st.markdown("<div class='out-of-month'>&nbsp;</div>", unsafe_allow_html=True)
                    continue
                if st.button(str(day.day), key=f"day-{day.isoformat()}"):
                    st.session_state["selected_date"] = day
                for event in events.get(day, []):
                    label = truncate_text(event["item"].get("company", ""))
                    st.markdown(
                        f"<div class='event {event['type']}'>{event['label']}: {label}</div>",
                        unsafe_allow_html=True,
                    )


def format_date(value: date) -> str:
    return value.strftime("%Y-%m-%d") if value else "N/A"


def build_terms_table(item: Dict) -> Dict:
    bookbuilding_label = item.get("bookbuilding_label", "Bookbuilding")
    trade_label = item.get("trade_label", "Trade date")
    terms = {
        "Stock code": item.get("stock_code") or "N/A",
        bookbuilding_label: _format_range(
            item.get("bookbuilding_start"), item.get("bookbuilding_end")
        ),
        trade_label: format_date(item.get("trade_date")),
    }
    if item.get("industry"):
        terms["Industry"] = item.get("industry")
    funds_raised_hkd = item.get("funds_raised_hkd")
    if funds_raised_hkd:
        terms["Funds raised (HKD)"] = format_hkd(funds_raised_hkd)
    subscription_price_hkd = item.get("subscription_price_hkd")
    if subscription_price_hkd:
        terms["IPO price (HKD)"] = format_hkd(subscription_price_hkd, is_price=True)
    return terms


def _format_range(start: date, end: date) -> str:
    if start and end:
        if start == end:
            return format_date(start)
        return f"{format_date(start)} to {format_date(end)}"
    return "N/A"


def format_hkd(amount: float, is_price: bool = False) -> str:
    if amount is None:
        return "N/A"
    if is_price:
        return f"HK${amount:,.2f}"
    if amount >= 1_000_000_000:
        return f"HK${amount / 1_000_000_000:.2f}B"
    if amount >= 1_000_000:
        return f"HK${amount / 1_000_000:.2f}M"
    return f"HK${amount:,.0f}"


def render_details(selected_date: date, events: List[Dict], enable_filings: bool):
    if not events:
        st.info("No IPO events on this date.")
        return

    st.subheader(f"IPO events for {format_date(selected_date)}")
    for event in events:
        item = event["item"]
        company = item.get("company", "Unknown")
        header = f"{company} - {event['label']}"
        with st.expander(header, expanded=True):
            terms = build_terms_table(item)
            st.table(terms)
            if item.get("company_page_url"):
                st.markdown(f"Company page: {item['company_page_url']}")

            details = (
                load_details(
                    company,
                    item.get("stock_code", ""),
                    item.get("prospectus_url", ""),
                    item.get("announcement_url", ""),
                    item.get("allotment_url", ""),
                    item.get("funds_raised_hkd"),
                    item.get("prospectus_date"),
                    item.get("listing_date"),
                )
                if enable_filings
                else {}
            )
            ipo_value = details.get("ipo_value_usd") if enable_filings else None
            raise_amount = details.get("raise_amount_usd") if enable_filings else None
            if raise_amount is None:
                funds_raised_hkd = item.get("funds_raised_hkd")
                if funds_raised_hkd:
                    raise_amount = convert_to_usd(funds_raised_hkd, "HKD")
            valuation_multiple = details.get("valuation_multiple") if enable_filings else None

            col1, col2, col3 = st.columns(3)
            col1.metric("IPO value (USD)", format_money(ipo_value))
            col2.metric("Raise amount (USD)", format_money(raise_amount))
            col3.metric("Valuation multiple", valuation_multiple or "N/A")

            if not enable_filings:
                st.caption("Enable filings fetch to load term sheet details.")
                continue

            term_sheet_url = details.get("term_sheet_url") or item.get("prospectus_url")
            if term_sheet_url:
                st.markdown(f"Term sheet: {term_sheet_url}")
            else:
                st.caption("Term sheet not found from filings.")

            business_model = details.get("business_model") or "N/A"
            financial_trend = details.get("financial_trend") or "N/A"

            st.markdown(f"Business model: {business_model}")
            st.markdown(f"Recent performance: {financial_trend}")

            filings = details.get("filings") or []
            if filings:
                st.markdown("Recent filings:")
                for filing in filings:
                    title = filing.get("title") or "Filing"
                    url = filing.get("url")
                    published = filing.get("published_date") or ""
                    if url:
                        st.markdown(f"- {published} {title}: {url}")
            else:
                st.caption("No filings matched the company name.")


st.set_page_config(page_title="Hong Kong IPO Calendar", layout="wide")

st.title("Hong Kong IPO Calendar")

with st.sidebar:
    st.header("Controls")
    use_live = st.toggle("Use live HKEX calendar", value=True)
    enable_filings = st.toggle("Fetch filings and term sheet", value=True)
    today = date.today()
    year = st.number_input("Year", min_value=2000, max_value=2100, value=today.year)
    month = st.selectbox("Month", list(range(1, 13)), index=today.month - 1)
    if st.button("Refresh data"):
        load_calendar.clear()
        load_details.clear()

items, meta = load_calendar(use_live)

if meta.get("source") == "sample":
    st.warning("Using sample IPO data. Enable live fetch or check network access.")

if meta.get("errors"):
    st.caption("; ".join(meta["errors"]))

st.markdown(
    "<span class='legend bookbuilding'>Prospectus/Bookbuilding</span>"
    "<span class='legend trade'>Listing/Trade</span>",
    unsafe_allow_html=True,
)

all_events = build_event_index(items)

selected_date = st.session_state.get("selected_date", today)
render_calendar(year, month, all_events)

selected_events = all_events.get(selected_date, [])
render_details(selected_date, selected_events, enable_filings)
