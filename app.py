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
.event.application { border-color: #ff7f0e; }
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
.legend.application { border: 1px solid #ff7f0e; }
.legend.trade { border: 1px solid #2ca02c; }
</style>
"""


@st.cache_data(ttl=1800)
def load_calendar(use_live: bool):
    return fetch_ipo_calendar(use_live=use_live)


@st.cache_data(ttl=1800)
def load_details(item: Dict) -> Dict:
    return fetch_ipo_details(item)


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


def build_terms_table(item: Dict, details: Dict | None = None) -> Dict:
    details = details or {}
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
    offer_price_text = item.get("offer_price_text")
    if offer_price_text and not subscription_price_hkd:
        terms["IPO price (HKD)"] = offer_price_text
    offer_price = details.get("offer_price")
    if offer_price and not subscription_price_hkd and not offer_price_text:
        terms["IPO price"] = format_currency_amount(offer_price)
    lot_size = item.get("lot_size")
    if lot_size:
        terms["Lot size"] = lot_size
    entry_fee_text = item.get("entry_fee_text")
    if entry_fee_text:
        terms["Entry fee (HKD)"] = entry_fee_text
    if item.get("application_board"):
        terms["Application board"] = item.get("application_board")
    if item.get("application_status"):
        terms["Application status"] = item.get("application_status")
    shares_issued = details.get("shares_issued")
    if shares_issued:
        terms["Shares issued"] = format_shares(shares_issued)
    market_cap = details.get("market_cap")
    if market_cap:
        terms["Market cap"] = format_currency_amount(market_cap)
    valuation_multiple = details.get("valuation_multiple")
    if valuation_multiple:
        terms["Valuation multiple"] = valuation_multiple
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


def format_shares(amount: float) -> str:
    if amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.2f}B shares"
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.2f}M shares"
    return f"{amount:,.0f} shares"


def format_currency_amount(value) -> str:
    if not value or len(value) != 2:
        return "N/A"
    amount, currency = value
    precision = 2 if amount < 1000 else 0
    formatted = f"{amount:,.{precision}f}"
    if currency == "HKD":
        return f"HK${formatted}"
    if currency == "USD":
        return f"${formatted}"
    return f"{currency} {formatted}"


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
            details = load_details(item) if enable_filings else {}
            terms = build_terms_table(item, details if enable_filings else None)
            st.table(terms)
            if item.get("company_page_url"):
                st.markdown(f"Company page: {item['company_page_url']}")
            ipo_value = details.get("ipo_value_usd") if enable_filings else None
            raise_amount = details.get("raise_amount_usd") if enable_filings else None
            if raise_amount is None:
                funds_raised_hkd = item.get("funds_raised_hkd")
                if funds_raised_hkd:
                    raise_amount = convert_to_usd(funds_raised_hkd, "HKD")
            market_cap = details.get("market_cap_usd") or ipo_value
            valuation_multiple = details.get("valuation_multiple") if enable_filings else None

            col1, col2, col3 = st.columns(3)
            col1.metric("Market cap (USD)", format_money(market_cap))
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
    "<span class='legend application'>Application proof</span>"
    "<span class='legend trade'>Listing/Trade</span>",
    unsafe_allow_html=True,
)

all_events = build_event_index(items)

selected_date = st.session_state.get("selected_date", today)
render_calendar(year, month, all_events)

selected_events = all_events.get(selected_date, [])
render_details(selected_date, selected_events, enable_filings)

application_only = [item for item in items if not item.get("trade_date")]
if application_only:
    st.subheader("No listing date yet")
    rows = [
        {
            "Company": item.get("company", "Unknown"),
            "First posting": format_date(item.get("bookbuilding_start")),
            "Category": "Application proof"
            if item.get("bookbuilding_type") == "application"
            else (item.get("bookbuilding_label") or "N/A"),
            "Board": item.get("application_board") or "N/A",
            "Status": item.get("application_status") or "N/A",
        }
        for item in sorted(
            application_only,
            key=lambda entry: (entry.get("bookbuilding_start") or date.min, entry.get("company", "")),
            reverse=True,
        )
    ]
    st.dataframe(rows, use_container_width=True, height=320)
