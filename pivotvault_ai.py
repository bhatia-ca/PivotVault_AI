import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from io import StringIO
from datetime import datetime, timedelta
import time
import smtplib
import io
import base64
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, HRFlowable, KeepTogether)
from reportlab.platypus import PageBreak
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="PivotVault AI",
    layout="wide",
    page_icon="🏦",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

:root {
    --bg:        #0d0f14;
    --surface:   #141720;
    --border:    #1e2330;
    --accent:    #00e5a0;
    --accent2:   #4d7cfe;
    --danger:    #ff4d6a;
    --warn:      #f5a623;
    --muted:     #4a5068;
    --text:      #d4daf0;
    --text-dim:  #6b7490;
}
html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: var(--bg);
    color: var(--text);
}
#MainMenu, footer { visibility: hidden; }
header { visibility: hidden; }
/* Keep sidebar toggle button visible on mobile */
header [data-testid="stSidebarCollapsedControl"],
button[kind="header"],
div[data-testid="collapsedControl"] {
    visibility: visible !important;
    display: flex !important;
}
.block-container { padding: 1.5rem 2rem 2rem; max-width: 1500px; }

/* ── SIDEBAR — light olive green ─────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: #e8eddf !important;
    border-right: 2px solid #c5cfa8;
}
section[data-testid="stSidebar"] * {
    color: #2d3318 !important;
}
section[data-testid="stSidebar"] .stRadio label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.84rem;
    font-weight: 500;
    letter-spacing: 0.03em;
    color: #3d4a1e !important;
    padding: 0.45rem 0.6rem;
    border-radius: 5px;
    transition: background 0.18s, color 0.18s;
}
section[data-testid="stSidebar"] .stRadio label:hover {
    background: #d4dbb8;
    color: #1a2208 !important;
}
section[data-testid="stSidebar"] hr {
    border-color: #c5cfa8 !important;
}
/* Sidebar nav buttons */
section[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    border: none !important;
    border-radius: 6px !important;
    color: #3d4a1e !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.84rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.02em !important;
    text-align: left !important;
    justify-content: flex-start !important;
    padding: 0.45rem 0.75rem !important;
    transition: background 0.15s !important;
    width: 100% !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #d4dbb8 !important;
    color: #1a2208 !important;
}
/* Logout button — keep it styled differently */
section[data-testid="stSidebar"] div:has(> button[kind="secondary"]#logout_btn) button,
section[data-testid="stSidebar"] [data-testid="stButton"]:last-of-type button {
    background: #3d4a1e !important;
    color: #e8eddf !important;
    text-align: center !important;
    justify-content: center !important;
}

div[data-testid="metric-container"] {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1rem 1.25rem;
}
div[data-testid="metric-container"] label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-dim) !important;
}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.4rem;
    font-weight: 600;
    color: var(--text) !important;
}
div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
}
.stButton > button {
    background: transparent;
    border: 1px solid var(--accent);
    color: var(--accent);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    letter-spacing: 0.06em;
    border-radius: 4px;
    padding: 0.4rem 1rem;
    transition: background 0.2s, color 0.2s;
}
.stButton > button:hover { background: var(--accent); color: #000; }
.stDataFrame, .stTable { font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
/* ── SELECTBOXES & INPUTS — light olive green ───────────────────────── */
.stSelectbox > div > div,
.stSelectbox [data-baseweb="select"] > div,
div[data-baseweb="select"] > div {
    background: #edf0e3 !important;
    border: 1px solid #b5c27a !important;
    border-radius: 5px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.82rem !important;
    color: #2d3318 !important;
}
div[data-baseweb="select"] span,
div[data-baseweb="select"] div {
    color: #2d3318 !important;
    font-family: 'IBM Plex Mono', monospace !important;
}
/* Dropdown menu list */
ul[data-baseweb="menu"],
div[data-baseweb="popover"] > div {
    background: #f2f5e8 !important;
    border: 1px solid #b5c27a !important;
}
li[role="option"] {
    background: #f2f5e8 !important;
    color: #2d3318 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.82rem !important;
}
li[role="option"]:hover,
li[aria-selected="true"] {
    background: #d4dbb8 !important;
    color: #1a2208 !important;
}
/* Text inputs */
.stTextInput > div > div > input,
input[type="text"], input[type="password"] {
    background: #edf0e3 !important;
    border: 1px solid #b5c27a !important;
    border-radius: 5px !important;
    color: #2d3318 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.85rem !important;
}
.stTextInput > div > div > input::placeholder {
    color: #7d8c55 !important;
}
.stTextInput > div > div > input:focus {
    border-color: #6b8c2a !important;
    box-shadow: 0 0 0 2px rgba(107,140,42,0.18) !important;
}
h1, h2, h3 { font-family: 'IBM Plex Mono', monospace; letter-spacing: -0.02em; color: var(--text); }
hr { border-color: var(--border); margin: 1rem 0; }

.wl-pill {
    display: inline-block;
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--accent);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    padding: 0.2rem 0.65rem;
    border-radius: 20px;
    margin: 0.2rem;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.live-dot {
    display: inline-block; width: 8px; height: 8px;
    background: var(--accent); border-radius: 50%;
    margin-right: 6px; animation: pulse 1.6s ease-in-out infinite;
}
.title-bar {
    display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1.25rem;
}
.title-bar h1 { margin: 0; font-size: 1.4rem; }
.title-bar .ts {
    margin-left: auto; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; color: var(--text-dim);
}
.pb-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.75rem;
}
.pb-card-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.68rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--text-dim);
    margin-bottom: 0.5rem;
}
.pb-card-value { font-family: 'IBM Plex Mono', monospace; font-size: 1.1rem; font-weight: 600; }
.pb-bull  { color: #00e5a0; }
.pb-bear  { color: #ff4d6a; }
.pb-neut  { color: #f5a623; }
.signal-badge {
    display: inline-block;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.25rem 0.75rem;
    border-radius: 3px;
    margin: 0.15rem 0.1rem;
}
.sig-bull { background: rgba(0,229,160,0.12); color: #00e5a0; border: 1px solid rgba(0,229,160,0.3); }
.sig-bear { background: rgba(255,77,106,0.12); color: #ff4d6a; border: 1px solid rgba(255,77,106,0.3); }
.sig-neut { background: rgba(245,166,35,0.12); color: #f5a623;  border: 1px solid rgba(245,166,35,0.3); }

/* ── MOBILE BOTTOM NAV ───────────────────────────────────────────────── */
@media (max-width: 768px) {
    /* Hide sidebar entirely on mobile */
    section[data-testid="stSidebar"],
    [data-testid="collapsedControl"] { display: none !important; }

    /* Give content breathing room above bottom nav */
    .block-container { padding: 1rem 0.75rem 80px !important; }

    /* Bottom nav bar */
    .mobile-nav {
        position: fixed;
        bottom: 0; left: 0; right: 0;
        height: 62px;
        background: #1a1f2e;
        border-top: 1px solid #2a3050;
        display: flex;
        align-items: stretch;
        z-index: 9999;
        box-shadow: 0 -4px 20px rgba(0,0,0,0.4);
    }
    .mobile-nav a {
        flex: 1;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 3px;
        text-decoration: none;
        color: #6b7490;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.55rem;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        transition: color 0.15s, background 0.15s;
        border-top: 2px solid transparent;
        padding: 0 4px;
    }
    .mobile-nav a .nav-icon { font-size: 1.2rem; line-height: 1; }
    .mobile-nav a.active {
        color: #00e5a0;
        border-top-color: #00e5a0;
        background: rgba(0,229,160,0.06);
    }
    .mobile-nav a:hover { color: #00e5a0; }
}
@media (min-width: 769px) {
    .mobile-nav { display: none !important; }
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────
defaults = {
    'watchlist': [],
    'cpr_scan_df': None,
    'logged_in': False,
    'wl_data': {},
    'wl_last_refresh': None,
    'mobile_page': 'Market Snapshot',
    'screener_symbol': None,
    'screener_nav_pending': False,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
#  DATA HELPERS
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_nse500_list() -> pd.DataFrame:
    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        return pd.DataFrame({
            "Symbol": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
                       "WIPRO", "TATAMOTORS", "SBIN", "AXISBANK", "LT"],
            "Industry": ["Energy", "IT", "IT", "Financial Services", "Financial Services",
                         "IT", "Auto", "Financial Services", "Financial Services", "Construction"],
            "Company Name": ["Reliance Industries", "TCS", "Infosys", "HDFC Bank", "ICICI Bank",
                             "Wipro", "Tata Motors", "SBI", "Axis Bank", "L&T"],
        })


@st.cache_data(ttl=60)
def get_market_movers():
    try:
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        }
        session.get("https://www.nseindia.com", headers=headers, timeout=8)
        time.sleep(0.5)

        def _fetch(endpoint):
            r = session.get(
                f"https://www.nseindia.com/api/live-analysis-variations?index={endpoint}",
                headers=headers, timeout=8,
            )
            r.raise_for_status()
            return r.json()

        def _parse(j):
            data = j.get("NIFTY", {}).get("data", [])
            df = pd.DataFrame(data)
            if df.empty:
                return df
            rename = {}
            for c in df.columns:
                cl = c.lower()
                if "symbol" in cl:                      rename[c] = "Symbol"
                elif "ltp" in cl:                       rename[c] = "LTP"
                elif "net" in cl or "percent" in cl:    rename[c] = "Chg %"
            df = df.rename(columns=rename)
            keep = [c for c in ["Symbol", "LTP", "Chg %"] if c in df.columns]
            return df[keep].head(10)

        return _parse(_fetch("gainers")), _parse(_fetch("loosers"))
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=120)
def fetch_index_data(ticker: str) -> dict:
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if len(hist) < 2:
            return {"ltp": None, "change": None}
        ltp  = round(hist["Close"].iloc[-1], 2)
        prev = round(hist["Close"].iloc[-2], 2)
        chg  = round(((ltp - prev) / prev) * 100, 2)
        return {"ltp": ltp, "change": chg}
    except Exception:
        return {"ltp": None, "change": None}


@st.cache_data(ttl=300)
def fetch_stock_history(symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.Ticker(symbol + ".NS").history(period=period, interval=interval)
        df.index = df.index.tz_localize(None)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_stock_info(symbol: str) -> dict:
    try:
        return yf.Ticker(symbol + ".NS").info
    except Exception:
        return {}


def refresh_watchlist_prices(symbols: list) -> dict:
    result = {}
    for sym in symbols:
        try:
            hist = yf.Ticker(sym + ".NS").history(period="2d")
            if len(hist) >= 2:
                ltp  = round(hist["Close"].iloc[-1], 2)
                prev = hist["Close"].iloc[-2]
                chg  = round(((ltp - prev) / prev) * 100, 2)
                result[sym] = {"ltp": ltp, "change": chg}
            elif len(hist) == 1:
                result[sym] = {"ltp": round(hist["Close"].iloc[-1], 2), "change": 0.0}
        except Exception:
            result[sym] = {"ltp": None, "change": None}
    return result


# ─────────────────────────────────────────────
#  PIVOT BOSS — FRANK OCHOA METHODOLOGY
# ─────────────────────────────────────────────

def compute_pivot_points(df: pd.DataFrame, pivot_type: str = "Traditional") -> dict:
    """
    Compute pivot points from prior completed candle.
    Supports: Traditional, Woodie, Camarilla, DeMark, Fibonacci.
    """
    if df.empty or len(df) < 2:
        return {}
    ref  = df.iloc[-2]
    H, L, C, O = ref["High"], ref["Low"], ref["Close"], ref["Open"]
    rng  = H - L
    pivots = {}

    if pivot_type == "Traditional":
        P = (H + L + C) / 3
        pivots = {
            "R3": round(H + 2 * (P - L), 2),
            "R2": round(P + rng, 2),
            "R1": round(2 * P - L, 2),
            "P":  round(P, 2),
            "S1": round(2 * P - H, 2),
            "S2": round(P - rng, 2),
            "S3": round(L - 2 * (H - P), 2),
        }

    elif pivot_type == "Woodie":
        P = (H + L + 2 * C) / 4
        pivots = {
            "R3": round(H + 2 * (P - L), 2),
            "R2": round(P + rng, 2),
            "R1": round(2 * P - L, 2),
            "P":  round(P, 2),
            "S1": round(2 * P - H, 2),
            "S2": round(P - rng, 2),
            "S3": round(L - 2 * (H - P), 2),
        }

    elif pivot_type == "Camarilla":
        P = (H + L + C) / 3
        pivots = {
            "R4": round(C + rng * 1.1 / 2, 2),
            "R3": round(C + rng * 1.1 / 4, 2),
            "R2": round(C + rng * 1.1 / 6, 2),
            "R1": round(C + rng * 1.1 / 12, 2),
            "P":  round(P, 2),
            "S1": round(C - rng * 1.1 / 12, 2),
            "S2": round(C - rng * 1.1 / 6, 2),
            "S3": round(C - rng * 1.1 / 4, 2),
            "S4": round(C - rng * 1.1 / 2, 2),
        }

    elif pivot_type == "DeMark":
        if C < O:
            X = H + 2 * L + C
        elif C > O:
            X = 2 * H + L + C
        else:
            X = H + L + 2 * C
        P = X / 4
        pivots = {
            "R1": round(X / 2 - L, 2),
            "P":  round(P, 2),
            "S1": round(X / 2 - H, 2),
        }

    elif pivot_type == "Fibonacci":
        P = (H + L + C) / 3
        pivots = {
            "R3": round(P + 1.000 * rng, 2),
            "R2": round(P + 0.618 * rng, 2),
            "R1": round(P + 0.382 * rng, 2),
            "P":  round(P, 2),
            "S1": round(P - 0.382 * rng, 2),
            "S2": round(P - 0.618 * rng, 2),
            "S3": round(P - 1.000 * rng, 2),
        }

    return pivots


def compute_cpr(df: pd.DataFrame) -> dict:
    """
    Central Pivot Range (CPR) — Frank Ochoa's core tool.
    Narrow CPR = trending; Wide CPR = range-bound.
    """
    if df.empty or len(df) < 2:
        return {}
    ref  = df.iloc[-2]
    H, L, C = ref["High"], ref["Low"], ref["Close"]
    P  = (H + L + C) / 3
    BC = (H + L) / 2
    TC = (P - BC) + P
    width_pct = abs(TC - BC) / P * 100

    if width_pct < 0.25:
        bias  = "Narrow — Strong Trending Day Expected"
        color = "bull"
    elif width_pct < 0.5:
        bias  = "Moderate — Mild Trend Possible"
        color = "neut"
    else:
        bias  = "Wide — Range-Bound Day Expected"
        color = "bear"

    return {
        "Pivot":  round(P, 2),
        "TC":     round(TC, 2),
        "BC":     round(BC, 2),
        "Width%": round(width_pct, 3),
        "Bias":   bias,
        "Color":  color,
    }


def compute_virgin_cprs(df: pd.DataFrame) -> list:
    """
    Virgin CPRs: Prior CPR bands that price never re-visited.
    These are Ochoa's high-conviction magnet levels.
    """
    result = []
    if len(df) < 3:
        return result
    for i in range(2, min(len(df), 15)):
        ref  = df.iloc[i - 1]
        H, L, C = ref["High"], ref["Low"], ref["Close"]
        P  = (H + L + C) / 3
        BC = (H + L) / 2
        TC = (P - BC) + P
        future  = df.iloc[i:]
        touched = ((future["High"] >= BC) & (future["Low"] <= TC)).any()
        result.append({
            "Date":   df.index[i - 1].strftime("%d-%b"),
            "TC":     round(TC, 2),
            "BC":     round(BC, 2),
            "Virgin": not touched,
        })
    return result


def compute_market_profile(df: pd.DataFrame, bins: int = 60) -> dict:
    """
    Simplified Market Profile: POC, Value Area High, Value Area Low.
    Uses volume-at-price histogram over the full lookback window.
    """
    if df.empty or "Volume" not in df.columns:
        return {}
    try:
        prices, vols = [], []
        for _, row in df.iterrows():
            ticks = np.linspace(row["Low"], row["High"], 10)
            v_per = row["Volume"] / 10
            prices.extend(ticks)
            vols.extend([v_per] * 10)

        counts, edges = np.histogram(prices, bins=bins, weights=vols)
        poc_idx = int(np.argmax(counts))
        poc     = round((edges[poc_idx] + edges[poc_idx + 1]) / 2, 2)

        # Value Area = 70% of total volume centred on POC
        target = counts.sum() * 0.70
        lo = hi = poc_idx
        accum   = counts[poc_idx]
        while accum < target:
            ext_lo = counts[lo - 1] if lo > 0            else 0
            ext_hi = counts[hi + 1] if hi < len(counts)-1 else 0
            if ext_lo == 0 and ext_hi == 0:
                break
            if ext_lo >= ext_hi and lo > 0:
                lo -= 1; accum += counts[lo]
            elif hi < len(counts) - 1:
                hi += 1; accum += counts[hi]
            else:
                lo -= 1; accum += counts[lo]

        vah = round((edges[hi] + edges[hi + 1]) / 2, 2)
        val = round((edges[lo] + edges[lo + 1]) / 2, 2)
        return {"POC": poc, "VAH": vah, "VAL": val}
    except Exception:
        return {}


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    All Pivot Boss indicators:
    - 3/10 Oscillator (Ochoa's momentum signature)
    - HMA-20 (Hull MA for trend)
    - ATR-14
    - RSI-14
    - Stochastic 14,3,3
    """
    df = df.copy()
    close, high, low = df["Close"], df["High"], df["Low"]

    # 3/10 Oscillator ──────────────────────────────────────────────────────────
    df["MA3"]   = close.rolling(3).mean()
    df["MA10"]  = close.rolling(10).mean()
    df["DIFF"]  = df["MA3"] - df["MA10"]
    df["SIG16"] = df["DIFF"].rolling(16).mean()
    df["HIST"]  = df["DIFF"] - df["SIG16"]

    # Hull Moving Average ──────────────────────────────────────────────────────
    def wma(s, n):
        w = np.arange(1, n + 1)
        return s.rolling(n).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)

    def hma(s, n=20):
        return wma(2 * wma(s, n // 2) - wma(s, n), int(np.sqrt(n)))

    df["HMA20"]  = hma(close, 20)
    df["HMA_UP"] = df["HMA20"] > df["HMA20"].shift(1)

    # ATR ─────────────────────────────────────────────────────────────────────
    df["TR"] = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    df["ATR14"] = df["TR"].rolling(14).mean()

    # RSI ─────────────────────────────────────────────────────────────────────
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["RSI14"] = 100 - (100 / (1 + gain / loss.replace(0, np.nan)))

    # Stochastic ──────────────────────────────────────────────────────────────
    lo14 = low.rolling(14).min()
    hi14 = high.rolling(14).max()
    df["STOCH_K"] = 100 * (close - lo14) / (hi14 - lo14).replace(0, np.nan)
    df["STOCH_D"] = df["STOCH_K"].rolling(3).mean()

    return df


def full_pivot_boss_analysis(df: pd.DataFrame, pivot_type: str) -> dict:
    """Master analysis: runs all Pivot Boss tools and produces a signal dict."""
    if df.empty or len(df) < 20:
        return {}

    df_ind  = compute_indicators(df)
    pivots  = compute_pivot_points(df, pivot_type)
    cpr     = compute_cpr(df)
    mp      = compute_market_profile(df)
    virgins = compute_virgin_cprs(df)

    last = df_ind.iloc[-1]
    prev = df_ind.iloc[-2]
    ltp  = round(float(last["Close"]), 2)

    # CPR position
    if cpr:
        if   ltp > cpr["TC"]: cpr_pos, cpr_col = "Bullish (Above CPR)",    "bull"
        elif ltp < cpr["BC"]: cpr_pos, cpr_col = "Bearish (Below CPR)",    "bear"
        else:                 cpr_pos, cpr_col = "Inside CPR (Neutral)",   "neut"
    else:
        cpr_pos, cpr_col = "N/A", "neut"

    # Nearest pivot
    nearest = None
    if pivots:
        nearest = min(pivots.items(), key=lambda kv: abs(ltp - kv[1]))

    # 3/10 Oscillator signal
    d_now, d_prev = float(last["DIFF"]),  float(prev["DIFF"])
    s_now, s_prev = float(last["SIG16"]), float(prev["SIG16"])
    if   d_now > s_now and d_prev <= s_prev: osc_sig, osc_col = "Bullish Crossover ▲", "bull"
    elif d_now < s_now and d_prev >= s_prev: osc_sig, osc_col = "Bearish Crossover ▼", "bear"
    elif d_now > 0:                          osc_sig, osc_col = "Positive Momentum",   "bull"
    elif d_now < 0:                          osc_sig, osc_col = "Negative Momentum",   "bear"
    else:                                    osc_sig, osc_col = "Neutral",              "neut"

    # HMA
    hma_up = bool(last["HMA_UP"])
    hma_sig, hma_col = ("Uptrend ▲", "bull") if hma_up else ("Downtrend ▼", "bear")

    # RSI
    rsi = round(float(last["RSI14"]), 1) if not np.isnan(last["RSI14"]) else None
    if rsi:
        if   rsi >= 70: rsi_sig, rsi_col = "Overbought", "bear"
        elif rsi <= 30: rsi_sig, rsi_col = "Oversold",   "bull"
        else:           rsi_sig, rsi_col = "Neutral",    "neut"
    else:
        rsi_sig, rsi_col = "N/A", "neut"

    # Stochastic
    stk = round(float(last["STOCH_K"]), 1) if not np.isnan(last["STOCH_K"]) else None
    std = round(float(last["STOCH_D"]), 1) if not np.isnan(last["STOCH_D"]) else None

    # ATR
    atr     = round(float(last["ATR14"]), 2) if not np.isnan(last["ATR14"]) else None
    atr_pct = round(atr / ltp * 100, 2) if atr else None

    # Overall bias
    bull_n = sum([cpr_col == "bull", osc_col == "bull", hma_col == "bull", rsi_col == "bull"])
    bear_n = sum([cpr_col == "bear", osc_col == "bear", hma_col == "bear", rsi_col == "bear"])
    if   bull_n >= 3: overall, ov_col = "BULLISH",        "bull"
    elif bear_n >= 3: overall, ov_col = "BEARISH",        "bear"
    else:             overall, ov_col = "NEUTRAL / MIXED", "neut"

    return dict(
        ltp=ltp, pivots=pivots, cpr=cpr,
        cpr_position=cpr_pos, cpr_col=cpr_col,
        market_profile=mp, virgin_cprs=virgins,
        osc_sig=osc_sig, osc_col=osc_col,
        hma_sig=hma_sig, hma_col=hma_col,
        rsi=rsi, rsi_sig=rsi_sig, rsi_col=rsi_col,
        stoch_k=stk, stoch_d=std,
        atr=atr, atr_pct=atr_pct,
        nearest=nearest,
        overall=overall, ov_col=ov_col,
        df_ind=df_ind,
    )


# ─────────────────────────────────────────────
#  CHART BUILDERS
# ─────────────────────────────────────────────
def build_pivot_boss_chart(df: pd.DataFrame, symbol: str,
                            analysis: dict, pivot_type: str) -> go.Figure:
    """
    4-panel Pivot Boss chart:
    Row 1 (55%): Candles + HMA + Pivot levels + CPR band + POC/VAH/VAL
    Row 2 (18%): 3/10 Oscillator (histogram + Diff + Signal)
    Row 3 (14%): RSI-14
    Row 4 (13%): Volume
    """
    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.18, 0.14, 0.13],
        vertical_spacing=0.02,
    )
    df_ind = analysis.get("df_ind", df)
    pivots = analysis.get("pivots", {})
    cpr    = analysis.get("cpr", {})
    mp     = analysis.get("market_profile", {})

    # Candlesticks
    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
        name="Price",
        increasing_line_color="#00e5a0", decreasing_line_color="#ff4d6a",
        increasing_fillcolor="rgba(0,229,160,0.15)", decreasing_fillcolor="rgba(255,77,106,0.15)",
        line=dict(width=1), showlegend=False,
    ), row=1, col=1)

    # HMA
    if "HMA20" in df_ind.columns:
        fig.add_trace(go.Scatter(
            x=df_ind.index, y=df_ind["HMA20"],
            name="HMA(20)", line=dict(color="#4d7cfe", width=1.5, dash="dot"),
        ), row=1, col=1)

    # CPR band
    if cpr:
        x0, x1 = df.index[0], df.index[-1]
        fig.add_trace(go.Scatter(
            x=[x0, x1, x1, x0], y=[cpr["TC"], cpr["TC"], cpr["BC"], cpr["BC"]],
            fill="toself", fillcolor="rgba(77,124,254,0.07)",
            line=dict(color="rgba(77,124,254,0)", width=0),
            name="CPR Band", showlegend=True, mode="lines",
        ), row=1, col=1)
        for label, val, col in [("TC", cpr["TC"], "#4d7cfe"),
                                  ("P",  cpr["Pivot"], "#aab4d4"),
                                  ("BC", cpr["BC"], "#4d7cfe")]:
            fig.add_hline(y=val, line=dict(color=col, width=1, dash="dash"),
                          annotation_text=f"  {label} {val}",
                          annotation_font=dict(size=9, color=col), row=1, col=1)

    # Pivot levels
    color_map = {
        "R1": "#ff8c69", "R2": "#ff6b6b", "R3": "#ff4d6a", "R4": "#cc2e47",
        "S1": "#69ffb8", "S2": "#3dffa0", "S3": "#00e5a0", "S4": "#00b880",
    }
    for k, v in pivots.items():
        if k == "P":
            continue
        c = color_map.get(k, "#888")
        fig.add_hline(y=v, line=dict(color=c, width=0.8, dash="dot"),
                      annotation_text=f"  {k} {v}",
                      annotation_font=dict(size=9, color=c), row=1, col=1)

    # Market Profile
    for k, v, c in [("POC", mp.get("POC"), "#f5a623"),
                    ("VAH", mp.get("VAH"), "#b0b8d0"),
                    ("VAL", mp.get("VAL"), "#b0b8d0")]:
        if v:
            fig.add_hline(y=v, line=dict(color=c, width=1.2, dash="longdash"),
                          annotation_text=f"  {k} {v}",
                          annotation_font=dict(size=9, color=c), row=1, col=1)

    # 3/10 Oscillator
    if "HIST" in df_ind.columns:
        hist_vals   = df_ind["HIST"].fillna(0)
        hist_colors = ["#00e5a0" if v >= 0 else "#ff4d6a" for v in hist_vals]
        fig.add_trace(go.Bar(
            x=df_ind.index, y=hist_vals,
            marker_color=hist_colors, opacity=0.65, showlegend=False,
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df_ind.index, y=df_ind["DIFF"],
            name="Diff(3-10)", line=dict(color="#00e5a0", width=1.2),
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df_ind.index, y=df_ind["SIG16"],
            name="Signal(16)", line=dict(color="#f5a623", width=1.2, dash="dot"),
        ), row=2, col=1)
        fig.add_hline(y=0, line=dict(color="#1e2330", width=1), row=2, col=1)

    # RSI
    if "RSI14" in df_ind.columns:
        fig.add_trace(go.Scatter(
            x=df_ind.index, y=df_ind["RSI14"],
            name="RSI(14)", line=dict(color="#4d7cfe", width=1.2), showlegend=False,
        ), row=3, col=1)
        for level, color in [(70, "rgba(255,77,106,0.33)"), (30, "rgba(0,229,160,0.33)"), (50, "#1e2330")]:
            fig.add_hline(y=level, line=dict(color=color, width=0.8, dash="dot"), row=3, col=1)

    # Volume
    vol_colors = ["#00e5a0" if c >= o else "#ff4d6a"
                  for c, o in zip(df["Close"], df["Open"])]
    fig.add_trace(go.Bar(
        x=df.index, y=df["Volume"],
        marker_color=vol_colors, opacity=0.55, showlegend=False,
    ), row=4, col=1)

    # Layout
    fig.update_layout(
        height=800,
        paper_bgcolor="#0d0f14", plot_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Mono", color="#6b7490", size=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0,
                    font=dict(size=10), bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=0, r=90, t=30, b=0),
        xaxis_rangeslider_visible=False,
        title=dict(
            text=f"{symbol}  ·  {pivot_type} Pivots  ·  Pivot Boss Analysis",
            font=dict(family="IBM Plex Mono", size=12, color="#d4daf0"),
        ),
    )
    for i in range(1, 5):
        fig.update_xaxes(showgrid=True, gridcolor="#1a1e2a", row=i, col=1)
        fig.update_yaxes(showgrid=True, gridcolor="#1a1e2a", row=i, col=1)
    fig.update_yaxes(title_text="3/10", title_font_size=9, row=2, col=1)
    fig.update_yaxes(title_text="RSI",  title_font_size=9, row=3, col=1)
    fig.update_yaxes(title_text="Vol",  title_font_size=9, row=4, col=1)
    return fig


def build_stoch_chart(df_ind: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_ind.index, y=df_ind["STOCH_K"],
                             name="%K", line=dict(color="#00e5a0", width=1.2)))
    fig.add_trace(go.Scatter(x=df_ind.index, y=df_ind["STOCH_D"],
                             name="%D", line=dict(color="#f5a623", width=1.2, dash="dot")))
    for level, color in [(80, "rgba(255,77,106,0.33)"), (20, "rgba(0,229,160,0.33)"), (50, "#1e2330")]:
        fig.add_hline(y=level, line=dict(color=color, width=0.8, dash="dot"))
    fig.update_layout(
        height=200, paper_bgcolor="#0d0f14", plot_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Mono", color="#6b7490", size=10),
        margin=dict(l=0, r=60, t=28, b=0),
        legend=dict(orientation="h", font=dict(size=10), bgcolor="rgba(0,0,0,0)"),
        title=dict(text="Stochastic (14, 3, 3)", font=dict(size=11, color="#d4daf0")),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#1a1e2a")
    fig.update_yaxes(showgrid=True, gridcolor="#1a1e2a")
    return fig


# ─────────────────────────────────────────────
#  UI HELPERS
# ─────────────────────────────────────────────
def sig_badge(label: str, kind: str) -> str:
    css = {"bull": "sig-bull", "bear": "sig-bear", "neut": "sig-neut"}.get(kind, "sig-neut")
    return f'<span class="signal-badge {css}">{label}</span>'


def render_market_header():
    indices = {"NIFTY 50": "^NSEI", "SENSEX": "^BSESN", "NIFTY BANK": "^NSEBANK"}
    cols = st.columns(len(indices))
    for col, (name, ticker) in zip(cols, indices.items()):
        d = fetch_index_data(ticker)
        ltp, chg = d["ltp"], d["change"]
        if ltp is not None:
            col.metric(name, f"{ltp:,.2f}", f"{'+' if chg >= 0 else ''}{chg}%")
        else:
            col.metric(name, "—", "—")


def render_movers_table(df: pd.DataFrame, title: str, color: str):
    st.markdown(
        f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
        f"letter-spacing:0.08em;text-transform:uppercase;color:{color};"
        f"margin-bottom:0.5rem;'>{title}</div>", unsafe_allow_html=True,
    )
    if df.empty:
        st.caption("Data unavailable — NSE API may require VPN / direct browser session.")
        return

    # CSS: make symbol buttons look like hyperlinks, not buttons
    st.markdown("""
    <style>
    [data-testid="stHorizontalBlock"] .mover-link-btn button {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        text-decoration: underline !important;
        text-underline-offset: 3px !important;
        text-decoration-style: dotted !important;
        cursor: pointer !important;
        justify-content: flex-start !important;
        letter-spacing: 0.01em !important;
        min-height: unset !important;
        height: auto !important;
        line-height: 1.4 !important;
        box-shadow: none !important;
    }
    [data-testid="stHorizontalBlock"] .mover-link-btn button:hover {
        text-decoration-style: solid !important;
        background: transparent !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Header row
    h1, h2, h3 = st.columns([2.8, 1.8, 1.8])
    h1.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;color:#4a5068;letter-spacing:0.06em;'>SYMBOL</div>", unsafe_allow_html=True)
    h2.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;color:#4a5068;letter-spacing:0.06em;'>LTP</div>", unsafe_allow_html=True)
    h3.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;color:#4a5068;letter-spacing:0.06em;'>CHG %</div>", unsafe_allow_html=True)

    chg_color = "#00e5a0" if color == "#00e5a0" else "#ff4d6a"

    for _, row in df.iterrows():
        sym     = str(row.get("Symbol", "")).strip()
        ltp     = row.get("LTP", "—")
        chg     = row.get("Chg %", "")
        chg_str = f"{chg:+.2f}%" if isinstance(chg, (int, float)) else str(chg)

        col_sym, col_ltp, col_chg = st.columns([2.8, 1.8, 1.8])

        with col_sym:
            # Wrap in a div with our CSS class so the button looks like a link
            st.markdown('<div class="mover-link-btn">', unsafe_allow_html=True)
            clicked = st.button(
                sym,
                key=f"mover_lnk_{sym}_{color[-3:]}",
                use_container_width=False,
            )
            st.markdown('</div>', unsafe_allow_html=True)
            if clicked:
                st.session_state["screener_symbol"]      = sym
                st.session_state["screener_nav_pending"] = True
                st.session_state["current_page"]         = "Stock Screener"
                st.session_state["mobile_page"]          = "Stock Screener"
                st.query_params["nav"] = "screener"
                st.rerun()

        with col_ltp:
            st.markdown(
                f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.82rem;"
                f"color:#9aa3c0;padding-top:0.3rem;'>{ltp}</div>",
                unsafe_allow_html=True,
            )
        with col_chg:
            st.markdown(
                f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.82rem;"
                f"color:{chg_color};padding-top:0.3rem;'>{chg_str}</div>",
                unsafe_allow_html=True,
            )


@st.cache_data(ttl=300)
def fetch_heatmap_performance(symbols: list, max_stocks: int = 120) -> pd.DataFrame:
    """
    Batch-fetch 1-day % change for up to `max_stocks` NSE symbols using
    yfinance download (single request = much faster than per-ticker calls).
    Returns a DataFrame with columns: Symbol, Change%.
    """
    sample  = symbols[:max_stocks]
    tickers = [s + ".NS" for s in sample]
    result  = []
    try:
        raw = yf.download(
            tickers, period="2d", interval="1d",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True,
        )
        for sym, ticker in zip(sample, tickers):
            try:
                if len(tickers) == 1:
                    closes = raw["Close"]
                else:
                    closes = raw[ticker]["Close"]
                closes = closes.dropna()
                if len(closes) >= 2:
                    chg = round((closes.iloc[-1] - closes.iloc[-2]) / closes.iloc[-2] * 100, 2)
                    result.append({"Symbol": sym, "Change%": chg})
                else:
                    result.append({"Symbol": sym, "Change%": 0.0})
            except Exception:
                result.append({"Symbol": sym, "Change%": 0.0})
    except Exception:
        result = [{"Symbol": s, "Change%": 0.0} for s in sample]

    return pd.DataFrame(result)


def build_sector_treemap(nse500: pd.DataFrame, perf_df: pd.DataFrame) -> go.Figure:
    """
    Sector-level only treemap.
    Uses plain Industry names as both ids and labels so on_select returns
    a clean, matchable sector name string.
    """
    df = nse500.copy()
    if not perf_df.empty:
        df = df.merge(perf_df[["Symbol", "Change%"]], on="Symbol", how="left")
        df["Change%"] = df["Change%"].fillna(0.0)
    else:
        df["Change%"] = 0.0

    sector_df = (
        df.groupby("Industry")
        .agg(
            Avg_Change=("Change%", "mean"),
            Stock_Count=("Symbol", "count"),
            Gainers=("Change%", lambda x: (x > 0).sum()),
            Losers=("Change%",  lambda x: (x < 0).sum()),
        )
        .reset_index()
    )
    sector_df["Avg_Change"] = sector_df["Avg_Change"].round(2)
    clamp = 5.0
    sector_df["ColorVal"] = sector_df["Avg_Change"].clip(-clamp, clamp)

    # Display text on tile: sector name + change (shown inside tile)
    def _tile_text(row):
        chg   = row["Avg_Change"]
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "─")
        return f"{row['Industry']}<br>{arrow}{abs(chg):.2f}%"

    sector_df["TileText"] = sector_df.apply(_tile_text, axis=1)

    # Build customdata as list-of-lists (not np.column_stack) to preserve types
    customdata = [
        [row["Industry"], float(row["Avg_Change"]),
         int(row["Gainers"]), int(row["Losers"]), int(row["Stock_Count"])]
        for _, row in sector_df.iterrows()
    ]

    fig = go.Figure(go.Treemap(
        # ids = plain sector name → this is what on_select returns as point_index label
        ids=sector_df["Industry"].tolist(),
        labels=sector_df["TileText"].tolist(),
        parents=[""] * len(sector_df),
        values=sector_df["Stock_Count"].tolist(),
        customdata=customdata,
        marker=dict(
            colors=sector_df["ColorVal"].tolist(),
            colorscale=[
                [0.00, "#7b0020"],
                [0.20, "#c0392b"],
                [0.40, "#e74c3c"],
                [0.50, "#1e2330"],
                [0.60, "#1a6640"],
                [0.80, "#27ae60"],
                [1.00, "#00e5a0"],
            ],
            cmin=-clamp,
            cmax=clamp,
            line=dict(width=2, color="#0d0f14"),
            colorbar=dict(
                title=dict(text="Avg 1D %", font=dict(size=10, family="IBM Plex Mono")),
                tickfont=dict(size=9, family="IBM Plex Mono"),
                thickness=12, len=0.65,
            ),
        ),
        hovertemplate=(
            "<b>%{id}</b><br>"
            "Avg Change: %{customdata[1]:+.2f}%<br>"
            "▲ Gainers: %{customdata[2]}  ▼ Losers: %{customdata[3]}<br>"
            "Total Stocks: %{customdata[4]}<br>"
            "<i>Click to see top gainers & losers</i>"
            "<extra></extra>"
        ),
        textfont=dict(family="IBM Plex Mono", size=12, color="#ffffff"),
        textposition="middle center",
    ))

    fig.update_layout(
        paper_bgcolor="#0d0f14",
        margin=dict(l=0, r=0, t=0, b=0),
        height=480,
        font=dict(family="IBM Plex Mono", color="#d4daf0"),
    )
    return fig


def sector_treemap(nse500: pd.DataFrame, perf_df: pd.DataFrame) -> go.Figure:
    return build_sector_treemap(nse500, perf_df)


# ─────────────────────────────────────────────
#  PAGES
# ─────────────────────────────────────────────
def page_login():
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;
                justify-content:center;min-height:70vh;gap:1rem;">
        <div style="font-family:'IBM Plex Mono',monospace;font-size:2.8rem;
                    font-weight:600;letter-spacing:-0.03em;color:#d4daf0;">
            🏦 PivotVault <span style="color:#00e5a0;">AI</span>
        </div>
        <div style="font-family:'IBM Plex Mono',monospace;font-size:0.82rem;
                    color:#4a5068;letter-spacing:0.1em;text-transform:uppercase;">
            Indian Equity Intelligence Terminal · Pivot Boss Methodology
        </div>
    </div>""", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([2, 1, 2])
    with c2:
        username = st.text_input("Username", placeholder="demo", label_visibility="collapsed")
        password = st.text_input("Password", type="password", placeholder="password",
                                 label_visibility="collapsed")
        if st.button("→  Enter Terminal", use_container_width=True):
            if username.strip() and password.strip():
                st.session_state["logged_in"] = True
                st.session_state["username"]  = username.strip()
                st.rerun()
            else:
                st.error("Enter username and password.")


def page_market_snapshot(nse500: pd.DataFrame):
    st.markdown(
        '<div class="title-bar"><span class="live-dot"></span><h1>Market Snapshot</h1>'
        f'<span class="ts">{datetime.now().strftime("%d %b %Y  %H:%M")}</span></div>',
        unsafe_allow_html=True,
    )
    gainers, losers = get_market_movers()
    c1, c2 = st.columns(2)
    with c1: render_movers_table(gainers, "▲ Top Gainers", "#00e5a0")
    with c2: render_movers_table(losers,  "▼ Top Losers",  "#ff4d6a")
    st.divider()

    # ── Performance Heatmap ───────────────────────────────────────────────────
    hm_col, legend_col = st.columns([5, 1])
    with hm_col:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
            "letter-spacing:0.08em;text-transform:uppercase;color:#4a5068;"
            "margin-bottom:0.4rem;'>"
            "<span class='live-dot'></span>"
            "Sectoral Heatmap · Nifty 500 · Colour = Avg 1-Day % Change · Click a sector for detail</div>",
            unsafe_allow_html=True,
        )
    with legend_col:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
            "color:#4a5068;padding-top:0.1rem;line-height:1.9;'>"
            "<span style='color:#00e5a0;'>■</span> Strong Gain<br>"
            "<span style='color:#27ae60;'>■</span> Gain<br>"
            "<span style='color:#1e2330;border:1px solid #333;'>■</span> Flat<br>"
            "<span style='color:#e74c3c;'>■</span> Loss<br>"
            "<span style='color:#7b0020;'>■</span> Strong Loss"
            "</div>",
            unsafe_allow_html=True,
        )

    symbols = nse500["Symbol"].dropna().tolist()

    with st.spinner("Fetching live performance data for heatmap…"):
        perf_df = fetch_heatmap_performance(symbols, max_stocks=120)

    # (summary metrics removed — detail shown below on sector click)

    # ── Build sector lookup once (used both for chart and detail panel) ────────
    df_merged = nse500.merge(perf_df[["Symbol","Change%"]], on="Symbol", how="left") if not perf_df.empty else nse500.copy()
    df_merged["Change%"] = df_merged.get("Change%", pd.Series(0.0, index=df_merged.index)).fillna(0.0)
    valid_sectors = set(df_merged["Industry"].dropna().unique())

    # ── Treemap — sector level only, click to drill down ─────────────────────
    fig = build_sector_treemap(nse500, perf_df)

    # Persist selected sector across reruns in session_state
    if "heatmap_sector" not in st.session_state:
        st.session_state["heatmap_sector"] = None

    selection = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        key="sector_heatmap",
    )

    # ── Parse click — try every field Streamlit/Plotly might return ──────────
    clicked_sector = None
    try:
        if selection:
            # Streamlit wraps the event as either a dict or an object
            sel_data = selection if isinstance(selection, dict) else vars(selection)
            inner    = sel_data.get("selection") or sel_data
            pts      = inner.get("points") or inner.get("point_indices") or []

            if pts:
                pt = pts[0] if isinstance(pts[0], dict) else {}

                # Try every field that might carry the sector name
                candidates = [
                    pt.get("id"),
                    pt.get("label"),
                    pt.get("text"),
                    # customdata is a list; index 0 = Industry
                    (pt.get("customdata") or [None])[0],
                    pt.get("hovertext"),
                ]

                for c in candidates:
                    if c and isinstance(c, str):
                        # Strip any HTML tags from label field
                        import re as _re
                        clean = _re.sub(r"<[^>]+>", "", c).split("\n")[0].strip()
                        if clean and clean in valid_sectors:
                            clicked_sector = clean
                            break
    except Exception:
        pass

    # Persist across reruns (on_select causes rerun which clears local vars)
    if clicked_sector:
        st.session_state["heatmap_sector"] = clicked_sector
    clicked_sector = st.session_state.get("heatmap_sector")

    # ── Sector detail panel — shown directly beneath heatmap on click ──────────
    if clicked_sector and not perf_df.empty:
        sector_stocks = df_merged[df_merged["Industry"] == clicked_sector].copy()

        if not sector_stocks.empty:
            top_g = sector_stocks.nlargest(5, "Change%")
            top_l = sector_stocks.nsmallest(5, "Change%")
            avg_chg = sector_stocks["Change%"].mean()
            col_fg  = "#00e5a0" if avg_chg >= 0 else "#ff4d6a"

            # Thin header bar — sector name + avg change + clear button
            hdr1, hdr2 = st.columns([5, 1])
            with hdr1:
                arrow = "▲" if avg_chg > 0 else "▼"
                st.markdown(
                    f"<div style='font-family:IBM Plex Mono,monospace;padding:0.5rem 0;"
                    f"border-bottom:1px solid #1e2330;margin-bottom:0.6rem;'>"
                    f"<span style='font-size:1rem;font-weight:700;color:#d4daf0;'>"
                    f"{clicked_sector}</span>"
                    f"<span style='font-size:0.8rem;color:{col_fg};margin-left:1rem;'>"
                    f"{arrow} {avg_chg:+.2f}% avg  ·  {len(sector_stocks)} stocks</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with hdr2:
                if st.button("✕ Clear", key="clear_sector"):
                    st.session_state["heatmap_sector"] = None
                    st.rerun()

            d1, d2 = st.columns(2)
            with d1:
                st.markdown(
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    f"letter-spacing:0.08em;text-transform:uppercase;color:#00e5a0;"
                    f"margin-bottom:0.35rem;'>▲ Top 5 Gainers  "
                    f"<span style='color:#4a5068;font-size:0.62rem;'>(click to screen)</span></div>",
                    unsafe_allow_html=True,
                )
                for _, r in top_g.iterrows():
                    _sym = r["Symbol"]; _chg = r["Change%"]
                    ca, cb = st.columns([3.2, 2.2])
                    with ca:
                        st.markdown('<div class="mover-link-btn">', unsafe_allow_html=True)
                        if st.button(_sym, key=f"hm_g_{_sym}", use_container_width=False):
                            st.session_state["screener_symbol"]      = _sym
                            st.session_state["screener_nav_pending"] = True
                            st.session_state["current_page"]         = "Stock Screener"
                            st.session_state["mobile_page"]          = "Stock Screener"
                            st.query_params["nav"] = "screener"
                            st.rerun()
                        st.markdown('</div>', unsafe_allow_html=True)
                    cb.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.8rem;color:#00e5a0;padding-top:0.3rem;'>+{_chg:.2f}%</div>", unsafe_allow_html=True)

            with d2:
                st.markdown(
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    f"letter-spacing:0.08em;text-transform:uppercase;color:#ff4d6a;"
                    f"margin-bottom:0.35rem;'>▼ Top 5 Losers  "
                    f"<span style='color:#4a5068;font-size:0.62rem;'>(click to screen)</span></div>",
                    unsafe_allow_html=True,
                )
                for _, r in top_l.iterrows():
                    _sym = r["Symbol"]; _chg = r["Change%"]
                    ca, cb = st.columns([3.2, 2.2])
                    with ca:
                        st.markdown('<div class="mover-link-btn">', unsafe_allow_html=True)
                        if st.button(_sym, key=f"hm_l_{_sym}", use_container_width=False):
                            st.session_state["screener_symbol"]      = _sym
                            st.session_state["screener_nav_pending"] = True
                            st.session_state["current_page"]         = "Stock Screener"
                            st.session_state["mobile_page"]          = "Stock Screener"
                            st.query_params["nav"] = "screener"
                            st.rerun()
                        st.markdown('</div>', unsafe_allow_html=True)
                    cb.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.8rem;color:#ff4d6a;padding-top:0.3rem;'>{_chg:.2f}%</div>", unsafe_allow_html=True)

    else:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
            "color:#2a3040;text-align:center;padding:0.55rem;"
            "border:1px dashed #1e2330;border-radius:6px;margin-top:0.25rem;'>"
            "👆  Click any sector tile to see its Top 5 Gainers &amp; Losers</div>",
            unsafe_allow_html=True,
        )


def page_stock_screener(nse500: pd.DataFrame):
    # Light-theme style overrides for screener
    st.markdown("""
    <style>
    .screener-metrics div[data-testid="metric-container"] {
        background: #f8fafc !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 8px;
    }
    .screener-metrics div[data-testid="metric-container"] label {
        color: #64748b !important;
    }
    .screener-metrics div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #1e293b !important;
        font-size: 1.2rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(
        "<h2 style='font-family:IBM Plex Mono,monospace;color:#1e293b;font-size:1.15rem;"
        "margin-bottom:0.25rem;'>Stock Screener</h2>"
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;letter-spacing:0.08em;"
        "text-transform:uppercase;color:#94a3b8;margin-bottom:1rem;'>"
        "Search · Analyse · Add to Watchlist</div>",
        unsafe_allow_html=True,
    )

    symbols = nse500["Symbol"].dropna().sort_values().tolist()

    # ── Auto-select symbol if navigated from Market Snapshot ─────────────────
    if st.session_state.get("screener_nav_pending") and st.session_state.get("screener_symbol"):
        _pre = st.session_state["screener_symbol"]
        if _pre in symbols:
            _default_idx = symbols.index(_pre)
        else:
            _default_idx = 0
        st.session_state["screener_nav_pending"] = False
        st.toast(f"📊 Loaded {_pre} from Market Snapshot", icon="🚀")
    else:
        _default_idx = 0
        if st.session_state.get("screener_symbol") in symbols:
            _default_idx = symbols.index(st.session_state["screener_symbol"])

    # ── Interval → allowed periods mapping ──────────────────────────────────
    INTERVAL_CFG = {
        "5 min":   {"interval": "5m",  "periods": ["1d", "2d", "5d"],          "default": "5d"},
        "10 min":  {"interval": "10m", "periods": ["1d", "2d", "5d"],          "default": "5d"},
        "15 min":  {"interval": "15m", "periods": ["1d", "2d", "5d", "1mo"],   "default": "5d"},
        "30 min":  {"interval": "30m", "periods": ["1d", "5d", "1mo"],         "default": "1mo"},
        "1 hour":  {"interval": "1h",  "periods": ["5d", "1mo", "3mo"],        "default": "1mo"},
        "2 hour":  {"interval": "2h",  "periods": ["5d", "1mo", "3mo"],        "default": "1mo"},
        "4 hour":  {"interval": "1h",  "periods": ["1mo", "3mo", "6mo"],       "default": "3mo"},
        "Daily":   {"interval": "1d",  "periods": ["1mo","3mo","6mo","1y","2y","5y"], "default": "1y"},
        "Weekly":  {"interval": "1wk", "periods": ["6mo","1y","2y","5y"],      "default": "2y"},
        "Monthly": {"interval": "1mo", "periods": ["1y","2y","5y"],            "default": "5y"},
    }

    c1, c2, c3, c4 = st.columns([3, 1.2, 1.2, 1])
    with c1:
        choice = st.selectbox("Symbol", symbols, index=_default_idx, label_visibility="collapsed", key="screener_sym_select")
    with c2:
        tf_label = st.selectbox(
            "Interval",
            list(INTERVAL_CFG.keys()),
            index=7,                        # default = Daily
            label_visibility="collapsed",
            key="scr_tf",
        )
    with c3:
        cfg            = INTERVAL_CFG[tf_label]
        period_options = cfg["periods"]
        default_idx    = period_options.index(cfg["default"]) if cfg["default"] in period_options else 0
        period = st.selectbox(
            "Period",
            period_options,
            index=default_idx,
            label_visibility="collapsed",
            key="scr_period",
        )
    with c4:
        if st.button("＋ Watchlist"):
            if choice not in st.session_state["watchlist"]:
                st.session_state["watchlist"].append(choice)
                st.success(f"{choice} added.")
            else:
                st.info("Already in watchlist.")

    interval = cfg["interval"]

    st.divider()

    # Fundamentals strip
    info = fetch_stock_info(choice)
    def _fmt(val, fmt="{:.2f}"):
        return fmt.format(val) if val and val != "N/A" else "—"

    company_name = info.get("longName") or info.get("shortName") or choice
    sector       = info.get("sector") or info.get("industry") or ""

    st.markdown(
        f"<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;"
        f"padding:0.75rem 1.25rem;margin-bottom:1rem;'>"
        f"<div style='font-family:IBM Plex Mono,monospace;font-size:1rem;font-weight:600;"
        f"color:#1e293b;'>{company_name}</div>"
        f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#94a3b8;"
        f"text-transform:uppercase;letter-spacing:0.07em;'>"
        f"{(sector + '  ·  ') if sector else ''}NSE: {choice}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<div class='screener-metrics'>", unsafe_allow_html=True)
    fcols = st.columns(5)
    fcols[0].metric("Mkt Cap (Cr)", _fmt(info.get("marketCap", 0) / 1e7, "{:,.0f}"))
    fcols[1].metric("P/E Ratio",    _fmt(info.get("trailingPE")))
    fcols[2].metric("52W High",     _fmt(info.get("fiftyTwoWeekHigh")))
    fcols[3].metric("52W Low",      _fmt(info.get("fiftyTwoWeekLow")))
    fcols[4].metric("Div Yield %",  _fmt((info.get("dividendYield") or 0) * 100))
    st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    # ── Overlay toggles ───────────────────────────────────────────────────────
    ov1, ov2, ov3 = st.columns([1, 1, 2])
    with ov1:
        show_sma   = st.checkbox("Moving Averages (SMA 20 / 50)", value=True, key="scr_sma")
    with ov2:
        show_pivots = st.checkbox("Pivot Points", value=True, key="scr_piv")
    with ov3:
        scr_pivot_type = st.selectbox(
            "Pivot Type",
            ["Traditional", "Woodie", "Camarilla", "DeMark", "Fibonacci"],
            key="scr_pt",
            label_visibility="collapsed",
            disabled=not show_pivots,
        )

    # ── Fetch price history ───────────────────────────────────────────────────
    hist = fetch_stock_history(choice, period, interval)
    if hist.empty:
        st.warning("No price data available.")
        return

    # ── Optional overlays ────────────────────────────────────────────────────
    if show_sma:
        hist["SMA20"] = hist["Close"].rolling(20).mean()
        hist["SMA50"] = hist["Close"].rolling(50).mean()

    pivots = {}
    if show_pivots and len(hist) >= 2:
        pivots = compute_pivot_points(hist, scr_pivot_type)

    PIVOT_COLORS = {
        "R1": "#e05c2a", "R2": "#c0392b", "R3": "#922b21", "R4": "#7b241c",
        "S1": "#1e8449", "S2": "#196f3d", "S3": "#145a32", "S4": "#0e4025",
        "P":  "#7d3c98",
    }

    # ── Build chart ───────────────────────────────────────────────────────────
    fig = go.Figure()

    # Candlesticks
    fig.add_trace(go.Candlestick(
        x=hist.index,
        open=hist["Open"], high=hist["High"], low=hist["Low"], close=hist["Close"],
        name=choice,
        increasing_line_color="#16a34a",
        decreasing_line_color="#dc2626",
        increasing_fillcolor="rgba(22,163,74,0.18)",
        decreasing_fillcolor="rgba(220,38,38,0.18)",
        line=dict(width=1),
    ))

    # Moving averages (optional)
    if show_sma:
        fig.add_trace(go.Scatter(
            x=hist.index, y=hist["SMA20"],
            name="SMA 20", line=dict(color="#2563eb", width=1.4, dash="dot"),
        ))
        fig.add_trace(go.Scatter(
            x=hist.index, y=hist["SMA50"],
            name="SMA 50", line=dict(color="#d97706", width=1.4, dash="dash"),
        ))

    # Pivot lines (optional)
    if pivots:
        x0  = hist.index[0]
        x1  = hist.index[-1]
        ltp = float(hist["Close"].iloc[-1])

        for label, value in sorted(pivots.items(), key=lambda kv: kv[1], reverse=True):
            col   = PIVOT_COLORS.get(label, "#888888")
            dash  = "dash" if label == "P" else "dot"
            lw    = 1.8 if label == "P" else 1.2

            if label == "P":
                band = value * 0.0015
                fig.add_shape(
                    type="rect", xref="x", yref="y",
                    x0=x0, x1=x1, y0=value - band, y1=value + band,
                    fillcolor="rgba(125,60,152,0.07)",
                    line=dict(width=0), layer="below",
                )

            fig.add_shape(
                type="line", xref="x", yref="y",
                x0=x0, x1=x1, y0=value, y1=value,
                line=dict(color=col, width=lw, dash=dash),
                layer="below",
            )

            dist_pct = (value - ltp) / ltp * 100
            arrow    = "▲" if value > ltp else ("▼" if value < ltp else "─")
            fig.add_annotation(
                x=x1, y=value, xref="x", yref="y",
                text=f"  {label}  {value:,.2f}  {arrow}{abs(dist_pct):.1f}%",
                showarrow=False, xanchor="left",
                font=dict(family="IBM Plex Mono", size=9, color=col),
                bgcolor="rgba(255,255,255,0.82)", borderpad=2,
            )

    title_parts = [f"{choice}  ·  {tf_label}  ·  {period.upper()}"]
    if show_sma:    title_parts.append("SMA 20/50")
    if show_pivots: title_parts.append(f"{scr_pivot_type} Pivots")

    fig.update_layout(
        height=520,
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fafafa",
        font=dict(family="IBM Plex Mono", color="#475569", size=11),
        margin=dict(l=0, r=160, t=40, b=0),
        xaxis_rangeslider_visible=False,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0,
            font=dict(size=10), bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#e2e8f0", borderwidth=1,
        ),
        title=dict(
            text="  ·  ".join(title_parts),
            font=dict(family="IBM Plex Mono", size=12, color="#1e293b"),
        ),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e2e8f0", gridwidth=1,
                     showline=True, linecolor="#cbd5e1", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#e2e8f0", gridwidth=1,
                     showline=True, linecolor="#cbd5e1", zeroline=False)
    st.plotly_chart(fig, use_container_width=True)

    # ── Pivot levels table (only when pivots are on) ───────────────────────────
    if pivots:
        ltp  = float(hist["Close"].iloc[-1])
        rows = []
        for label, value in sorted(pivots.items(), key=lambda kv: kv[1], reverse=True):
            dist_pct = round((value - ltp) / ltp * 100, 2)
            role     = "Resistance" if value > ltp else ("Support" if value < ltp else "At Price")
            rows.append({
                "Level":    label,
                "Price":    f"₹{value:,.2f}",
                "Distance": f"{'▲' if value > ltp else '▼'} {abs(dist_pct):.2f}%",
                "Role":     role,
            })
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
            f"letter-spacing:0.08em;text-transform:uppercase;color:#5a6b30;"
            f"margin-bottom:0.4rem;'>{scr_pivot_type} Pivot Levels</div>",
            unsafe_allow_html=True,
        )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)



# ─────────────────────────────────────────────
#  NARROW CPR SCANNER
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def scan_narrow_cpr(symbols: list, max_stocks: int = 150) -> pd.DataFrame:
    """
    Scans NSE500 for stocks with Narrow CPR (< 0.25%) on daily timeframe.
    For each narrow-CPR stock, determines bullish / bearish pattern using:
      - Price vs CPR (Above TC = Bull, Below BC = Bear)
      - HMA-20 direction
      - 3/10 Oscillator histogram
      - RSI position
    Returns a sorted DataFrame.
    """
    rows = []
    batch = symbols[:max_stocks]

    for sym in batch:
        try:
            df = yf.Ticker(sym + ".NS").history(period="60d", interval="1d")
            if df.empty or len(df) < 22:
                continue
            df.index = df.index.tz_localize(None)

            # ── CPR ──────────────────────────────────────────────────────────
            ref   = df.iloc[-2]
            H, L, C = float(ref["High"]), float(ref["Low"]), float(ref["Close"])
            P     = (H + L + C) / 3
            BC    = (H + L) / 2
            TC    = (P - BC) + P
            width = abs(TC - BC) / P * 100

            if width >= 0.5:           # only narrow / moderate CPR stocks
                continue

            ltp = float(df["Close"].iloc[-1])

            # ── HMA-20 ───────────────────────────────────────────────────────
            close = df["Close"]
            def wma(s, n):
                w = np.arange(1, n + 1)
                return s.rolling(n).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)
            hma = wma(2 * wma(close, 10) - wma(close, 20), 4)
            hma_up = bool(hma.iloc[-1] > hma.iloc[-2]) if len(hma.dropna()) >= 2 else None

            # ── 3/10 Oscillator ──────────────────────────────────────────────
            diff  = close.rolling(3).mean() - close.rolling(10).mean()
            sig   = diff.rolling(16).mean()
            hist_val = float(diff.iloc[-1] - sig.iloc[-1]) if not np.isnan(diff.iloc[-1]) else 0

            # ── RSI ──────────────────────────────────────────────────────────
            delta = close.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rsi   = float(100 - (100 / (1 + gain.iloc[-1] / loss.iloc[-1]))) if loss.iloc[-1] != 0 else 50

            # ── Scoring ──────────────────────────────────────────────────────
            bull_pts = 0
            bear_pts = 0

            # Price vs CPR
            if ltp > TC:   bull_pts += 2
            elif ltp < BC: bear_pts += 2

            # HMA direction
            if hma_up is True:  bull_pts += 1
            elif hma_up is False: bear_pts += 1

            # 3/10 histogram
            if hist_val > 0: bull_pts += 1
            else:            bear_pts += 1

            # RSI
            if rsi >= 55:   bull_pts += 1
            elif rsi <= 45: bear_pts += 1

            total = bull_pts + bear_pts
            if total == 0:
                pattern     = "Neutral"
                pattern_col = "neut"
                strength    = 0
            elif bull_pts > bear_pts:
                pattern     = "Bullish"
                pattern_col = "bull"
                strength    = round(bull_pts / total * 100)
            elif bear_pts > bull_pts:
                pattern     = "Bearish"
                pattern_col = "bear"
                strength    = round(bear_pts / total * 100)
            else:
                pattern     = "Neutral"
                pattern_col = "neut"
                strength    = 50

            # CPR width label
            if width < 0.25:
                cpr_label = "Narrow"
            else:
                cpr_label = "Moderate"

            rows.append({
                "Symbol":    sym,
                "LTP":       round(ltp, 2),
                "CPR Width%": round(width, 3),
                "CPR Type":  cpr_label,
                "TC":        round(TC, 2),
                "BC":        round(BC, 2),
                "HMA":       "▲ Up" if hma_up else "▼ Down",
                "RSI":       round(rsi, 1),
                "Pattern":   pattern,
                "Strength%": strength,
                "_col":      pattern_col,
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df_out = pd.DataFrame(rows)
    # Sort: Narrow CPR first, then by pattern strength descending
    df_out = df_out.sort_values(
        ["CPR Width%", "Strength%"],
        ascending=[True, False]
    ).reset_index(drop=True)
    return df_out


# ─────────────────────────────────────────────
#  EMAIL REPORT HELPERS
# ─────────────────────────────────────────────

def compute_trade_levels(symbol: str, ltp: float, tc: float, bc: float,
                         pivot: float, pattern: str) -> dict:
    """
    Compute Target, Stop-Loss and key levels for Short / Medium / Long term
    based on CPR and pivot point framework.

    Short  term = intraday / 1-3 days  (tight ATR-based levels)
    Medium term = 1-4 weeks            (CPR / R1-R2 / S1-S2 targets)
    Long   term = 1-3 months           (R2-R3 / major pivots)
    """
    try:
        df = yf.Ticker(symbol + ".NS").history(period="60d", interval="1d")
        df.index = df.index.tz_localize(None)
        if df.empty or len(df) < 15:
            return {}

        close = df["Close"]
        high  = df["High"]
        low   = df["Low"]

        # ATR-14
        tr    = pd.concat([high - low,
                           (high - close.shift()).abs(),
                           (low  - close.shift()).abs()], axis=1).max(axis=1)
        atr   = float(tr.rolling(14).mean().iloc[-1])

        # 52-week high / low for long-term reference
        wk52h = float(high.tail(252).max()) if len(high) >= 252 else float(high.max())
        wk52l = float(low.tail(252).min())  if len(low)  >= 252 else float(low.min())

        # Pivot points from prior daily candle
        ref   = df.iloc[-2]
        H2, L2, C2 = float(ref["High"]), float(ref["Low"]), float(ref["Close"])
        P     = (H2 + L2 + C2) / 3
        R1    = 2 * P - L2
        R2    = P + (H2 - L2)
        R3    = H2 + 2 * (P - L2)
        S1    = 2 * P - H2
        S2    = P - (H2 - L2)
        S3    = L2 - 2 * (H2 - P)

        if pattern == "Bullish":
            short_entry  = round(ltp, 2)
            short_target = round(min(R1, ltp + atr * 1.5), 2)
            short_sl     = round(max(bc, ltp - atr * 0.8), 2)
            short_rr     = round((short_target - short_entry) / max(short_entry - short_sl, 0.01), 2)

            med_entry    = round(ltp, 2)
            med_target1  = round(R1, 2)
            med_target2  = round(R2, 2)
            med_sl       = round(S1, 2)
            med_rr       = round((med_target2 - med_entry) / max(med_entry - med_sl, 0.01), 2)

            long_entry   = round(ltp, 2)
            long_target1 = round(R2, 2)
            long_target2 = round(R3, 2)
            long_target3 = round(min(wk52h, R3 + atr * 5), 2)
            long_sl      = round(S2, 2)
            long_rr      = round((long_target2 - long_entry) / max(long_entry - long_sl, 0.01), 2)

        else:  # Bearish
            short_entry  = round(ltp, 2)
            short_target = round(max(S1, ltp - atr * 1.5), 2)
            short_sl     = round(min(tc, ltp + atr * 0.8), 2)
            short_rr     = round((short_entry - short_target) / max(short_sl - short_entry, 0.01), 2)

            med_entry    = round(ltp, 2)
            med_target1  = round(S1, 2)
            med_target2  = round(S2, 2)
            med_sl       = round(R1, 2)
            med_rr       = round((med_entry - med_target2) / max(med_sl - med_entry, 0.01), 2)

            long_entry   = round(ltp, 2)
            long_target1 = round(S2, 2)
            long_target2 = round(S3, 2)
            long_target3 = round(max(wk52l, S3 - atr * 5), 2)
            long_sl      = round(R2, 2)
            long_rr      = round((long_entry - long_target2) / max(long_sl - long_entry, 0.01), 2)

        return {
            "symbol": symbol, "ltp": ltp, "pattern": pattern,
            "pivot": round(P, 2), "tc": round(tc, 2), "bc": round(bc, 2),
            "atr": round(atr, 2),
            "R1": round(R1, 2), "R2": round(R2, 2), "R3": round(R3, 2),
            "S1": round(S1, 2), "S2": round(S2, 2), "S3": round(S3, 2),
            "52wH": round(wk52h, 2), "52wL": round(wk52l, 2),
            "short": {"entry": short_entry, "target": short_target,
                      "sl": short_sl,       "rr": short_rr},
            "medium": {"entry": med_entry, "target1": med_target1,
                       "target2": med_target2, "sl": med_sl, "rr": med_rr},
            "long":   {"entry": long_entry, "target1": long_target1,
                       "target2": long_target2, "target3": long_target3,
                       "sl": long_sl, "rr": long_rr},
        }
    except Exception:
        return {}


def build_email_html(scan_df: pd.DataFrame, scan_date: str) -> str:
    """Build a rich HTML email report from the scan results."""

    def row_block(lv: dict) -> str:
        """Render one stock block."""
        sym     = lv["symbol"]
        ltp     = lv["ltp"]
        pattern = lv["pattern"]
        emoji   = "🟢" if pattern == "Bullish" else "🔴"
        col     = "#1a7a4a" if pattern == "Bullish" else "#c0392b"
        bg      = "#f0fff8" if pattern == "Bullish" else "#fff0f0"

        sh = lv["short"]
        md = lv["medium"]
        lg = lv["long"]

        return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="margin-bottom:18px;border-radius:8px;overflow:hidden;
              border:1px solid {col}33;font-family:'Courier New',monospace;">
  <tr>
    <td style="background:{col};padding:10px 16px;">
      <span style="font-size:1.1rem;font-weight:700;color:#fff;">{emoji} {sym}</span>
      <span style="font-size:0.85rem;color:rgba(255,255,255,0.85);margin-left:12px;">
        LTP ₹{ltp:,.2f} &nbsp;|&nbsp; {pattern} Setup
      </span>
    </td>
  </tr>
  <tr>
    <td style="background:#fafafa;padding:10px 16px;font-size:0.78rem;color:#475569;">
      <b>Pivot:</b> ₹{lv["pivot"]:,.2f} &nbsp;
      <b>TC:</b> ₹{lv["tc"]:,.2f} &nbsp;
      <b>BC:</b> ₹{lv["bc"]:,.2f} &nbsp;
      <b>ATR:</b> ₹{lv["atr"]:,.2f} &nbsp;&nbsp;
      <b>52W H:</b> ₹{lv["52wH"]:,.2f} &nbsp;
      <b>52W L:</b> ₹{lv["52wL"]:,.2f}
    </td>
  </tr>
  <tr>
    <td style="padding:0 16px 12px;">
      <table width="100%" cellpadding="6" cellspacing="0" style="margin-top:8px;">
        <tr style="background:{col};color:#fff;font-size:0.72rem;text-transform:uppercase;letter-spacing:0.06em;">
          <th align="left">Horizon</th>
          <th align="right">Entry</th>
          <th align="right">Target 1</th>
          <th align="right">Target 2</th>
          <th align="right">Target 3</th>
          <th align="right">Stop Loss</th>
          <th align="right">R:R</th>
        </tr>
        <tr style="background:#fff;font-size:0.8rem;color:#1e293b;border-bottom:1px solid #e2e8f0;">
          <td><b>Short</b><br><span style="color:#94a3b8;font-size:0.7rem;">1–3 Days</span></td>
          <td align="right">₹{sh["entry"]:,.2f}</td>
          <td align="right" style="color:{col};font-weight:700;">₹{sh["target"]:,.2f}</td>
          <td align="right">—</td>
          <td align="right">—</td>
          <td align="right" style="color:#dc2626;">₹{sh["sl"]:,.2f}</td>
          <td align="right"><b>{sh["rr"]}x</b></td>
        </tr>
        <tr style="background:{bg};font-size:0.8rem;color:#1e293b;border-bottom:1px solid #e2e8f0;">
          <td><b>Medium</b><br><span style="color:#94a3b8;font-size:0.7rem;">1–4 Weeks</span></td>
          <td align="right">₹{md["entry"]:,.2f}</td>
          <td align="right" style="color:{col};font-weight:700;">₹{md["target1"]:,.2f}</td>
          <td align="right" style="color:{col};font-weight:700;">₹{md["target2"]:,.2f}</td>
          <td align="right">—</td>
          <td align="right" style="color:#dc2626;">₹{md["sl"]:,.2f}</td>
          <td align="right"><b>{md["rr"]}x</b></td>
        </tr>
        <tr style="background:#fff;font-size:0.8rem;color:#1e293b;">
          <td><b>Long</b><br><span style="color:#94a3b8;font-size:0.7rem;">1–3 Months</span></td>
          <td align="right">₹{lg["entry"]:,.2f}</td>
          <td align="right" style="color:{col};font-weight:700;">₹{lg["target1"]:,.2f}</td>
          <td align="right" style="color:{col};font-weight:700;">₹{lg["target2"]:,.2f}</td>
          <td align="right" style="color:{col};font-weight:700;">₹{lg["target3"]:,.2f}</td>
          <td align="right" style="color:#dc2626;">₹{lg["sl"]:,.2f}</td>
          <td align="right"><b>{lg["rr"]}x</b></td>
        </tr>
      </table>
      <div style="margin-top:6px;font-size:0.68rem;color:#94a3b8;">
        R1 ₹{lv["R1"]:,.2f} &nbsp; R2 ₹{lv["R2"]:,.2f} &nbsp; R3 ₹{lv["R3"]:,.2f}
        &nbsp;&nbsp;&nbsp;
        S1 ₹{lv["S1"]:,.2f} &nbsp; S2 ₹{lv["S2"]:,.2f} &nbsp; S3 ₹{lv["S3"]:,.2f}
      </div>
    </td>
  </tr>
</table>"""

    bull_rows = ""
    bear_rows = ""
    for _, row in scan_df.iterrows():
        sym  = row["Symbol"]
        ltp  = float(str(row["LTP"]).replace("₹","").replace(",",""))
        tc   = float(str(row["TC"]).replace("₹","").replace(",",""))
        bc   = float(str(row["BC"]).replace("₹","").replace(",",""))
        pat  = row["Pattern"]
        if pat not in ("Bullish", "Bearish"):
            continue
        lv = compute_trade_levels(sym, ltp, tc, bc, 0, pat)
        if not lv:
            continue
        if pat == "Bullish":
            bull_rows += row_block(lv)
        else:
            bear_rows += row_block(lv)

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:'Segoe UI',sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:24px 12px;">
<table width="680" cellpadding="0" cellspacing="0"
       style="background:#ffffff;border-radius:12px;overflow:hidden;
              box-shadow:0 4px 24px rgba(0,0,0,0.08);">

  <!-- Header -->
  <tr>
    <td style="background:linear-gradient(135deg,#0d1f0a 0%,#1a4a10 100%);
               padding:28px 32px;">
      <div style="font-family:'Courier New',monospace;font-size:1.4rem;
                  font-weight:700;color:#e8eddf;letter-spacing:-0.02em;">
        🏦 PivotVault AI — CPR Report
      </div>
      <div style="font-family:'Courier New',monospace;font-size:0.75rem;
                  color:#b5c77a;margin-top:4px;letter-spacing:0.08em;
                  text-transform:uppercase;">
        Narrow CPR Scanner · Frank Ochoa Methodology · {scan_date}
      </div>
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="padding:28px 28px 8px;">

      {"" if not bull_rows else f"""
      <div style="font-family:'Courier New',monospace;font-size:0.72rem;
                  letter-spacing:0.1em;text-transform:uppercase;color:#1a7a4a;
                  border-left:4px solid #1a7a4a;padding-left:10px;
                  margin-bottom:14px;">▲ Bullish Setups</div>
      {bull_rows}"""}

      {"" if not bear_rows else f"""
      <div style="font-family:'Courier New',monospace;font-size:0.72rem;
                  letter-spacing:0.1em;text-transform:uppercase;color:#c0392b;
                  border-left:4px solid #c0392b;padding-left:10px;
                  margin-bottom:14px;margin-top:8px;">▼ Bearish Setups</div>
      {bear_rows}"""}

    </td>
  </tr>

  <!-- Disclaimer -->
  <tr>
    <td style="padding:16px 28px 28px;">
      <div style="background:#f8fafc;border-radius:6px;padding:12px 16px;
                  font-size:0.68rem;color:#94a3b8;line-height:1.6;">
        ⚠️ <b>Disclaimer:</b> This report is generated by PivotVault AI using the
        Frank Ochoa Pivot Boss methodology. Targets and stop-losses are derived from
        pivot point mathematics and ATR. This is <b>not financial advice</b>.
        Always consult a SEBI-registered advisor before trading.
        Past performance is not indicative of future results.
      </div>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""
    return html


def send_report_email(to_email: str, smtp_host: str, smtp_port: int,
                      sender_email: str, sender_password: str,
                      html_body: str, scan_date: str) -> tuple[bool, str]:
    """Send the HTML report via SMTP. Returns (success, message)."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"PivotVault AI — Narrow CPR Report {scan_date}"
        msg["From"]    = sender_email
        msg["To"]      = to_email
        msg.attach(MIMEText(html_body, "html"))

        ctx = ssl.create_default_context()
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx) as server:
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, to_email, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.ehlo()
                server.starttls(context=ctx)
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, to_email, msg.as_string())
        return True, "Email sent successfully!"
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────
#  PDF REPORT BUILDER
# ─────────────────────────────────────────────

def generate_stock_pdf(symbol: str, tf_label: str, pivot_type: str,
                       analysis: dict, trade_levels: dict) -> bytes:
    """
    Build a professional A4 PDF report for a single stock's Pivot Boss analysis.
    Includes: Overall Bias, CPR, Pivot Grid, Market Profile,
              Short/Medium/Long term targets & stop-losses, and reasoning narrative.
    Returns PDF as bytes.
    """
    buf = io.BytesIO()

    # ── Colour palette ────────────────────────────────────────────────────────
    DARK        = colors.HexColor("#0d1a0a")
    OLIVE       = colors.HexColor("#3d4a1e")
    OLIVE_LIGHT = colors.HexColor("#e8eddf")
    OLIVE_MID   = colors.HexColor("#b5c77a")
    GREEN       = colors.HexColor("#1a7a4a")
    RED         = colors.HexColor("#c0392b")
    AMBER       = colors.HexColor("#d97706")
    BLUE        = colors.HexColor("#2563eb")
    SLATE       = colors.HexColor("#475569")
    LIGHT_GREY  = colors.HexColor("#f8fafc")
    BORDER      = colors.HexColor("#e2e8f0")
    WHITE       = colors.white

    overall = analysis.get("overall", "NEUTRAL")
    ov_col  = analysis.get("ov_col", "neut")
    BIAS_COLOR = GREEN if ov_col == "bull" else (RED if ov_col == "bear" else AMBER)

    ltp    = analysis.get("ltp", 0)
    cpr    = analysis.get("cpr", {})
    pivots = analysis.get("pivots", {})
    mp     = analysis.get("market_profile", {})
    tl     = trade_levels  # short / medium / long

    # ── Document setup ────────────────────────────────────────────────────────
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm,
        topMargin=16*mm,  bottomMargin=16*mm,
        title=f"PivotVault AI — {symbol} Analysis",
        author="PivotVault AI",
    )
    W = A4[0] - 36*mm   # usable width

    styles = getSampleStyleSheet()

    # Custom styles
    def S(name, font="Helvetica", **kw):
        return ParagraphStyle(name, fontName=font, **kw)

    s_title    = S("title",    fontSize=20, textColor=DARK,   leading=24, spaceAfter=2)
    s_sub      = S("sub",      fontSize=9,  textColor=SLATE,  leading=13, spaceAfter=6)
    s_h2       = S("h2",       fontSize=12, textColor=OLIVE,  leading=16, spaceBefore=10, spaceAfter=4,
                   font="Helvetica-Bold")
    s_h3       = S("h3",       fontSize=10, textColor=DARK,   leading=14, spaceBefore=6,  spaceAfter=3,
                   font="Helvetica-Bold")
    s_body     = S("body",     fontSize=9,  textColor=SLATE,  leading=14, spaceAfter=4)
    s_bull     = S("bull",     fontSize=10, textColor=GREEN,  leading=14, font="Helvetica-Bold")
    s_bear     = S("bear",     fontSize=10, textColor=RED,    leading=14, font="Helvetica-Bold")
    s_neut     = S("neut",     fontSize=10, textColor=AMBER,  leading=14, font="Helvetica-Bold")
    s_cell     = S("cell",     fontSize=8,  textColor=DARK,   leading=11)
    s_cell_b   = S("cell_b",   fontSize=8,  textColor=DARK,   leading=11, font="Helvetica-Bold")
    s_cell_g   = S("cell_g",   fontSize=8,  textColor=GREEN,  leading=11, font="Helvetica-Bold")
    s_cell_r   = S("cell_r",   fontSize=8,  textColor=RED,    leading=11, font="Helvetica-Bold")
    s_cell_hdr = S("cell_hdr", fontSize=8,  textColor=WHITE,  leading=11, font="Helvetica-Bold",
                   alignment=TA_CENTER)
    s_disc     = S("disc",     fontSize=7,  textColor=colors.HexColor("#94a3b8"),
                   leading=10, spaceAfter=0)

    def cell(txt, style=None):
        return Paragraph(str(txt), style or s_cell)

    def hdr(txt):
        return Paragraph(txt, s_cell_hdr)

    story = []

    # ════════════════════════════════════════════════════════════════
    # HEADER BLOCK
    # ════════════════════════════════════════════════════════════════
    header_data = [[
        Paragraph(f"<b>{symbol}</b>", ParagraphStyle("ht", fontSize=22,
                  textColor=WHITE, leading=26, fontName="Helvetica-Bold")),
        Paragraph(
            f"LTP &nbsp;<b>Rs.{ltp:,.2f}</b><br/>"
            f"{tf_label} &nbsp;|&nbsp; {pivot_type} Pivots<br/>"
            f"{cpr.get('Bias','')}<br/>"
            f"Generated: {datetime.now().strftime('%d %b %Y  %H:%M')}",
            ParagraphStyle("hs", fontSize=8, textColor=OLIVE_LIGHT,
                           leading=13, fontName="Helvetica"),
        ),
        Paragraph(
            f"<b>{overall}</b>",
            ParagraphStyle("hb", fontSize=14, textColor=WHITE, leading=18,
                           fontName="Helvetica-Bold", alignment=TA_RIGHT),
        ),
    ]]
    header_tbl = Table(header_data, colWidths=[W*0.32, W*0.42, W*0.26])
    header_tbl.setStyle(TableStyle([
        ("BACKGROUND",  (0,0), (-1,-1), OLIVE),
        ("ROUNDEDCORNERS", [6]),
        ("PADDING",     (0,0), (-1,-1), 10),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
        ("LINEBELOW",   (0,0), (-1,0), 1, OLIVE_MID),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 8*mm))

    # ════════════════════════════════════════════════════════════════
    # CPR + PIVOT LEVELS  (side by side)
    # ════════════════════════════════════════════════════════════════
    story.append(Paragraph("Central Pivot Range (CPR)", s_h2))
    story.append(HRFlowable(width=W, thickness=1, color=OLIVE_MID, spaceAfter=4))

    cpr_data = [
        [hdr("Level"), hdr("Value"), hdr("Width %"), hdr("CPR Bias")],
        [cell("Pivot (P)"),  cell(f"Rs.{cpr.get('Pivot',0):,.2f}"),
         cell(f"{cpr.get('Width%',0):.3f}%"),
         Paragraph(cpr.get("Bias","—"), s_bull if ov_col=="bull" else (s_bear if ov_col=="bear" else s_neut))],
        [cell("Top CPR (TC)"),  cell(f"Rs.{cpr.get('TC',0):,.2f}"),  cell(""), cell("")],
        [cell("Bot CPR (BC)"),  cell(f"Rs.{cpr.get('BC',0):,.2f}"),  cell(""), cell("")],
    ]
    cpr_tbl = Table(cpr_data, colWidths=[W*0.22, W*0.22, W*0.22, W*0.34])
    cpr_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  OLIVE),
        ("BACKGROUND",    (0,1), (-1,-1), LIGHT_GREY),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [WHITE, LIGHT_GREY]),
        ("GRID",          (0,0), (-1,-1), 0.5, BORDER),
        ("PADDING",       (0,0), (-1,-1), 5),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(cpr_tbl)
    story.append(Spacer(1, 5*mm))

    # ── Pivot Grid ────────────────────────────────────────────────────────────
    story.append(Paragraph("Pivot Level Grid", s_h2))
    story.append(HRFlowable(width=W, thickness=1, color=OLIVE_MID, spaceAfter=4))

    sorted_pivots = sorted(pivots.items(), key=lambda x: x[1], reverse=True) if pivots else []
    piv_rows = [[hdr("Level"), hdr("Price (Rs.)"), hdr("vs LTP"), hdr("Role")]]
    for lbl, val in sorted_pivots:
        diff     = val - ltp
        diff_pct = diff / ltp * 100
        role     = "Resistance" if val > ltp else ("Support" if val < ltp else "At Price")
        arr      = "+" if diff >= 0 else ""
        r_style  = s_cell_g if role == "Support" else (s_cell_r if role == "Resistance" else s_cell_b)
        piv_rows.append([
            cell(lbl, s_cell_b),
            cell(f"Rs.{val:,.2f}"),
            Paragraph(f"{arr}{diff_pct:.2f}%", s_cell_g if diff >= 0 else s_cell_r),
            Paragraph(role, r_style),
        ])
    piv_tbl = Table(piv_rows, colWidths=[W*0.15, W*0.25, W*0.25, W*0.35])
    piv_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  OLIVE),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [WHITE, LIGHT_GREY]),
        ("GRID",          (0,0), (-1,-1), 0.5, BORDER),
        ("PADDING",       (0,0), (-1,-1), 5),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(piv_tbl)
    story.append(Spacer(1, 5*mm))

    # ── Market Profile ────────────────────────────────────────────────────────
    if mp:
        story.append(Paragraph("Market Profile", s_h2))
        story.append(HRFlowable(width=W, thickness=1, color=OLIVE_MID, spaceAfter=4))
        mp_data = [
            [hdr("POC"), hdr("Value Area High (VAH)"), hdr("Value Area Low (VAL)")],
            [cell(f"Rs.{mp.get('POC',0):,.2f}", s_cell_b),
             cell(f"Rs.{mp.get('VAH',0):,.2f}"),
             cell(f"Rs.{mp.get('VAL',0):,.2f}")],
        ]
        mp_tbl = Table(mp_data, colWidths=[W/3, W/3, W/3])
        mp_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), OLIVE),
            ("BACKGROUND", (0,1), (-1,1), LIGHT_GREY),
            ("GRID",       (0,0), (-1,-1), 0.5, BORDER),
            ("PADDING",    (0,0), (-1,-1), 5),
            ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ]))
        story.append(mp_tbl)
        story.append(Spacer(1, 5*mm))

    # ════════════════════════════════════════════════════════════════
    # TRADE PLAN  — Short / Medium / Long
    # ════════════════════════════════════════════════════════════════
    story.append(Paragraph("Trade Plan — Targets & Stop-Loss", s_h2))
    story.append(HRFlowable(width=W, thickness=1, color=OLIVE_MID, spaceAfter=4))

    if tl:
        sh = tl.get("short",  {})
        md = tl.get("medium", {})
        lg = tl.get("long",   {})
        pat = tl.get("pattern","")
        PAT_COL = GREEN if pat == "Bullish" else RED

        plan_data = [
            [hdr("Horizon"), hdr("Entry"), hdr("Target 1"),
             hdr("Target 2"), hdr("Target 3"), hdr("Stop Loss"), hdr("R:R")],
            # Short
            [Paragraph("<b>Short Term</b><br/><font size='7' color='#94a3b8'>1 – 3 Days</font>",
                       ParagraphStyle("stl", fontSize=8, leading=12, textColor=DARK)),
             cell(f"Rs.{sh.get('entry',0):,.2f}"),
             Paragraph(f"Rs.{sh.get('target',0):,.2f}",
                       ParagraphStyle("tg", fontSize=8, textColor=PAT_COL, fontName="Helvetica-Bold", leading=11)),
             cell("—"), cell("—"),
             Paragraph(f"Rs.{sh.get('sl',0):,.2f}",
                       ParagraphStyle("sl", fontSize=8, textColor=RED, fontName="Helvetica-Bold", leading=11)),
             cell(f"{sh.get('rr',0)}x", s_cell_b)],
            # Medium
            [Paragraph("<b>Medium Term</b><br/><font size='7' color='#94a3b8'>1 – 4 Weeks</font>",
                       ParagraphStyle("stl2", fontSize=8, leading=12, textColor=DARK)),
             cell(f"Rs.{md.get('entry',0):,.2f}"),
             Paragraph(f"Rs.{md.get('target1',0):,.2f}",
                       ParagraphStyle("tg2", fontSize=8, textColor=PAT_COL, fontName="Helvetica-Bold", leading=11)),
             Paragraph(f"Rs.{md.get('target2',0):,.2f}",
                       ParagraphStyle("tg3", fontSize=8, textColor=PAT_COL, fontName="Helvetica-Bold", leading=11)),
             cell("—"),
             Paragraph(f"Rs.{md.get('sl',0):,.2f}",
                       ParagraphStyle("sl2", fontSize=8, textColor=RED, fontName="Helvetica-Bold", leading=11)),
             cell(f"{md.get('rr',0)}x", s_cell_b)],
            # Long
            [Paragraph("<b>Long Term</b><br/><font size='7' color='#94a3b8'>1 – 3 Months</font>",
                       ParagraphStyle("stl3", fontSize=8, leading=12, textColor=DARK)),
             cell(f"Rs.{lg.get('entry',0):,.2f}"),
             Paragraph(f"Rs.{lg.get('target1',0):,.2f}",
                       ParagraphStyle("tg4", fontSize=8, textColor=PAT_COL, fontName="Helvetica-Bold", leading=11)),
             Paragraph(f"Rs.{lg.get('target2',0):,.2f}",
                       ParagraphStyle("tg5", fontSize=8, textColor=PAT_COL, fontName="Helvetica-Bold", leading=11)),
             Paragraph(f"Rs.{lg.get('target3',0):,.2f}",
                       ParagraphStyle("tg6", fontSize=8, textColor=PAT_COL, fontName="Helvetica-Bold", leading=11)),
             Paragraph(f"Rs.{lg.get('sl',0):,.2f}",
                       ParagraphStyle("sl3", fontSize=8, textColor=RED, fontName="Helvetica-Bold", leading=11)),
             cell(f"{lg.get('rr',0)}x", s_cell_b)],
        ]
        plan_col_w = [W*0.18, W*0.13, W*0.13, W*0.13, W*0.13, W*0.15, W*0.15]
        plan_tbl = Table(plan_data, colWidths=plan_col_w)
        plan_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,0),  OLIVE),
            ("ROWBACKGROUNDS",(0,1), (-1,-1), [WHITE, LIGHT_GREY, WHITE]),
            ("GRID",          (0,0), (-1,-1), 0.5, BORDER),
            ("PADDING",       (0,0), (-1,-1), 5),
            ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
            ("LINEABOVE",     (0,1), (-1,1), 1.5, PAT_COL),
        ]))
        story.append(plan_tbl)
        story.append(Spacer(1, 5*mm))

        # Also show full R1-R3 / S1-S3 reference
        ref_data = [
            [hdr("R3"), hdr("R2"), hdr("R1"), hdr("PIVOT"), hdr("S1"), hdr("S2"), hdr("S3")],
            [cell(f"Rs.{tl.get('R3',0):,.2f}", s_cell_r),
             cell(f"Rs.{tl.get('R2',0):,.2f}", s_cell_r),
             cell(f"Rs.{tl.get('R1',0):,.2f}", s_cell_r),
             cell(f"Rs.{tl.get('pivot',0):,.2f}", s_cell_b),
             cell(f"Rs.{tl.get('S1',0):,.2f}", s_cell_g),
             cell(f"Rs.{tl.get('S2',0):,.2f}", s_cell_g),
             cell(f"Rs.{tl.get('S3',0):,.2f}", s_cell_g)],
        ]
        ref_tbl = Table(ref_data, colWidths=[W/7]*7)
        ref_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), OLIVE),
            ("BACKGROUND", (0,1), (-1,1), LIGHT_GREY),
            ("GRID",       (0,0), (-1,-1), 0.5, BORDER),
            ("PADDING",    (0,0), (-1,-1), 5),
            ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ]))
        story.append(ref_tbl)

    story.append(Spacer(1, 6*mm))

    # ════════════════════════════════════════════════════════════════
    # SIGNAL SUMMARY
    # ════════════════════════════════════════════════════════════════
    story.append(Paragraph("Signal Summary", s_h2))
    story.append(HRFlowable(width=W, thickness=1, color=OLIVE_MID, spaceAfter=4))

    sig_data = [
        [hdr("Indicator"), hdr("Signal"), hdr("Bias")],
        [cell("CPR Position"),      cell(analysis.get("cpr_position","—")),
         Paragraph(analysis.get("cpr_col","neut").upper(),
                   s_bull if analysis.get("cpr_col")=="bull" else
                   (s_bear if analysis.get("cpr_col")=="bear" else s_neut))],
        [cell("3/10 Oscillator"),   cell(analysis.get("osc_sig","—")),
         Paragraph(analysis.get("osc_col","neut").upper(),
                   s_bull if analysis.get("osc_col")=="bull" else
                   (s_bear if analysis.get("osc_col")=="bear" else s_neut))],
        [cell("HMA-20 Trend"),      cell(analysis.get("hma_sig","—")),
         Paragraph(analysis.get("hma_col","neut").upper(),
                   s_bull if analysis.get("hma_col")=="bull" else
                   (s_bear if analysis.get("hma_col")=="bear" else s_neut))],
        [cell("RSI-14"),
         cell(f"{analysis.get('rsi','—')} — {analysis.get('rsi_sig','—')}"),
         Paragraph(analysis.get("rsi_col","neut").upper(),
                   s_bull if analysis.get("rsi_col")=="bull" else
                   (s_bear if analysis.get("rsi_col")=="bear" else s_neut))],
    ]
    sig_tbl = Table(sig_data, colWidths=[W*0.28, W*0.46, W*0.26])
    sig_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  OLIVE),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [WHITE, LIGHT_GREY]),
        ("GRID",          (0,0), (-1,-1), 0.5, BORDER),
        ("PADDING",       (0,0), (-1,-1), 5),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(sig_tbl)
    story.append(Spacer(1, 6*mm))

    # ════════════════════════════════════════════════════════════════
    # NARRATIVE ANALYSIS
    # ════════════════════════════════════════════════════════════════
    story.append(Paragraph("Analysis Narrative", s_h2))
    story.append(HRFlowable(width=W, thickness=1, color=OLIVE_MID, spaceAfter=5))

    # Build narrative paragraphs
    def narrative_para(text):
        return Paragraph(text, s_body)

    # 1. Overall bias
    bias_word = "bullish" if ov_col=="bull" else ("bearish" if ov_col=="bear" else "neutral / mixed")
    story.append(narrative_para(
        f"<b>Overall Bias:</b>  The Pivot Boss analysis on <b>{symbol}</b> on the "
        f"<b>{tf_label}</b> timeframe using <b>{pivot_type}</b> pivots indicates a "
        f"<b>{overall}</b> setup. The current LTP of <b>Rs.{ltp:,.2f}</b> is positioned "
        f"{'above the Top CPR (TC), signalling bullish control' if ltp > cpr.get('TC',ltp) else ('below the Bottom CPR (BC), signalling bearish control' if ltp < cpr.get('BC',ltp) else 'inside the CPR band, indicating indecision')}."
    ))

    # 2. CPR width narrative
    w = cpr.get("Width%", 0)
    if w < 0.25:
        cpr_narr = (f"The CPR width of <b>{w:.3f}%</b> is <b>Narrow</b>, a key Pivot Boss setup. "
                    "Frank Ochoa identifies narrow CPR days as high-probability trending days — "
                    "price typically breaks decisively in one direction and does not look back.")
    elif w < 0.5:
        cpr_narr = (f"The CPR width of <b>{w:.3f}%</b> is <b>Moderate</b>. "
                    "A mild trend is possible, but the stock may see some consolidation "
                    "around the CPR zone before committing to a direction.")
    else:
        cpr_narr = (f"The CPR width of <b>{w:.3f}%</b> is <b>Wide</b>, indicating a "
                    "range-bound session is likely. Price may oscillate between TC and BC "
                    "without a strong directional move.")
    story.append(narrative_para(f"<b>CPR Analysis:</b>  {cpr_narr}"))

    # 3. Momentum narrative
    story.append(narrative_para(
        f"<b>Momentum (3/10 Oscillator):</b>  {analysis.get('osc_sig','—')}. "
        "The 3/10 oscillator is Frank Ochoa's primary momentum tool. "
        + ("A bullish crossover or positive histogram confirms upside momentum is building."
           if analysis.get("osc_col")=="bull" else
           "A bearish crossover or negative histogram warns that downside momentum is dominant."
           if analysis.get("osc_col")=="bear" else
           "The oscillator is near neutral — no strong momentum signal.")
    ))

    # 4. HMA narrative
    story.append(narrative_para(
        f"<b>Trend Filter (HMA-20):</b>  {analysis.get('hma_sig','—')}. "
        "The Hull Moving Average (HMA-20) eliminates lag and gives a cleaner trend read. "
        + ("A rising HMA confirms the short-term trend is up, supporting long positions."
           if analysis.get("hma_col")=="bull" else
           "A declining HMA confirms the short-term trend is down, supporting short positions.")
    ))

    # 5. RSI narrative
    rsi_val = analysis.get("rsi")
    if rsi_val:
        if rsi_val >= 70:
            rsi_narr = f"RSI at <b>{rsi_val}</b> is in overbought territory. Momentum is stretched — watch for reversal signals near resistance."
        elif rsi_val <= 30:
            rsi_narr = f"RSI at <b>{rsi_val}</b> is oversold. A bounce or recovery is possible if price holds key support."
        elif rsi_val >= 55:
            rsi_narr = f"RSI at <b>{rsi_val}</b> is in bullish territory (above 55), indicating buyers have the upper hand."
        elif rsi_val <= 45:
            rsi_narr = f"RSI at <b>{rsi_val}</b> is in bearish territory (below 45), indicating sellers remain in control."
        else:
            rsi_narr = f"RSI at <b>{rsi_val}</b> is neutral — no extreme reading."
        story.append(narrative_para(f"<b>RSI-14:</b>  {rsi_narr}"))

    # 6. Market Profile narrative
    if mp:
        poc = mp.get("POC", 0)
        if abs(poc - ltp) / ltp < 0.01:
            mp_narr = (f"Price is trading near the Point of Control (POC) at Rs.{poc:,.2f}, "
                       "indicating high-volume price acceptance. A breakout above or breakdown "
                       "below POC could trigger an impulsive move.")
        elif ltp > poc:
            mp_narr = (f"Price is trading above the POC (Rs.{poc:,.2f}), suggesting buyers "
                       "are in control of the value area. POC acts as key support on a pullback.")
        else:
            mp_narr = (f"Price is trading below the POC (Rs.{poc:,.2f}), suggesting sellers "
                       "are in control. A reclaim of POC would be needed to shift the bias.")
        story.append(narrative_para(f"<b>Market Profile:</b>  {mp_narr}"))

    # 7. Trade plan summary
    if tl:
        sh = tl.get("short",  {})
        md = tl.get("medium", {})
        lg = tl.get("long",   {})
        direction = "long (buy)" if ov_col == "bull" else "short (sell)"
        story.append(narrative_para(
            f"<b>Trade Plan Summary:</b>  Based on the above analysis, the suggested bias is "
            f"<b>{direction}</b> on {symbol}. "
            f"Short-term traders (1–3 days) can target <b>Rs.{sh.get('target',0):,.2f}</b> with a "
            f"stop at <b>Rs.{sh.get('sl',0):,.2f}</b> (R:R {sh.get('rr',0)}x). "
            f"Medium-term traders (1–4 weeks) have targets at "
            f"<b>Rs.{md.get('target1',0):,.2f}</b> and <b>Rs.{md.get('target2',0):,.2f}</b> "
            f"with a stop at <b>Rs.{md.get('sl',0):,.2f}</b> (R:R {md.get('rr',0)}x). "
            f"Long-term investors (1–3 months) can look towards "
            f"<b>Rs.{lg.get('target1',0):,.2f}</b> / <b>Rs.{lg.get('target2',0):,.2f}</b> / "
            f"<b>Rs.{lg.get('target3',0):,.2f}</b> with a stop at "
            f"<b>Rs.{lg.get('sl',0):,.2f}</b> (R:R {lg.get('rr',0)}x)."
        ))

    story.append(Spacer(1, 6*mm))

    # ════════════════════════════════════════════════════════════════
    # DISCLAIMER
    # ════════════════════════════════════════════════════════════════
    story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=4))
    story.append(Paragraph(
        "DISCLAIMER: This report is generated by PivotVault AI using the Frank Ochoa Pivot Boss "
        "methodology. Targets and stop-losses are derived from pivot point mathematics and ATR-based "
        "volatility. This report is for educational and informational purposes only and does NOT "
        "constitute financial advice. Always consult a SEBI-registered investment advisor before "
        "making any trading or investment decisions. Past performance is not indicative of future results.",
        s_disc,
    ))

    doc.build(story)
    buf.seek(0)
    return buf.read()

def page_pivot_boss(nse500: pd.DataFrame):
    """★  Full Frank Ochoa / Pivot Boss analysis page."""
    st.markdown(
        '<div class="title-bar"><span class="live-dot"></span>'
        '<h1>Pivot Boss Analysis</h1>'
        '<span style="font-family:IBM Plex Mono,monospace;font-size:0.68rem;'
        'color:#4a5068;margin-left:0.5rem;">Frank Ochoa Methodology</span>'
        f'<span class="ts">{datetime.now().strftime("%d %b %Y  %H:%M")}</span></div>',
        unsafe_allow_html=True,
    )

    symbols = nse500["Symbol"].dropna().sort_values().tolist()

    # ── Controls ─────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns([3, 1.5, 1.5, 1])
    with c1:
        symbol = st.selectbox("Symbol", symbols, key="pb_sym",
                              label_visibility="collapsed")
    with c2:
        tf_label = st.selectbox(
            "Timeframe",
            ["5 Min", "15 Min", "30 Min", "1 Hour", "4 Hour",
             "Daily", "Weekly", "Monthly"],
            index=5, key="pb_tf", label_visibility="collapsed",
        )
    with c3:
        pivot_type = st.selectbox(
            "Pivot Type",
            ["Traditional", "Woodie", "Camarilla", "DeMark", "Fibonacci"],
            key="pb_pt", label_visibility="collapsed",
        )
    with c4:
        run_btn = st.button("▶  Analyse")

    TF_MAP = {
        "5 Min":   ("5d",  "5m",   False),
        "15 Min":  ("10d", "15m",  False),
        "30 Min":  ("20d", "30m",  False),
        "1 Hour":  ("60d", "1h",   False),
        "4 Hour":  ("90d", "1h",   True),   # resample 1h → 4h
        "Daily":   ("1y",  "1d",   False),
        "Weekly":  ("5y",  "1wk",  False),
        "Monthly": ("10y", "1mo",  False),
    }
    period, interval, resample_4h = TF_MAP[tf_label]
    st.divider()

    with st.spinner(f"Loading {symbol} [{tf_label}] …"):
        df = fetch_stock_history(symbol, period, interval)
        if resample_4h and not df.empty:
            df = df.resample("4h").agg({
                "Open": "first", "High": "max",
                "Low": "min",    "Close": "last", "Volume": "sum",
            }).dropna()

    if df.empty or len(df) < 20:
        st.warning("Not enough data for this timeframe. Try Daily or a longer period.")
        return

    analysis = full_pivot_boss_analysis(df, pivot_type)
    if not analysis:
        st.warning("Analysis failed.")
        return

    ltp    = analysis["ltp"]
    cpr    = analysis["cpr"]
    mp     = analysis["market_profile"]
    pivots = analysis["pivots"]

    # ── Overall Bias Banner ───────────────────────────────────────────────────
    bias_palette = {
        "bull": ("#0a2318", "#00e5a0"),
        "bear": ("#21080f", "#ff4d6a"),
        "neut": ("#1a1505", "#f5a623"),
    }
    bg, fg = bias_palette.get(analysis["ov_col"], ("#141720", "#d4daf0"))
    st.markdown(
        f"<div style='background:{bg};border:1px solid {fg}33;border-left:4px solid {fg};"
        f"border-radius:6px;padding:0.75rem 1.25rem;margin-bottom:1rem;'>"
        f"<span style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;color:{fg}88;"
        f"letter-spacing:0.12em;text-transform:uppercase;'>Overall Bias  ·  {tf_label}  ·  {pivot_type}</span><br>"
        f"<span style='font-family:IBM Plex Mono,monospace;font-size:1.3rem;font-weight:700;color:{fg};'>"
        f"{analysis['overall']}</span>"
        f"<span style='font-family:IBM Plex Mono,monospace;font-size:0.8rem;color:{fg}aa;margin-left:1.5rem;'>"
        f"LTP ₹{ltp:,.2f}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Main Chart ─────────────────────────────────────────────────────────
    fig = build_pivot_boss_chart(df, symbol, analysis, pivot_type)
    st.plotly_chart(fig, use_container_width=True)

    # ── Signal Cards ──────────────────────────────────────────────────────────
    st.markdown("<h3 style='font-size:0.9rem;margin:1rem 0 0.5rem;'>Signal Summary</h3>",
                unsafe_allow_html=True)
    ca, cb, cc, cd = st.columns(4)

    with ca:
        cpr_detail = (
            f"TC {cpr['TC']} · P {cpr['Pivot']} · BC {cpr['BC']}<br>"
            f"Width: {cpr['Width%']}%<br>{cpr['Bias']}"
        ) if cpr else ""
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>Central Pivot Range (CPR)</div>"
            f"<div class='pb-card-value pb-{analysis['cpr_col']}'>{analysis['cpr_position']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#6b7490;"
            f"margin-top:0.4rem;'>{cpr_detail}</div></div>",
            unsafe_allow_html=True,
        )

    with cb:
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>3/10 Oscillator</div>"
            f"<div class='pb-card-value pb-{analysis['osc_col']}'>{analysis['osc_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#6b7490;"
            f"margin-top:0.4rem;'>Ochoa's momentum gauge<br>3-MA minus 10-MA vs 16-Signal</div>"
            f"</div>", unsafe_allow_html=True,
        )

    with cc:
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>HMA(20) Trend</div>"
            f"<div class='pb-card-value pb-{analysis['hma_col']}'>{analysis['hma_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#6b7490;"
            f"margin-top:0.4rem;'>Hull Moving Average<br>Low-lag trend direction filter</div>"
            f"</div>", unsafe_allow_html=True,
        )

    with cd:
        rsi_val   = f"{analysis['rsi']}" if analysis["rsi"] else "—"
        stoch_txt = (f"Stoch %K {analysis['stoch_k']} / %D {analysis['stoch_d']}"
                     if analysis["stoch_k"] else "")
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>RSI (14)</div>"
            f"<div class='pb-card-value pb-{analysis['rsi_col']}'>"
            f"{rsi_val} · {analysis['rsi_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#6b7490;"
            f"margin-top:0.4rem;'>{stoch_txt}</div></div>",
            unsafe_allow_html=True,
        )

    # ── Pivot Levels + Market Profile ─────────────────────────────────────────
    st.markdown(f"<h3 style='font-size:0.9rem;margin:1rem 0 0.5rem;'>"
                f"{pivot_type} Pivot Levels</h3>", unsafe_allow_html=True)
    col_piv, col_mp = st.columns(2)

    with col_piv:
        if pivots:
            rows = []
            for k, v in sorted(pivots.items(), key=lambda x: x[1], reverse=True):
                dist  = round((v - ltp) / ltp * 100, 2)
                arrow = "▲" if v > ltp else "▼"
                star  = " ★" if analysis.get("nearest") and analysis["nearest"][0] == k else ""
                rows.append({"Level": k + star, "Price": v,
                             "Distance": f"{arrow} {abs(dist)}%"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with col_mp:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
            "letter-spacing:0.1em;text-transform:uppercase;color:#6b7490;"
            "margin-bottom:0.5rem;'>Market Profile (Volume at Price)</div>",
            unsafe_allow_html=True,
        )
        if mp:
            for label, val, col in [
                ("POC — Point of Control", mp.get("POC"), "#f5a623"),
                ("VAH — Value Area High",  mp.get("VAH"), "#b0b8d0"),
                ("VAL — Value Area Low",   mp.get("VAL"), "#b0b8d0"),
            ]:
                if val:
                    dist = round((val - ltp) / ltp * 100, 2)
                    arr  = "▲" if val > ltp else "▼"
                    st.markdown(
                        f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.8rem;"
                        f"padding:0.3rem 0;border-bottom:1px solid #1e2330;'>"
                        f"<span style='color:{col};'>{label}</span>"
                        f"<b style='color:#d4daf0;float:right;'>{val} "
                        f"<span style='color:#6b7490;font-size:0.7rem;'>{arr}{abs(dist)}%</span>"
                        f"</b></div>",
                        unsafe_allow_html=True,
                    )
        else:
            st.caption("Market profile unavailable.")

    # ── ATR + Nearest Pivot + Signals ─────────────────────────────────────────
    st.divider()
    v1, v2, v3 = st.columns(3)

    with v1:
        st.markdown(
            f"<div class='pb-card'><div class='pb-card-title'>ATR(14) Volatility</div>"
            f"<div class='pb-card-value pb-neut'>"
            f"{'₹' + str(analysis['atr']) if analysis['atr'] else '—'}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#6b7490;margin-top:0.4rem;'>"
            f"{'% of price: ' + str(analysis['atr_pct']) + '%' if analysis['atr_pct'] else ''}"
            f"</div></div>", unsafe_allow_html=True,
        )

    with v2:
        nl = analysis.get("nearest")
        st.markdown(
            f"<div class='pb-card'><div class='pb-card-title'>Nearest Pivot ★</div>"
            f"<div class='pb-card-value pb-neut'>"
            f"{nl[0] + '  ₹' + str(nl[1]) if nl else '—'}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#6b7490;margin-top:0.4rem;'>"
            f"Immediate support / resistance</div></div>", unsafe_allow_html=True,
        )

    with v3:
        badges = (
            sig_badge(analysis["osc_sig"],      analysis["osc_col"])
            + sig_badge(analysis["hma_sig"],     analysis["hma_col"])
            + sig_badge(analysis["rsi_sig"],     analysis["rsi_col"])
            + sig_badge(analysis["cpr_position"].split("(")[0].strip(), analysis["cpr_col"])
        )
        st.markdown(
            f"<div class='pb-card'><div class='pb-card-title'>Active Signals</div>"
            f"<div style='margin-top:0.4rem;'>{badges}</div></div>",
            unsafe_allow_html=True,
        )

    # ── Virgin CPRs ────────────────────────────────────────────────────────────
    virgins = [v for v in analysis.get("virgin_cprs", []) if v["Virgin"]]
    if virgins:
        st.divider()
        st.markdown(
            "<h3 style='font-size:0.9rem;margin:1rem 0 0.25rem;'>🔲 Virgin CPR Levels</h3>"
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;color:#4a5068;"
            "margin-bottom:0.5rem;'>Untouched CPR bands — Ochoa's high-significance price magnets</div>",
            unsafe_allow_html=True,
        )
        st.dataframe(pd.DataFrame(virgins)[["Date", "TC", "BC"]],
                     use_container_width=True, hide_index=True)

    # ── Stochastic ────────────────────────────────────────────────────────────
    df_ind = analysis.get("df_ind")
    if df_ind is not None and "STOCH_K" in df_ind.columns:
        st.divider()
        st.plotly_chart(build_stoch_chart(df_ind), use_container_width=True)

    # ════════════════════════════════════════════════════════════════════════
    #  PDF REPORT — Trade Plan with Targets & Stop-Loss
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown(
        '<div class="title-bar" style="margin-top:0.25rem;">'
        '<h2 style="font-size:1.05rem;margin:0;">📄  Download Analysis Report (PDF)</h2>'
        '<span style="font-family:IBM Plex Mono,monospace;font-size:0.65rem;'
        'color:#4a5068;margin-left:0.75rem;">'
        'Short Term · Medium Term · Long Term Targets · Stop-Loss · Narrative</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    pdf_col1, pdf_col2 = st.columns([2, 1])
    with pdf_col1:
        pdf_pattern = st.selectbox(
            "Trade Direction for PDF",
            ["Auto (from analysis)", "Bullish", "Bearish"],
            key="pdf_pattern",
            label_visibility="collapsed",
        )
    with pdf_col2:
        gen_pdf_btn = st.button("📄  Generate PDF Report", key="gen_pdf_btn",
                                use_container_width=True)

    if gen_pdf_btn:
        # Determine pattern
        if pdf_pattern == "Auto (from analysis)":
            auto_pat = "Bullish" if analysis.get("ov_col") == "bull" else "Bearish"
        else:
            auto_pat = pdf_pattern

        with st.spinner("Computing trade levels & building PDF…"):
            # Compute trade levels
            tl = compute_trade_levels(
                symbol=symbol,
                ltp=ltp,
                tc=cpr.get("TC", ltp),
                bc=cpr.get("BC", ltp),
                pivot=cpr.get("Pivot", ltp),
                pattern=auto_pat,
            )

            if not tl:
                st.error("Could not compute trade levels. Try again or switch to Daily timeframe.")
            else:
                pdf_bytes = generate_stock_pdf(
                    symbol=symbol,
                    tf_label=tf_label,
                    pivot_type=pivot_type,
                    analysis=analysis,
                    trade_levels=tl,
                )

                # Inline preview of key trade levels
                sh = tl.get("short",  {})
                md = tl.get("medium", {})
                lg = tl.get("long",   {})
                col_fg = "#00e5a0" if auto_pat == "Bullish" else "#ff4d6a"
                arrow  = "▲" if auto_pat == "Bullish" else "▼"

                st.markdown(
                    f"<div style='background:#0d1f0a;border:1px solid {col_fg}33;"
                    f"border-left:4px solid {col_fg};border-radius:8px;"
                    f"padding:1rem 1.5rem;margin:0.5rem 0;'>"

                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;"
                    f"letter-spacing:0.12em;text-transform:uppercase;color:{col_fg}88;'>"
                    f"{arrow} {auto_pat} Trade Plan  ·  {symbol}</div>"

                    f"<div style='display:flex;gap:2rem;margin-top:0.6rem;flex-wrap:wrap;'>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#4a5068;text-transform:uppercase;'>Short Term (1-3d)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#d4daf0;'>T: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{sh.get('target',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#ff4d6a;'>₹{sh.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{sh.get('rr',0)}x</b></div></div>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#4a5068;text-transform:uppercase;'>Medium Term (1-4w)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#d4daf0;'>T1: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{md.get('target1',0):,.2f}</span>"
                    f" T2: <span style='color:{col_fg};font-weight:700;'>₹{md.get('target2',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#ff4d6a;'>₹{md.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{md.get('rr',0)}x</b></div></div>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#4a5068;text-transform:uppercase;'>Long Term (1-3m)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#d4daf0;'>T1: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{lg.get('target1',0):,.2f}</span>"
                    f" T2: <span style='color:{col_fg};font-weight:700;'>₹{lg.get('target2',0):,.2f}</span>"
                    f" T3: <span style='color:{col_fg};font-weight:700;'>₹{lg.get('target3',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#ff4d6a;'>₹{lg.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{lg.get('rr',0)}x</b></div></div>"

                    f"</div></div>",
                    unsafe_allow_html=True,
                )

                st.download_button(
                    label=f"⬇️  Download {symbol} PDF Report",
                    data=pdf_bytes,
                    file_name=f"PivotVault_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="pdf_download_btn",
                )

    # ════════════════════════════════════════════════════════════════════════
    #  NARROW CPR SCANNER  —  Daily Watchlist
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown(
        '<div class="title-bar" style="margin-top:0.5rem;">'
        '<h2 style="font-size:1.1rem;margin:0;">📡  Narrow CPR Scanner — Daily</h2>'
        '<span style="font-family:IBM Plex Mono,monospace;font-size:0.65rem;'
        'color:#4a5068;margin-left:0.75rem;">Nifty 500 · CPR Width &lt; 0.5% · Bullish / Bearish Pattern</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    sc1, sc2, sc3 = st.columns([2, 2, 2])
    with sc1:
        scan_filter = st.selectbox(
            "Show",
            ["All (Narrow + Moderate)", "Narrow Only (< 0.25%)", "Bullish Only", "Bearish Only"],
            key="cpr_scan_filter",
            label_visibility="collapsed",
        )
    with sc2:
        scan_max = st.selectbox(
            "Stocks to scan",
            [50, 100, 150, 200],
            index=1,
            key="cpr_scan_max",
            label_visibility="collapsed",
        )
    with sc3:
        run_scan = st.button("🔍  Run CPR Scan", key="run_cpr_scan", use_container_width=True)

    # ── Run / show cached results ─────────────────────────────────────────────
    if run_scan or st.session_state.get("cpr_scan_df") is not None:
        if run_scan:
            with st.spinner(f"Scanning {scan_max} stocks for Narrow CPR…"):
                scan_df = scan_narrow_cpr(
                    nse500["Symbol"].dropna().tolist(),
                    max_stocks=scan_max,
                )
            st.session_state["cpr_scan_df"] = scan_df
        else:
            scan_df = st.session_state.get("cpr_scan_df", pd.DataFrame())

        if scan_df.empty:
            st.warning("No stocks found with Narrow / Moderate CPR. Try scanning more stocks.")
        else:
            # Apply filter
            fdf = scan_df.copy()
            if scan_filter == "Narrow Only (< 0.25%)":
                fdf = fdf[fdf["CPR Width%"] < 0.25]
            elif scan_filter == "Bullish Only":
                fdf = fdf[fdf["Pattern"] == "Bullish"]
            elif scan_filter == "Bearish Only":
                fdf = fdf[fdf["Pattern"] == "Bearish"]

            if fdf.empty:
                st.info("No stocks match the selected filter.")
            else:
                # ── Summary badges ────────────────────────────────────────────
                n_narrow  = int((fdf["CPR Width%"] < 0.25).sum())
                n_bull    = int((fdf["Pattern"] == "Bullish").sum())
                n_bear    = int((fdf["Pattern"] == "Bearish").sum())
                n_neut    = int((fdf["Pattern"] == "Neutral").sum())

                b1, b2, b3, b4 = st.columns(4)
                b1.metric("🎯 Narrow CPR",  n_narrow)
                b2.metric("🟢 Bullish",     n_bull)
                b3.metric("🔴 Bearish",     n_bear)
                b4.metric("⚪ Neutral",     n_neut)

                st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)

                # ── Bullish table ─────────────────────────────────────────────
                bull_df = fdf[fdf["Pattern"] == "Bullish"].copy()
                if not bull_df.empty:
                    st.markdown(
                        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                        "letter-spacing:0.08em;text-transform:uppercase;"
                        "color:#00e5a0;margin-bottom:0.4rem;border-left:3px solid #00e5a0;"
                        "padding-left:0.6rem;'>"
                        f"▲ Bullish Setup — {len(bull_df)} stocks</div>",
                        unsafe_allow_html=True,
                    )
                    display_bull = bull_df[[
                        "Symbol","LTP","CPR Width%","CPR Type","TC","BC","HMA","RSI","Strength%"
                    ]].copy()
                    display_bull["CPR Width%"] = display_bull["CPR Width%"].apply(lambda x: f"{x:.3f}%")
                    display_bull["Strength%"]  = display_bull["Strength%"].apply(lambda x: f"{x}%")
                    display_bull["LTP"]        = display_bull["LTP"].apply(lambda x: f"₹{x:,.2f}")
                    display_bull["TC"]         = display_bull["TC"].apply(lambda x: f"₹{x:,.2f}")
                    display_bull["BC"]         = display_bull["BC"].apply(lambda x: f"₹{x:,.2f}")
                    st.dataframe(display_bull, use_container_width=True, hide_index=True)

                # ── Bearish table ─────────────────────────────────────────────
                bear_df = fdf[fdf["Pattern"] == "Bearish"].copy()
                if not bear_df.empty:
                    st.markdown(
                        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                        "letter-spacing:0.08em;text-transform:uppercase;"
                        "color:#ff4d6a;margin-bottom:0.4rem;margin-top:1rem;"
                        "border-left:3px solid #ff4d6a;padding-left:0.6rem;'>"
                        f"▼ Bearish Setup — {len(bear_df)} stocks</div>",
                        unsafe_allow_html=True,
                    )
                    display_bear = bear_df[[
                        "Symbol","LTP","CPR Width%","CPR Type","TC","BC","HMA","RSI","Strength%"
                    ]].copy()
                    display_bear["CPR Width%"] = display_bear["CPR Width%"].apply(lambda x: f"{x:.3f}%")
                    display_bear["Strength%"]  = display_bear["Strength%"].apply(lambda x: f"{x}%")
                    display_bear["LTP"]        = display_bear["LTP"].apply(lambda x: f"₹{x:,.2f}")
                    display_bear["TC"]         = display_bear["TC"].apply(lambda x: f"₹{x:,.2f}")
                    display_bear["BC"]         = display_bear["BC"].apply(lambda x: f"₹{x:,.2f}")
                    st.dataframe(display_bear, use_container_width=True, hide_index=True)

                # ── Neutral table (collapsed) ─────────────────────────────────
                neut_df = fdf[fdf["Pattern"] == "Neutral"].copy()
                if not neut_df.empty:
                    with st.expander(f"⚪ Neutral / Mixed — {len(neut_df)} stocks", expanded=False):
                        display_neut = neut_df[[
                            "Symbol","LTP","CPR Width%","CPR Type","TC","BC","HMA","RSI","Strength%"
                        ]].copy()
                        display_neut["CPR Width%"] = display_neut["CPR Width%"].apply(lambda x: f"{x:.3f}%")
                        display_neut["Strength%"]  = display_neut["Strength%"].apply(lambda x: f"{x}%")
                        display_neut["LTP"]        = display_neut["LTP"].apply(lambda x: f"₹{x:,.2f}")
                        display_neut["TC"]         = display_neut["TC"].apply(lambda x: f"₹{x:,.2f}")
                        display_neut["BC"]         = display_neut["BC"].apply(lambda x: f"₹{x:,.2f}")
                        st.dataframe(display_neut, use_container_width=True, hide_index=True)

                st.markdown(
                    "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    "color:#2e3448;margin-top:0.75rem;'>"
                    "Scoring: Price vs CPR (2pt) · HMA direction (1pt) · 3/10 Oscillator (1pt) · RSI (1pt)  "
                    "·  Results cached 1hr — click <b>Run CPR Scan</b> to refresh.</div>",
                    unsafe_allow_html=True,
                )
    else:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;"
            "color:#2a3040;text-align:center;padding:1rem;"
            "border:1px dashed #1e2330;border-radius:6px;'>"
            "Click <b>🔍 Run CPR Scan</b> to scan Nifty 500 for Narrow CPR stocks "
            "with Bullish / Bearish patterns.</div>",
            unsafe_allow_html=True,
        )

    # ════════════════════════════════════════════════════════════════════════
    #  EMAIL REPORT SECTION
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown(
        '<div class="title-bar" style="margin-top:0.25rem;">'
        '<h2 style="font-size:1.05rem;margin:0;">📧  Email CPR Report</h2>'
        '<span style="font-family:IBM Plex Mono,monospace;font-size:0.65rem;'
        'color:#4a5068;margin-left:0.75rem;">'
        'Send Narrow CPR stocks with Targets, Stop-Loss &amp; Pivot Levels to any email</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    cur_scan = st.session_state.get("cpr_scan_df")
    if cur_scan is None or cur_scan.empty:
        st.info("Run the CPR Scanner above first — then come back here to email the report.")
    else:
        # ── SMTP config (stored in session state so it persists) ──────────────
        if "smtp_cfg" not in st.session_state:
            st.session_state["smtp_cfg"] = {
                "host": "smtp.gmail.com", "port": 587,
                "sender": "", "password": "",
            }

        with st.expander("⚙️  SMTP / Sender Settings", expanded=False):
            smtp_c1, smtp_c2 = st.columns(2)
            with smtp_c1:
                new_host = st.text_input(
                    "SMTP Host",
                    value=st.session_state["smtp_cfg"]["host"],
                    key="smtp_host_input",
                    help="e.g. smtp.gmail.com  |  smtp.office365.com",
                )
                new_sender = st.text_input(
                    "Sender Email",
                    value=st.session_state["smtp_cfg"]["sender"],
                    key="smtp_sender_input",
                )
            with smtp_c2:
                new_port = st.selectbox(
                    "SMTP Port",
                    [587, 465, 25],
                    index=[587, 465, 25].index(st.session_state["smtp_cfg"]["port"]),
                    key="smtp_port_input",
                    help="587 = TLS (recommended)  |  465 = SSL",
                )
                new_pwd = st.text_input(
                    "App Password",
                    value=st.session_state["smtp_cfg"]["password"],
                    type="password",
                    key="smtp_pwd_input",
                    help="For Gmail: generate an App Password in Google Account → Security",
                )
            if st.button("💾 Save SMTP Settings", key="save_smtp"):
                st.session_state["smtp_cfg"] = {
                    "host": new_host, "port": new_port,
                    "sender": new_sender, "password": new_pwd,
                }
                st.success("Settings saved for this session.")

        # ── Recipient + filter + send ─────────────────────────────────────────
        em1, em2, em3 = st.columns([3, 2, 1])
        with em1:
            to_email = st.text_input(
                "Recipient Email",
                placeholder="admin@example.com",
                label_visibility="collapsed",
                key="report_to_email",
            )
        with em2:
            email_filter = st.selectbox(
                "Include in report",
                ["Bullish + Bearish", "Bullish Only", "Bearish Only"],
                key="email_filter",
                label_visibility="collapsed",
            )
        with em3:
            send_btn = st.button("📤  Send Report", key="send_email_btn",
                                 use_container_width=True)

        # ── Preview toggle ────────────────────────────────────────────────────
        show_preview = st.checkbox("Preview email before sending", value=False,
                                   key="email_preview_toggle")

        if send_btn or show_preview:
            # Filter scan_df based on selection
            report_df = cur_scan.copy()
            if email_filter == "Bullish Only":
                report_df = report_df[report_df["Pattern"] == "Bullish"]
            elif email_filter == "Bearish Only":
                report_df = report_df[report_df["Pattern"] == "Bearish"]
            else:
                report_df = report_df[report_df["Pattern"].isin(["Bullish","Bearish"])]

            if report_df.empty:
                st.warning("No Bullish/Bearish stocks in the scan results to report.")
            else:
                scan_date = datetime.now().strftime("%d %b %Y %H:%M")

                with st.spinner("Building report with targets & stop-losses…"):
                    html_body = build_email_html(report_df, scan_date)

                if show_preview:
                    st.markdown(
                        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                        "color:#4a5068;margin-bottom:0.4rem;'>Email Preview:</div>",
                        unsafe_allow_html=True,
                    )
                    st.components.v1.html(html_body, height=600, scrolling=True)

                if send_btn:
                    cfg = st.session_state["smtp_cfg"]
                    if not to_email.strip():
                        st.error("Please enter a recipient email address.")
                    elif not cfg["sender"] or not cfg["password"]:
                        st.error("Configure SMTP settings first (expand ⚙️ above).")
                    else:
                        with st.spinner("Sending email…"):
                            ok, msg = send_report_email(
                                to_email=to_email.strip(),
                                smtp_host=cfg["host"],
                                smtp_port=cfg["port"],
                                sender_email=cfg["sender"],
                                sender_password=cfg["password"],
                                html_body=html_body,
                                scan_date=scan_date,
                            )
                        if ok:
                            st.success(f"✅ Report sent to {to_email}")
                        else:
                            st.error(f"❌ Failed: {msg}")
                            st.markdown(
                                "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                                "color:#4a5068;margin-top:0.5rem;'>"
                                "💡 <b>Gmail users:</b> Enable 2-Factor Auth and use an App Password "
                                "(Google Account → Security → App Passwords). "
                                "Do NOT use your regular Gmail password.</div>",
                                unsafe_allow_html=True,
                            )

    # ── Methodology footnote ──────────────────────────────────────────────────
    st.divider()
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#2e3448;line-height:1.8;'>"
        "📖  Based on <i>Secrets of a Pivot Boss</i> by Frank Ochoa.  "
        "Tools implemented: CPR · 3/10 Oscillator · Virgin CPRs · Market Profile (POC/VAH/VAL) · "
        "HMA Trend Filter · ATR · RSI · Stochastic.  "
        "For educational purposes only — not financial advice.</div>",
        unsafe_allow_html=True,
    )


def page_watchlist():
    st.markdown("<h2 style='margin-bottom:1rem;'>Watchlist</h2>", unsafe_allow_html=True)
    wl = st.session_state["watchlist"]
    if not wl:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;color:#4a5068;font-size:0.85rem;"
            "padding:2rem 0;'>No stocks added yet. Head to <b>Stock Screener</b> to build your list.</div>",
            unsafe_allow_html=True,
        )
        return

    st.markdown("".join(f'<span class="wl-pill">{s}</span>' for s in wl), unsafe_allow_html=True)
    st.divider()

    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        if st.button("⟳ Refresh"):
            st.session_state["wl_data"]         = refresh_watchlist_prices(wl)
            st.session_state["wl_last_refresh"] = datetime.now()
    with c2:
        if st.button("✕ Clear All"):
            st.session_state["watchlist"] = []
            st.session_state["wl_data"]   = {}
            st.rerun()

    if not st.session_state["wl_data"] and wl:
        with st.spinner("Fetching prices…"):
            st.session_state["wl_data"]         = refresh_watchlist_prices(wl)
            st.session_state["wl_last_refresh"] = datetime.now()

    data = st.session_state["wl_data"]
    if st.session_state["wl_last_refresh"]:
        st.caption(f"Last updated: {st.session_state['wl_last_refresh'].strftime('%H:%M:%S')}")

    rows = []
    for sym in wl:
        d   = data.get(sym, {})
        ltp = d.get("ltp")
        chg = d.get("change")
        rows.append({
            "Symbol": sym,
            "LTP":    f"{ltp:,.2f}" if ltp else "—",
            "Chg %":  f"{'+' if chg and chg >= 0 else ''}{chg:.2f}%" if chg is not None else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                "color:#4a5068;letter-spacing:0.06em;text-transform:uppercase;"
                "margin-bottom:0.5rem;'>Remove</div>", unsafe_allow_html=True)
    rm_cols = st.columns(min(len(wl), 6))
    for i, sym in enumerate(wl[:6]):
        with rm_cols[i]:
            if st.button(f"✕ {sym}", key=f"rm_{sym}"):
                st.session_state["watchlist"].remove(sym)
                st.session_state["wl_data"].pop(sym, None)
                st.rerun()


# ─────────────────────────────────────────────
#  MOBILE BOTTOM NAV
# ─────────────────────────────────────────────
_NAV_PAGES = ["Market Snapshot", "Stock Screener", "Pivot Boss Analysis", "Watchlist"]
_NAV_ICONS = ["📊", "🔍", "🎯", "⭐"]
_NAV_LABELS = ["Market", "Screener", "Pivot Boss", "Watchlist"]
_NAV_KEYS   = ["market", "screener", "pivotboss", "watchlist"]

def render_mobile_nav(current_page: str):
    """Renders a fixed bottom nav bar (visible only on mobile via CSS)."""
    items_html = ""
    for icon, label, key, page in zip(_NAV_ICONS, _NAV_LABELS, _NAV_KEYS, _NAV_PAGES):
        active_cls = "active" if current_page == page else ""
        items_html += (
            f'<a href="?nav={key}" class="{active_cls}" target="_self">'
            f'<span class="nav-icon">{icon}</span>{label}</a>'
        )
    st.markdown(
        f'<nav class="mobile-nav">{items_html}</nav>',
        unsafe_allow_html=True,
    )

def get_mobile_page() -> str:
    """Read page from query param on mobile, fall back to session state."""
    params = st.query_params
    nav_key = params.get("nav", None)
    key_to_page = dict(zip(_NAV_KEYS, _NAV_PAGES))
    if nav_key in key_to_page:
        page = key_to_page[nav_key]
        st.session_state["mobile_page"] = page
        return page
    return st.session_state.get("mobile_page", "Market Snapshot")

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:1.05rem;"
            "font-weight:700;color:#1a2208;padding:0.5rem 0 0.1rem;'>"
            "🏦 PivotVault <span style='color:#4a6a0a;'>AI</span></div>"
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.63rem;"
            "color:#5a6b30;letter-spacing:0.09em;text-transform:uppercase;"
            "margin-bottom:1.25rem;'>Pivot Boss · Equity Terminal</div>",
            unsafe_allow_html=True,
        )
        # Use session_state key so we can change it programmatically
        if "current_page" not in st.session_state:
            st.session_state["current_page"] = "Market Snapshot"

        for page in _NAV_PAGES:
            is_active = st.session_state["current_page"] == page
            label = ("▶ " if is_active else "   ") + page
            btn_style = (
                "background:#d4dbb8 !important;color:#1a2208 !important;font-weight:700 !important;"
                if is_active else ""
            )
            st.markdown(
                f"<style>.sidebar-btn-{_NAV_PAGES.index(page)} button{{{btn_style}}}</style>",
                unsafe_allow_html=True,
            )
            col = st.container()
            with col:
                if st.button(label, key=f"nav_btn_{page}", use_container_width=True):
                    st.session_state["current_page"]         = page
                    st.session_state["mobile_page"]          = page
                    st.session_state["screener_nav_pending"] = False
                    st.query_params.clear()
                    st.rerun()

        st.divider()
        wl_count = len(st.session_state["watchlist"])
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;color:#3d4a1e;'>"
            f"Watchlist: <span style='color:#4a6a0a;font-weight:700;'>{wl_count}</span> "
            f"stock{'s' if wl_count != 1 else ''}</div>",
            unsafe_allow_html=True,
        )
        st.divider()
        user = st.session_state.get("username", "user")
        st.markdown(
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
            f"color:#3d4a1e;margin-bottom:0.5rem;'>Logged in as "
            f"<span style='color:#1a2208;font-weight:600;'>{user}</span></div>",
            unsafe_allow_html=True,
        )
        if st.button("Logout", key="logout_btn", use_container_width=True):
            st.session_state["logged_in"] = False
            st.rerun()


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    if not st.session_state["logged_in"]:
        page_login()
        return

    # Initialise current_page if missing
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "Market Snapshot"

    # Handle mobile bottom-nav query param
    nav_key = st.query_params.get("nav")
    key_to_page = dict(zip(_NAV_KEYS, _NAV_PAGES))
    if nav_key in key_to_page:
        st.session_state["current_page"] = key_to_page[nav_key]

    # Stock click from Market Snapshot overrides everything
    if st.session_state.get("screener_nav_pending"):
        st.session_state["current_page"]         = "Stock Screener"
        st.session_state["screener_nav_pending"] = False

    menu = st.session_state["current_page"]

    render_sidebar()
    render_mobile_nav(menu)
    render_market_header()
    st.divider()
    nse500 = fetch_nse500_list()

    if   menu == "Market Snapshot":     page_market_snapshot(nse500)
    elif menu == "Stock Screener":      page_stock_screener(nse500)
    elif menu == "Pivot Boss Analysis": page_pivot_boss(nse500)
    elif menu == "Watchlist":           page_watchlist()


if __name__ == "__main__":
    main()
