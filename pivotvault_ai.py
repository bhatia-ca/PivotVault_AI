import streamlit as st
import pandas as pd
import os
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False
import numpy as np
import secrets
import re
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from io import StringIO
from datetime import datetime, timedelta
import time
import smtplib
try:
    from mobile_patch import inject_mobile
    _MOBILE_PATCH = True
except ImportError:
    _MOBILE_PATCH = False
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
import streamlit.components.v1 as _stc

# ══════════════════════════════════════════════════════════════
#  UPSTOX FREE DATA FEED
#  Free tier: Historical OHLCV + Market quotes (no WebSocket)
#  Docs: https://upstox.com/developer/api-documentation/
# ══════════════════════════════════════════════════════════════

UPSTOX_BASE = "https://api.upstox.com/v2"

def _upstox_redirect_uri() -> str:
    """Detect correct redirect URI based on where the app is running."""
    try:
        ctx = getattr(st, "context", None)
        if ctx and hasattr(ctx, "headers"):
            host = dict(ctx.headers).get("host", "")
            if host and "localhost" not in host and "127.0.0.1" not in host:
                return f"https://{host}"
    except Exception:
        pass
    return "http://localhost:8501"

# Upstox instrument key format: NSE_EQ|INE002A01018 (RELIANCE)
# We use NSE symbol name format: NSE_EQ|{ISIN}
# For simplicity we use NSE_INDEX for indices
UPSTOX_INDEX_KEYS = {
    "^NSEI":    "NSE_INDEX|Nifty 50",
    "^BSESN":   "BSE_INDEX|SENSEX",
    "^NSEBANK": "NSE_INDEX|Nifty Bank",
}

def _upstox_headers() -> dict:
    """Return Upstox auth headers using token from session state."""
    token = st.session_state.get("upstox_access_token", "")
    if not token:
        return {}
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

def _upstox_connected() -> bool:
    return bool(st.session_state.get("upstox_access_token", "").strip())

@st.cache_data(ttl=15)
def upstox_get_quote(instrument_key: str) -> dict:
    """
    Fetch live LTP + OHLC for one instrument via Upstox Market Quote API.
    Free tier — no subscription needed, just access token.
    Returns: {ltp, open, high, low, close, change, pct_change, volume}
    """
    if not _upstox_connected():
        return {}
    try:
        r = requests.get(
            f"{UPSTOX_BASE}/market-quote/quotes",
            headers=_upstox_headers(),
            params={"instrument_key": instrument_key},
            timeout=5,
        )
        if r.status_code != 200:
            return {}
        data = r.json().get("data", {})
        # data is keyed by instrument_key
        q = data.get(instrument_key, {})
        ohlc = q.get("ohlc", {})
        return {
            "ltp":        round(q.get("last_price", 0), 2),
            "open":       round(ohlc.get("open", 0),   2),
            "high":       round(ohlc.get("high", 0),   2),
            "low":        round(ohlc.get("low",  0),   2),
            "close":      round(ohlc.get("close",0),   2),
            "volume":     q.get("volume", 0),
            "change":     round(q.get("net_change", 0), 2),
            "pct_change": round(q.get("net_change", 0) / max(ohlc.get("close",1),1) * 100, 2),
        }
    except Exception:
        return {}

@st.cache_data(ttl=60)
def upstox_get_historical(symbol: str, interval: str = "1d",
                           from_date: str = "", to_date: str = "") -> pd.DataFrame:
    """
    Fetch historical OHLCV candles from Upstox Historical Candle API (free).
    symbol: NSE symbol e.g. 'RELIANCE'
    interval: '1minute','30minute','day','week','month'
    Returns DataFrame with Open/High/Low/Close/Volume columns.
    """
    if not _upstox_connected():
        return pd.DataFrame()
    # Upstox instrument key for NSE equity
    # Format: NSE_EQ|ISIN — we use symbol directly for simplicity
    instrument_key = f"NSE_EQ|{symbol}"

    # Interval mapping
    interval_map = {
        "1m": "1minute", "5m": "30minute", "15m": "30minute",
        "30m": "30minute", "1h": "60minute", "1d": "day",
        "1wk": "week", "1mo": "month",
    }
    up_interval = interval_map.get(interval, "day")

    if not from_date:
        from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")

    try:
        r = requests.get(
            f"{UPSTOX_BASE}/historical-candle/{instrument_key}/{up_interval}/{to_date}/{from_date}",
            headers=_upstox_headers(),
            timeout=10,
        )
        if r.status_code != 200:
            return pd.DataFrame()
        candles = r.json().get("data", {}).get("candles", [])
        if not candles:
            return pd.DataFrame()
        # Upstox candle format: [timestamp, open, high, low, close, volume, oi]
        df = pd.DataFrame(candles, columns=["Datetime","Open","High","Low","Close","Volume","OI"])
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.set_index("Datetime").sort_index()
        df = df[["Open","High","Low","Close","Volume"]].astype(float)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=15)
def upstox_get_index_quote(yf_ticker: str) -> dict:
    """Get index LTP via Upstox using index instrument key."""
    inst_key = UPSTOX_INDEX_KEYS.get(yf_ticker)
    if not inst_key:
        return {}
    return upstox_get_quote(inst_key)

def upstox_get_ltp(symbol: str) -> float:
    """Get live LTP for a stock. Returns 0.0 if Upstox not connected."""
    if not _upstox_connected():
        return 0.0
    q = upstox_get_quote(f"NSE_EQ|{symbol}")
    return q.get("ltp", 0.0)

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="PivotVault AI",
    layout="wide",
    page_icon="🏦",
    initial_sidebar_state="expanded",
)

# Mobile PWA injection
if _MOBILE_PATCH:
    inject_mobile()

# ─────────────────────────────────────────────
#  CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&display=swap');

/* ═══════════════════════════════════════════════════════════
   PIVOTVAULT AI — OLIVE THEME · HIGH CONTRAST MOBILE-FIRST
   ═══════════════════════════════════════════════════════════ */

:root {
    /* Olive palette — darker for better contrast */
    --olive-900: #141808;
    --olive-800: #1e2c0d;
    --olive-700: #2e4214;
    --olive-600: #3d5a1c;
    --olive-500: #4e6e26;
    --olive-400: #638534;
    --olive-300: #7da048;
    --olive-200: #a8c070;
    --olive-100: #d4e4a8;
    --olive-50:  #eef5dc;
    /* Surfaces */
    --bg:        #f0f4e8;
    --surface:   #ffffff;
    --surface2:  #f5f8ed;
    --border:    #b8c89a;
    --border2:   #9ab07a;
    /* Text — high contrast */
    --text:      #0e1308;
    --text-dim:  #2e3d1a;
    --text-muted:#4a5e32;
    /* Accents */
    --bull:      #1a6b2e;
    --bull-bg:   #e4f5e8;
    --bull-bdr:  #8dcc9a;
    --bear:      #9e2018;
    --bear-bg:   #fbe8e6;
    --bear-bdr:  #dc9090;
    --warn:      #7a5800;
    --warn-bg:   #fdf3d4;
    /* Shadows */
    --shadow-sm: 0 1px 4px rgba(20,30,8,0.12);
    --shadow-md: 0 4px 14px rgba(20,30,8,0.14);
    --shadow-lg: 0 8px 28px rgba(20,30,8,0.16);
    /* Radius */
    --r-sm: 7px;
    --r-md: 11px;
    --r-lg: 16px;
    /* Font sizes — larger for mobile readability */
    --fs-xs:   0.8rem;
    --fs-sm:   0.875rem;
    --fs-base: 1rem;
    --fs-md:   1.0625rem;
    --fs-lg:   1.2rem;
}

/* ── GLOBAL ────────────────────────────────────────────────── */
html, body {
    background: var(--bg) !important;
    font-family: 'DM Sans', sans-serif !important;
    color: var(--text) !important;
    -webkit-font-smoothing: antialiased;
    font-size: 16px !important;
    line-height: 1.6 !important;
}
.stApp, .stApp > div,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stMainBlockContainer"] {
    background: var(--bg) !important;
}
[data-testid="stVerticalBlock"] { background: transparent !important; }
.block-container {
    background: var(--bg) !important;
    padding: 1rem 1.25rem 2rem !important;
    max-width: 1440px !important;
}
#MainMenu, footer { visibility: hidden !important; }
header[data-testid="stHeader"] {
    background: transparent !important;
    border-bottom: none !important;
}

/* ── TYPOGRAPHY — mobile-first sizes ──────────────────────── */
h1, h2, h3, h4 {
    font-family: 'DM Sans', sans-serif !important;
    color: var(--text) !important;
    font-weight: 700 !important;
    letter-spacing: -0.01em !important;
    line-height: 1.3 !important;
}
h1 { font-size: 1.6rem !important; }
h2 { font-size: 1.35rem !important; }
h3 { font-size: 1.15rem !important; }
p, span, label, li, td, th {
    font-family: 'DM Sans', sans-serif !important;
    color: var(--text) !important;
    font-size: var(--fs-base) !important;
}
div {
    font-family: 'DM Sans', sans-serif !important;
}
code, pre, [class*="mono"] {
    font-family: 'DM Mono', monospace !important;
    font-size: var(--fs-sm) !important;
}

/* ── SIDEBAR ───────────────────────────────────────────────── */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div {
    background: var(--olive-900) !important;
    border-right: 2px solid var(--olive-700) !important;
    box-shadow: 3px 0 16px rgba(0,0,0,0.2) !important;
}
section[data-testid="stSidebar"] * { color: #e8f0d0 !important; }
section[data-testid="stSidebar"] hr { border-color: var(--olive-700) !important; }
section[data-testid="stSidebar"] .stCaption p {
    color: var(--olive-300) !important;
    font-size: var(--fs-xs) !important;
    font-family: 'DM Mono', monospace !important;
}
section[data-testid="stSidebar"] .stRadio label {
    border-radius: var(--r-sm) !important;
    padding: 0.65rem 1rem !important;
    font-size: var(--fs-sm) !important;
    font-weight: 500 !important;
    color: #c8dca0 !important;
    cursor: pointer !important;
    border-left: 3px solid transparent !important;
    transition: all 0.15s !important;
    min-height: 44px !important;
    display: flex !important;
    align-items: center !important;
}
section[data-testid="stSidebar"] .stRadio label:hover {
    background: rgba(255,255,255,0.08) !important;
    color: #f0f8d8 !important;
    border-left-color: var(--olive-300) !important;
}
section[data-testid="stSidebar"] .stButton > div > button {
    background: rgba(255,255,255,0.08) !important;
    border: 1px solid var(--olive-600) !important;
    color: #c8dca0 !important;
    border-radius: var(--r-sm) !important;
    font-size: var(--fs-sm) !important;
    min-height: 44px !important;
}

/* ── METRIC CARDS ─────────────────────────────────────────── */
div[data-testid="metric-container"] {
    background: var(--surface) !important;
    border: 1.5px solid var(--border) !important;
    border-radius: var(--r-md) !important;
    padding: 1rem 1.1rem !important;
    box-shadow: var(--shadow-sm) !important;
    border-top: 3px solid var(--olive-400) !important;
}
div[data-testid="metric-container"] label {
    font-family: 'DM Mono', monospace !important;
    font-size: var(--fs-xs) !important;
    letter-spacing: 0.07em !important;
    text-transform: uppercase !important;
    color: var(--text-muted) !important;
    font-weight: 600 !important;
}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Mono', monospace !important;
    font-size: 1.5rem !important;
    font-weight: 700 !important;
    color: var(--text) !important;
}
div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-family: 'DM Mono', monospace !important;
    font-size: var(--fs-xs) !important;
    font-weight: 600 !important;
}

/* ── BUTTONS ──────────────────────────────────────────────── */
.stButton > div > button {
    background: var(--olive-600) !important;
    border: none !important;
    color: #ffffff !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: var(--fs-sm) !important;
    font-weight: 700 !important;
    border-radius: var(--r-sm) !important;
    padding: 0.6rem 1.2rem !important;
    min-height: 46px !important;
    transition: background 0.18s !important;
    box-shadow: 0 2px 5px rgba(20,30,8,0.18) !important;
    cursor: pointer !important;
    letter-spacing: 0.01em !important;
}
.stButton > div > button:hover {
    background: var(--olive-500) !important;
    box-shadow: 0 4px 12px rgba(20,30,8,0.22) !important;
}
.stButton > div > button:active {
    background: var(--olive-700) !important;
}

/* ── INPUTS ───────────────────────────────────────────────── */
input[type="text"], input[type="password"], input[type="number"],
.stTextInput input, .stNumberInput input {
    background: var(--surface) !important;
    border: 2px solid var(--border) !important;
    border-radius: var(--r-sm) !important;
    color: var(--text) !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 16px !important;
    min-height: 48px !important;
    padding: 0.5rem 0.75rem !important;
}
input:focus {
    border-color: var(--olive-500) !important;
    box-shadow: 0 0 0 3px rgba(78,110,38,0.18) !important;
    outline: none !important;
}
input::placeholder { color: var(--text-muted) !important; }
/* Labels above inputs */
.stTextInput label, .stNumberInput label,
.stSelectbox label, .stMultiSelect label,
.stSlider label, .stRadio label {
    font-size: var(--fs-sm) !important;
    font-weight: 600 !important;
    color: var(--text-dim) !important;
}

/* ── SELECTBOXES ──────────────────────────────────────────── */
div[data-baseweb="select"] > div {
    background: var(--surface) !important;
    border: 2px solid var(--border) !important;
    border-radius: var(--r-sm) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
    min-height: 48px !important;
    font-size: var(--fs-sm) !important;
}
div[data-baseweb="select"] span,
div[data-baseweb="select"] div { color: var(--text) !important; background: transparent !important; }
ul[data-baseweb="menu"],
div[data-baseweb="popover"] > div {
    background: var(--surface) !important;
    border: 2px solid var(--border) !important;
    border-radius: var(--r-md) !important;
    box-shadow: var(--shadow-lg) !important;
}
li[role="option"] {
    background: transparent !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: var(--fs-sm) !important;
    padding: 0.65rem 1rem !important;
    min-height: 44px !important;
    display: flex !important;
    align-items: center !important;
}
li[role="option"]:hover { background: var(--olive-50) !important; }
li[aria-selected="true"] {
    background: var(--olive-100) !important;
    color: var(--olive-800) !important;
    font-weight: 700 !important;
}

/* ── DATAFRAMES ───────────────────────────────────────────── */
.stDataFrame {
    border-radius: var(--r-md) !important;
    overflow: hidden !important;
    box-shadow: var(--shadow-sm) !important;
    font-size: var(--fs-xs) !important;
}
[data-testid="stDataFrameContainer"] {
    background: var(--surface) !important;
    border: 1.5px solid var(--border) !important;
    border-radius: var(--r-md) !important;
}

/* ── EXPANDER ─────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: var(--surface) !important;
    border: 1.5px solid var(--border) !important;
    border-radius: var(--r-md) !important;
    box-shadow: var(--shadow-sm) !important;
}
[data-testid="stExpander"] summary {
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 700 !important;
    font-size: var(--fs-sm) !important;
    padding: 0.85rem 1rem !important;
    min-height: 48px !important;
}

/* ── TABS ─────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    background: var(--surface2) !important;
    border-bottom: 2px solid var(--border) !important;
    gap: 0 !important;
    padding: 0 0.25rem !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border: none !important;
    color: var(--text-dim) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: var(--fs-sm) !important;
    font-weight: 600 !important;
    padding: 0.7rem 1rem !important;
    border-bottom: 3px solid transparent !important;
    margin-bottom: -2px !important;
    min-height: 46px !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: var(--olive-600) !important;
    background: rgba(78,110,38,0.06) !important;
}
.stTabs [aria-selected="true"] {
    color: var(--olive-700) !important;
    border-bottom-color: var(--olive-500) !important;
    font-weight: 800 !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: var(--surface) !important;
    border: 1.5px solid var(--border) !important;
    border-top: none !important;
    border-radius: 0 0 var(--r-md) var(--r-md) !important;
    padding: 1rem !important;
}

/* ── RADIO & CHECKBOX ─────────────────────────────────────── */
.stRadio label, .stCheckbox label {
    color: var(--text) !important;
    font-size: var(--fs-sm) !important;
    font-weight: 500 !important;
    min-height: 40px !important;
    display: flex !important;
    align-items: center !important;
}

/* ── ALERTS ───────────────────────────────────────────────── */
[data-testid="stInfo"]    {
    background: #e8f5e4 !important;
    border-left: 4px solid var(--olive-500) !important;
    border-radius: var(--r-sm) !important;
    color: var(--olive-800) !important;
}
[data-testid="stSuccess"] {
    background: var(--bull-bg) !important;
    border-left: 4px solid var(--bull) !important;
    border-radius: var(--r-sm) !important;
}
[data-testid="stWarning"] {
    background: var(--warn-bg) !important;
    border-left: 4px solid var(--warn) !important;
    border-radius: var(--r-sm) !important;
}
[data-testid="stError"] {
    background: var(--bear-bg) !important;
    border-left: 4px solid var(--bear) !important;
    border-radius: var(--r-sm) !important;
}
/* Alert text always dark */
[data-testid="stInfo"] p,
[data-testid="stSuccess"] p,
[data-testid="stWarning"] p,
[data-testid="stError"] p {
    color: var(--text) !important;
    font-size: var(--fs-sm) !important;
    font-weight: 500 !important;
}
.stCaption, .stCaption p {
    color: var(--text-muted) !important;
    font-family: 'DM Mono', monospace !important;
    font-size: var(--fs-xs) !important;
}

/* ── DIVIDERS ─────────────────────────────────────────────── */
hr { border-color: var(--border) !important; margin: 0.75rem 0 !important; border-width: 1.5px !important; }

/* ── COMPONENT CLASSES ────────────────────────────────────── */
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }
@keyframes slideIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:none} }

.live-dot {
    display: inline-block;
    width: 9px; height: 9px;
    background: var(--bull);
    border-radius: 50%;
    margin-right: 6px;
    animation: pulse 1.8s ease-in-out infinite;
    box-shadow: 0 0 6px rgba(26,107,46,0.5);
    flex-shrink: 0;
}
.title-bar {
    display: flex; align-items: center; gap: 0.75rem;
    margin-bottom: 1rem;
    animation: slideIn 0.25s ease;
    flex-wrap: wrap;
}
.title-bar h1 {
    margin: 0 !important;
    font-size: 1.4rem !important;
    color: var(--text) !important;
    font-weight: 800 !important;
}

/* Signal badges */
.signal-badge {
    display: inline-block;
    font-family: 'DM Mono', monospace;
    font-size: var(--fs-xs);
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    padding: 0.25rem 0.7rem;
    border-radius: 5px;
    margin: 0.1rem;
}
.sig-bull { background: var(--bull-bg); color: var(--bull); border: 1.5px solid var(--bull-bdr); }
.sig-bear { background: var(--bear-bg); color: var(--bear); border: 1.5px solid var(--bear-bdr); }
.sig-neut { background: var(--warn-bg); color: var(--warn); border: 1.5px solid #e0c060; }

/* Multiselect tags */
[data-baseweb="tag"] {
    background: var(--olive-100) !important;
    border-radius: 5px !important;
    color: var(--olive-800) !important;
    font-size: var(--fs-xs) !important;
    font-weight: 600 !important;
}

/* Progress bar */
.stProgress > div > div > div {
    background: var(--olive-500) !important;
    border-radius: 4px !important;
}

/* Scrollbars */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--surface2); }
::-webkit-scrollbar-thumb { background: var(--olive-300); border-radius: 4px; }

/* Nav button overrides */
.nav-btn > div > button {
    background: transparent !important;
    border: none !important;
    border-bottom: 3px solid transparent !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    color: var(--text-dim) !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: var(--fs-xs) !important;
    font-weight: 600 !important;
    padding: 0.5rem 0.2rem !important;
    min-height: 48px !important;
    transition: color 0.15s, border-color 0.15s !important;
    white-space: nowrap !important;
}
.nav-btn > div > button:hover {
    color: var(--olive-700) !important;
    border-bottom-color: var(--olive-400) !important;
    background: rgba(78,110,38,0.05) !important;
}
.nav-btn-active > div > button {
    color: var(--olive-700) !important;
    border-bottom-color: var(--olive-600) !important;
    font-weight: 800 !important;
    background: rgba(78,110,38,0.07) !important;
}

/* ── MOBILE SPECIFIC ───────────────────────────────────────── */
@media (max-width: 768px) {
    :root {
        --fs-xs:   0.85rem;
        --fs-sm:   0.95rem;
        --fs-base: 1rem;
    }
    .block-container {
        padding: 0.5rem 0.6rem 1rem !important;
        max-width: 100vw !important;
    }
    /* Bigger touch targets */
    .stButton > div > button {
        min-height: 52px !important;
        font-size: 1rem !important;
        font-weight: 700 !important;
        padding: 0.75rem 1rem !important;
    }
    div[data-baseweb="select"] > div { min-height: 52px !important; }
    input { font-size: 16px !important; min-height: 52px !important; }
    /* Tabs bigger on mobile */
    .stTabs [data-baseweb="tab"] {
        padding: 0.75rem 0.75rem !important;
        font-size: 0.9rem !important;
        min-height: 50px !important;
    }
    /* Metric values bigger */
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
    }
    div[data-testid="metric-container"] label {
        font-size: 0.75rem !important;
    }
    /* Nav buttons on mobile */
    .nav-btn > div > button,
    .nav-btn-active > div > button {
        font-size: 0.72rem !important;
        padding: 0.4rem 0.1rem !important;
        min-height: 48px !important;
    }
    /* Hide sidebar on mobile */
    section[data-testid="stSidebar"]       { display: none !important; }
    [data-testid="collapsedControl"]        { display: none !important; }
    button[data-testid="baseButton-header"] { display: none !important; }
    /* Touch handling */
    * { touch-action: manipulation; -webkit-tap-highlight-color: transparent; }
    body { overscroll-behavior-y: none; }
    /* Card padding tighter on mobile */
    [data-testid="stExpander"] summary { padding: 0.75rem 0.75rem !important; }
}

/* ── DESKTOP ───────────────────────────────────────────────── */
@media (min-width: 769px) {
    .block-container { padding: 1.25rem 2rem 2rem !important; }
    div[data-testid="metric-container"]:hover {
        box-shadow: var(--shadow-md) !important;
        transform: translateY(-1px) !important;
        transition: all 0.2s !important;
    }
}
</style>
""", unsafe_allow_html=True)

# ── Global notification bootstrap (injected once per page load) ───────────
# Must use window.parent to escape Streamlit's iframe sandbox
st.markdown("""
<script>
(function() {
    // Always work on the TOP window, not the iframe
    var win = window.parent || window;

    // Expose helper functions on parent window so any iframe can call them
    win._pvNotify = function(title, body, tag) {
        if (!("Notification" in win)) return;
        if (win.Notification.permission === "granted") {
            var n = new win.Notification(title, {
                body: body,
                icon: "/static/icon-192.png",
                tag:  tag || "pivotvault",
                requireInteraction: false,
                silent: false,
            });
            n.onclick = function() { win.focus(); n.close(); };
        }
    };

    win._pvRequestNotif = function(cb) {
        if (!("Notification" in win)) {
            if (cb) cb(false);
            return;
        }
        if (win.Notification.permission === "granted") {
            if (cb) cb(true);
            return;
        }
        win.Notification.requestPermission().then(function(p) {
            if (cb) cb(p === "granted");
        });
    };

    // Store permission state on parent
    win._pvNotifEnabled = (
        "Notification" in win &&
        win.Notification.permission === "granted"
    );

    // Auto-request permission after 2s if not yet decided
    if ("Notification" in win && win.Notification.permission === "default") {
        setTimeout(function() {
            win.Notification.requestPermission().then(function(p) {
                win._pvNotifEnabled = (p === "granted");
                if (p === "granted") {
                    new win.Notification("🏦 PivotVault AI", {
                        body: "Trade signal notifications enabled!",
                        icon: "/static/icon-192.png",
                        tag:  "pv-welcome",
                    });
                }
            });
        }, 2000);
    }
})();
</script>
""", unsafe_allow_html=True)

# ── Global notification permission manager ────────────────────────────────
# Injected on every page load — uses window.parent to escape Streamlit iframe
st.markdown("""
<script>
(function initPVNotif() {
    // Must use window.parent to escape Streamlit iframe
    var w = window.parent || window;

    // Store permission state globally
    w._pvNotifReady = false;

    function checkAndRequest() {
        if (!("Notification" in w)) return;
        if (w.Notification.permission === "granted") {
            w._pvNotifReady = true;
            return;
        }
        if (w.Notification.permission === "default") {
            // Auto-request after 1s — browser requires user gesture
            // so we store a flag and show a button instead
            w._pvNeedPermission = true;
        }
    }

    // Global function to fire a notification from anywhere in the app
    w.pvNotify = function(title, body, tag) {
        if (!("Notification" in w)) return;
        if (w.Notification.permission === "granted") {
            try {
                var n = new w.Notification(title, {
                    body: body,
                    icon: "/static/icon-192.png",
                    tag:  tag || "pivotvault",
                    requireInteraction: true,
                    silent: false,
                });
                n.onclick = function() { w.focus(); n.close(); };
            } catch(e) { console.log("Notif error:", e); }
        }
    };

    checkAndRequest();
})();
</script>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────
defaults = {
    'watchlist': [],
    'cpr_scan_df':  None,
    'user_id':      None,
    'user_email':   '',
    'user_phone':   '',
    'auth_mode':    'login',
    'otp_code':     '',
    'otp_target':   '',
    # Broker config
    'broker':           'none',   # none | zerodha | upstox | groww
    'zerodha_api_key':  '',
    'zerodha_api_secret':'',
    'zerodha_access_token':'',
    'upstox_api_key':   '',
    'upstox_api_secret':'',
    'upstox_access_token':'',
    'broker_connected': False,
    # Paper trading
    'paper_trades':     [],
    'paper_balance':    100000.0,  # Rs 1 lakh starting capital
    'paper_positions':  {},
    'cpr_scan_15m': None,
    'cpr_scan_1h':  None,
    'cpr_scan_1d':  None,
    'cpr_scan_1wk': None,
    'cpr_scan_1mo': None,
    'logged_in':    False,
    'wl_data':      {},
    'wl_last_refresh': None,
    'smtp_cfg': {"host": "smtp.gmail.com", "port": 587, "sender": "", "password": ""},
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


@st.cache_data(ttl=3600)
def fetch_nifty200_list() -> list:
    """Fetch Nifty 200 symbols from NSE. Falls back to hardcoded top-200 subset."""
    url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        df.columns = df.columns.str.strip()
        return df["Symbol"].dropna().tolist()
    except Exception:
        # Hardcoded Nifty 200 fallback (top liquid stocks)
        return [
            "RELIANCE","TCS","HDFCBANK","ICICIBANK","INFY","HDFC","SBIN","BHARTIARTL",
            "KOTAKBANK","ITC","LT","AXISBANK","ASIANPAINT","MARUTI","WIPRO","ULTRACEMCO",
            "BAJFINANCE","NESTLEIND","TITAN","SUNPHARMA","POWERGRID","NTPC","TECHM","HCLTECH",
            "TATAMOTORS","ONGC","COALINDIA","JSWSTEEL","TATASTEEL","ADANIPORTS","BAJAJFINSV",
            "HINDALCO","GRASIM","CIPLA","DIVISLAB","DRREDDY","EICHERMOT","BPCL","HEROMOTOCO",
            "BRITANNIA","INDUSINDBK","M&M","APOLLOHOSP","TATACONSUM","PIDILITIND","SIEMENS",
            "DABUR","GODREJCP","BERGEPAINT","HAVELLS","MUTHOOTFIN","LUPIN","BIOCON","TORNTPHARM",
            "BOSCHLTD","COLPAL","MARICO","ICICIPRULI","SBILIFE","HDFCLIFE","BAJAJ-AUTO",
            "SHREECEM","AMBUJACEM","ACC","VEDL","SAIL","NMDC","IOCL","HINDPETRO","PGHL",
            "MCDOWELL-N","UNITED SPIRITS","TATAPOWER","ADANIENT","ADANITRANS","ADANIGREEN",
            "NAUKRI","ZOMATO","PAYTM","DMART","IRCTC","MOTHERSON","BALKRISIND","CONCOR",
            "CHOLAFIN","MANAPPURAM","RECLTD","PFC","CANBK","BANKBARODA","PNB","FEDERALBNK",
            "IDFCFIRSTB","RBLBANK","BANDHANBNK","INDHOTEL","JUBLFOOD","DOMINOS","VOLTAS",
            "WHIRLPOOL","BLUEDART","DELHIVERY","ZYDUSLIFE","ALKEM","AUROPHARMA","CADILAHC",
            "GLENMARK","IPCA","LALPATHLAB","METROPOLIS","THYROCARE","FORTIS","MAXHEALTH",
            "NARAYANA","AARTIIND","DEEPAKNI","SRF","PIDILITIND","AIAENG","CUMMINSIND",
            "THERMAX","ABB","BHEL","BEL","HAL","BEML","MFSL","LICHSGFIN","HDFCAMC","NIPPONLIFE",
            "UTIAMC","ABCAPITAL","ICICIGI","NIACL","GICRE","STARHEALTH","PGHH","EMAMILTD",
            "JYOTHYLAB","VSTIND","RADICO","UNITDSPR","TATACOMM","LTTS","MPHASIS","COFORGE",
            "PERSISTENT","ZENSARTECH","HEXAWARE","KPITTECH","TATAELXSI","INFY","OFSS",
            "RAMCOCEM","JKCEMENT","PRISM","HEIDELBERG","BIRLASOFT","MINDTREE","L&TFH","SRTRANSFIN",
            "SUNDARMFIN","M&MFIN","SCUF","AUBANK","UJJIVAN","EQUITAS","SURYODAY","ESAFSFB",
            "CROMPTON","ORIENTELEC","POLYCAB","FINOLEX","KEI","STERLITE","KPIL","NCC","AHLUCONT",
            "PNCINFRA","IRB","HG INFRA","SADBHAV","ASHOKA","KNRCON","GPPL","ADANIPORTS",
            "MUNDRAPORT","RITES","IRFC","HUDCO","NBCC","DLF","PRESTIGE","OBEROIRLTY",
            "GODREJPROP","PHOENIXLTD","BRIGADE","SOBHA","SUNTECK","MAHINDCIE","SCHAEFFLER",
        ]

@st.cache_data(ttl=3600)
def fetch_nifty200_by_marketcap() -> list:
    """
    Returns Nifty 200 symbols sorted by market cap (highest first).
    Fetches market cap from yfinance info in batches.
    Falls back to a pre-ranked hardcoded list if fetch fails.
    """
    # Pre-ranked Nifty 200 by approximate market cap (as of 2025)
    RANKED = [
        "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","INFY","SBIN","LICI",
        "HINDUNILVR","ITC","LT","BAJFINANCE","HCLTECH","KOTAKBANK","MARUTI","SUNPHARMA",
        "AXISBANK","TITAN","ADANIENT","ADANIPORTS","ASIANPAINT","WIPRO","ULTRACEMCO",
        "NTPC","POWERGRID","NESTLEIND","TATAMOTORS","BAJAJFINSV","JSWSTEEL","TATASTEEL",
        "COALINDIA","ONGC","BPCL","TECHM","HINDALCO","GRASIM","M&M","INDUSINDBK",
        "CIPLA","DRREDDY","DIVISLAB","EICHERMOT","HEROMOTOCO","BAJAJ-AUTO","BRITANNIA",
        "APOLLOHOSP","TATACONSUM","PIDILITIND","SIEMENS","DABUR","GODREJCP","HAVELLS",
        "BERGEPAINT","ICICIPRULI","SBILIFE","HDFCLIFE","SHREECEM","AMBUJACEM","VEDL",
        "SAIL","NMDC","IOCL","HINDPETRO","TATAPOWER","ADANIGREEN","ADANITRANS",
        "NAUKRI","ZOMATO","DMART","IRCTC","CHOLAFIN","RECLTD","PFC","BANKBARODA",
        "CANBK","PNB","FEDERALBNK","IDFCFIRSTB","MUTHOOTFIN","LUPIN","BIOCON",
        "TORNTPHARM","BOSCHLTD","COLPAL","MARICO","INDHOTEL","JUBLFOOD","VOLTAS",
        "MOTHERSON","BALKRISIND","CONCOR","MANAPPURAM","BANDHANBNK","RBLBANK",
        "ZYDUSLIFE","ALKEM","AUROPHARMA","GLENMARK","IPCA","LALPATHLAB","FORTIS",
        "MAXHEALTH","ABB","BHEL","BEL","HAL","BEML","LICHSGFIN","HDFCAMC",
        "NIPPONLIFE","UTIAMC","ABCAPITAL","ICICIGI","NIACL","GICRE","STARHEALTH",
        "PGHH","EMAMILTD","JYOTHYLAB","RADICO","TATACOMM","LTTS","MPHASIS","COFORGE",
        "PERSISTENT","TATAELXSI","OFSS","RAMCOCEM","JKCEMENT","KPITTECH","BIRLASOFT",
        "SRF","DEEPAKNI","AARTIIND","CUMMINSIND","THERMAX","CROMPTON","POLYCAB",
        "KEI","FINOLEX","KPIL","NCC","IRB","DLF","PRESTIGE","OBEROIRLTY",
        "GODREJPROP","PHOENIXLTD","BRIGADE","SOBHA","MCDOWELL-N","SCHAEFFLER",
        "ACC","HEIDELBERG","PRISM","UNITDSPR","VSTIND","PAYTM","DELHIVERY",
        "BLUEDART","UJJIVAN","EQUITAS","AUBANK","M&MFIN","SRTRANSFIN","SUNDARMFIN",
        "SCUF","ORIENTELEC","NHPC","SJVN","NBCC","HUDCO","IRFC","RITES","GPPL",
        "AHLUCONT","PNCINFRA","KNRCON","ASHOKA","SADBHAV","HG INFRA","NAKODA",
        "NARAYANA","METROPOLIS","THYROCARE","LALPATHLAB","PGHL","SUNTECK","MAHINDCIE",
    ]

    # Use pre-ranked list (avoids slow yfinance calls on every load)
    try:
        n200_set = set(fetch_nifty200_list())
        ranked = [s for s in RANKED if s in n200_set]
        extras = [s for s in fetch_nifty200_list() if s not in set(RANKED)]
        return ranked + sorted(extras)
    except Exception:
        return RANKED


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


@st.cache_data(ttl=15)
def fetch_index_data(ticker: str) -> dict:
    """
    Fetch live index price.
    Priority: Upstox API → NSE API → yfinance fast_info → yfinance history
    """
    # ── Method 0: Upstox (if access token configured) ─────────────────────
    if _upstox_connected():
        try:
            q = upstox_get_index_quote(ticker)
            if q and q.get("ltp"):
                return {
                    "ltp":    q["ltp"],
                    "change": q.get("pct_change", 0),
                    "prev":   q.get("close", 0),
                    "high":   q.get("high", q["ltp"]),
                    "low":    q.get("low",  q["ltp"]),
                    "source": "Upstox",
                }
        except Exception:
            pass

    # NSE API mapping
    NSE_MAP = {
        "^NSEI":    "NIFTY 50",
        "^BSESN":   None,
        "^NSEBANK": "NIFTY BANK",
    }

    # ── Method 1: NSE India API (real-time) ──────────────────────────────
    try:
        nse_name = NSE_MAP.get(ticker)
        if nse_name:
            session = requests.Session()
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 Chrome/122 Safari/537.36",
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com/",
                "Accept-Language": "en-US,en;q=0.9",
            }
            # Set cookie first
            session.get("https://www.nseindia.com", headers=headers, timeout=5)
            r = session.get(
                f"https://www.nseindia.com/api/allIndices",
                headers=headers, timeout=5,
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                for item in data:
                    if item.get("index") == nse_name:
                        ltp  = round(float(item["last"]), 2)
                        prev = round(float(item["previousClose"]), 2)
                        chg  = round(float(item["percentChange"]), 2)
                        return {"ltp": ltp, "change": chg, "prev": prev,
                                "high": round(float(item.get("high", ltp)), 2),
                                "low":  round(float(item.get("low",  ltp)), 2)}
    except Exception:
        pass

    # ── Method 2: yfinance fast_info (near real-time) ─────────────────────
    try:
        fi  = yf.Ticker(ticker).fast_info
        ltp = round(float(fi.last_price), 2)
        prev = round(float(fi.previous_close), 2)
        chg  = round((ltp - prev) / prev * 100, 2) if prev else 0
        return {"ltp": ltp, "change": chg, "prev": prev}
    except Exception:
        pass

    # ── Method 3: yfinance history (delayed ~15 min, last resort) ────────
    try:
        hist = yf.Ticker(ticker).history(period="5d", interval="1m")
        if len(hist) >= 2:
            ltp  = round(float(hist["Close"].iloc[-1]), 2)
            prev = round(float(hist["Close"].iloc[-2]), 2)
            chg  = round((ltp - prev) / prev * 100, 2)
            return {"ltp": ltp, "change": chg}
    except Exception:
        pass

    return {"ltp": None, "change": None}


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
            fill="toself", fillcolor="rgba(29,78,216,0.05)",
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
        for level, color in [(70, "rgba(220,38,38,0.15)"), (30, "rgba(22,163,74,0.15)"), (50, "#1e2330")]:
            fig.add_hline(y=level, line=dict(color=color, width=0.8, dash="dot"), row=3, col=1)

    # Volume
    vol_colors = ["#00e5a0" if c >= o else "#ff4d6a"
                  for c, o in zip(df["Close"], df["Open"])]
    fig.add_trace(go.Bar(
        x=df.index, y=df["Volume"],
        marker_color=vol_colors, opacity=0.55, showlegend=False,
    ), row=4, col=1)

    # Layout — clean, professional light theme
    fig.update_layout(
        height=820,
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fafbfc",
        font=dict(family="IBM Plex Mono", color="#5a6a80", size=10),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0,
            font=dict(size=9, color="#1a2332"),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#dce3ed", borderwidth=1,
        ),
        margin=dict(l=10, r=100, t=40, b=10),
        xaxis_rangeslider_visible=False,
        title=dict(
            text=f"<b>{symbol}</b>  ·  {pivot_type} Pivots",
            font=dict(family="IBM Plex Mono", size=13, color="#1a2332"),
            x=0.01,
        ),
        shapes=[],   # clean baseline
    )
    # Shared axis styling
    axis_style = dict(
        showgrid=True, gridcolor="#eef0f4", gridwidth=1,
        showline=True, linecolor="#dce3ed", linewidth=1,
        zeroline=False, tickfont=dict(size=9, color="#8a9ab0"),
        ticks="outside", ticklen=3,
    )
    for i in range(1, 5):
        fig.update_xaxes(**axis_style, row=i, col=1)
        fig.update_yaxes(**axis_style, row=i, col=1)
    # Row labels
    fig.update_yaxes(title_text="3/10 OSC", title_font=dict(size=8, color="#8a9ab0"), row=2, col=1)
    fig.update_yaxes(title_text="RSI 14",   title_font=dict(size=8, color="#8a9ab0"), row=3, col=1)
    fig.update_yaxes(title_text="Volume",   title_font=dict(size=8, color="#8a9ab0"), row=4, col=1)
    # Add subtle row separator lines
    for row_y in [0.55, 0.73, 0.87]:
        fig.add_hline(y=0, line=dict(color="#eef0f4", width=1))
    return fig


def build_stoch_chart(df_ind: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_ind.index, y=df_ind["STOCH_K"],
                             name="%K", line=dict(color="#00e5a0", width=1.2)))
    fig.add_trace(go.Scatter(x=df_ind.index, y=df_ind["STOCH_D"],
                             name="%D", line=dict(color="#f5a623", width=1.2, dash="dot")))
    for level, color in [(80, "rgba(220,38,38,0.15)"), (20, "rgba(22,163,74,0.15)"), (50, "#1e2330")]:
        fig.add_hline(y=level, line=dict(color=color, width=0.8, dash="dot"))
    fig.update_layout(
        height=200, paper_bgcolor="#f0f4f8", plot_bgcolor="#ffffff",
        font=dict(family="IBM Plex Mono", color="#475569", size=10),
        margin=dict(l=0, r=60, t=28, b=0),
        legend=dict(orientation="h", font=dict(size=10), bgcolor="rgba(255,255,255,0.9)"),
        title=dict(text="Stochastic (14, 3, 3)", font=dict(size=11, color="#d4daf0")),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#e2e8f0")
    fig.update_yaxes(showgrid=True, gridcolor="#e2e8f0")
    return fig


# ─────────────────────────────────────────────
#  UI HELPERS
# ─────────────────────────────────────────────

def render_lw_chart(symbol: str, tf_label: str, analysis: dict,
                    pivot_type: str, height: int = 660):
    """
    TradingView Lightweight Charts v4.1.1 (free, open-source — unpkg CDN).
    Renders a professional candlestick chart with:
    - Live OHLCV data from yfinance
    - CPR band (TC/P/BC) as horizontal lines
    - All pivot levels (R1/R2/R3/S1/S2/S3)
    - Market Profile (POC/VAH/VAL)
    - Volume bars
    - Overall bias header
    """
    import json

    # ── Get price data ────────────────────────────────────────────────────
    TF_MAP = {
        "5 Min":   ("5d",  "5m"),
        "15 Min":  ("10d", "15m"),
        "30 Min":  ("20d", "30m"),
        "1 Hour":  ("60d", "1h"),
        "4 Hour":  ("90d", "1h"),
        "Daily":   ("1y",  "1d"),
        "Weekly":  ("5y",  "1wk"),
        "Monthly": ("10y", "1mo"),
    }
    period, interval = TF_MAP.get(tf_label, ("1y","1d"))

    try:
        df_raw = yf.Ticker(symbol + ".NS").history(period=period, interval=interval)
        df_raw.index = df_raw.index.tz_localize(None)
        if df_raw.empty or len(df_raw) < 5:
            st.warning("No price data available.")
            return
    except Exception as e:
        st.error(f"Data error: {e}")
        return

    # ── Build candlestick data (LW Charts format) ─────────────────────────
    candles = []
    volumes = []
    for ts, row in df_raw.iterrows():
        t = int(ts.timestamp())
        candles.append({
            "time": t,
            "open":  round(float(row["Open"]),  2),
            "high":  round(float(row["High"]),  2),
            "low":   round(float(row["Low"]),   2),
            "close": round(float(row["Close"]), 2),
        })
        volumes.append({
            "time":  t,
            "value": int(row["Volume"]),
            "color": "#16a34a44" if float(row["Close"]) >= float(row["Open"]) else "#dc262644",
        })

    # ── Pivot & CPR levels ────────────────────────────────────────────────
    cpr     = analysis.get("cpr", {})
    pivots  = analysis.get("pivots", {})
    mp      = analysis.get("market_profile", {})
    ltp     = analysis.get("ltp", 0)
    overall = analysis.get("overall", "NEUTRAL")
    ov_col  = analysis.get("ov_col", "neut")
    bias_color = {"bull":"#16a34a","bear":"#dc2626","neut":"#d97706"}.get(ov_col,"#888")

    # Build price lines config for LW Charts
    price_lines = []

    PIVOT_COLORS = {
        "R3":"#ff2222","R2":"#ff5555","R1":"#ff9999",
        "P":"#888888",
        "S1":"#99ff99","S2":"#55ff55","S3":"#22ff22",
        "R4":"#cc0000","S4":"#00cc00",
    }

    for k, v in pivots.items():
        price_lines.append({
            "price": v, "color": PIVOT_COLORS.get(k,"#888888"),
            "lineWidth": 1, "lineStyle": 1,
            "axisLabelVisible": True,
            "title": k,
        })

    if cpr:
        price_lines.append({"price": cpr.get("TC",0),    "color":"#4d7cfe","lineWidth":2,"lineStyle":0,"axisLabelVisible":True,"title":"TC"})
        price_lines.append({"price": cpr.get("Pivot",0), "color":"#8899bb","lineWidth":1,"lineStyle":1,"axisLabelVisible":True,"title":"P"})
        price_lines.append({"price": cpr.get("BC",0),    "color":"#4d7cfe","lineWidth":2,"lineStyle":0,"axisLabelVisible":True,"title":"BC"})

    if mp:
        price_lines.append({"price": mp.get("POC",0), "color":"#f5a623","lineWidth":2,"lineStyle":2,"axisLabelVisible":True,"title":"POC"})
        price_lines.append({"price": mp.get("VAH",0), "color":"#94a3b8","lineWidth":1,"lineStyle":2,"axisLabelVisible":True,"title":"VAH"})
        price_lines.append({"price": mp.get("VAL",0), "color":"#94a3b8","lineWidth":1,"lineStyle":2,"axisLabelVisible":True,"title":"VAL"})

    # ── Serialize to JSON ─────────────────────────────────────────────────
    candles_json     = json.dumps(candles)
    volumes_json     = json.dumps(volumes)
    price_lines_json = json.dumps(price_lines)

    # CPR band fill
    tc_val = cpr.get("TC", 0)
    bc_val = cpr.get("BC", 0)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:#f7f9f2; font-family:'IBM Plex Mono',monospace; }}

.hdr {{
    display:flex; align-items:center; justify-content:space-between;
    padding:8px 14px; background:#ffffff;
    border-bottom:1px solid #dce3ed;
}}
.hdr-left {{ display:flex; align-items:baseline; gap:10px; }}
.sym {{ font-size:1rem; font-weight:700; color:#1a1f0e; }}
.tf  {{ font-size:0.72rem; color:#5a6a48; }}
.ltp {{ font-size:0.9rem; font-weight:600; color:#1a1f0e; }}
.bias {{
    background:{bias_color}18; color:{bias_color};
    border:1px solid {bias_color}44;
    border-radius:4px; padding:3px 10px;
    font-size:0.7rem; font-weight:700; letter-spacing:0.06em;
}}

.legend {{
    display:flex; flex-wrap:wrap; gap:8px;
    padding:5px 14px; background:#ffffff;
    border-bottom:1px solid #eef0f4;
    font-size:0.65rem;
}}
.leg-item {{ display:flex; align-items:center; gap:4px; color:#5a6a48; }}
.leg-dot  {{ width:10px; height:3px; border-radius:2px; flex-shrink:0; }}

#chart {{ width:100%; height:{height}px; }}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-left">
    <span class="sym">{symbol}</span>
    <span class="tf">{tf_label} · {pivot_type}</span>
    <span class="ltp">₹{ltp:,.2f}</span>
  </div>
  <span class="bias">{overall}</span>
</div>

<div class="legend">
  <div class="leg-item"><div class="leg-dot" style="background:#4d7cfe;height:2px;"></div>CPR Band</div>
  <div class="leg-item"><div class="leg-dot" style="background:#ff5555;"></div>Resistance</div>
  <div class="leg-item"><div class="leg-dot" style="background:#55ff55;"></div>Support</div>
  <div class="leg-item"><div class="leg-dot" style="background:#f5a623;"></div>POC</div>
  <div class="leg-item"><div class="leg-dot" style="background:#94a3b8;"></div>Value Area</div>
</div>

<div id="chart"></div>

<script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<script>
const chart = LightweightCharts.createChart(document.getElementById('chart'), {{
    width:  document.getElementById('chart').clientWidth,
    height: {height},
    layout: {{
        background:  {{ type: 'solid', color: '#ffffff' }},
        textColor:   '#5a6a80',
        fontSize:    11,
        fontFamily:  "'IBM Plex Mono', monospace",
    }},
    grid: {{
        vertLines:  {{ color: '#f0f2f5', style: 1 }},
        horzLines:  {{ color: '#f0f2f5', style: 1 }},
    }},
    crosshair: {{
        mode: LightweightCharts.CrosshairMode.Normal,
        vertLine:  {{ color: '#1a6b3c44', width: 1, style: 1, labelBackgroundColor: '#1a6b3c' }},
        horzLine:  {{ color: '#1a6b3c44', width: 1, style: 1, labelBackgroundColor: '#1a6b3c' }},
    }},
    rightPriceScale: {{
        borderColor: '#dce3ed',
        scaleMargins: {{ top: 0.05, bottom: 0.25 }},
    }},
    timeScale: {{
        borderColor:     '#dce3ed',
        timeVisible:     true,
        secondsVisible:  false,
        barSpacing:      8,
    }},
    localization: {{
        priceFormatter: p => '\u20b9' + p.toFixed(2),
    }},
}});

// ── Candlestick series ─────────────────────────────────────────────
const candleSeries = chart.addCandlestickSeries({{
    upColor:           '#16a34a',
    downColor:         '#dc2626',
    borderUpColor:     '#16a34a',
    borderDownColor:   '#dc2626',
    wickUpColor:       '#16a34a',
    wickDownColor:     '#dc2626',
}});
candleSeries.setData({candles_json});

// ── Add pivot + CPR price lines ────────────────────────────────────
const priceLines = {price_lines_json};
priceLines.forEach(function(pl) {{
    if (pl.price && pl.price > 0) {{
        candleSeries.createPriceLine(pl);
    }}
}});

// ── CPR band shading ───────────────────────────────────────────────
// Draw as a band between TC and BC using two area series
const tcVal = {tc_val};
const bcVal = {bc_val};
if (tcVal > 0 && bcVal > 0) {{
    const cprBandUpper = chart.addLineSeries({{
        color: 'rgba(77,124,254,0.5)',
        lineWidth: 2,
        lineStyle: 0,
        priceLineVisible: false,
        lastValueVisible: false,
    }});
    const cprBandLower = chart.addLineSeries({{
        color: 'rgba(77,124,254,0.5)',
        lineWidth: 2,
        lineStyle: 0,
        priceLineVisible: false,
        lastValueVisible: false,
    }});
    const allTimes = {candles_json}.map(c => c.time);
    cprBandUpper.setData(allTimes.map(t => ({{ time: t, value: tcVal }})));
    cprBandLower.setData(allTimes.map(t => ({{ time: t, value: bcVal }})));
}}

// ── Volume series (bottom pane) ────────────────────────────────────
const volSeries = chart.addHistogramSeries({{
    priceFormat:      {{ type: 'volume' }},
    priceScaleId:     'volume',
    scaleMargins:     {{ top: 0.8, bottom: 0 }},
}});
volSeries.priceScale().applyOptions({{
    scaleMargins: {{ top: 0.8, bottom: 0 }},
}});
volSeries.setData({volumes_json});

// ── Responsive resize ──────────────────────────────────────────────
new ResizeObserver(entries => {{
    for (const entry of entries) {{
        const {{ width, height }} = entry.contentRect;
        chart.applyOptions({{ width, height }});
    }}
}}).observe(document.getElementById('chart'));

// Fit content on load
chart.timeScale().fitContent();

// ── Crosshair tooltip ─────────────────────────────────────────────
const tooltip = document.createElement('div');
tooltip.style.cssText = 'position:absolute;top:50px;left:14px;z-index:99;background:#1e293b;color:#e2e8f0;padding:6px 10px;border-radius:6px;font-size:0.7rem;pointer-events:none;line-height:1.6;display:none;';
document.body.appendChild(tooltip);

chart.subscribeCrosshairMove(param => {{
    if (!param.time || !param.seriesData.has(candleSeries)) {{
        tooltip.style.display = 'none';
        return;
    }}
    const d = param.seriesData.get(candleSeries);
    const chg = d.close - d.open;
    const pct  = ((chg / d.open) * 100).toFixed(2);
    const col  = chg >= 0 ? '#16a34a' : '#dc2626';
    tooltip.style.display = 'block';
    tooltip.innerHTML =
        '<span style="color:#8a9a78;">O</span> ₹' + d.open.toLocaleString('en-IN', {{minimumFractionDigits:2}}) + ' &nbsp;' +
        '<span style="color:#8a9a78;">H</span> ₹' + d.high.toLocaleString('en-IN', {{minimumFractionDigits:2}}) + ' &nbsp;' +
        '<span style="color:#8a9a78;">L</span> ₹' + d.low.toLocaleString('en-IN', {{minimumFractionDigits:2}}) + ' &nbsp;' +
        '<span style="color:#8a9a78;">C</span> <b style="color:' + col + ';">₹' + d.close.toLocaleString('en-IN', {{minimumFractionDigits:2}}) + '</b>' +
        ' <span style="color:' + col + ';">(' + (chg >= 0 ? '+' : '') + pct + '%)</span>';
}});
</script>
</body>
</html>"""

    _stc.html(html, height=height + 75, scrolling=False)


def sig_badge(label: str, kind: str) -> str:
    css = {"bull": "sig-bull", "bear": "sig-bear", "neut": "sig-neut"}.get(kind, "sig-neut")
    return f'<span class="signal-badge {css}">{label}</span>'


def is_market_open() -> bool:
    """Check if NSE market is currently open (Mon-Fri 9:15-15:30 IST)."""
    from datetime import timezone
    IST   = timezone(timedelta(hours=5, minutes=30))
    now   = datetime.now(IST)
    if now.weekday() >= 5:          # Saturday / Sunday
        return False
    market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


def render_market_header():
    from datetime import timezone
    IST    = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)
    open_  = is_market_open()

    # Market status pill + refresh button
    status_col, refresh_col = st.columns([6, 1])
    with status_col:
        dot_color = "#16a34a" if open_ else "#dc2626"
        status    = "LIVE · NSE Open" if open_ else "Market Closed"
        next_info = ""
        if not open_:
            if now_ist.weekday() >= 5:
                next_info = " · Opens Monday 9:15 AM IST"
            elif now_ist.hour < 9 or (now_ist.hour == 9 and now_ist.minute < 15):
                next_info = f" · Opens at 9:15 AM IST"
            else:
                next_info = " · Opens tomorrow 9:15 AM IST"

        st.markdown(
            f"<div style='display:flex;align-items:center;gap:8px;padding:0.3rem 0;"
            f"font-family:IBM Plex Mono,monospace;font-size:0.72rem;'>"
            f"<span style='width:8px;height:8px;border-radius:50%;background:{dot_color};"
            f"display:inline-block;{'animation:pulse 1.5s infinite;' if open_ else ''}'></span>"
            f"<span style='color:{dot_color};font-weight:600;'>{status}</span>"
            f"<span style='color:#8a9a78;'>{next_info}</span>"
            f"<span style='color:#8a9a78;margin-left:auto;'>"
            f"{now_ist.strftime('%d %b %Y  %H:%M:%S IST')}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with refresh_col:
        if st.button("🔄 Refresh", use_container_width=True, key="global_refresh"):
            # Clear all data caches so everything reloads fresh
            st.cache_data.clear()
            st.rerun()

    # Auto-refresh using streamlit-autorefresh (reliable on Streamlit Cloud)
    if open_ and _HAS_AUTOREFRESH:
        # 30s during market hours
        st_autorefresh(interval=30_000, limit=None, key="mkt_autorefresh")
    elif open_ and not _HAS_AUTOREFRESH:
        st.caption("💡 Install streamlit-autorefresh for live auto-refresh")

    # Index metrics
    indices = {"NIFTY 50": "^NSEI", "SENSEX": "^BSESN", "NIFTY BANK": "^NSEBANK"}
    cols = st.columns(len(indices))
    for col, (name, ticker) in zip(cols, indices.items()):
        d = fetch_index_data(ticker)
        ltp, chg = d.get("ltp"), d.get("change")
        if ltp is not None:
            hi  = d.get("high")
            lo  = d.get("low")
            sub = f"H:{hi:,.0f}  L:{lo:,.0f}" if hi and lo else ""
            col.metric(
                name,
                f"{ltp:,.2f}",
                f"{'+' if chg and chg >= 0 else ''}{chg}%" if chg is not None else "—",
            )
            if sub:
                col.caption(sub)
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
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


@st.cache_data(ttl=180)
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
        paper_bgcolor="#f0f4f8",
        margin=dict(l=0, r=0, t=0, b=0),
        height=480,
        font=dict(family="IBM Plex Mono", color="#d4daf0"),
    )
    return fig


# ─────────────────────────────────────────────
#  PAGES
# ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
#  USER CREDENTIALS — 5 demo accounts
# ══════════════════════════════════════════════════════════════

USERS = {
    "admin@pivotvault.ai":   {"pin": "1234", "name": "Admin User",    "phone": "9876543210"},
    "trader@pivotvault.ai":  {"pin": "2345", "name": "Rahul Sharma",  "phone": "9123456780"},
    "analyst@pivotvault.ai": {"pin": "3456", "name": "Priya Patel",   "phone": "9234567891"},
    "demo@pivotvault.ai":    {"pin": "4567", "name": "Demo Account",  "phone": "9345678902"},
    "test@pivotvault.ai":    {"pin": "5678", "name": "Test User",     "phone": "9456789013"},
}
_PHONE_MAP = {v["phone"]: k for k, v in USERS.items()}

def generate_otp() -> str:
    return str(secrets.randbelow(900000) + 100000)

def verify_login(identifier: str, pin: str) -> tuple:
    idf = identifier.strip().lower()
    if idf in _PHONE_MAP:
        idf = _PHONE_MAP[idf]
    u = USERS.get(idf)
    if u and u["pin"] == pin.strip():
        return True, {"id": idf, "name": u["name"], "email": idf, "phone": u["phone"]}
    return False, {}

def get_user_by_email(email: str) -> dict:
    u = USERS.get(email.lower().strip())
    if u:
        return {"id": email, "name": u["name"], "email": email, "phone": u["phone"]}
    return {}

# Stub functions — keep API compatible so rest of code doesn't break
def create_user(name, email, phone, pin, google_id=None):
    return False, "Sign-up disabled in demo mode. Use one of the 5 accounts below."

def reset_pin(email, new_pin):
    return False, "PIN reset disabled in demo mode."

def db_watchlist_get(user_id):   return []
def db_watchlist_add(user_id, symbol): return False
def db_watchlist_remove(user_id, symbol): return False
def db_save_signals(user_id, signals): pass


def page_login():
    """Login page with 5 demo credentials."""

    st.markdown("""
    <div style="text-align:center;padding:2rem 1rem 1.5rem;">
        <div style="font-size:2.8rem;margin-bottom:0.4rem;">🏦</div>
        <div style="font-family:'DM Sans',sans-serif;font-size:2rem;font-weight:800;
                    color:#1a1f0e;letter-spacing:-0.03em;">
            PivotVault <span style="color:#4e6130;">AI</span>
        </div>
        <div style="font-family:'DM Mono',monospace;font-size:0.7rem;color:#8a9a78;
                    letter-spacing:0.12em;text-transform:uppercase;margin-top:4px;">
            Indian Equity Intelligence · Pivot Boss Methodology
        </div>
    </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:

        tab_login, tab_accounts = st.tabs(["🔐 Sign In", "👥 Accounts"])

        # ── Sign In ───────────────────────────────────────────────
        with tab_login:
            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            method = st.radio("Method", ["🔢 PIN", "📱 OTP"],
                              horizontal=True, label_visibility="collapsed",
                              key="login_method")

            if "PIN" in method:
                identifier = st.text_input("Email or Phone",
                    placeholder="email  or  phone number", key="login_id")
                pin = st.text_input("4-Digit PIN", type="password",
                    max_chars=4, placeholder="••••", key="login_pin")

                if st.button("🔓 Sign In", use_container_width=True, key="btn_login"):
                    if not identifier or not pin:
                        st.error("Enter email/phone and PIN.")
                    else:
                        ok, user = verify_login(identifier, pin)
                        if ok:
                            st.session_state["logged_in"]  = True
                            st.session_state["username"]   = user["name"]
                            st.session_state["user_id"]    = user["email"]
                            st.session_state["user_email"] = user["email"]
                            st.session_state["user_phone"] = user.get("phone","")
                            st.rerun()
                        else:
                            st.error("❌ Wrong email/phone or PIN. See Accounts tab.")

            else:
                # OTP flow
                if not st.session_state.get("otp_code"):
                    otp_id = st.text_input("Email or Phone",
                        placeholder="email  or  phone number", key="otp_input")
                    if st.button("📨 Send OTP", use_container_width=True, key="btn_otp"):
                        idf = otp_id.strip().lower()
                        if idf in _PHONE_MAP:
                            idf = _PHONE_MAP[idf]
                        if idf in USERS:
                            otp = generate_otp()
                            st.session_state["otp_code"]   = otp
                            st.session_state["otp_target"] = otp_id.strip()
                            st.session_state["otp_email"]  = idf
                            st.success(f"OTP generated: **{otp}**  (demo — shown here)")
                            st.rerun()
                        else:
                            st.error("Email/phone not found. See Accounts tab.")
                else:
                    st.info(f"OTP sent to: {st.session_state.get('otp_target','')}")
                    entered = st.text_input("Enter 6-digit OTP",
                        max_chars=6, placeholder="______", key="otp_entered")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("✅ Verify", use_container_width=True, key="btn_verify"):
                            if entered.strip() == st.session_state.get("otp_code",""):
                                em   = st.session_state["otp_email"]
                                user = get_user_by_email(em)
                                st.session_state.update({
                                    "logged_in": True, "username": user["name"],
                                    "user_id": em, "user_email": em,
                                    "user_phone": user.get("phone",""),
                                    "otp_code": "",
                                })
                                st.rerun()
                            else:
                                st.error("Wrong OTP.")
                    with c2:
                        if st.button("🔄 Resend", use_container_width=True, key="btn_resend"):
                            st.session_state["otp_code"] = ""
                            st.rerun()

        # ── Accounts tab ──────────────────────────────────────────
        with tab_accounts:
            st.markdown(
                "<div style='font-family:DM Mono,monospace;font-size:0.75rem;"
                "color:#5a6a48;margin:0.5rem 0 1rem;'>"
                "Use any of these accounts to sign in:</div>",
                unsafe_allow_html=True,
            )
            for email, u in USERS.items():
                if st.button(
                    f"👤 {u['name']}",
                    key=f"quick_{email}",
                    use_container_width=True,
                ):
                    st.session_state.update({
                        "logged_in": True,
                        "username":  u["name"],
                        "user_id":   email,
                        "user_email": email,
                        "user_phone": u["phone"],
                    })
                    st.rerun()
                st.markdown(
                    f"<div style='font-family:DM Mono,monospace;font-size:0.72rem;"
                    f"color:#5a6a48;margin:-0.4rem 0 0.5rem;padding:0.5rem 0.75rem;"
                    f"background:#f7f9f2;border:1px solid #dae0cb;border-radius:6px;'>"
                    f"📧 {email}  &nbsp;&nbsp;"
                    f"<span style='background:#4e6130;color:#f4f7ec;border-radius:4px;"
                    f"padding:1px 7px;font-weight:700;'>PIN: {u['pin']}</span>"
                    f"&nbsp;&nbsp; 📱 {u['phone']}"
                    f"</div>",
                    unsafe_allow_html=True,
                )


def page_market_snapshot(nse500: pd.DataFrame):
    st.markdown(
        '<div class="title-bar"><span class="live-dot"></span><h1 style="color:#1a1f0e;">Market Snapshot</h1>'
        f'<span class="ts" style="color:#5a6a48;">{datetime.now().strftime("%d %b %Y  %H:%M")}</span></div>',
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
            "letter-spacing:0.08em;text-transform:uppercase;color:#5a6a48;"
            "margin-bottom:0.4rem;'>"
            "<span class='live-dot'></span>"
            "Sectoral Heatmap · Nifty 500 · Colour = Avg 1-Day % Change · Click a sector for detail</div>",
            unsafe_allow_html=True,
        )
    with legend_col:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
            "color:#5a6a48;padding-top:0.1rem;line-height:1.9;'>"
            "<span style='color:#2d7a3a;'>■</span> Strong Gain<br>"
            "<span style='color:#27ae60;'>■</span> Gain<br>"
            "<span style='color:#e2e8f0;border:1px solid #333;'>■</span> Flat<br>"
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
                    f"border-bottom:1px solid #dce3ed;margin-bottom:0.6rem;'>"
                    f"<span style='font-size:1rem;font-weight:700;color:#1a1f0e;'>"
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
                    f"letter-spacing:0.08em;text-transform:uppercase;color:#2d7a3a;"
                    f"margin-bottom:0.35rem;'>▲ Top 5 Gainers</div>",
                    unsafe_allow_html=True,
                )
                g_rows = [{"Symbol": r["Symbol"], "Change %": f"+{r['Change%']:.2f}%"}
                          for _, r in top_g.iterrows()]
                st.dataframe(pd.DataFrame(g_rows), use_container_width=True, hide_index=True)

            with d2:
                st.markdown(
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    f"letter-spacing:0.08em;text-transform:uppercase;color:#c0392b;"
                    f"margin-bottom:0.35rem;'>▼ Top 5 Losers</div>",
                    unsafe_allow_html=True,
                )
                l_rows = [{"Symbol": r["Symbol"], "Change %": f"{r['Change%']:.2f}%"}
                          for _, r in top_l.iterrows()]
                st.dataframe(pd.DataFrame(l_rows), use_container_width=True, hide_index=True)

    else:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
            "color:#8a9a78;text-align:center;padding:0.55rem;"
            "border:1px dashed #e2e8f0;border-radius:6px;margin-top:0.25rem;'>"
            "👆  Click any sector tile to see its Top 5 Gainers &amp; Losers</div>",
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────
#  NARROW CPR SCANNER
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

def compute_trade_levels(symbol: str, ltp: float, tc: float, bc: float,
                         pivot: float, pattern: str) -> dict:
    """Compute trade targets and SL from pivot levels and ATR."""
    try:
        df = yf.Ticker(symbol + ".NS").history(period="60d", interval="1d")
        df.index = df.index.tz_localize(None)
        if df.empty or len(df) < 15:
            return {}
        close, high, low = df["Close"], df["High"], df["Low"]
        tr  = pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        wk52h = float(high.tail(252).max()) if len(high)>=252 else float(high.max())
        wk52l = float(low.tail(252).min())  if len(low)>=252  else float(low.min())
        ref = df.iloc[-2]
        H2,L2,C2 = float(ref["High"]),float(ref["Low"]),float(ref["Close"])
        P  = (H2+L2+C2)/3
        R1 = 2*P-L2; R2 = P+(H2-L2); R3 = H2+2*(P-L2)
        S1 = 2*P-H2; S2 = P-(H2-L2); S3 = L2-2*(H2-P)
        if pattern == "Bullish":
            sh = {"entry":round(ltp,2),"target":round(min(R1,ltp+atr*1.5),2),"sl":round(max(bc,ltp-atr*0.8),2)}
            sh["rr"] = round((sh["target"]-sh["entry"])/max(sh["entry"]-sh["sl"],0.01),2)
            md = {"entry":round(ltp,2),"target1":round(R1,2),"target2":round(R2,2),"sl":round(S1,2)}
            md["rr"] = round((md["target2"]-md["entry"])/max(md["entry"]-md["sl"],0.01),2)
            lg = {"entry":round(ltp,2),"target1":round(R2,2),"target2":round(R3,2),"target3":round(min(wk52h,R3+atr*5),2),"sl":round(S2,2)}
            lg["rr"] = round((lg["target2"]-lg["entry"])/max(lg["entry"]-lg["sl"],0.01),2)
        else:
            sh = {"entry":round(ltp,2),"target":round(max(S1,ltp-atr*1.5),2),"sl":round(min(tc,ltp+atr*0.8),2)}
            sh["rr"] = round((sh["entry"]-sh["target"])/max(sh["sl"]-sh["entry"],0.01),2)
            md = {"entry":round(ltp,2),"target1":round(S1,2),"target2":round(S2,2),"sl":round(R1,2)}
            md["rr"] = round((md["entry"]-md["target2"])/max(md["sl"]-md["entry"],0.01),2)
            lg = {"entry":round(ltp,2),"target1":round(S2,2),"target2":round(S3,2),"target3":round(max(wk52l,S3-atr*5),2),"sl":round(R2,2)}
            lg["rr"] = round((lg["entry"]-lg["target2"])/max(lg["sl"]-lg["entry"],0.01),2)
        return {"symbol":symbol,"ltp":ltp,"pattern":pattern,"pivot":round(P,2),"tc":round(tc,2),"bc":round(bc,2),
                "atr":round(atr,2),"R1":round(R1,2),"R2":round(R2,2),"R3":round(R3,2),
                "S1":round(S1,2),"S2":round(S2,2),"S3":round(S3,2),
                "52wH":round(wk52h,2),"52wL":round(wk52l,2),"short":sh,"medium":md,"long":lg}
    except Exception:
        return {}


@st.cache_data(ttl=60)
def fetch_stock_history(symbol: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    # ── Try Upstox first (free, accurate, NSE data) ───────────────────────
    if _upstox_connected():
        try:
            # Map period to from_date
            period_days = {
                "5d":60,"10d":10,"15d":15,"20d":20,"30d":30,
                "60d":60,"90d":90,"1y":365,"2y":730,"5y":1825,"10y":3650,
            }
            days = period_days.get(period, 365)
            from_dt = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            to_dt   = datetime.now().strftime("%Y-%m-%d")
            df = upstox_get_historical(symbol, interval, from_dt, to_dt)
            if not df.empty:
                return df
        except Exception:
            pass
    # ── Fallback: yfinance ────────────────────────────────────────────────
    try:
        df = yf.Ticker(symbol + ".NS").history(period=period, interval=interval)
        if not df.empty:
            df.index = df.index.tz_localize(None)
            return df
    except Exception:
        pass
    return pd.DataFrame()


def send_report_email(to_email: str, smtp_host: str, smtp_port: int,
                      sender_email: str, sender_password: str,
                      html_body: str, scan_date: str) -> tuple:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"PivotVault AI — CPR Report {scan_date}"
        msg["From"]    = sender_email
        msg["To"]      = to_email
        msg.attach(MIMEText(html_body, "html"))
        ctx = ssl.create_default_context()
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx) as s:
                s.login(sender_email, sender_password)
                s.sendmail(sender_email, to_email, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as s:
                s.ehlo(); s.starttls(context=ctx); s.login(sender_email, sender_password)
                s.sendmail(sender_email, to_email, msg.as_string())
        return True, "Sent!"
    except Exception as e:
        return False, str(e)


def page_pivot_boss(nse500: pd.DataFrame):
    """★  Full Frank Ochoa / Pivot Boss analysis page."""
    _n200 = fetch_nifty200_list()
    st.markdown(
        '<div class="title-bar"><span class="live-dot"></span>'
        '<h1>Pivot Boss Analysis</h1>'
        '<span style="font-family:IBM Plex Mono,monospace;font-size:0.68rem;'
        'color:#5a6a48;margin-left:0.5rem;">Frank Ochoa Methodology</span>'
        f'<span class="ts" style="color:#5a6a48;">{datetime.now().strftime("%d %b %Y  %H:%M")}</span></div>',
        unsafe_allow_html=True,
    )

    symbols = sorted(_n200)

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
        "bull": ("#edf7ee", "#00e5a0"),
        "bear": ("#fdf0ee", "#ff4d6a"),
        "neut": ("#fdf9ec", "#f5a623"),
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
    chart_tab1, chart_tab2 = st.tabs(["📊 LW Chart + Pivots", "📈 Multi-Panel (Oscillators)"])
    with chart_tab1:
        render_lw_chart(symbol, tf_label, analysis, pivot_type, height=650)
    with chart_tab2:
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
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#5a6a48;"
            f"margin-top:0.4rem;'>{cpr_detail}</div></div>",
            unsafe_allow_html=True,
        )

    with cb:
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>3/10 Oscillator</div>"
            f"<div class='pb-card-value pb-{analysis['osc_col']}'>{analysis['osc_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#5a6a48;"
            f"margin-top:0.4rem;'>Ochoa's momentum gauge<br>3-MA minus 10-MA vs 16-Signal</div>"
            f"</div>", unsafe_allow_html=True,
        )

    with cc:
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>HMA(20) Trend</div>"
            f"<div class='pb-card-value pb-{analysis['hma_col']}'>{analysis['hma_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#5a6a48;"
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
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#5a6a48;"
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
            "letter-spacing:0.1em;text-transform:uppercase;color:#5a6a48;"
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
                        f"padding:0.3rem 0;border-bottom:1px solid #dce3ed;'>"
                        f"<span style='color:{col};'>{label}</span>"
                        f"<b style='color:#1a1f0e;float:right;'>{val} "
                        f"<span style='color:#5a6a48;font-size:0.7rem;'>{arr}{abs(dist)}%</span>"
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
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#5a6a48;margin-top:0.4rem;'>"
            f"{'% of price: ' + str(analysis['atr_pct']) + '%' if analysis['atr_pct'] else ''}"
            f"</div></div>", unsafe_allow_html=True,
        )

    with v2:
        nl = analysis.get("nearest")
        st.markdown(
            f"<div class='pb-card'><div class='pb-card-title'>Nearest Pivot ★</div>"
            f"<div class='pb-card-value pb-neut'>"
            f"{nl[0] + '  ₹' + str(nl[1]) if nl else '—'}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#5a6a48;margin-top:0.4rem;'>"
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
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;color:#5a6a48;"
            "margin-bottom:0.5rem;'>Untouched CPR bands — Ochoa's high-significance price magnets</div>",
            unsafe_allow_html=True,
        )
        st.dataframe(pd.DataFrame(virgins)[["Date", "TC", "BC"]],
                     use_container_width=True, hide_index=True)

    # ── Stochastic ────────────────────────────────────────────────────────────
    # Stochastic is embedded in TV chart above; show Plotly fallback only
    df_ind = analysis.get("df_ind")
    if not _TV_CHARTS and df_ind is not None and "STOCH_K" in df_ind.columns:
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
        'color:#5a6a48;margin-left:0.75rem;">'
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
                    f"<div style='background:#edf7ee;border:1px solid {col_fg}33;"
                    f"border-left:4px solid {col_fg};border-radius:8px;"
                    f"padding:1rem 1.5rem;margin:0.5rem 0;'>"

                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;"
                    f"letter-spacing:0.12em;text-transform:uppercase;color:{col_fg}88;'>"
                    f"{arrow} {auto_pat} Trade Plan  ·  {symbol}</div>"

                    f"<div style='display:flex;gap:2rem;margin-top:0.6rem;flex-wrap:wrap;'>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#5a6a48;text-transform:uppercase;'>Short Term (1-3d)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#1a1f0e;'>T: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{sh.get('target',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#c0392b;'>₹{sh.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{sh.get('rr',0)}x</b></div></div>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#5a6a48;text-transform:uppercase;'>Medium Term (1-4w)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#1a1f0e;'>T1: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{md.get('target1',0):,.2f}</span>"
                    f" T2: <span style='color:{col_fg};font-weight:700;'>₹{md.get('target2',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#c0392b;'>₹{md.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{md.get('rr',0)}x</b></div></div>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#5a6a48;text-transform:uppercase;'>Long Term (1-3m)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#1a1f0e;'>T1: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{lg.get('target1',0):,.2f}</span>"
                    f" T2: <span style='color:{col_fg};font-weight:700;'>₹{lg.get('target2',0):,.2f}</span>"
                    f" T3: <span style='color:{col_fg};font-weight:700;'>₹{lg.get('target3',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#c0392b;'>₹{lg.get('sl',0):,.2f}</span>"
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

    # ── Methodology footnote ──────────────────────────────────────────────────
    st.divider()
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#8a9a78;line-height:1.8;'>"
        "📖  Based on <i>Secrets of a Pivot Boss</i> by Frank Ochoa.  "
        "Tools implemented: CPR · 3/10 Oscillator · Virgin CPRs · Market Profile (POC/VAH/VAL) · "
        "HMA Trend Filter · ATR · RSI · Stochastic.  "
        "For educational purposes only — not financial advice.</div>",
        unsafe_allow_html=True,
    )


def page_watchlist():
    st.markdown("""
    <div class="title-bar">
        <span style="font-size:1.5rem;">⭐</span>
        <h1 style="color:#1a1f0e;">Watchlist</h1>
    </div>
    """, unsafe_allow_html=True)

    nifty200 = sorted(fetch_nifty200_list())
    wl       = st.session_state.get("watchlist", [])

    # ── Always-visible stock selector ────────────────────────────────────
    st.markdown("<div style='font-family:DM Mono,monospace;font-size:0.72rem;color:#5a6a48;margin-bottom:4px;'>Select stocks from Nifty 200</div>", unsafe_allow_html=True)
    selected = st.multiselect(
        "wl_stocks",
        options=nifty200,
        default=[s for s in wl if s in nifty200],
        placeholder="Search — RELIANCE, TCS, INFY…",
        label_visibility="collapsed",
        key="wl_multiselect",
    )
    ac1, ac2, ac3, ac4 = st.columns([2, 1.5, 1.5, 1.5])
    with ac1:
        if st.button("Save Watchlist", use_container_width=True, key="wl_save"):
            st.session_state["watchlist"] = list(selected)
            st.session_state["wl_data"]   = {}
            st.rerun()
    with ac2:
        if st.button("Refresh Prices", use_container_width=True, key="wl_refresh"):
            with st.spinner("Fetching…"):
                st.session_state["wl_data"]         = refresh_watchlist_prices(selected)
                st.session_state["wl_last_refresh"] = datetime.now()
            st.rerun()
    with ac3:
        if st.button("Clear All", use_container_width=True, key="wl_clear"):
            st.session_state["watchlist"] = []
            st.session_state["wl_data"]   = {}
            st.rerun()
    with ac4:
        last = st.session_state.get("wl_last_refresh")
        if last:
            st.caption(f"Updated {last.strftime('%H:%M:%S')}")

    # Reload wl after possible save
    wl = st.session_state.get("watchlist", [])

    if not wl:
        st.markdown(
            "<div style='text-align:center;padding:3rem 1rem;margin-top:1rem;"
            "background:#f7f9f2;border:2px dashed #dae0cb;border-radius:12px;"
            "font-family:DM Mono,monospace;'>"
            "<div style='font-size:2rem;'>⭐</div>"
            "<div style='font-size:0.95rem;font-weight:700;color:#1a1f0e;margin-top:0.5rem;'>Watchlist is empty</div>"
            "<div style='font-size:0.8rem;color:#5a6a48;margin-top:0.4rem;'>"
            "Select stocks above and click Save Watchlist</div></div>",
            unsafe_allow_html=True,
        )
        return

    # ── Build active signals map (15m + 1h only) ─────────────────────────
    active_signals = {}
    for tf_key, tf_label, tf_col in [("cpr_scan_15m","15Min","#e67e22"),("cpr_scan_1h","1Hour","#2980b9")]:
        raw = st.session_state.get(tf_key)
        df  = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame()
        if df.empty: continue
        for _, r in df.iterrows():
            sym = r["Symbol"]
            if sym not in active_signals: active_signals[sym] = []
            active_signals[sym].append({
                "tf": tf_label, "tf_col": tf_col,
                "side": "BUY" if r["Pattern"]=="Bullish" else "SELL",
                "entry": r["Entry"], "t1": r["T1"], "t2": r.get("T2",r["T1"]),
                "sl": r["SL"], "rr1": r["RR1"], "rr": r["RR1"],
                "strength": int(r["Strength%"]), "candle": r.get("Candle","—"),
            })

    # Auto-fetch prices
    if not st.session_state.get("wl_data") and wl:
        with st.spinner("Fetching live prices…"):
            st.session_state["wl_data"]         = refresh_watchlist_prices(wl)
            st.session_state["wl_last_refresh"] = datetime.now()
    data = st.session_state.get("wl_data", {})

    live_count = sum(1 for s in wl if s in active_signals)

    st.divider()

    # Summary metrics
    m1, m2, m3 = st.columns(3)
    m1.metric("Watchlist",    f"{len(wl)} stocks")
    m2.metric("Live Signals", f"{live_count} stocks")
    m3.metric("Watching",     f"{len(wl)-live_count} stocks")

    # Signal alert banner
    if live_count:
        st.markdown(
            f"<div style='background:#edf7ee;border:1px solid #b8dfc0;"
            f"border-left:4px solid #2d7a3a;border-radius:8px;"
            f"padding:0.6rem 1rem;margin:0.5rem 0;font-family:DM Mono,monospace;"
            f"font-size:0.78rem;display:flex;align-items:center;gap:10px;'>"
            f"<span class='live-dot'></span>"
            f"<b style='color:#2d7a3a;'>{live_count} stock(s) have LIVE signals now!</b></div>",
            unsafe_allow_html=True,
        )
    else:
        st.info("No watchlist stocks have active signals. Run CPR Scanner (15Min/1Hour) to generate signals.")
        if st.button("Go to CPR Scanner", key="wl_go_scanner"):
            st.session_state["current_page"] = "CPR Scanner"
            st.rerun()

    st.divider()

    # ── Stocks WITH live signals ──────────────────────────────────────────
    wl_live  = [s for s in wl if s in active_signals]
    wl_quiet = [s for s in wl if s not in active_signals]

    if wl_live:
        st.markdown(
            "<div style='font-family:DM Mono,monospace;font-size:0.72rem;"
            "color:#2d7a3a;font-weight:700;letter-spacing:0.08em;"
            "text-transform:uppercase;margin-bottom:0.75rem;'>"
            "🔔 LIVE SIGNALS</div>",
            unsafe_allow_html=True,
        )
        for sym in wl_live:
            d       = data.get(sym, {})
            ltp     = d.get("ltp")
            chg     = d.get("change")
            sigs    = active_signals[sym]
            ltp_str = f"Rs.{ltp:,.2f}" if ltp else "—"
            chg_col = "#2d7a3a" if chg and chg >= 0 else "#c0392b"
            chg_str = f"{'+' if chg and chg>=0 else ''}{chg:.2f}%" if chg is not None else "—"

            # Signal detail per timeframe
            sig_info = ""
            for sg in sigs:
                bull  = sg["side"]=="BUY"
                ac    = "#2d7a3a" if bull else "#c0392b"
                tc    = sg["tf_col"]
                rc    = "#2d7a3a" if sg["rr1"]>=2 else ("#b8860b" if sg["rr1"]>=1.5 else "#c0392b")
                sig_info += (
                    f"<div style='display:flex;flex-wrap:wrap;gap:5px;margin-top:6px;'>"
                    f"<span style='background:{tc}18;color:{tc};border:1px solid {tc}44;"
                    f"border-radius:12px;padding:1px 8px;font-size:0.65rem;font-weight:700;'>{sg['tf']}</span>"
                    f"<span style='background:{'#edf7ee' if bull else '#fdf0ee'};"
                    f"color:{ac};border:1px solid {'#b8dfc0' if bull else '#f0c0b8'};"
                    f"border-radius:20px;padding:1px 8px;font-size:0.65rem;font-weight:700;'>"
                    f"{'▲ BUY' if bull else '▼ SELL'}</span>"
                    f"<span style='font-size:0.67rem;color:#5a6a48;background:#f7f9f2;"
                    f"border:1px solid #dae0cb;border-radius:5px;padding:2px 7px;'>"
                    f"Entry {sg['entry']}</span>"
                    f"<span style='font-size:0.67rem;color:#2d7a3a;background:#edf7ee;"
                    f"border:1px solid #b8dfc0;border-radius:5px;padding:2px 7px;'>T1 {sg['t1']}</span>"
                    f"<span style='font-size:0.67rem;color:#c0392b;background:#fdf0ee;"
                    f"border:1px solid #f0c0b8;border-radius:5px;padding:2px 7px;'>SL {sg['sl']}</span>"
                    f"<span style='font-size:0.67rem;color:{rc};background:{rc}12;"
                    f"border:1px solid {rc}33;border-radius:5px;padding:2px 7px;font-weight:700;'>"
                    f"R:R {sg['rr1']}x</span>"
                    f"<span style='font-size:0.67rem;color:#5a6a48;background:#f7f9f2;"
                    f"border:1px solid #dae0cb;border-radius:5px;padding:2px 7px;'>"
                    f"{sg['strength']}% {sg['candle']}</span>"
                    f"</div>"
                )

            st.markdown(
                f"<div style='background:#ffffff;border:1px solid #b8dfc0;"
                f"border-left:4px solid #2d7a3a;border-radius:10px;"
                f"padding:0.85rem 1rem;margin-bottom:0.4rem;"
                f"box-shadow:0 2px 8px rgba(45,122,58,0.08);'>"
                f"<div style='display:flex;align-items:center;flex-wrap:wrap;"
                f"gap:8px;font-family:DM Mono,monospace;'>"
                f"<span style='font-size:1rem;font-weight:900;color:#1a1f0e;'>{sym}</span>"
                f"<span style='font-size:0.92rem;font-weight:700;color:#1a1f0e;'>{ltp_str}</span>"
                f"<span style='font-size:0.82rem;font-weight:600;color:{chg_col};'>{chg_str}</span>"
                f"</div>{sig_info}</div>",
                unsafe_allow_html=True,
            )
            for sg in sigs:
                _trade_buttons({**sg, "symbol": sym,
                                "t2": sg.get("t2", sg["t1"]),
                                "t3": sg.get("t1"), "rr1": sg["rr1"]})

    # ── Stocks WITHOUT signals ─────────────────────────────────────────────
    if wl_quiet:
        if wl_live: st.divider()
        st.markdown(
            f"<div style='font-family:DM Mono,monospace;font-size:0.72rem;"
            f"color:#8a9a78;letter-spacing:0.08em;text-transform:uppercase;"
            f"margin-bottom:0.6rem;'>Watching — No Signal ({len(wl_quiet)})</div>",
            unsafe_allow_html=True,
        )
        cols = st.columns(2)
        for i, sym in enumerate(wl_quiet):
            d       = data.get(sym, {})
            ltp     = d.get("ltp")
            chg     = d.get("change")
            ltp_str = f"Rs.{ltp:,.2f}" if ltp else "—"
            chg_col = "#2d7a3a" if chg and chg >= 0 else "#c0392b"
            chg_str = f"{'+' if chg and chg>=0 else ''}{chg:.2f}%" if chg is not None else "—"
            with cols[i % 2]:
                st.markdown(
                    f"<div style='background:#ffffff;border:1px solid #dae0cb;"
                    f"border-left:3px solid #dae0cb;border-radius:8px;"
                    f"padding:0.6rem 0.9rem;margin-bottom:0.4rem;"
                    f"font-family:DM Mono,monospace;"
                    f"display:flex;align-items:center;justify-content:space-between;'>"
                    f"<b style='font-size:0.9rem;color:#1a1f0e;'>{sym}</b>"
                    f"<span style='font-size:0.85rem;font-weight:700;color:#1a1f0e;'>{ltp_str}</span>"
                    f"<span style='font-size:0.78rem;font-weight:600;color:{chg_col};'>{chg_str}</span>"
                    f"<span style='font-size:0.6rem;color:#8a9a78;'>watching</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    st.divider()
    with st.expander("Remove stocks from watchlist", expanded=False):
        if wl:
            rcols = st.columns(min(len(wl), 5))
            for i, sym in enumerate(wl):
                with rcols[i % 5]:
                    if st.button(f"x {sym}", key=f"rm_{sym}", use_container_width=True):
                        st.session_state["watchlist"].remove(sym)
                        if isinstance(st.session_state.get("wl_data"), dict):
                            st.session_state["wl_data"].pop(sym, None)
                        st.rerun()



# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════
#  MULTI-TIMEFRAME CPR SCANNER  (new standalone tab)
# ═══════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────
#  FRANK OCHOA — CANDLESTICK PATTERN DETECTOR
# ─────────────────────────────────────────────────────────────────────

def detect_candlestick_pattern(df: pd.DataFrame) -> tuple:
    """
    Detect key Frank Ochoa / classic candlestick patterns on last 3 candles.
    Returns (pattern_name, signal_direction, pattern_strength_bonus)
    Based on: Bullish/Bearish Engulfing, Hammer, Shooting Star,
              Doji at CPR, Morning/Evening Star, Inside Bar, Pin Bar.
    """
    if len(df) < 3:
        return ("None", "neut", 0)

    c0 = df.iloc[-1]   # current candle
    c1 = df.iloc[-2]   # prev candle
    c2 = df.iloc[-3]   # 2 candles ago

    O0,H0,L0,C0 = float(c0["Open"]),float(c0["High"]),float(c0["Low"]),float(c0["Close"])
    O1,H1,L1,C1 = float(c1["Open"]),float(c1["High"]),float(c1["Low"]),float(c1["Close"])
    O2,H2,L2,C2 = float(c2["Open"]),float(c2["High"]),float(c2["Low"]),float(c2["Close"])

    body0 = abs(C0 - O0)
    body1 = abs(C1 - O1)
    body2 = abs(C2 - O2)
    rng0  = H0 - L0 if H0 > L0 else 1e-9
    rng1  = H1 - L1 if H1 > L1 else 1e-9

    upper_wick0 = H0 - max(O0, C0)
    lower_wick0 = min(O0, C0) - L0
    upper_wick1 = H1 - max(O1, C1)
    lower_wick1 = min(O1, C1) - L1

    bull0 = C0 > O0
    bear0 = C0 < O0
    bull1 = C1 > O1
    bear1 = C1 < O1

    # ── 1. Bullish Engulfing at CPR ──────────────────────────────────────────
    # Ochoa: When price engulfs prior candle after touching BC — powerful bull signal
    if (bear1 and bull0 and
        O0 <= C1 and C0 >= O1 and
        body0 > body1 * 1.1):
        return ("Bullish Engulfing", "bull", 15)

    # ── 2. Bearish Engulfing at CPR ──────────────────────────────────────────
    if (bull1 and bear0 and
        O0 >= C1 and C0 <= O1 and
        body0 > body1 * 1.1):
        return ("Bearish Engulfing", "bear", 15)

    # ── 3. Hammer (bullish reversal) ─────────────────────────────────────────
    # Long lower wick >= 2x body, small upper wick, at support / BC
    if (lower_wick0 >= 2 * body0 and
        upper_wick0 <= body0 * 0.4 and
        body0 > 0 and
        lower_wick0 >= rng0 * 0.55):
        return ("Hammer", "bull", 12)

    # ── 4. Shooting Star (bearish reversal) ──────────────────────────────────
    # Long upper wick >= 2x body, small lower wick, at resistance / TC
    if (upper_wick0 >= 2 * body0 and
        lower_wick0 <= body0 * 0.4 and
        body0 > 0 and
        upper_wick0 >= rng0 * 0.55):
        return ("Shooting Star", "bear", 12)

    # ── 5. Doji (indecision — strong at CPR) ─────────────────────────────────
    # Ochoa: Doji inside CPR = explosive breakout coming
    if body0 <= rng0 * 0.1 and rng0 > 0:
        return ("Doji at CPR", "neut", 8)

    # ── 6. Morning Star (3-candle bullish reversal) ───────────────────────────
    if (bear2 and body2 > 0 and
        abs(C1 - O1) <= min(body2, body0) * 0.3 and  # small middle
        bull0 and C0 > (O2 + C2) / 2):
        return ("Morning Star", "bull", 18)

    # ── 7. Evening Star (3-candle bearish reversal) ───────────────────────────
    if (bull2 and body2 > 0 and
        abs(C1 - O1) <= min(body2, body0) * 0.3 and
        bear0 and C0 < (O2 + C2) / 2):
        return ("Evening Star", "bear", 18)

    # ── 8. Inside Bar (Ochoa: compression before breakout) ───────────────────
    if H0 < H1 and L0 > L1:
        direction = "bull" if bull1 else "bear"
        return ("Inside Bar", direction, 10)

    # ── 9. Pin Bar / Rejection Candle ────────────────────────────────────────
    # Tail >= 3x body — strong rejection at key level
    if lower_wick0 >= 3 * body0 and body0 > 0:
        return ("Bull Pin Bar", "bull", 14)
    if upper_wick0 >= 3 * body0 and body0 > 0:
        return ("Bear Pin Bar", "bear", 14)

    # ── 10. Strong Bullish / Bearish candle (Marubozu) ───────────────────────
    if bull0 and body0 >= rng0 * 0.85:
        return ("Bullish Marubozu", "bull", 10)
    if bear0 and body0 >= rng0 * 0.85:
        return ("Bearish Marubozu", "bear", 10)

    return ("None", "neut", 0)


def compute_rr_levels(ltp: float, pattern_dir: str, tc: float, bc: float,
                      P: float, R1: float, R2: float, R3: float,
                      S1: float, S2: float, S3: float, atr: float) -> dict:
    """
    Compute Entry, Target, Stop-Loss and Risk:Reward ratio
    using Frank Ochoa pivot-based methodology.

    Ochoa Rule:
    - Bull: Entry above TC, SL below BC (or 0.5 ATR below entry)
    - Bear: Entry below BC, SL above TC (or 0.5 ATR above entry)
    - Targets: R1/R2/R3 for bull, S1/S2/S3 for bear
    - Minimum acceptable R:R = 1.5x
    """
    if pattern_dir == "bull":
        entry  = round(tc + atr * 0.1, 2)           # slight buffer above TC
        sl     = round(min(bc, ltp - atr * 0.5), 2)  # below BC or 0.5 ATR
        risk   = max(entry - sl, atr * 0.25)
        tgt1   = round(R1, 2)
        tgt2   = round(R2, 2)
        tgt3   = round(R3, 2)
        rr1    = round((tgt1 - entry) / risk, 2) if risk > 0 else 0
        rr2    = round((tgt2 - entry) / risk, 2) if risk > 0 else 0
        trail_sl = round(entry + (tgt1 - entry) * 0.5, 2)  # trail after 50% to T1
    else:
        entry  = round(bc - atr * 0.1, 2)
        sl     = round(max(tc, ltp + atr * 0.5), 2)
        risk   = max(sl - entry, atr * 0.25)
        tgt1   = round(S1, 2)
        tgt2   = round(S2, 2)
        tgt3   = round(S3, 2)
        rr1    = round((entry - tgt1) / risk, 2) if risk > 0 else 0
        rr2    = round((entry - tgt2) / risk, 2) if risk > 0 else 0
        trail_sl = round(entry - (entry - tgt1) * 0.5, 2)

    return {
        "entry": entry, "sl": sl, "risk": round(risk, 2),
        "tgt1": tgt1, "tgt2": tgt2, "tgt3": tgt3,
        "rr1": rr1, "rr2": rr2, "trail_sl": trail_sl,
    }


@st.cache_data(ttl=900)
def scan_cpr_multi_tf(symbols: list, interval: str, period: str,
                      max_stocks: int = 200) -> pd.DataFrame:
    """
    Frank Ochoa CPR Scanner:
    - NO hard CPR width cutoff (scan all, rank by width)
    - NO R:R cutoff (show all, display R:R on card)
    - Targets & SL purely from pivot levels + candlestick context
    - Strength score from 6 weighted factors
    """
    rows = []
    for sym in symbols[:max_stocks]:
        try:
            df = yf.Ticker(sym + ".NS").history(period=period, interval=interval)
            if df.empty or len(df) < 22:
                continue
            df.index = df.index.tz_localize(None)

            # ── CPR from prior completed candle ──────────────────────────────
            ref  = df.iloc[-2]
            H, L, C = float(ref["High"]), float(ref["Low"]), float(ref["Close"])
            P  = (H + L + C) / 3
            BC = (H + L) / 2
            TC = (P - BC) + P
            width = abs(TC - BC) / P * 100

            # Only skip truly wide CPR (> 2%) — include narrow + moderate
            if width > 2.0:
                continue

            ltp   = float(df["Close"].iloc[-1])
            close = df["Close"]
            high  = df["High"]
            low_s = df["Low"]

            # ── Pivot levels (Traditional) ───────────────────────────────────
            R1 = round(2*P - L, 2);  R2 = round(P + (H-L), 2);  R3 = round(H + 2*(P-L), 2)
            S1 = round(2*P - H, 2);  S2 = round(P - (H-L), 2);  S3 = round(L - 2*(H-P), 2)

            # ── HMA-20 ───────────────────────────────────────────────────────
            def wma(s, n):
                w = np.arange(1, n+1)
                return s.rolling(n).apply(lambda x: np.dot(x,w)/w.sum(), raw=True)
            hma    = wma(2*wma(close,10) - wma(close,20), 4)
            hma_up = bool(hma.iloc[-1] > hma.iloc[-2]) if len(hma.dropna()) >= 2 else None

            # ── 3/10 Oscillator ──────────────────────────────────────────────
            diff  = close.rolling(3).mean() - close.rolling(10).mean()
            sig16 = diff.rolling(16).mean()
            hist_val       = float(diff.iloc[-1] - sig16.iloc[-1]) if not np.isnan(diff.iloc[-1]) else 0
            osc_cross_bull = bool(diff.iloc[-1] > sig16.iloc[-1] and diff.iloc[-2] <= sig16.iloc[-2])
            osc_cross_bear = bool(diff.iloc[-1] < sig16.iloc[-1] and diff.iloc[-2] >= sig16.iloc[-2])

            # ── RSI-14 ───────────────────────────────────────────────────────
            delta = close.diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rsi   = float(100 - (100/(1 + gain.iloc[-1]/max(loss.iloc[-1], 1e-9))))

            # ── ATR-14 ───────────────────────────────────────────────────────
            tr  = pd.concat([high-low_s,(high-close.shift()).abs(),(low_s-close.shift()).abs()],axis=1).max(axis=1)
            atr = float(tr.rolling(14).mean().iloc[-1])

            # ── VWAP (20-bar proxy) ──────────────────────────────────────────
            tp   = (high + low_s + close) / 3
            vwap = (tp * df["Volume"]).rolling(20).sum() / df["Volume"].rolling(20).sum()
            above_vwap = bool(ltp > float(vwap.iloc[-1])) if not np.isnan(vwap.iloc[-1]) else None

            # ── Volume surge ─────────────────────────────────────────────────
            vol_avg   = float(df["Volume"].rolling(20).mean().iloc[-1])
            vol_cur   = float(df["Volume"].iloc[-1])
            vol_surge = vol_cur > vol_avg * 1.5 if vol_avg > 0 else False

            # ── Candlestick pattern ──────────────────────────────────────────
            candle_name, candle_dir, candle_bonus = detect_candlestick_pattern(df)

            # ── Frank Ochoa Scoring ──────────────────────────────────────────
            bull_pts = bear_pts = 0

            # 1. CPR Position — most important (3 pts)
            if ltp > TC:          bull_pts += 3
            elif ltp < BC:        bear_pts += 3
            else:                 pass  # inside CPR — neutral

            # 2. HMA trend direction (2 pts)
            if hma_up is True:    bull_pts += 2
            elif hma_up is False: bear_pts += 2

            # 3. 3/10 Oscillator — fresh crossover strongest (2 pts), else 1 pt
            if osc_cross_bull:    bull_pts += 2
            elif osc_cross_bear:  bear_pts += 2
            elif hist_val > 0:    bull_pts += 1
            else:                 bear_pts += 1

            # 4. RSI zone (1 pt)
            if rsi >= 55:         bull_pts += 1
            elif rsi <= 45:       bear_pts += 1

            # 5. VWAP position (1 pt)
            if above_vwap:        bull_pts += 1
            elif above_vwap is False: bear_pts += 1

            # 6. Volume surge confirms direction (1 pt)
            if vol_surge:
                if ltp >= P:      bull_pts += 1
                else:             bear_pts += 1

            # 7. Candlestick pattern confirmation (2 pts bonus)
            if candle_dir == "bull":   bull_pts += 2
            elif candle_dir == "bear": bear_pts += 2

            total = bull_pts + bear_pts
            if   bull_pts > bear_pts: pattern_main = "Bullish"
            elif bear_pts > bull_pts: pattern_main = "Bearish"
            else:                     pattern_main = "Neutral"

            strength = round(max(bull_pts, bear_pts) / max(total, 1) * 100)

            # Skip neutral — no clear direction
            if pattern_main == "Neutral":
                continue

            # ── Targets & SL from Pivot Points + Candlestick context ─────────
            # Ochoa principle: always anchor to pivot levels
            # Entry = CPR breakout level; SL = opposite CPR wall
            # Targets = sequential pivot resistances/supports
            trade_dir = "bull" if pattern_main == "Bullish" else "bear"

            if trade_dir == "bull":
                # Entry: just above TC (CPR breakout)
                entry = round(TC + atr * 0.05, 2)
                # SL: below BC — if candle has hammer/engulf, tighten to low
                if candle_name in ("Hammer", "Bull Pin Bar", "Bullish Engulfing", "Morning Star"):
                    sl = round(min(BC, float(df["Low"].iloc[-1])) - atr * 0.1, 2)
                else:
                    sl = round(BC - atr * 0.1, 2)
                risk = max(entry - sl, atr * 0.2)
                # Targets at R1, R2, R3 — classic Ochoa levels
                t1, t2, t3 = R1, R2, R3
                # If candle is Morning Star or Engulfing — target can stretch to R2 minimum
                if candle_name in ("Morning Star", "Bullish Engulfing", "Bullish Marubozu"):
                    t1 = R1 if R1 > entry else R2

            else:
                # Entry: just below BC (CPR breakdown)
                entry = round(BC - atr * 0.05, 2)
                if candle_name in ("Shooting Star", "Bear Pin Bar", "Bearish Engulfing", "Evening Star"):
                    sl = round(max(TC, float(df["High"].iloc[-1])) + atr * 0.1, 2)
                else:
                    sl = round(TC + atr * 0.1, 2)
                risk = max(sl - entry, atr * 0.2)
                t1, t2, t3 = S1, S2, S3
                if candle_name in ("Evening Star", "Bearish Engulfing", "Bearish Marubozu"):
                    t1 = S1 if S1 < entry else S2

            rr1 = round(abs(t1 - entry) / risk, 2) if risk > 0 else 0
            rr2 = round(abs(t2 - entry) / risk, 2) if risk > 0 else 0

            cpr_type = "Narrow" if width < 0.25 else ("Moderate" if width < 0.5 else "Wide")

            rows.append({
                "Symbol":     sym,
                "LTP":        round(ltp, 2),
                "CPR Width%": round(width, 3),
                "CPR Type":   cpr_type,
                "TC":         round(TC, 2),
                "BC":         round(BC, 2),
                "Pivot P":    round(P, 2),
                "R1": R1, "R2": R2, "R3": R3,
                "S1": S1, "S2": S2, "S3": S3,
                "Pattern":    pattern_main,
                "Candle":     candle_name,
                "Strength%":  min(strength, 100),
                "RSI":        round(rsi, 1),
                "HMA":        "▲" if hma_up else "▼",
                "ATR":        round(atr, 2),
                "Vol Surge":  "✅" if vol_surge else "—",
                "Osc Cross":  "🔼" if osc_cross_bull else ("🔽" if osc_cross_bear else "—"),
                "Entry":      entry,
                "SL":         sl,
                "T1":         round(t1, 2),
                "T2":         round(t2, 2),
                "T3":         round(t3, 2),
                "RR1":        rr1,
                "RR2":        rr2,
                "Risk Rs":    round(risk, 2),
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()
    df_out = pd.DataFrame(rows)
    return df_out.sort_values(["Strength%","CPR Width%"], ascending=[False,True]).reset_index(drop=True)


def build_scanner_pdf(top_bull: pd.DataFrame, top_bear: pd.DataFrame,
                      tf_choice: str, scan_time_str: str) -> bytes:
    """Build CPR Scanner PDF report. Returns bytes."""
    import io as _io
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors as rl_c
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle as PS

    buf  = _io.BytesIO()
    doc  = SimpleDocTemplate(buf, pagesize=A4,
                              leftMargin=14*mm, rightMargin=14*mm,
                              topMargin=13*mm,  bottomMargin=13*mm)
    W    = A4[0] - 28*mm

    OLIVE  = rl_c.HexColor("#1e293b")
    GREEN  = rl_c.HexColor("#16a34a")
    RED    = rl_c.HexColor("#dc2626")
    AMBER  = rl_c.HexColor("#d97706")
    LIGHT  = rl_c.HexColor("#f8fafc")
    BORDER = rl_c.HexColor("#e2e8f0")
    WHITE  = rl_c.white

    s_title = PS("t", fontSize=15, fontName="Helvetica-Bold",
                 textColor=rl_c.HexColor("#1a2332"), leading=20, spaceAfter=3)
    s_sub   = PS("s", fontSize=8,  fontName="Helvetica",
                 textColor=rl_c.HexColor("#5a6a80"), leading=12, spaceAfter=8)
    s_h     = PS("h", fontSize=9,  fontName="Helvetica-Bold",
                 textColor=rl_c.HexColor("#1a2332"), leading=13, spaceBefore=8, spaceAfter=4)
    s_disc  = PS("d", fontSize=7,  fontName="Helvetica",
                 textColor=rl_c.HexColor("#94a3b8"), leading=10)

    story = []
    story.append(Paragraph("PivotVault AI — CPR Scanner Report", s_title))
    story.append(Paragraph(
        f"{tf_choice}  ·  Frank Ochoa Strategy  ·  {scan_time_str}", s_sub))
    story.append(HRFlowable(width=W, thickness=1, color=BORDER, spaceAfter=5))

    def _tbl(df, direction):
        is_bull = direction == "Bullish"
        hdr_c   = GREEN if is_bull else RED
        arrow   = "▲" if is_bull else "▼"
        story.append(Paragraph(
            f"{arrow} {direction} Setups — Top 10 · Pivot-Based Targets · Frank Ochoa", s_h))
        if df.empty:
            story.append(Paragraph(f"No {direction} setups found.", s_disc))
            return
        hdrs = ["Symbol","LTP","Score","Candle","Entry","T1","T2","SL","R:R","RSI","CPR%"]
        data = [hdrs]
        for _, r in df.iterrows():
            rr   = r.get("RR1", 0)
            data.append([
                str(r["Symbol"]),
                f"Rs.{r['LTP']:,.2f}",
                f"{int(r['Strength%'])}%",
                str(r.get("Candle","—")),
                f"Rs.{r['Entry']:,.2f}",
                f"Rs.{r['T1']:,.2f}",
                f"Rs.{r['T2']:,.2f}",
                f"Rs.{r['SL']:,.2f}",
                f"{rr}x",
                str(r["RSI"]),
                f"{r.get('CPR Width%',0):.3f}%",
            ])
        cw = [W*0.1,W*0.09,W*0.07,W*0.13,W*0.09,W*0.09,W*0.09,W*0.08,W*0.07,W*0.08,W*0.1]
        tbl = Table(data, colWidths=cw, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,0), OLIVE),
            ("TEXTCOLOR",     (0,0), (-1,0), WHITE),
            ("FONTNAME",      (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",      (0,0), (-1,-1), 7),
            ("ROWBACKGROUNDS",(0,1), (-1,-1), [WHITE, LIGHT]),
            ("GRID",          (0,0), (-1,-1), 0.4, BORDER),
            ("PADDING",       (0,0), (-1,-1), 4),
            ("TEXTCOLOR",     (2,1), (2,-1),  hdr_c),
            ("FONTNAME",      (2,1), (2,-1),  "Helvetica-Bold"),
            ("TEXTCOLOR",     (4,1), (6,-1),  hdr_c),
            ("TEXTCOLOR",     (7,1), (7,-1),  RED),
            ("FONTNAME",      (0,1), (0,-1),  "Helvetica-Bold"),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 5*mm))

    _tbl(top_bull, "Bullish")
    _tbl(top_bear, "Bearish")
    story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=3))
    story.append(Paragraph(
        "DISCLAIMER: Educational/informational purposes only. Not financial advice. "
        "Entry/Target/SL levels derived from Frank Ochoa Pivot Boss methodology + ATR-14. "
        "Always use proper risk management. Consult a SEBI-registered advisor.",
        s_disc,
    ))
    doc.build(story)
    buf.seek(0)
    return buf.read()


def send_scanner_pdf_email(pdf_bytes: bytes, to_email: str, tf_label: str,
                           scan_time: str, smtp_cfg: dict) -> tuple:
    """Send CPR Scanner PDF as email attachment."""
    import smtplib, ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text    import MIMEText
    from email.mime.base    import MIMEBase
    from email              import encoders
    try:
        msg            = MIMEMultipart()
        msg["Subject"] = f"PivotVault AI — CPR Scanner {tf_label} — {scan_time}"
        msg["From"]    = smtp_cfg["sender"]
        msg["To"]      = to_email

        body = MIMEText(
            f"<html><body style='font-family:monospace;'>"
            f"<h2 style='color:#1a1f0e;'>PivotVault AI — CPR Scanner Auto-Report</h2>"
            f"<p style='color:#5a6a48;'>{tf_label} &nbsp;|&nbsp; {scan_time}</p>"
            f"<p>Please find the latest CPR Scanner report attached as PDF.</p>"
            f"<p style='color:#2d7a3a;'>Scan completed automatically. Top 10 Bullish + Top 10 Bearish stocks.</p>"
            f"<hr/>"
            f"<p style='color:#8a9a78;font-size:0.8em;'>For educational purposes only. Not financial advice.</p>"
            f"</body></html>",
            "html"
        )
        msg.attach(body)

        # Attach PDF
        part = MIMEBase("application", "octet-stream")
        part.set_payload(pdf_bytes)
        encoders.encode_base64(part)
        fname = f"PivotVault_Scanner_{scan_time.replace(' ','_').replace(':','')}.pdf"
        part.add_header("Content-Disposition", f"attachment; filename={fname}")
        msg.attach(part)

        ctx = ssl.create_default_context()
        if smtp_cfg["port"] == 465:
            with smtplib.SMTP_SSL(smtp_cfg["host"], 465, context=ctx) as s:
                s.login(smtp_cfg["sender"], smtp_cfg["password"])
                s.sendmail(smtp_cfg["sender"], to_email, msg.as_string())
        else:
            with smtplib.SMTP(smtp_cfg["host"], smtp_cfg["port"]) as s:
                s.ehlo(); s.starttls(context=ctx)
                s.login(smtp_cfg["sender"], smtp_cfg["password"])
                s.sendmail(smtp_cfg["sender"], to_email, msg.as_string())
        return True, "✅ Auto-report sent!"
    except Exception as e:
        return False, str(e)


# ══════════════════════════════════════════════════════════════
#  BROKER INTEGRATION — Zerodha Kite · Upstox · Groww
# ══════════════════════════════════════════════════════════════

def _zerodha_place_order(symbol: str, side: str, qty: int,
                          order_type: str = "MARKET") -> tuple:
    """Place order via Zerodha Kite API. Returns (success, order_id/error)."""
    try:
        from kiteconnect import KiteConnect
        api_key      = st.session_state.get("zerodha_api_key", "")
        access_token = st.session_state.get("zerodha_access_token", "")
        if not api_key or not access_token:
            return False, "Zerodha not configured. Add API key & access token in ⚙️ Broker Settings."
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        tx = kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL
        order_id = kite.place_order(
            tradingsymbol   = symbol,
            exchange        = kite.EXCHANGE_NSE,
            transaction_type= tx,
            quantity        = qty,
            order_type      = kite.ORDER_TYPE_MARKET,
            product         = kite.PRODUCT_MIS,
            variety         = kite.VARIETY_REGULAR,
        )
        return True, str(order_id)
    except ImportError:
        return False, "kiteconnect not installed. Run: pip install kiteconnect"
    except Exception as e:
        return False, str(e)


def _upstox_place_order(symbol: str, side: str, qty: int) -> tuple:
    """Place order via Upstox API v2. Returns (success, order_id/error)."""
    try:
        import upstox_client
        access_token = st.session_state.get("upstox_access_token", "")
        if not access_token:
            return False, "Upstox not configured. Add access token in ⚙️ Broker Settings."
        config = upstox_client.Configuration()
        config.access_token = access_token
        api = upstox_client.OrderApi(upstox_client.ApiClient(config))
        req = upstox_client.PlaceOrderRequest(
            quantity        = qty,
            product         = "I",
            validity        = "DAY",
            price           = 0,
            tag             = "PivotVault",
            instrument_token= f"NSE_EQ|{symbol}",
            order_type      = "MARKET",
            transaction_type= side,
            disclosed_quantity = 0,
            trigger_price   = 0,
            is_amo          = False,
        )
        res = api.place_order(req, "2.0")
        return True, str(res.data.order_id)
    except ImportError:
        return False, "upstox-python-sdk not installed. Run: pip install upstox-python-sdk"
    except Exception as e:
        return False, str(e)


def _render_groww_signals(signals: list):
    """
    Show trade signal popup cards with:
    - Desktop browser notification
    - Groww deep link
    - Zerodha one-click order
    - Upstox one-click order
    - Paper trade button
    """
    import json
    broker      = st.session_state.get("broker", "none")
    signals_json = json.dumps(signals)

    groww_html = f"""
<style>
@keyframes slideDown {{
    from {{ opacity:0; transform:translateY(-30px) scale(0.96); }}
    to   {{ opacity:1; transform:translateY(0) scale(1); }}
}}
@keyframes glow {{
    0%,100% {{ box-shadow: 0 0 0 0 rgba(78,97,48,0.4); }}
    50%      {{ box-shadow: 0 0 0 8px rgba(78,97,48,0); }}
}}
.pv-signal-popup {{
    position: fixed; top: 16px; right: 16px;
    z-index: 999999;
    display: flex; flex-direction: column; gap: 10px;
    max-width: 360px; width: 92vw;
}}
.pv-signal-card {{
    background: #fff;
    border-radius: 14px;
    padding: 14px 15px;
    box-shadow: 0 10px 40px rgba(0,0,0,0.18);
    animation: slideDown 0.3s cubic-bezier(.2,.8,.3,1);
    border-top: 4px solid #4e6130;
    font-family: DM Sans, sans-serif;
    position: relative;
}}
.pv-signal-card.bear {{ border-top-color: #c0392b; }}
.pv-sc-head {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:8px; }}
.pv-sc-sym {{ font-size:1.05rem; font-weight:800; color:#1a1f0e; }}
.pv-sc-badge {{
    border-radius:20px; padding:2px 10px;
    font-size:0.7rem; font-weight:700; letter-spacing:0.06em;
}}
.bull-badge {{ background:#edf7ee; color:#2d7a3a; border:1px solid #b8dfc0; }}
.bear-badge {{ background:#fdf0ee; color:#c0392b; border:1px solid #f0c0b8; }}
.pv-sc-levels {{
    display:grid; grid-template-columns:1fr 1fr 1fr;
    gap:5px; margin-bottom:8px;
}}
.pv-sc-lev {{
    background:#f7f9f2; border-radius:7px;
    padding:5px 3px; text-align:center;
}}
.pv-sc-ll {{ font-size:0.58rem; color:#8a9a78; text-transform:uppercase; letter-spacing:0.07em; font-family:DM Mono,monospace; }}
.pv-sc-lv {{ font-size:0.82rem; font-weight:700; color:#1a1f0e; font-family:DM Mono,monospace; }}
.pv-sc-meta {{ font-size:0.68rem; color:#8a9a78; font-family:DM Mono,monospace; margin-bottom:10px; }}
.pv-broker-btns {{ display:flex; flex-direction:column; gap:6px; }}
.pv-btn {{
    display:block; width:100%; padding:9px 0;
    border:none; border-radius:8px;
    font-size:0.82rem; font-weight:700;
    cursor:pointer; text-align:center;
    text-decoration:none; color:#fff !important;
    transition:opacity 0.15s, transform 0.1s;
    letter-spacing:0.02em;
}}
.pv-btn:hover {{ opacity:0.88; transform:scale(0.99); }}
.btn-groww  {{ background:linear-gradient(135deg,#00b386,#007a60); animation:glow 2.5s ease infinite; }}
.btn-zerodha {{ background:linear-gradient(135deg,#387ed1,#2563b0); }}
.btn-upstox  {{ background:linear-gradient(135deg,#7c3aed,#5b21b6); }}
.btn-paper   {{ background:linear-gradient(135deg,#f59e0b,#d97706); }}
.btn-bear-groww   {{ background:linear-gradient(135deg,#e74c3c,#c0392b); }}
.btn-bear-zerodha {{ background:linear-gradient(135deg,#e74c3c,#991b1b); }}
.btn-bear-upstox  {{ background:linear-gradient(135deg,#db2777,#9d174d); }}
.pv-dismiss {{
    position:absolute; top:8px; right:10px;
    background:none; border:none; font-size:0.95rem;
    cursor:pointer; color:#8a9a78; padding:2px 5px;
}}
.pv-dismiss:hover {{ color:#c0392b; }}
.pv-notif-bar {{
    background:#1a1f0e; color:#f4f7ec;
    border-radius:10px; padding:9px 13px;
    font-size:0.76rem; font-family:DM Mono,monospace;
    display:flex; align-items:center; gap:8px;
    animation:slideDown 0.2s ease;
}}
.pv-notif-bar button {{
    margin-left:auto; background:#4e6130; color:#fff;
    border:none; border-radius:6px;
    padding:4px 10px; font-size:0.73rem;
    font-family:DM Mono,monospace; cursor:pointer; font-weight:700;
}}
</style>

<div class="pv-signal-popup" id="pvPopup"></div>

<script>
var PV_SIGNALS   = {signals_json};
var PV_BROKER    = "{broker}";

function growwUrl(sym) {{
    return "https://groww.in/stocks/" + sym.toLowerCase() + "-share-price";
}}
function zerodhaUrl(sym, side) {{
    // Zerodha Kite web order form deep link
    return "https://kite.zerodha.com/orders?exchange=NSE&tradingsymbol=" +
           sym + "&transaction_type=" + side;
}}
function upstoxUrl(sym, side) {{
    return "https://login.upstox.com/?f=1&tradingsymbol=" + sym +
           "&exchange=NSE&transaction_type=" + side;
}}

function buildCards() {{
    var popup = document.getElementById("pvPopup");
    if (!popup) return;
    popup.innerHTML = "";

    var w = window.parent || window;
    if (!w._pvNotifEnabled && w.Notification && w.Notification.permission !== "denied") {{
        var bar = document.createElement("div");
        bar.className = "pv-notif-bar";
        bar.innerHTML = "🔔 Enable desktop notifications for instant trade alerts &nbsp;<button onclick=\"reqNotif()\">Allow</button>";
        popup.appendChild(bar);
    }}

    PV_SIGNALS.forEach(function(sig, idx) {{
        var bull = sig.side === "BUY";
        var card = document.createElement("div");
        card.className = "pv-signal-card" + (bull ? "" : " bear");
        card.id = "pvCard" + idx;

        var btns = "";

        // Groww button (always show)
        btns += "<a href=\"" + growwUrl(sig.symbol) + "\" target=\"_blank\" class=\"pv-btn " +
                (bull ? "btn-groww" : "btn-bear-groww") + "\">" +
                (bull ? "🟢 Buy on Groww" : "🔴 Sell on Groww") + "</a>";

        // Zerodha button
        btns += "<a href=\"" + zerodhaUrl(sig.symbol, sig.side) + "\" target=\"_blank\" class=\"pv-btn " +
                (bull ? "btn-zerodha" : "btn-bear-zerodha") + "\">" +
                "⚡ " + sig.side + " on Zerodha Kite</a>";

        // Upstox button
        btns += "<a href=\"" + upstoxUrl(sig.symbol, sig.side) + "\" target=\"_blank\" class=\"pv-btn " +
                (bull ? "btn-upstox" : "btn-bear-upstox") + "\">" +
                "💜 " + sig.side + " on Upstox</a>";

        // Paper trade button
        btns += "<button class=\"pv-btn btn-paper\" onclick=\"paperTrade(" + idx + ")\">" +
                "📝 Paper Trade (Test)</button>";

        card.innerHTML =
            "<button class=\"pv-dismiss\" onclick=\"dismissCard(" + idx + ")\">✕</button>" +
            "<div class=\"pv-sc-head\">" +
            "  <span class=\"pv-sc-sym\">" + sig.symbol + "</span>" +
            "  <span class=\"pv-sc-badge " + (bull ? "bull-badge" : "bear-badge") + "\">" + sig.side + "</span>" +
            "</div>" +
            "<div class=\"pv-sc-levels\">" +
            "  <div class=\"pv-sc-lev\"><div class=\"pv-sc-ll\">Entry</div><div class=\"pv-sc-lv\">₹" + sig.entry + "</div></div>" +
            "  <div class=\"pv-sc-lev\"><div class=\"pv-sc-ll\">Target</div><div class=\"pv-sc-lv\">₹" + sig.t1 + "</div></div>" +
            "  <div class=\"pv-sc-lev\"><div class=\"pv-sc-ll\">SL</div><div class=\"pv-sc-lv\">₹" + sig.sl + "</div></div>" +
            "</div>" +
            "<div class=\"pv-sc-meta\">R:R " + sig.rr + "x · Strength " + sig.strength + "% · " + sig.candle + "</div>" +
            "<div class=\"pv-broker-btns\">" + btns + "</div>";

        popup.appendChild(card);

        // Desktop notification — use parent window to escape iframe sandbox
        (function() {{
            var w = window.parent || window;
            if (w._pvNotify) {{
                w._pvNotify(
                    (bull ? "🟢 BUY" : "🔴 SELL") + " — " + sig.symbol + " (" + sig.strength + "%)",
                    "Entry ₹" + sig.entry + "  T1 ₹" + sig.t1 + "  SL ₹" + sig.sl + "  R:R " + sig.rr + "x",
                    "pv-" + sig.symbol
                );
            }} else if (w.Notification && w.Notification.permission === "granted") {{
                var n = new w.Notification(
                    (bull ? "🟢 BUY" : "🔴 SELL") + " — " + sig.symbol + " (" + sig.strength + "%)",
                    {{ body: "Entry ₹" + sig.entry + "  T1 ₹" + sig.t1 + "  SL ₹" + sig.sl + "  R:R " + sig.rr + "x",
                       icon: "/static/icon-192.png", tag: "pv-" + sig.symbol, requireInteraction: false }}
                );
                n.onclick = function() {{ w.focus(); n.close(); }};
            }}
        }})();
    }});

    setTimeout(function() {{
        var p = document.getElementById("pvPopup");
        if (p) p.style.opacity = "0.3";
    }}, 90000);
}}

function dismissCard(idx) {{
    var c = document.getElementById("pvCard" + idx);
    if (c) {{ c.style.transform = "translateX(110%)"; c.style.opacity = "0";
               c.style.transition = "all 0.3s"; setTimeout(function(){{ c.remove(); }}, 300); }}
}}

function paperTrade(idx) {{
    var sig = PV_SIGNALS[idx];
    // Store in sessionStorage for paper trade tab to pick up
    var trades = JSON.parse(sessionStorage.getItem("pv_paper_queue") || "[]");
    trades.push(sig);
    sessionStorage.setItem("pv_paper_queue", JSON.stringify(trades));
    alert("📝 Paper trade queued for " + sig.symbol + " " + sig.side +
          " @ ₹" + sig.entry + "\nGo to Test Trading tab to see result.");
    dismissCard(idx);
}}

function reqNotif() {{
    var w = window.parent || window;
    if (w._pvRequestNotif) {{
        w._pvRequestNotif(function(ok) {{
            w._pvNotifEnabled = ok;
            if (ok) buildCards();
        }});
    }} else if (w.Notification) {{
        w.Notification.requestPermission().then(function(p) {{
            w._pvNotifEnabled = (p === "granted");
            if (p === "granted") buildCards();
        }});
    }}
}}

buildCards();
</script>
"""
    st.markdown(groww_html, unsafe_allow_html=True)


def page_cpr_scanner(nse500: pd.DataFrame):
    """
    CPR Scanner — one timeframe at a time.
    Each timeframe auto-refreshes at its own natural interval:
      15m  → every 15 minutes
      1h   → every 1 hour
      1d   → every 4 hours (daily chart doesn't change intraday)
      1wk  → every 24 hours
      1mo  → every 24 hours
    Filters: Narrow CPR < 0.25% + Strength 85–100% + Top 10 only.
    """

    TF_CONFIG = {
        "⚡ 15 Min  — Fast Scalping":   {"interval":"15m","period":"5d",  "tag":"15m","refresh":900,   "color":"#7c3aed","bg":"#f5f3ff","label":"Fast Scalping",  "refresh_label":"15 min"},
        "🕐 1 Hour  — Swing Scalping":  {"interval":"1h", "period":"30d", "tag":"1h", "refresh":3600,  "color":"#1d4ed8","bg":"#eff6ff","label":"Swing Scalping", "refresh_label":"1 hour"},
        "📅 1 Day   — Swing Trading":   {"interval":"1d", "period":"90d", "tag":"1d", "refresh":14400, "color":"#1a6b3c","bg":"#edf7ee","label":"Swing Trading",  "refresh_label":"4 hours"},
        "📆 1 Week  — Positional":      {"interval":"1wk","period":"2y",  "tag":"1wk","refresh":86400, "color":"#d97706","bg":"#fdf9ec","label":"Positional",     "refresh_label":"24 hours"},
        "🗓️ 1 Month — Prime Trading":   {"interval":"1mo","period":"5y",  "tag":"1mo","refresh":86400, "color":"#dc2626","bg":"#fdf0ee","label":"Prime Trading",  "refresh_label":"24 hours"},
    }

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("""
    <div style="display:flex;align-items:center;gap:14px;margin-bottom:1.25rem;
                padding:1.25rem 1.5rem;background:#ffffff;border:1px solid #dae0cb;
                border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.06);">
        <div style="font-size:2rem;">📡</div>
        <div style="flex:1;">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;
                        font-weight:700;color:#1a1f0e;">CPR Scanner</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:0.68rem;
                        color:#5a6a48;letter-spacing:0.08em;text-transform:uppercase;margin-top:2px;">
                Nifty 200 · All CPR Setups · Best 10 Bullish + 10 Bearish · Pivot-Based Targets
            </div>
        </div>
        <div id="countdown-wrap" style="text-align:right;font-family:'IBM Plex Mono',monospace;">
            <div style="font-size:0.62rem;color:#5a6a48;text-transform:uppercase;letter-spacing:0.07em;">Next refresh in</div>
            <div id="countdown" style="font-size:1.3rem;font-weight:700;color:#1a6b3c;">—</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Notification permission bar ─────────────────────────────────────────
    st.markdown("""
    <div id="pv-notif-bar" style="background:#1a1f0e;color:#f4f7ec;
         border-radius:10px;padding:10px 16px;margin-bottom:0.75rem;
         font-family:DM Mono,monospace;font-size:0.78rem;
         display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
        <span>🔔 Enable desktop notifications to get instant alerts when signals appear</span>
        <button id="pv-allow-btn" onclick="pvRequestNotif()"
            style="margin-left:auto;background:#4e6130;color:#fff;border:none;
                   border-radius:6px;padding:6px 16px;font-size:0.75rem;
                   font-family:DM Mono,monospace;cursor:pointer;font-weight:700;
                   transition:background 0.2s;">
            🔔 Allow Notifications
        </button>
    </div>
    <script>
    function pvRequestNotif() {
        var w = window.parent || window;
        if (!("Notification" in w)) {
            alert("Your browser does not support desktop notifications.");
            return;
        }
        w.Notification.requestPermission().then(function(perm) {
            var btn = document.getElementById("pv-allow-btn");
            var bar = document.getElementById("pv-notif-bar");
            if (perm === "granted") {
                w._pvNotifReady = true;
                if (btn) btn.innerText = "✅ Notifications ON";
                if (btn) btn.style.background = "#2d7a3a";
                // Fire a test notification immediately
                w.pvNotify(
                    "✅ PivotVault AI Notifications Active",
                    "You will now get instant alerts when trade signals appear.",
                    "pv-test"
                );
                // Hide bar after 3s
                setTimeout(function() {
                    if (bar) bar.style.display = "none";
                }, 3000);
            } else {
                if (btn) btn.innerText = "❌ Blocked — Enable in browser settings";
                if (btn) btn.style.background = "#c0392b";
            }
        });
    }
    // Auto-hide bar if already granted
    (function() {
        var w = window.parent || window;
        if ("Notification" in w && w.Notification.permission === "granted") {
            var bar = document.getElementById("pv-notif-bar");
            if (bar) bar.style.display = "none";
            w._pvNotifReady = true;
        }
    })();
    </script>
    """, unsafe_allow_html=True)

    # ── Timeframe selector ────────────────────────────────────────────────────
    c1, c2 = st.columns([4, 1])
    with c1:
        tf_choice = st.selectbox(
            "Timeframe",
            list(TF_CONFIG.keys()),
            index=2,
            label_visibility="collapsed",
            key="scanner_tf",
        )
    with c2:
        manual_btn = st.button("🔄 Scan Now", use_container_width=True, key="run_cpr_scan_btn")

    cfg        = TF_CONFIG[tf_choice]
    tf_col     = cfg["color"]
    tf_bg      = cfg["bg"]
    tf_tag     = cfg["tag"]
    refresh_s  = cfg["refresh"]

    scan_key      = f"cpr_scan_{tf_tag}"
    scan_time_key = f"cpr_scan_time_{tf_tag}"

    now           = time.time()
    last_scan     = st.session_state.get(scan_time_key, 0)
    age           = now - last_scan
    needs_refresh = manual_btn or (age >= refresh_s) or (scan_key not in st.session_state)

    # ── Run scan only for selected timeframe ──────────────────────────────────
    if needs_refresh:
        with st.spinner(f"Scanning Nifty 200 on {tf_tag.upper()} ({cfg['label']})…"):
            result = scan_cpr_multi_tf(
                fetch_nifty200_list(),
                interval=cfg["interval"],
                period=cfg["period"],
                max_stocks=200,
            )
        st.session_state[scan_key]      = result
        st.session_state[scan_time_key] = now
        last_scan = now

        # ── Always sync canonical keys read by Trade Signals tab ──────────────
        # Trade signals reads cpr_scan_15m / cpr_scan_1h directly
        if tf_tag in ("15m", "1h"):
            st.session_state[f"cpr_scan_{tf_tag}"]      = result
            st.session_state[f"cpr_scan_time_{tf_tag}"] = now

        # ── Store signals + fire desktop notifications ────────────────────────
        if not result.empty:
            import json as _json
            top3_bull = result[result["Pattern"]=="Bullish"].head(3)
            top3_bear = result[result["Pattern"]=="Bearish"].head(3)
            notif_signals = []
            for _, r in top3_bull.iterrows():
                notif_signals.append({
                    "symbol": r["Symbol"], "side": "BUY",
                    "entry": r["Entry"], "t1": r["T1"], "sl": r["SL"],
                    "rr": r["RR1"], "strength": int(r["Strength%"]),
                    "candle": r.get("Candle","—"),
                })
            for _, r in top3_bear.iterrows():
                notif_signals.append({
                    "symbol": r["Symbol"], "side": "SELL",
                    "entry": r["Entry"], "t1": r["T1"], "sl": r["SL"],
                    "rr": r["RR1"], "strength": int(r["Strength%"]),
                    "candle": r.get("Candle","—"),
                })
            st.session_state["pending_signals"] = notif_signals
            # Also update the per-tag scan time key used by Trade Signals tab
            st.session_state[f"cpr_scan_time_{tf_tag}"] = now

            # ── Fire desktop notifications via window.parent ──────────────────
            # window.parent escapes the Streamlit iframe — works on Chrome/Edge/Firefox
            notif_js_list = _json.dumps([
                {"sym": s["symbol"], "side": s["side"],
                 "entry": s["entry"], "t1": s["t1"], "sl": s["sl"],
                 "rr": s["rr"], "str": s["strength"]}
                for s in notif_signals[:6]
            ])
            st.markdown(f"""
<script>
(function fireNotifs() {{
    var sigs = {notif_js_list};
    var w    = window.parent || window;
    if (!("Notification" in w)) return;
    if (w.Notification.permission !== "granted") {{
        // Flash the allow button if not granted
        var btn = document.getElementById("pv-allow-btn");
        if (btn) {{
            btn.style.animation = "none";
            btn.style.background = "#c0392b";
            btn.innerText = "⚠️ Allow Notifications!";
        }}
        return;
    }}
    sigs.forEach(function(s, i) {{
        setTimeout(function() {{
            var emoji = s.side === "BUY" ? "🟢" : "🔴";
            w.pvNotify(
                emoji + " " + s.side + " Signal — " + s.sym + " (" + s.str + "%)",
                "Entry ₹" + s.entry + "  |  T1 ₹" + s.t1 + "  |  SL ₹" + s.sl + "  |  R:R " + s.rr + "x",
                "pv-" + s.sym
            );
        }}, i * 800);  // Stagger by 800ms so they don't all fire at once
    }});
}})();
</script>
""", unsafe_allow_html=True)

    scan_df  = st.session_state.get(scan_key, pd.DataFrame())
    elapsed  = int(now - last_scan)
    remaining = max(0, refresh_s - elapsed)

    # ── Countdown JS ──────────────────────────────────────────────────────────
    st.markdown(f"""
    <script>
    (function() {{
        var secs = {remaining};
        function pad(n) {{ return n < 10 ? "0"+n : n; }}
        function fmt(s) {{
            if (s >= 3600) return pad(Math.floor(s/3600))+"h "+pad(Math.floor((s%3600)/60))+"m";
            return pad(Math.floor(s/60))+":"+pad(s%60);
        }}
        function tick() {{
            if (secs <= 0) {{ window.location.reload(); return; }}
            var el = document.getElementById("countdown");
            if (el) el.innerText = fmt(secs);
            secs--;
            setTimeout(tick, 1000);
        }}
        tick();
    }})();
    </script>
    """, unsafe_allow_html=True)

    # ── Status bar ────────────────────────────────────────────────────────────
    scan_dt = datetime.fromtimestamp(last_scan).strftime("%d %b  %H:%M:%S") if last_scan else "—"
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:1rem;flex-wrap:wrap;"
        f"font-family:IBM Plex Mono,monospace;font-size:0.72rem;color:#5a6a48;"
        f"margin-bottom:1rem;padding:0.5rem 0.9rem;background:{tf_bg};"
        f"border:1px solid {tf_col}33;border-left:3px solid {tf_col};border-radius:6px;'>"
        f"<span style='color:{tf_col};font-weight:700;'>{tf_choice}</span>"
        f"<span>Last scan: <b>{scan_dt}</b></span>"
        f"<span>Auto-refresh: every <b>{cfg['refresh_label']}</b></span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    if scan_df.empty:
        st.info("No data found. Try clicking 🔄 Scan Now or switch timeframe.")
        return

    # ── All bullish & bearish — no strength cutoff ────────────────────────────
    all_bull = scan_df[scan_df["Pattern"] == "Bullish"].copy()
    all_bear = scan_df[scan_df["Pattern"] == "Bearish"].copy()

    # ── Summary metrics ───────────────────────────────────────────────────────
    n_scanned = len(scan_df)
    n_narrow  = int((scan_df["CPR Width%"] < 0.25).sum())
    n_bull    = len(all_bull)
    n_bear    = len(all_bear)
    n_qual    = n_bull + n_bear   # all directional stocks

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("📊 Scanned",   n_scanned)
    m2.metric("🎯 Narrow CPR", n_narrow)
    m3.metric("📈 Directional", n_qual)
    m4.metric("🟢 Bullish",   n_bull)
    m5.metric("🔴 Bearish",   n_bear)

    st.markdown("<div style='height:0.25rem'></div>", unsafe_allow_html=True)

    if n_qual == 0:
        st.markdown(
            f"<div style='text-align:center;padding:2rem;background:#f7f9f2;"
            f"border:2px dashed #dce3ed;border-radius:10px;"
            f"font-family:IBM Plex Mono,monospace;font-size:0.82rem;color:#8a9a78;'>"
            f"No directional setups found on {tf_tag.upper()} right now.<br>"
            f"<span style='font-size:0.72rem;'>Try 🔄 Scan Now or switch to a different timeframe.</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
        return

    # ── Top 10 each side — sorted by Strength then tightest CPR ──────────────
    top_bull = all_bull.sort_values(["Strength%","CPR Width%"], ascending=[False,True]).head(10)
    top_bear = all_bear.sort_values(["Strength%","CPR Width%"], ascending=[False,True]).head(10)

    def _cards(df, direction):
        is_bull = direction == "Bullish"
        hc  = "#16a34a" if is_bull else "#dc2626"
        hbg = "#edf7ee" if is_bull else "#fdf0ee"
        hbd = "#b8dfc0" if is_bull else "#f0c0b8"
        arr = "▲" if is_bull else "▼"

        if df.empty:
            return (f"<div style='padding:2rem;text-align:center;background:#f7f9f2;"
                    f"border:2px dashed #dce3ed;border-radius:10px;"
                    f"font-family:IBM Plex Mono,monospace;font-size:0.78rem;color:#8a9a78;'>"
                    f"No {direction} picks match criteria on this timeframe</div>")

        html = (f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;"
                f"font-weight:700;color:{hc};letter-spacing:0.05em;text-transform:uppercase;"
                f"padding:0.5rem 0.9rem;background:{hbg};border:1px solid {hbd};"
                f"border-left:4px solid {hc};border-radius:6px;margin-bottom:0.6rem;'>"
                f"{arr} Top 10 {direction} · Narrow CPR · Frank Ochoa Strategy</div>")

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        for rank, (_, row) in enumerate(df.iterrows(), 1):
            prob     = int(row["Strength%"])
            rsi_c    = "#16a34a" if row["RSI"] >= 55 else ("#dc2626" if row["RSI"] <= 45 else "#d97706")
            medal    = medals.get(rank, f"#{rank}")
            candle   = str(row.get("Candle", "None"))
            candle_icon = "🕯️" if candle != "None" else ""
            rr1      = float(row.get("RR1", 0))
            rr2      = float(row.get("RR2", 0))
            rr_col   = "#16a34a" if rr1 >= 2 else ("#d97706" if rr1 >= 1.5 else "#dc2626")
            osc      = str(row.get("Osc Cross", "—"))
            vol      = str(row.get("Vol Surge", "—"))
            cpr_w    = float(row.get("CPR Width%", 0))

            html += (
                f'<div style="background:#fff;border:1px solid {hbd};border-radius:10px;'
                f'padding:0.85rem 1rem;margin-bottom:0.5rem;box-shadow:0 1px 5px rgba(0,0,0,0.05);">'
                f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.4rem;">'
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<span style="font-size:1rem;">{medal}</span>'
                f'<div>'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.95rem;font-weight:700;color:#1a1f0e;">{row["Symbol"]}</div>'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#5a6a48;">'
                f'&#8377;{row["LTP"]:,.2f} &nbsp;·&nbsp; ATR &#8377;{row["ATR"]:,.2f} &nbsp;·&nbsp; {candle_icon} {candle}</div>'
                f'</div></div>'
                f'<div style="text-align:right;">'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:1rem;font-weight:700;color:{hc};">{prob}%</div>'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.62rem;color:#5a6a48;">Strength</div>'
                f'</div></div>'
                f'<div style="background:#f1f5f9;border-radius:3px;height:5px;margin-bottom:0.5rem;">'
                f'<div style="background:{hc};width:{prob}%;height:100%;border-radius:3px;"></div></div>'
                f'<div style="display:flex;flex-wrap:wrap;gap:0.5rem;margin-bottom:0.45rem;'
                f'padding:0.4rem 0.6rem;background:#f7f9f2;border-radius:6px;'
                f'font-family:IBM Plex Mono,monospace;font-size:0.68rem;">'
                f'<span style="color:#5a6a48;">Entry <b style="color:#1a1f0e;">&#8377;{row["Entry"]:,.2f}</b></span>'
                f'<span>|</span>'
                f'<span style="color:#5a6a48;">T1 <b style="color:{hc};">&#8377;{row["T1"]:,.2f}</b></span>'
                f'<span style="color:#5a6a48;">T2 <b style="color:{hc};">&#8377;{row["T2"]:,.2f}</b></span>'
                f'<span>|</span>'
                f'<span style="color:#5a6a48;">SL <b style="color:#c0392b;">&#8377;{row["SL"]:,.2f}</b></span>'
                f'<span>|</span>'
                f'<span style="color:#5a6a48;">R:R <b style="color:{rr_col};">{rr1}x / {rr2}x</b></span>'
                f'</div>'
                f'<div style="display:flex;flex-wrap:wrap;gap:0.3rem;">'
                f'<span style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a1f0e;">CPR {cpr_w:.3f}%</span>'
                f'<span style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a1f0e;">TC &#8377;{row["TC"]:,.2f} / BC &#8377;{row["BC"]:,.2f}</span>'
                f'<span style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:{hc};">HMA {row["HMA"]}</span>'
                f'<span style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:{rsi_c};">RSI {row["RSI"]}</span>'
                f'<span style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a1f0e;">Osc {osc}</span>'
                f'<span style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a1f0e;">Vol {vol}</span>'
                f'<span style="background:{hbg};border:1px solid {hbd};border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:{hc};font-weight:600;">{arr} NARROW</span>'
                f'</div></div>'
            )
        return html

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(_cards(top_bull, "Bullish"), unsafe_allow_html=True)
    with col_r:
        st.markdown(_cards(top_bear, "Bearish"), unsafe_allow_html=True)

    # Full results table
    if n_qual > 0:
        with st.expander(f"📋 All {n_qual} stocks ({n_bull} Bullish + {n_bear} Bearish)", expanded=False):
            disp = scan_df[scan_df["Pattern"] != "Neutral"].sort_values(["Strength%","CPR Width%"], ascending=[False,True]).copy()
            for c in ["LTP","Entry","T1","T2","T3","SL","TC","BC"]:
                if c in disp.columns:
                    disp[c] = disp[c].apply(lambda x: f"Rs.{x:,.2f}")
            disp["CPR Width%"] = disp["CPR Width%"].apply(lambda x: f"{x:.3f}%" if isinstance(x, float) else x)
            disp["Strength%"]  = disp["Strength%"].apply(lambda x: f"{x}%")
            show_cols = [c for c in ["Symbol","LTP","Strength%","Candle","Entry","T1","T2","SL","RR1","RR2","RSI","HMA","Vol Surge","CPR Width%"] if c in disp.columns]
            st.dataframe(disp[show_cols], use_container_width=True, hide_index=True)

    # ═══════════════════════════════════════════════════════════════════
    #  SEND REPORT
    # ═══════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;font-weight:700;"
        "color:#1a1f0e;margin-bottom:0.75rem;'>📤  Send / Download Scanner Report</div>",
        unsafe_allow_html=True,
    )

    scan_time_str = datetime.now().strftime("%d %b %Y  %H:%M")

    # Build WhatsApp message text
    def _wa_text(bull_df, bear_df, tf_lbl, scan_t):
        lines = [
            "🏦 *PivotVault AI — CPR Scanner*",
            f"📅 {tf_lbl}  |  {scan_t}",
            "🔍 Frank Ochoa Strategy  |  Narrow CPR  |  R:R >= 1.5x",
            "",
            "🟢 *BULLISH SETUPS*",
        ]
        if bull_df.empty:
            lines.append("No bullish picks found.")
        else:
            for i, (_, r) in enumerate(bull_df.head(10).iterrows(), 1):
                lines.append(
                    f"{i}. *{r['Symbol']}* Rs.{r['LTP']:,.2f}  Score {int(r['Strength%'])}%  "
                    f"{r.get('Candle','—')}  "
                    f"Entry Rs.{r['Entry']:,.2f}  T1 Rs.{r['T1']:,.2f}  SL Rs.{r['SL']:,.2f}  R:R {r['RR1']}x"
                )
        lines += ["", "🔴 *BEARISH SETUPS*"]
        if bear_df.empty:
            lines.append("No bearish picks found.")
        else:
            for i, (_, r) in enumerate(bear_df.head(10).iterrows(), 1):
                lines.append(
                    f"{i}. *{r['Symbol']}* Rs.{r['LTP']:,.2f}  Score {int(r['Strength%'])}%  "
                    f"{r.get('Candle','—')}  "
                    f"Entry Rs.{r['Entry']:,.2f}  T1 Rs.{r['T1']:,.2f}  SL Rs.{r['SL']:,.2f}  R:R {r['RR1']}x"
                )
        lines += ["", "⚠️ Educational use only. Not financial advice.", "📱 Sent via PivotVault AI"]
        return "\n".join(lines)

    # Build HTML email body
    def _html_email(bull_df, bear_df, tf_lbl, scan_t):
        def _tbl_rows(df, is_bull):
            if df.empty:
                return "<tr><td colspan='9' style='padding:8px;color:#8a9a78;font-style:italic;'>No qualifying stocks found.</td></tr>"
            hc = "#16a34a" if is_bull else "#dc2626"
            out = ""
            for _, r in df.iterrows():
                rr_c = "#16a34a" if r.get("RR1",0)>=2 else ("#d97706" if r.get("RR1",0)>=1.5 else "#dc2626")
                out += (
                    f"<tr style='border-bottom:1px solid #f1f5f9;'>"
                    f"<td style='padding:7px 5px;font-weight:700;font-family:Courier New,monospace;color:#1a1f0e;'>{r['Symbol']}</td>"
                    f"<td style='padding:7px 5px;font-size:0.83rem;'>Rs.{r['LTP']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:{hc};font-weight:700;'>{int(r['Strength%'])}%</td>"
                    f"<td style='padding:7px 5px;font-size:0.8rem;'>{r.get('Candle','—')}</td>"
                    f"<td style='padding:7px 5px;font-size:0.8rem;'>Rs.{r['Entry']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:{hc};'>Rs.{r['T1']:,.2f} / Rs.{r['T2']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:#c0392b;'>Rs.{r['SL']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:{rr_c};font-weight:700;'>{r.get('RR1',0)}x</td>"
                    f"<td style='padding:7px 5px;color:#5a6a48;'>{r['RSI']}</td>"
                    f"</tr>"
                )
            return out

        TH = "background:#1e293b;color:#e2e8f0;padding:7px 5px;text-align:left;font-size:0.7rem;letter-spacing:0.06em;text-transform:uppercase;"
        TBLS = "width:100%;border-collapse:collapse;font-family:Courier New,monospace;font-size:0.82rem;"
        HDR_COL = "background:linear-gradient(135deg,#0d1f0a,#1a4a10)"

        return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:20px 10px;">
<table width="700" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.08);">
<tr><td style="{HDR_COL};padding:22px 26px;">
  <div style="font-family:Courier New,monospace;font-size:1.25rem;font-weight:700;color:#e8eddf;">🏦 PivotVault AI — CPR Scanner</div>
  <div style="font-family:Courier New,monospace;font-size:0.72rem;color:#b5c77a;margin-top:4px;letter-spacing:0.07em;text-transform:uppercase;">{tf_lbl} · Frank Ochoa Strategy · {scan_t}</div>
</td></tr>
<tr><td style="padding:20px 22px;">
  <div style="font-family:Courier New,monospace;font-size:0.72rem;font-weight:700;color:#2d7a3a;border-left:4px solid #16a34a;padding-left:8px;margin-bottom:10px;text-transform:uppercase;letter-spacing:0.07em;">▲ BULLISH SETUPS</div>
  <table style="{TBLS}"><tr><th style="{TH}">Symbol</th><th style="{TH}">LTP</th><th style="{TH}">Score</th><th style="{TH}">Candle</th><th style="{TH}">Entry</th><th style="{TH}">T1 / T2</th><th style="{TH}">SL</th><th style="{TH}">R:R</th><th style="{TH}">RSI</th></tr>
  {_tbl_rows(bull_df, True)}</table>
  <div style="font-family:Courier New,monospace;font-size:0.72rem;font-weight:700;color:#c0392b;border-left:4px solid #dc2626;padding-left:8px;margin:18px 0 10px;text-transform:uppercase;letter-spacing:0.07em;">▼ BEARISH SETUPS</div>
  <table style="{TBLS}"><tr><th style="{TH}">Symbol</th><th style="{TH}">LTP</th><th style="{TH}">Score</th><th style="{TH}">Candle</th><th style="{TH}">Entry</th><th style="{TH}">T1 / T2</th><th style="{TH}">SL</th><th style="{TH}">R:R</th><th style="{TH}">RSI</th></tr>
  {_tbl_rows(bear_df, False)}</table>
</td></tr>
<tr><td style="padding:12px 22px 20px;"><div style="background:#f7f9f2;border-radius:6px;padding:10px 14px;font-size:0.68rem;color:#8a9a78;line-height:1.6;font-family:Courier New,monospace;">⚠️ For educational purposes only. Not financial advice. Entry/Target/SL from Frank Ochoa Pivot Boss + ATR-14. Always use proper risk management.</div></td></tr>
</table></td></tr></table></body></html>"""

    rtab1, rtab2, rtab3 = st.tabs(["📧 Gmail / Email", "💬 WhatsApp", "⬇️ Download PDF"])

    with rtab1:
        st.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a48;margin-bottom:0.75rem;'>Send report to any Gmail or SMTP email inbox.</div>", unsafe_allow_html=True)
        cfg = st.session_state.get("smtp_cfg", {"host": "smtp.gmail.com", "port": 587, "sender": "", "password": ""})
        with st.expander("⚙️ SMTP Settings", expanded=not bool(cfg.get("sender"))):
            sc1, sc2 = st.columns(2)
            with sc1:
                nh = st.text_input("SMTP Host",     value=cfg["host"],     key="sc_host")
                ns = st.text_input("Sender Email",  value=cfg["sender"],   key="sc_sender")
            with sc2:
                np = st.selectbox("Port", [587, 465], index=0 if cfg["port"] == 587 else 1, key="sc_port")
                nw = st.text_input("App Password",  value=cfg["password"], type="password", key="sc_pwd",
                                   help="Gmail: Google Account → Security → App Passwords (not your normal password)")
            if st.button("💾 Save", key="sc_save"):
                st.session_state["smtp_cfg"] = {"host": nh, "port": np, "sender": ns, "password": nw}
                st.success("SMTP settings saved!")

        ec1, ec2 = st.columns([3, 1])
        with ec1:
            to_em = st.text_input("Recipient Email", placeholder="you@gmail.com", label_visibility="collapsed", key="sc_to")
        with ec2:
            send_em = st.button("📧 Send", use_container_width=True, key="sc_send_em")

        if send_em:
            cfg2 = st.session_state.get("smtp_cfg", {})
            if not to_em.strip():
                st.error("Enter recipient email address.")
            elif not cfg2.get("sender") or not cfg2.get("password"):
                st.error("Configure SMTP settings above first.")
            else:
                body = _html_email(top_bull, top_bear, tf_choice, scan_time_str)
                with st.spinner("Sending email…"):
                    ok, msg = send_report_email(to_em.strip(), cfg2["host"], cfg2["port"], cfg2["sender"], cfg2["password"], body, scan_time_str)
                if ok:
                    st.success(f"✅ Report sent to {to_em.strip()}")
                else:
                    st.error(f"❌ {msg}")
                    st.caption("Gmail tip: use an App Password not your regular password. Requires 2FA enabled.")

    with rtab2:
        st.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a48;margin-bottom:0.75rem;'>Share scanner results via WhatsApp.</div>", unsafe_allow_html=True)
        wa_msg = _wa_text(top_bull, top_bear, tf_choice, scan_time_str)
        st.text_area("Message Preview (copy or use button below)", wa_msg, height=200, key="wa_prev")
        wc1, wc2 = st.columns([3, 1])
        with wc1:
            wa_ph = st.text_input("Phone number with country code", placeholder="919876543210", label_visibility="collapsed", key="wa_ph")
        with wc2:
            wa_go = st.button("💬 Open WhatsApp", use_container_width=True, key="wa_go")
        if wa_go and wa_ph.strip():
            import urllib.parse as _up
            wa_url = "https://wa.me/" + wa_ph.strip().replace("+","") + "?text=" + _up.quote(wa_msg)
            st.markdown(
                f"<a href='{wa_url}' target='_blank' style='display:inline-block;background:#25d366;color:#fff;"
                f"font-family:IBM Plex Mono,monospace;font-size:0.82rem;font-weight:600;"
                f"padding:0.55rem 1.5rem;border-radius:8px;text-decoration:none;margin-top:0.5rem;'>"
                f"💬 Open WhatsApp →</a>",
                unsafe_allow_html=True,
            )
            st.caption("Opens WhatsApp with message pre-filled. Just tap Send.")
        elif wa_go:
            st.warning("Enter phone number with country code (e.g. 919876543210)")
        st.caption("💡 You can also copy the message above and paste into any chat — WhatsApp, Telegram, SMS, etc.")

    with rtab3:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a48;"
            "margin-bottom:0.75rem;'>Download the scanner report as a PDF. "
            "Download a snapshot of the current scan results.</div>",
            unsafe_allow_html=True,
        )
        if st.button("📄 Generate & Download PDF", use_container_width=True, key="sc_gen_pdf"):
            with st.spinner("Building PDF…"):
                try:
                    pdf_bytes = build_scanner_pdf(top_bull, top_bear, tf_choice, scan_time_str)
                    st.download_button(
                        label=f"⬇️ Download PDF — {tf_tag.upper()} Scanner",
                        data=pdf_bytes,
                        file_name=f"PivotVault_Scanner_{tf_tag}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                        key="sc_pdf_dl",
                    )
                    st.success("PDF ready — click button above to download!")
                except Exception as ex:
                    st.error(f"PDF error: {ex}")

    # ── Footer ────────────────────────────────────────────────────────────────

        # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div style="background:#f7f9f2;border:1px solid #dae0cb;border-radius:10px;
                padding:0.9rem 1.1rem;margin-top:0.75rem;
                font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#5a6a48;line-height:1.9;">
    <b style="color:#1a1f0e;">Auto-Refresh Schedule</b><br>
    ⚡ 15 Min chart → refreshes every <b>15 minutes</b> &nbsp;|&nbsp;
    🕐 1 Hour chart → refreshes every <b>1 hour</b> &nbsp;|&nbsp;
    📅 1 Day chart → refreshes every <b>4 hours</b> &nbsp;|&nbsp;
    📆 1 Week / 🗓️ 1 Month → refresh every <b>24 hours</b><br>
    <b style="color:#1a1f0e;">Filter:</b> Narrow CPR &lt; 0.25% · Strength 85–100% · Top 10 per direction · Nifty 200 only
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  TRADE SIGNALS PAGE  (push notifications + live signal board)
# ═══════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60)
def compute_signals_for_symbol(symbol: str, interval: str = "1d", period: str = "90d") -> dict:
    """
    Compute all trading signals for a symbol on a given timeframe.
    Returns a rich signal dict with entry, targets, SL and confidence.
    """
    try:
        df = yf.Ticker(symbol + ".NS").history(period=period, interval=interval)
        if df.empty or len(df) < 20:
            return {}
        df.index = df.index.tz_localize(None)

        close = df["Close"]
        high  = df["High"]
        low   = df["Low"]
        ltp   = float(close.iloc[-1])

        # ── Pivot Points (Traditional) ────────────────────────────────────────
        ref  = df.iloc[-2]
        H, L, C = float(ref["High"]), float(ref["Low"]), float(ref["Close"])
        P  = (H + L + C) / 3
        R1 = 2 * P - L
        R2 = P + (H - L)
        R3 = H + 2 * (P - L)
        S1 = 2 * P - H
        S2 = P - (H - L)
        S3 = L - 2 * (H - P)

        # ── CPR ───────────────────────────────────────────────────────────────
        BC = (H + L) / 2
        TC = (P - BC) + P
        cpr_width = abs(TC - BC) / P * 100

        # ── ATR-14 ────────────────────────────────────────────────────────────
        tr  = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])

        # ── RSI-14 ────────────────────────────────────────────────────────────
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rsi   = float(100 - (100 / (1 + gain.iloc[-1] / max(loss.iloc[-1], 1e-9))))

        # ── HMA ───────────────────────────────────────────────────────────────
        def wma(s, n):
            w = np.arange(1, n + 1)
            return s.rolling(n).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)
        hma    = wma(2 * wma(close, 10) - wma(close, 20), 4)
        hma_up = bool(hma.iloc[-1] > hma.iloc[-2]) if len(hma.dropna()) >= 2 else None

        # ── 3/10 Osc ─────────────────────────────────────────────────────────
        diff  = close.rolling(3).mean() - close.rolling(10).mean()
        sig16 = diff.rolling(16).mean()
        osc_bull = bool(diff.iloc[-1] > sig16.iloc[-1])
        osc_cross_bull = bool(diff.iloc[-1] > sig16.iloc[-1] and diff.iloc[-2] <= sig16.iloc[-2])
        osc_cross_bear = bool(diff.iloc[-1] < sig16.iloc[-1] and diff.iloc[-2] >= sig16.iloc[-2])

        # ── Stochastic ────────────────────────────────────────────────────────
        lo14 = low.rolling(14).min()
        hi14 = high.rolling(14).max()
        stk  = float(100 * (close.iloc[-1] - lo14.iloc[-1]) / max(hi14.iloc[-1] - lo14.iloc[-1], 1e-9))

        # ── Signal logic ─────────────────────────────────────────────────────
        score = 0
        signals = []

        # CPR position (strongest signal)
        if ltp > TC:
            score += 25
            signals.append(("🟢", "Price above TC (CPR Bullish)", "bull"))
        elif ltp < BC:
            score -= 25
            signals.append(("🔴", "Price below BC (CPR Bearish)", "bear"))
        else:
            signals.append(("🟡", "Price inside CPR (Indecision)", "neut"))

        # Narrow CPR
        if cpr_width < 0.25:
            signals.append(("🎯", f"Narrow CPR ({cpr_width:.3f}%) — Trending Day Setup", "bull" if ltp > P else "bear"))

        # HMA
        if hma_up is True:
            score += 15
            signals.append(("📈", "HMA-20 Rising (Uptrend)", "bull"))
        elif hma_up is False:
            score -= 15
            signals.append(("📉", "HMA-20 Falling (Downtrend)", "bear"))

        # 3/10 Crossover (strongest momentum signal)
        if osc_cross_bull:
            score += 25
            signals.append(("⚡", "3/10 Bullish Crossover (Fresh Signal!)", "bull"))
        elif osc_cross_bear:
            score -= 25
            signals.append(("⚡", "3/10 Bearish Crossover (Fresh Signal!)", "bear"))
        elif osc_bull:
            score += 10
            signals.append(("📊", "3/10 Oscillator Positive", "bull"))
        else:
            score -= 10
            signals.append(("📊", "3/10 Oscillator Negative", "bear"))

        # RSI
        if rsi >= 70:
            score -= 10
            signals.append(("⚠️", f"RSI {rsi:.0f} — Overbought (caution)", "bear"))
        elif rsi <= 30:
            score += 10
            signals.append(("⚠️", f"RSI {rsi:.0f} — Oversold (watch for bounce)", "bull"))
        elif rsi >= 55:
            score += 10
            signals.append(("✅", f"RSI {rsi:.0f} — Bullish Zone", "bull"))
        elif rsi <= 45:
            score -= 10
            signals.append(("❌", f"RSI {rsi:.0f} — Bearish Zone", "bear"))

        # Stochastic
        if stk >= 80:
            signals.append(("📛", f"Stoch %K {stk:.0f} — Overbought", "bear"))
        elif stk <= 20:
            signals.append(("💡", f"Stoch %K {stk:.0f} — Oversold Reversal Zone", "bull"))

        # Pivot proximity
        for label, val in [("R3",R3),("R2",R2),("R1",R1),("P",P),("S1",S1),("S2",S2),("S3",S3)]:
            if abs(ltp - val) / ltp < 0.004:
                signals.append(("📍", f"Price at {label} ({val:,.2f}) — Key Level", "neut"))

        # Overall bias
        if   score >= 40:  bias, bias_col = "STRONG BUY",  "bull"
        elif score >= 15:  bias, bias_col = "BUY",          "bull"
        elif score <= -40: bias, bias_col = "STRONG SELL", "bear"
        elif score <= -15: bias, bias_col = "SELL",         "bear"
        else:              bias, bias_col = "NEUTRAL",      "neut"

        confidence = min(abs(score), 75)

        # Trade levels
        if bias_col == "bull":
            entry  = round(ltp, 2)
            tgt1   = round(R1, 2)
            tgt2   = round(R2, 2)
            sl     = round(max(S1, ltp - atr * 1.2), 2)
            rr     = round((tgt1 - entry) / max(entry - sl, 0.01), 2)
        else:
            entry  = round(ltp, 2)
            tgt1   = round(S1, 2)
            tgt2   = round(S2, 2)
            sl     = round(min(R1, ltp + atr * 1.2), 2)
            rr     = round((entry - tgt1) / max(sl - entry, 0.01), 2)

        return {
            "symbol": symbol, "ltp": ltp, "bias": bias, "bias_col": bias_col,
            "score": score, "confidence": confidence,
            "signals": signals,
            "P": round(P,2), "R1": round(R1,2), "R2": round(R2,2), "R3": round(R3,2),
            "S1": round(S1,2), "S2": round(S2,2), "S3": round(S3,2),
            "TC": round(TC,2), "BC": round(BC,2), "cpr_width": round(cpr_width,3),
            "rsi": round(rsi,1), "atr": round(atr,2), "stoch_k": round(stk,1),
            "hma_up": hma_up,
            "entry": entry, "tgt1": tgt1, "tgt2": tgt2, "sl": sl, "rr": rr,
        }
    except Exception:
        return {}


def _signal_card(sig: dict) -> str:
    """Render a single signal card as HTML."""
    col_map = {
        "bull": ("#16a34a", "#edf7ee", "#b8dfc0"),
        "bear": ("#dc2626", "#fdf0ee", "#f0c0b8"),
        "neut": ("#d97706", "#fdf9ec", "#fde68a"),
    }
    fc, bg, bdr = col_map.get(sig["bias_col"], col_map["neut"])
    bias_labels = {
        "STRONG BUY": "🚀 STRONG BUY", "BUY": "✅ BUY",
        "STRONG SELL": "🔻 STRONG SELL", "SELL": "❌ SELL",
        "NEUTRAL": "⚪ NEUTRAL"
    }
    bias_label = bias_labels.get(sig["bias"], sig["bias"])

    sig_rows = ""
    for icon, text, kind in sig["signals"][:6]:
        c = col_map.get(kind, col_map["neut"])[0]
        sig_rows += (
            f"<div style='display:flex;align-items:flex-start;gap:6px;padding:3px 0;"
            f"border-bottom:1px solid #f1f5f9;font-size:0.72rem;'>"
            f"<span>{icon}</span>"
            f"<span style='color:#1a1f0e;'>{text}</span></div>"
        )

    return f"""
<div style="background:#ffffff;border:1px solid {bdr};border-top:4px solid {fc};
            border-radius:10px;padding:1rem 1.1rem;margin-bottom:1rem;
            box-shadow:0 2px 8px rgba(0,0,0,0.06);">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:0.6rem;">
    <div>
      <div style="font-family:IBM Plex Mono,monospace;font-size:1rem;font-weight:700;color:#1a1f0e;">
        {sig['symbol']}
      </div>
      <div style="font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a48;">
        ₹{sig['ltp']:,.2f} &nbsp;·&nbsp; ATR ₹{sig['atr']:,.2f}
      </div>
    </div>
    <div style="text-align:right;">
      <div style="background:{bg};border:1px solid {bdr};border-radius:6px;
                  padding:0.3rem 0.7rem;font-family:IBM Plex Mono,monospace;
                  font-size:0.78rem;font-weight:700;color:{fc};">{bias_label}</div>
      <div style="font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#5a6a48;margin-top:3px;">
        Confidence: {sig['confidence']}%
      </div>
    </div>
  </div>
  {sig_rows}
  <div style="display:flex;gap:1rem;margin-top:0.6rem;padding-top:0.5rem;
              border-top:1px solid #f1f5f9;flex-wrap:wrap;">
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a48;">Entry</span>
      <span style="color:#1a1f0e;font-weight:700;"> ₹{sig['entry']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a48;">T1</span>
      <span style="color:{fc};font-weight:700;"> ₹{sig['tgt1']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a48;">T2</span>
      <span style="color:{fc};font-weight:700;"> ₹{sig['tgt2']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a48;">SL</span>
      <span style="color:#c0392b;font-weight:700;"> ₹{sig['sl']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a48;">R:R</span>
      <span style="color:#1a1f0e;font-weight:700;"> {sig['rr']}x</span>
    </div>
  </div>
  <div style="margin-top:0.5rem;font-family:IBM Plex Mono,monospace;font-size:0.68rem;
              color:#8a9ab0;display:flex;flex-wrap:wrap;gap:0.5rem;">
    <span>P:{sig['P']:,.0f}</span>
    <span style="color:#c0392b;">R1:{sig['R1']:,.0f} R2:{sig['R2']:,.0f}</span>
    <span style="color:#2d7a3a;">S1:{sig['S1']:,.0f} S2:{sig['S2']:,.0f}</span>
    <span>RSI:{sig['rsi']}</span>
    <span>CPR:{sig['cpr_width']}%</span>
  </div>
</div>"""


def page_trade_signals(nse500: pd.DataFrame):
    """
    Trade Signal Board — synced live from CPR Scanner.
    Shows 15Min and 1Hour scanner results as actionable trade cards.
    Refreshes automatically when scanner refreshes.
    """
    import json

    # ── Header ────────────────────────────────────────────────────────────
    h1, h2 = st.columns([5, 1])
    with h1:
        st.markdown("""
        <div class="title-bar">
            <span style="font-size:1.5rem;">🔔</span>
            <h1 style="color:#1a1f0e;">Trade Signals</h1>
            <span style="margin-left:auto;background:#edf7ee;border:1px solid #b8dfc0;
                         color:#2d7a3a;padding:3px 12px;border-radius:20px;
                         font-family:DM Mono,monospace;font-size:0.72rem;font-weight:700;">
                LIVE · CPR SCANNER SYNC
            </span>
        </div>
        """, unsafe_allow_html=True)
    with h2:
        # Notification enable button — calls window.parent
        st.markdown("""
        <button onclick="(function(){
            var w = window.parent || window;
            if (!w.Notification) { alert('Notifications not supported in this browser.'); return; }
            w.Notification.requestPermission().then(function(p){
                if (p === 'granted') {
                    w._pvNotifEnabled = true;
                    new w.Notification('🏦 PivotVault AI', {
                        body: 'Trade signal notifications are now ON!',
                        icon: '/static/icon-192.png',
                        tag:  'pv-enable'
                    });
                } else {
                    alert('Notification permission denied. Please allow notifications in your browser settings.');
                }
            });
        })()"
        style="width:100%;padding:8px 6px;background:#4e6130;color:#fff;
               border:none;border-radius:8px;font-family:DM Sans,sans-serif;
               font-size:0.75rem;font-weight:700;cursor:pointer;
               transition:opacity 0.2s;" id="notif-enable-btn">
        🔔 Enable Alerts
        </button>
        <script>
        // Update button text based on current permission
        (function checkPerm(){
            var w = window.parent || window;
            var btn = document.getElementById("notif-enable-btn");
            if (!btn) { setTimeout(checkPerm, 300); return; }
            if (w.Notification && w.Notification.permission === "granted") {
                btn.style.background = "#2d7a3a";
                btn.innerText = "✅ Alerts ON";
            } else if (w.Notification && w.Notification.permission === "denied") {
                btn.style.background = "#c0392b";
                btn.innerText = "🔕 Blocked";
                btn.title = "Allow notifications in browser settings (🔒 icon in address bar)";
            }
        })();
        </script>
        """, unsafe_allow_html=True)

    # ── Auto-refresh: inherit from scanner (15m & 1h only) ────────────────
    if _HAS_AUTOREFRESH and is_market_open():
        st_autorefresh(interval=15_000, limit=None, key="signals_autorefresh")

    # ── Pull data from CPR scanner session state ──────────────────────────
    # Only use 15Min and 1Hour scans as requested
    TF_LABELS = {
        "cpr_scan_15m":  ("⚡ 15 Min",  "#e67e22", "15m"),
        "cpr_scan_1h":   ("🕐 1 Hour",  "#2980b9", "1h"),
    }

    all_signals = []
    scan_times  = {}

    for key, (label, color, tag) in TF_LABELS.items():
        _raw = st.session_state.get(key)
        df = _raw if isinstance(_raw, pd.DataFrame) else pd.DataFrame()
        ts = st.session_state.get(f"cpr_scan_time_{tag}", 0)
        if not df.empty:
            scan_times[label] = datetime.fromtimestamp(ts).strftime("%d %b %H:%M") if ts else "—"
            for _, r in df.iterrows():
                all_signals.append({
                    "tf":       label,
                    "tf_color": color,
                    "symbol":   r["Symbol"],
                    "side":     "BUY"  if r["Pattern"] == "Bullish" else "SELL",
                    "ltp":      r["LTP"],
                    "entry":    r["Entry"],
                    "t1":       r["T1"],
                    "t2":       r["T2"],
                    "t3":       r["T3"],
                    "sl":       r["SL"],
                    "rr1":      r["RR1"],
                    "rr2":      r.get("RR2", 0),
                    "strength": int(r["Strength%"]),
                    "candle":   r.get("Candle", "—"),
                    "rsi":      r.get("RSI", 0),
                    "hma":      r.get("HMA", "—"),
                    "vol":      r.get("Vol Surge", "—"),
                    "cpr_w":    r.get("CPR Width%", 0),
                    "atr":      r.get("ATR", 0),
                })

    # ── Status bar ────────────────────────────────────────────────────────
    if not all_signals:
        st.markdown("""
        <div style="text-align:center;padding:3rem 1rem;background:#f7f9f2;
                    border:2px dashed #dae0cb;border-radius:12px;
                    font-family:DM Mono,monospace;">
            <div style="font-size:2rem;margin-bottom:0.75rem;">📡</div>
            <div style="font-size:1rem;font-weight:700;color:#1a1f0e;margin-bottom:0.5rem;">
                No signals yet
            </div>
            <div style="font-size:0.82rem;color:#5a6a48;">
                Go to <b>📡 CPR Scanner</b> → select <b>15 Min</b> or <b>1 Hour</b>
                → click <b>🔄 Scan Now</b><br>
                Signals will appear here automatically and refresh with every scan.
            </div>
        </div>
        """, unsafe_allow_html=True)
        # Quick scan shortcut buttons
        st.markdown("<div style='height:1rem;'></div>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            if st.button("📡 Go to CPR Scanner → Run Scan", use_container_width=True, key="goto_scanner"):
                st.session_state["current_page"] = "CPR Scanner"
                st.rerun()
        return

    # Scan time info
    time_pills = " &nbsp;·&nbsp; ".join(
        f"<span style='color:{TF_LABELS[k][1] if k in TF_LABELS else '#5a6a48'};font-weight:700;'>{label}</span> "
        f"<span style='color:#8a9a78;'>scanned {t}</span>"
        for label, t in scan_times.items()
    ) if scan_times else ""

    st.markdown(
        f"<div style='font-family:DM Mono,monospace;font-size:0.72rem;"
        f"color:#5a6a48;margin-bottom:1rem;padding:0.5rem 0.9rem;"
        f"background:#f7f9f2;border:1px solid #dae0cb;border-radius:8px;"
        f"border-left:3px solid #4e6130;display:flex;flex-wrap:wrap;gap:12px;align-items:center;'>"
        f"<span class='live-dot'></span>"
        f"<span style='font-weight:700;color:#4e6130;'>LIVE SIGNALS</span>"
        f"{time_pills}"
        f"<span style='margin-left:auto;color:#8a9a78;'>{len(all_signals)} signals total</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Filters ───────────────────────────────────────────────────────────
    fc1, fc2, fc3, fc4 = st.columns([2, 2, 1.5, 1.5])
    with fc1:
        tf_filter = st.multiselect("Timeframe", ["⚡ 15 Min","🕐 1 Hour"],
                                    default=["⚡ 15 Min","🕐 1 Hour"],
                                    key="sig_tf_filter", label_visibility="collapsed")
    with fc2:
        side_filter = st.radio("Direction", ["All","BUY only","SELL only"],
                                horizontal=True, key="sig_side_filter", label_visibility="collapsed")
    with fc3:
        min_str = st.slider("Min Strength%", 0, 100, 60, key="sig_min_str")
    with fc4:
        min_rr = st.slider("Min R:R", 0.0, 5.0, 1.0, step=0.1, key="sig_min_rr")

    # Apply filters
    filtered = [s for s in all_signals
                if s["tf"] in (tf_filter if tf_filter else ["⚡ 15 Min","🕐 1 Hour"])
                and (side_filter == "All"
                     or (side_filter == "BUY only"  and s["side"] == "BUY")
                     or (side_filter == "SELL only" and s["side"] == "SELL"))
                and s["strength"] >= min_str
                and s["rr1"] >= min_rr]

    # Sort: strength desc, then CPR width asc
    filtered.sort(key=lambda x: (-x["strength"], x["cpr_w"]))

    if not filtered:
        st.info(f"No signals match current filters. Try reducing Min Strength or Min R:R.")
        return

    bull_sigs = [s for s in filtered if s["side"] == "BUY"]
    bear_sigs = [s for s in filtered if s["side"] == "SELL"]

    st.markdown(
        f"<div style='font-family:DM Mono,monospace;font-size:0.75rem;color:#5a6a48;"
        f"margin-bottom:0.75rem;'>Showing <b>{len(filtered)}</b> signals — "
        f"<span style='color:#2d7a3a;font-weight:700;'>▲ {len(bull_sigs)} Bullish</span> &nbsp;"
        f"<span style='color:#c0392b;font-weight:700;'>▼ {len(bear_sigs)} Bearish</span></div>",
        unsafe_allow_html=True,
    )

    broker = st.session_state.get("broker", "none")

    # ── Signal cards ──────────────────────────────────────────────────────
    def _signal_card_html(s: dict) -> str:
        bull    = s["side"] == "BUY"
        ac      = "#2d7a3a" if bull else "#c0392b"
        bg      = "#edf7ee" if bull else "#fdf0ee"
        bdr     = "#b8dfc0" if bull else "#f0c0b8"
        arrow   = "▲" if bull else "▼"
        rr_col  = "#2d7a3a" if s["rr1"] >= 2 else ("#b8860b" if s["rr1"] >= 1.5 else "#c0392b")
        str_w   = min(s["strength"], 100)
        tf_c    = s["tf_color"]
        return f"""
<div style="background:#ffffff;border:1px solid #dae0cb;border-radius:12px;
            padding:1rem 1.1rem;border-top:4px solid {ac};
            box-shadow:0 2px 10px rgba(50,70,20,0.07);
            animation:slideIn 0.25s ease;font-family:DM Sans,sans-serif;">
  <!-- Header row -->
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
    <div style="display:flex;align-items:center;gap:8px;">
      <span style="font-size:1.1rem;font-weight:900;color:#1a1f0e;">{s['symbol']}</span>
      <span style="background:{bg};color:{ac};border:1px solid {bdr};
                   border-radius:20px;padding:2px 9px;font-size:0.68rem;font-weight:700;">
        {arrow} {s['side']}
      </span>
      <span style="background:{tf_c}18;color:{tf_c};border:1px solid {tf_c}44;
                   border-radius:12px;padding:1px 7px;font-size:0.65rem;font-weight:700;">
        {s['tf']}
      </span>
    </div>
    <span style="font-family:DM Mono,monospace;font-size:0.72rem;color:#5a6a48;">
      LTP ₹{s['ltp']:,.2f}
    </span>
  </div>
  <!-- Level pills -->
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:5px;margin-bottom:8px;">
    <div style="background:#f7f9f2;border-radius:7px;padding:5px 3px;text-align:center;">
      <div style="font-size:0.58rem;color:#8a9a78;font-family:DM Mono,monospace;text-transform:uppercase;">Entry</div>
      <div style="font-size:0.8rem;font-weight:700;color:#1a1f0e;font-family:DM Mono,monospace;">₹{s['entry']}</div>
    </div>
    <div style="background:#f7f9f2;border-radius:7px;padding:5px 3px;text-align:center;">
      <div style="font-size:0.58rem;color:#8a9a78;font-family:DM Mono,monospace;text-transform:uppercase;">T1</div>
      <div style="font-size:0.8rem;font-weight:700;color:#2d7a3a;font-family:DM Mono,monospace;">₹{s['t1']}</div>
    </div>
    <div style="background:#f7f9f2;border-radius:7px;padding:5px 3px;text-align:center;">
      <div style="font-size:0.58rem;color:#8a9a78;font-family:DM Mono,monospace;text-transform:uppercase;">T2</div>
      <div style="font-size:0.8rem;font-weight:700;color:#2d7a3a;font-family:DM Mono,monospace;">₹{s['t2']}</div>
    </div>
    <div style="background:#f7f9f2;border-radius:7px;padding:5px 3px;text-align:center;">
      <div style="font-size:0.58rem;color:#8a9a78;font-family:DM Mono,monospace;text-transform:uppercase;">SL</div>
      <div style="font-size:0.8rem;font-weight:700;color:#c0392b;font-family:DM Mono,monospace;">₹{s['sl']}</div>
    </div>
    <div style="background:{rr_col}15;border-radius:7px;padding:5px 3px;text-align:center;border:1px solid {rr_col}33;">
      <div style="font-size:0.58rem;color:#8a9a78;font-family:DM Mono,monospace;text-transform:uppercase;">R:R</div>
      <div style="font-size:0.8rem;font-weight:700;color:{rr_col};font-family:DM Mono,monospace;">{s['rr1']}x</div>
    </div>
  </div>
  <!-- Strength bar -->
  <div style="margin-bottom:8px;">
    <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
      <span style="font-family:DM Mono,monospace;font-size:0.65rem;color:#8a9a78;">
        {s['candle']} &nbsp;·&nbsp; RSI {s['rsi']} &nbsp;·&nbsp; HMA {s['hma']} &nbsp;·&nbsp; Vol {s['vol']}
      </span>
      <span style="font-family:DM Mono,monospace;font-size:0.65rem;font-weight:700;color:{ac};">
        {s['strength']}%
      </span>
    </div>
    <div style="background:#f0f3ea;border-radius:4px;height:5px;overflow:hidden;">
      <div style="background:{ac};width:{str_w}%;height:100%;border-radius:4px;
                  transition:width 0.5s;"></div>
    </div>
  </div>
</div>"""

    # Render in 2-column grid (bull left, bear right on desktop)
    if bull_sigs and bear_sigs:
        col_bull, col_bear = st.columns(2)
        with col_bull:
            st.markdown(f"<div style='font-family:DM Mono,monospace;font-size:0.72rem;"
                        f"color:#2d7a3a;font-weight:700;margin-bottom:0.5rem;'>▲ BULLISH ({len(bull_sigs)})</div>",
                        unsafe_allow_html=True)
            for s in bull_sigs:
                st.markdown(_signal_card_html(s), unsafe_allow_html=True)
                _trade_buttons(s)
        with col_bear:
            st.markdown(f"<div style='font-family:DM Mono,monospace;font-size:0.72rem;"
                        f"color:#c0392b;font-weight:700;margin-bottom:0.5rem;'>▼ BEARISH ({len(bear_sigs)})</div>",
                        unsafe_allow_html=True)
            for s in bear_sigs:
                st.markdown(_signal_card_html(s), unsafe_allow_html=True)
                _trade_buttons(s)
    else:
        for s in filtered:
            st.markdown(_signal_card_html(s), unsafe_allow_html=True)
            _trade_buttons(s)

    # Desktop notifications for new signals
    if filtered:
        notif_js = json.dumps([{
            "symbol":s["symbol"],"side":s["side"],
            "entry":s["entry"],"t1":s["t1"],"sl":s["sl"],
            "rr":s["rr1"],"strength":s["strength"],"candle":s["candle"]
        } for s in filtered[:6]])
        st.markdown(f"""
        <script>
        (function(){{
            var sigs = {notif_js};
            var w = window.parent || window;
            if (w._pvNotifEnabled || (w.Notification && w.Notification.permission === "granted")) {{
                sigs.forEach(function(s){{
                    if (w._pvNotify) {{
                        w._pvNotify(
                            (s.side==="BUY"?"🟢 BUY":"🔴 SELL")+" — "+s.symbol+" ("+s.strength+"%)",
                            "Entry ₹"+s.entry+"  T1 ₹"+s.t1+"  SL ₹"+s.sl+"  R:R "+s.rr+"x",
                            "pv-"+s.symbol
                        );
                    }} else {{
                        var n = new w.Notification(
                            (s.side==="BUY"?"🟢 BUY":"🔴 SELL")+" — "+s.symbol+" ("+s.strength+"%)",
                            {{body:"Entry ₹"+s.entry+"  T1 ₹"+s.t1+"  SL ₹"+s.sl+"  R:R "+s.rr+"x",
                              icon:"/static/icon-192.png",tag:"pv-"+s.symbol,requireInteraction:false}}
                        );
                    }}
                }});
            }}
        }})();
        </script>
        """, unsafe_allow_html=True)

    st.caption("⚠️ Signals from CPR Scanner (15Min + 1Hour). Frank Ochoa Pivot methodology. Not financial advice.")


def _trade_buttons(s: dict):
    """
    Trade action buttons with market hours awareness.
    - During market hours (9:15–15:30 IST): Live trade buttons (Groww/Zerodha/Upstox) are active
    - Outside market hours: Live buttons show 'Market Closed' state, Paper trade always available
    """
    from datetime import timezone
    bull    = s["side"] == "BUY"
    sym     = s["symbol"]
    mkt_open = is_market_open()

    IST     = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)

    # Next market open message
    if not mkt_open:
        if now_ist.weekday() >= 5:
            next_open = "Opens Monday 9:15 AM IST"
        elif now_ist.hour < 9 or (now_ist.hour == 9 and now_ist.minute < 15):
            next_open = f"Opens today at 9:15 AM IST"
        else:
            next_open = "Opens tomorrow 9:15 AM IST"
    else:
        next_open = ""

    # Market status badge
    if mkt_open:
        st.markdown(
            "<div style='font-family:DM Mono,monospace;font-size:0.65rem;"
            "color:#2d7a3a;margin-bottom:4px;display:flex;align-items:center;gap:5px;'>"
            "<span style='width:6px;height:6px;background:#2d7a3a;border-radius:50%;"
            "display:inline-block;animation:pulse 1.5s infinite;'></span>"
            "NSE Open · Live trading available</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"<div style='font-family:DM Mono,monospace;font-size:0.65rem;"
            f"color:#b8860b;margin-bottom:4px;display:flex;align-items:center;gap:5px;'>"
            f"<span style='width:6px;height:6px;background:#b8860b;border-radius:50%;"
            f"display:inline-block;'></span>"
            f"Market Closed · {next_open} · Paper trade available</div>",
            unsafe_allow_html=True,
        )

    # ── Button row ────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    groww_url  = f"https://groww.in/stocks/{sym.lower()}-share-price"
    kite_url   = f"https://kite.zerodha.com/orders?exchange=NSE&tradingsymbol={sym}&transaction_type={s['side']}"
    upstox_url = f"https://login.upstox.com/?tradingsymbol={sym}&exchange=NSE&transaction_type={s['side']}"

    if mkt_open:
        # ── MARKET OPEN — full live trade buttons ─────────────────────────
        with c1:
            ac = "#00b386" if bull else "#e74c3c"
            st.markdown(
                f"<a href='{groww_url}' target='_blank' style='"
                f"display:block;text-align:center;padding:7px 0;"
                f"background:{ac};color:#fff;border-radius:7px;"
                f"font-size:0.75rem;font-weight:700;text-decoration:none;"
                f"font-family:DM Sans,sans-serif;'>"
                f"{'🟢 Groww' if bull else '🔴 Groww'}</a>",
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(
                f"<a href='{kite_url}' target='_blank' style='"
                f"display:block;text-align:center;padding:7px 0;"
                f"background:#387ed1;color:#fff;border-radius:7px;"
                f"font-size:0.75rem;font-weight:700;text-decoration:none;"
                f"font-family:DM Sans,sans-serif;'>"
                f"⚡ Zerodha</a>",
                unsafe_allow_html=True,
            )
        with c3:
            st.markdown(
                f"<a href='{upstox_url}' target='_blank' style='"
                f"display:block;text-align:center;padding:7px 0;"
                f"background:#7c3aed;color:#fff;border-radius:7px;"
                f"font-size:0.75rem;font-weight:700;text-decoration:none;"
                f"font-family:DM Sans,sans-serif;'>"
                f"💜 Upstox</a>",
                unsafe_allow_html=True,
            )
    else:
        # ── MARKET CLOSED — greyed out with tooltip ───────────────────────
        closed_style = (
            "display:block;text-align:center;padding:7px 0;"
            "background:#e8eddf;color:#8a9a78;border-radius:7px;"
            "font-size:0.7rem;font-weight:600;text-decoration:none;"
            "font-family:DM Sans,sans-serif;cursor:not-allowed;"
            "border:1px dashed #c4d49a;"
        )
        with c1:
            st.markdown(
                f"<div title='{next_open}' style='{closed_style}'>"
                f"🔒 Groww</div>",
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(
                f"<div title='{next_open}' style='{closed_style}'>"
                f"🔒 Zerodha</div>",
                unsafe_allow_html=True,
            )
        with c3:
            st.markdown(
                f"<div title='{next_open}' style='{closed_style}'>"
                f"🔒 Upstox</div>",
                unsafe_allow_html=True,
            )

    # ── Paper Trade — always available ────────────────────────────────────
    with c4:
        if st.button("📝 Paper", key=f"paper_{sym}_{s['side']}_{s['tf']}", use_container_width=True):
            balance = st.session_state.get("paper_balance", 100000.0)
            try:
                live = round(float(yf.Ticker(sym+".NS").fast_info.last_price), 2)
            except:
                live = s["entry"]
            qty  = max(1, int(balance * 0.05 / max(live, 1)))
            cost = round(live * qty, 2)
            if cost > balance:
                qty  = max(1, int(balance * 0.02 / max(live, 1)))
                cost = round(live * qty, 2)
            if cost > balance:
                st.error("Insufficient virtual balance.")
            else:
                trade = {
                    "id":     len(st.session_state.get("paper_trades", [])) + 1,
                    "symbol": sym,
                    "side":   s["side"],
                    "entry":  live,
                    "qty":    qty,
                    "target": s["t1"],
                    "sl":     s["sl"],
                    "rr":     s["rr1"],
                    "cost":   cost,
                    "status": "OPEN",
                    "pnl":    0.0,
                    "exit_px": None,
                    "source": f"Scanner {s['tf']}",
                    "time":   datetime.now().strftime("%d %b %H:%M"),
                }
                if "paper_trades" not in st.session_state:
                    st.session_state["paper_trades"] = []
                st.session_state["paper_trades"].append(trade)
                st.session_state["paper_balance"] = balance - cost
                st.toast(f"📝 {s['side']} {qty}×{sym} @ ₹{live} → Test Trade tab", icon="✅")

    st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)


def page_broker_settings():
    st.markdown("""
    <div class="title-bar">
        <span style="font-size:1.5rem;">⚙️</span>
        <h1>Broker & Data Feed Settings</h1>
    </div>
    """, unsafe_allow_html=True)

    # ── DATA FEED STATUS ──────────────────────────────────────────────────
    upstox_ok = _upstox_connected()
    st.markdown(
        f"<div style='background:{'#e4f5e8' if upstox_ok else '#fdf3d4'};"
        f"border:1.5px solid {'#8dcc9a' if upstox_ok else '#e0c060'};"
        f"border-radius:10px;padding:0.85rem 1.1rem;margin-bottom:1.25rem;"
        f"font-family:DM Mono,monospace;font-size:0.82rem;'>"
        f"<b>{'✅ Upstox Live Data Feed ACTIVE' if upstox_ok else '⚪ Live Data Feed: Not connected (using yfinance)'}</b><br>"
        f"<span style='color:#4a5e32;font-size:0.75rem;'>"
        f"{'All market data (indices, charts, scanner) now uses Upstox real-time feed' if upstox_ok else 'Connect Upstox below for free NSE real-time data'}"
        f"</span></div>",
        unsafe_allow_html=True,
    )

    tab_upstox, tab_zerodha, tab_groww = st.tabs(["📡 Upstox (Data + Trading)", "⚡ Zerodha Kite", "🟢 Groww"])

    # ══════════════════════════════
    #  UPSTOX — Data Feed + Trading
    # ══════════════════════════════
    with tab_upstox:
        st.markdown("### 📡 Upstox — Free Live Data + Trading")
        st.info("""
**Upstox Free API gives you:**
- ✅ Real-time LTP (last traded price)
- ✅ OHLCV historical data (all timeframes)
- ✅ Market depth (bids/asks)
- ✅ Index data (Nifty 50, Bank Nifty, Sensex)
- ✅ Place orders programmatically
- ✅ Free tier — no monthly charge

**Setup (5 minutes):**
1. Go to [upstox.com/developer](https://upstox.com/developer/api-documentation/introduction/)
2. Login → My Apps → **Create New App**
3. Add ALL these Redirect URIs in your Upstox app:
   `http://localhost:8501`  `http://127.0.0.1:8501`  `https://your-app.streamlit.app`
4. Copy **API Key** and **API Secret**
5. Generate **Access Token** (see steps below)
        """)

        st.markdown("#### Step 1 — Enter API credentials")
        c1, c2 = st.columns(2)
        with c1:
            uak  = st.text_input("API Key",    value=st.session_state.get("upstox_api_key",""),    key="up_ak",  type="password", placeholder="your-api-key")
        with c2:
            uaks = st.text_input("API Secret", value=st.session_state.get("upstox_api_secret",""), key="up_aks", type="password", placeholder="your-api-secret")

        st.markdown("#### Step 2 — Generate Access Token")
        if uak and uaks:
            _redir   = _upstox_redirect_uri()
            auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={uak}&redirect_uri={_redir}"
            st.markdown(
                f"<a href='{auth_url}' target='_blank' style='"
                f"display:inline-block;background:#7c3aed;color:#fff;"
                f"padding:9px 20px;border-radius:7px;font-size:0.85rem;"
                f"font-weight:700;text-decoration:none;font-family:DM Sans,sans-serif;'>"
                f"🔐 Login to Upstox & Authorize</a>",
                unsafe_allow_html=True,
            )
            st.markdown("""
<small style='color:#4a5e32;font-family:DM Mono,monospace;'>
After clicking above:<br>
1. Login with your Upstox account<br>
2. Copy the <b>code=</b> value from the redirect URL<br>
   Example: <code>http://localhost:8501?code=<b>ABC123XYZ</b></code><br>
3. Paste the code below and click Generate Token
</small>
            """, unsafe_allow_html=True)

            auth_code = st.text_input("Authorization Code (from redirect URL)", key="up_auth_code", placeholder="ABC123XYZ")

            if st.button("🔑 Generate Access Token", use_container_width=True, key="btn_gen_token"):
                if auth_code.strip():
                    try:
                        import base64
                        credentials = base64.b64encode(f"{uak}:{uaks}".encode()).decode()
                        r = requests.post(
                            "https://api.upstox.com/v2/login/authorization/token",
                            headers={
                                "Authorization": f"Basic {credentials}",
                                "Content-Type": "application/x-www-form-urlencoded",
                                "Accept": "application/json",
                            },
                            data={
                                "code":         auth_code.strip(),
                                "grant_type":   "authorization_code",
                                "redirect_uri": _upstox_redirect_uri(),
                            },
                            timeout=10,
                        )
                        if r.status_code == 200:
                            token = r.json().get("access_token","")
                            if token:
                                st.session_state["upstox_access_token"] = token
                                st.session_state["upstox_api_key"]      = uak
                                st.session_state["upstox_api_secret"]   = uaks
                                st.session_state["broker"]              = "upstox"
                                st.session_state["broker_connected"]    = True
                                st.success("✅ Access token generated! Upstox live data feed is now active.")
                                st.rerun()
                            else:
                                st.error(f"Token not found in response: {r.json()}")
                        else:
                            st.error(f"Error {r.status_code}: {r.text[:300]}")
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.warning("Paste the authorization code first.")

        st.markdown("#### Or — paste an existing Access Token directly")
        uat = st.text_input("Access Token (if you already have one)", value=st.session_state.get("upstox_access_token",""), key="up_at", type="password")
        if st.button("💾 Save Token & Activate Feed", use_container_width=True, key="save_up_token"):
            if uat.strip():
                # Verify token works
                r = requests.get(
                    f"{UPSTOX_BASE}/profile",
                    headers={"Authorization": f"Bearer {uat.strip()}", "Accept": "application/json"},
                    timeout=5,
                )
                if r.status_code == 200:
                    profile = r.json().get("data", {})
                    name = profile.get("name", "User")
                    st.session_state.update({
                        "upstox_access_token": uat.strip(),
                        "upstox_api_key":      uak,
                        "upstox_api_secret":   uaks,
                        "broker":              "upstox",
                        "broker_connected":    True,
                    })
                    # Clear caches so new data source takes effect
                    st.cache_data.clear()
                    st.success(f"✅ Connected as **{name}**! Live data feed activated. All caches cleared.")
                    st.rerun()
                else:
                    st.error(f"Token invalid ({r.status_code}). Generate a fresh one above.")
            else:
                st.error("Enter the access token.")

        if _upstox_connected():
            if st.button("🔌 Disconnect Upstox", key="btn_disconnect_up"):
                st.session_state.update({
                    "upstox_access_token": "",
                    "broker": "none",
                    "broker_connected": False,
                })
                st.cache_data.clear()
                st.info("Disconnected. Switched back to yfinance.")
                st.rerun()

        st.markdown("""
        **Note:** Access token expires daily. You need to regenerate it each day.
        For auto-renewal, consider running a daily script using the Upstox OAuth flow.
        """)

    # ══════════════════════════════
    #  ZERODHA
    # ══════════════════════════════
    with tab_zerodha:
        st.markdown("### ⚡ Zerodha Kite")
        st.info("📋 Go to kite.zerodha.com/apps → Create app → Copy API Key & Secret. Access token regenerates daily after login.")
        c1, c2 = st.columns(2)
        with c1:
            zak  = st.text_input("API Key",    value=st.session_state.get("zerodha_api_key",""),    key="zk_ak",  type="password")
        with c2:
            zaks = st.text_input("API Secret", value=st.session_state.get("zerodha_api_secret",""), key="zk_aks", type="password")
        zat  = st.text_input("Access Token",   value=st.session_state.get("zerodha_access_token",""), key="zk_at", type="password")
        if st.button("💾 Save Zerodha Config", use_container_width=True, key="save_zk"):
            st.session_state.update({"zerodha_api_key":zak,"zerodha_api_secret":zaks,
                                     "zerodha_access_token":zat,"broker":"zerodha",
                                     "broker_connected": bool(zak and zat)})
            st.success("✅ Zerodha saved!" if zak and zat else "⚠️ Enter API key and access token.")
        st.markdown("""
        ```
        pip install kiteconnect
        ```
        """)

    # ══════════════════════════════
    #  GROWW
    # ══════════════════════════════
    with tab_groww:
        st.markdown("### 🟢 Groww")
        st.info("ℹ️ Groww does not have a public trading API. Signal cards open Groww web/app — you place the order manually (one tap).")
        if st.button("✅ Use Groww (web links)", use_container_width=True, key="set_groww"):
            st.session_state["broker"] = "groww"
            st.session_state["broker_connected"] = True
            st.success("Groww selected. Signal cards will link to Groww stock pages.")

    # ── Status summary ────────────────────────────────────────────────────
    st.divider()
    connected = st.session_state.get("broker_connected", False)
    bname     = st.session_state.get("broker","none").replace("upstox","Upstox").replace("zerodha","Zerodha").replace("groww","Groww").replace("none","None")
    up_active = _upstox_connected()
    st.markdown(
        f"<div style='padding:0.85rem 1.1rem;"
        f"background:{'#e4f5e8' if up_active else ('#fdf3d4' if connected else '#f5f8ed')};"
        f"border:1.5px solid {'#8dcc9a' if up_active else ('#e0c060' if connected else '#b8c89a')};"
        f"border-radius:10px;font-family:DM Mono,monospace;font-size:0.82rem;'>"
        f"{'📡 Data Feed: Upstox LIVE  ·  Trading: ' + bname if up_active else ('⚙️ Broker: ' + bname + ' (no live data feed)' if connected else '⚪ No broker connected')}"
        f"</div>",
        unsafe_allow_html=True,
    )


def page_paper_trading():
    st.markdown("""
    <div class="title-bar">
        <span style="font-size:1.5rem;">📝</span>
        <h1 style="color:#1a1f0e;">Test Trading</h1>
        <span style="margin-left:auto;background:#fdf9ec;border:1px solid #f0d898;
                     color:#b8860b;padding:3px 12px;border-radius:20px;
                     font-family:DM Mono,monospace;font-size:0.72rem;font-weight:700;">
            PAPER TRADING · VIRTUAL CAPITAL
        </span>
    </div>
    """, unsafe_allow_html=True)

    balance   = st.session_state.get("paper_balance", 100000.0)
    trades    = st.session_state.get("paper_trades", [])
    closed    = [t for t in trades if t.get("status") == "CLOSED"]
    open_tr   = [t for t in trades if t.get("status") == "OPEN"]
    wins      = [t for t in closed if t.get("pnl",0) > 0]
    losses    = [t for t in closed if t.get("pnl",0) <= 0]
    win_rate  = round(len(wins)/len(closed)*100) if closed else 0
    total_pnl = sum(t.get("pnl",0) for t in closed)

    mc1,mc2,mc3,mc4,mc5 = st.columns(5)
    mc1.metric("💰 Virtual Balance",  f"₹{balance:,.0f}")
    mc2.metric("📈 Total P&L",        f"₹{total_pnl:+,.0f}")
    mc3.metric("🔄 Open Trades",      len(open_tr))
    mc4.metric("✅ Win Rate",         f"{win_rate}%", f"{len(wins)}W/{len(losses)}L")
    mc5.metric("📊 Total Trades",     len(trades))
    st.divider()

    # ── Place new trade ───────────────────────────────────────────────────
    with st.expander("➕ Place Paper Trade", expanded=len(trades)==0):
        syms = ["RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN",
                "BAJFINANCE","BHARTIARTL","KOTAKBANK","LT","ASIANPAINT","AXISBANK",
                "MARUTI","TITAN","SUNPHARMA","WIPRO","HCLTECH","NTPC","ADANIENT"]
        pc1,pc2,pc3 = st.columns(3)
        with pc1:
            sym  = st.selectbox("Symbol", syms, key="pt_sym")
            side = st.radio("Direction", ["BUY","SELL"], horizontal=True, key="pt_side")
        with pc2:
            try:    live = round(float(yf.Ticker(sym+".NS").fast_info.last_price),2)
            except: live = 0.0
            entry  = st.number_input("Entry ₹",  value=live,                  step=0.05, key="pt_entry")
            qty    = st.number_input("Quantity",  value=1, min_value=1,        step=1,    key="pt_qty")
        with pc3:
            target = st.number_input("Target ₹", value=round(live*1.03,2),    step=0.05, key="pt_tgt")
            sl     = st.number_input("Stop Loss ₹",value=round(live*0.98,2),  step=0.05, key="pt_sl")

        risk   = max(abs(entry-sl)*qty, 0.01)
        reward = abs(target-entry)*qty
        rr     = round(reward/risk,2)
        cost   = round(entry*qty,2)
        st.markdown(
            f"<div style='background:#f7f9f2;border:1px solid #dae0cb;border-radius:8px;"
            f"padding:0.6rem 1rem;font-family:DM Mono,monospace;font-size:0.78rem;"
            f"display:flex;gap:1.5rem;flex-wrap:wrap;margin:0.5rem 0;'>"
            f"<span>💰 Cost <b>₹{cost:,.0f}</b></span>"
            f"<span>⚠️ Risk <b>₹{round(risk,0):,.0f}</b></span>"
            f"<span>🎯 Reward <b>₹{round(reward,0):,.0f}</b></span>"
            f"<span>📊 R:R <b>{rr}x</b></span></div>",
            unsafe_allow_html=True)

        if st.button("📝 Place Paper Trade", use_container_width=True, key="pt_place"):
            if cost > balance:
                st.error(f"Insufficient balance. Need ₹{cost:,.0f}, have ₹{balance:,.0f}")
            else:
                trade = {"id":len(trades)+1,"symbol":sym,"side":side,"entry":entry,
                         "qty":qty,"target":target,"sl":sl,"rr":rr,"cost":cost,
                         "status":"OPEN","pnl":0.0,"exit_px":None,
                         "source":"Manual","time":datetime.now().strftime("%d %b %H:%M")}
                st.session_state["paper_trades"].append(trade)
                st.session_state["paper_balance"] -= cost
                st.success(f"✅ Opened: {side} {qty}×{sym} @ ₹{entry}")
                st.rerun()

    # ── Open positions ────────────────────────────────────────────────────
    if open_tr:
        st.markdown("### 📂 Open Positions")
        for t in open_tr:
            try:    cur = round(float(yf.Ticker(t["symbol"]+".NS").fast_info.last_price),2)
            except: cur = t["entry"]
            upnl = round((cur-t["entry"])*t["qty"] if t["side"]=="BUY" else (t["entry"]-cur)*t["qty"],2)
            pct  = round(upnl/t["cost"]*100,2)
            col  = "#2d7a3a" if upnl>=0 else "#c0392b"
            flag = " 🎯 TARGET" if (t["side"]=="BUY" and cur>=t["target"]) or (t["side"]=="SELL" and cur<=t["target"])                    else (" 🛑 SL HIT" if (t["side"]=="BUY" and cur<=t["sl"]) or (t["side"]=="SELL" and cur>=t["sl"]) else "")
            st.markdown(
                f"<div style='background:#fff;border:1px solid #dae0cb;border-radius:10px;"
                f"padding:0.9rem 1.1rem;margin-bottom:0.5rem;"
                f"border-left:4px solid {'#2d7a3a' if t['side']=='BUY' else '#c0392b'};'>"
                f"<div style='font-family:DM Mono,monospace;display:flex;flex-wrap:wrap;gap:12px;align-items:center;'>"
                f"<b style='font-size:1rem;color:#1a1f0e;'>{t['symbol']}</b>"
                f"<span style='background:{'#edf7ee' if t['side']=='BUY' else '#fdf0ee'};"
                f"color:{'#2d7a3a' if t['side']=='BUY' else '#c0392b'};"
                f"border-radius:20px;padding:1px 8px;font-size:0.7rem;font-weight:700;'>{t['side']}</span>"
                f"<span style='font-size:0.8rem;color:#5a6a48;'>{t['qty']}× ₹{t['entry']} → ₹{cur}</span>"
                f"<span style='font-weight:700;color:{col};'>{'+'if upnl>=0 else''}₹{upnl:,.2f} ({pct:+.1f}%){flag}</span>"
                f"<span style='font-size:0.7rem;color:#8a9a78;'>T:₹{t['target']} SL:₹{t['sl']} R:R {t['rr']}x</span>"
                f"</div></div>", unsafe_allow_html=True)
            ec1,ec2 = st.columns([3,1])
            with ec1:
                xpx = st.number_input(f"Exit price (#{t['id']})", value=cur, step=0.05, key=f"xpx_{t['id']}")
            with ec2:
                st.markdown("<div style='padding-top:1.7rem;'>", unsafe_allow_html=True)
                if st.button("Close", key=f"close_{t['id']}", use_container_width=True):
                    for j,tt in enumerate(st.session_state["paper_trades"]):
                        if tt["id"]==t["id"] and tt["status"]=="OPEN":
                            fpnl = round((xpx-tt["entry"])*tt["qty"] if tt["side"]=="BUY" else (tt["entry"]-xpx)*tt["qty"],2)
                            st.session_state["paper_trades"][j].update({"status":"CLOSED","pnl":fpnl,"exit_px":xpx})
                            st.session_state["paper_balance"] += tt["cost"]+fpnl
                            (st.success if fpnl>=0 else st.error)(f"Closed {tt['symbol']}: {'+'if fpnl>=0 else''}₹{fpnl:,.2f}")
                            st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)

    # ── Closed trade history ──────────────────────────────────────────────
    if closed:
        st.markdown("### 📋 Trade History")
        avg_win  = round(sum(t["pnl"] for t in wins)/max(len(wins),1),2)
        avg_loss = round(sum(t["pnl"] for t in losses)/max(len(losses),1),2)
        pf       = round(abs(sum(t["pnl"] for t in wins))/max(abs(sum(t["pnl"] for t in losses)),0.01),2)
        exp      = round(total_pnl/len(closed),2)
        sc1,sc2,sc3,sc4 = st.columns(4)
        sc1.metric("Avg Win",      f"₹{avg_win:+,.0f}")
        sc2.metric("Avg Loss",     f"₹{avg_loss:+,.0f}")
        sc3.metric("Profit Factor",f"{pf}x")
        sc4.metric("Expectancy",   f"₹{exp:+,.0f}")
        rows = [{"#":t["id"],"Symbol":t["symbol"],"Side":t["side"],"Qty":t["qty"],
                 "Entry":f"₹{t['entry']:,.2f}","Exit":f"₹{t['exit_px']:,.2f}" if t["exit_px"] else "—",
                 "P&L":f"{'+'if t['pnl']>=0 else''}₹{t['pnl']:,.2f}",
                 "R:R":f"{t['rr']}x","Source":t.get("source","Manual"),"Time":t.get("time","—")}
                for t in reversed(closed)]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        if len(closed)>=2:
            cum,run = [],0
            for t in closed:
                run+=t["pnl"]; cum.append({"Trade":t["id"],"Cumulative P&L":round(run,2)})
            st.line_chart(pd.DataFrame(cum).set_index("Trade"), color="#4e6130", use_container_width=True)

    st.divider()
    if st.button("🔄 Reset Paper Account (₹1,00,000)", use_container_width=True, key="reset_paper"):
        st.session_state.update({"paper_trades":[],"paper_balance":100000.0,"paper_positions":{}})
        st.success("Paper account reset!"); st.rerun()
    st.caption("⚠️ Paper trading uses virtual capital only. Not real trades. For strategy testing purposes only.")


def render_sidebar():
    PAGES = [
        ("📊", "Market",     "Market Snapshot"),
        ("📈", "Pivot Boss", "Pivot Boss Analysis"),
        ("📡", "Scanner",    "CPR Scanner"),
        ("🔔", "Signals",    "Trade Signals"),
        ("📝", "Test Trade", "Paper Trading"),
        ("⚙️",  "Broker",    "Broker Settings"),
        ("⭐", "Watchlist",  "Watchlist"),
    ]

    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "Market Snapshot"
    current = st.session_state["current_page"]

    st.markdown("""
    <style>
    .block-container { padding-top: 0.25rem !important; }
    section[data-testid="stSidebar"]        { display: none !important; }
    [data-testid="collapsedControl"]         { display: none !important; }
    button[data-testid="baseButton-header"]  { display: none !important; }
    .nav-btn > div > button {
        background: transparent !important;
        border: none !important;
        border-bottom: 3px solid transparent !important;
        border-radius: 0 !important;
        box-shadow: none !important;
        color: #5a6a48 !important;
        font-family: DM Sans, sans-serif !important;
        font-size: 0.78rem !important;
        font-weight: 600 !important;
        padding: 0.45rem 0.2rem !important;
        width: 100% !important;
        transition: color 0.15s, border-color 0.15s !important;
        white-space: nowrap !important;
    }
    .nav-btn > div > button:hover {
        color: #3a4a22 !important;
        border-bottom-color: #7a9651 !important;
        background: rgba(97,122,61,0.05) !important;
    }
    .nav-btn-active > div > button {
        color: #3a4a22 !important;
        border-bottom-color: #4e6130 !important;
        font-weight: 800 !important;
        background: rgba(78,97,48,0.07) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns([1.6] + [1]*len(PAGES) + [1.3])

    with cols[0]:
        st.markdown(
            "<div style='padding:0.45rem 0.4rem 0.3rem;white-space:nowrap;'>"
            "<span style='font-family:DM Sans,sans-serif;font-weight:900;"
            "font-size:0.92rem;color:#1a1f0e;'>🏦 PivotVault</span>"
            "<span style='font-family:DM Sans,sans-serif;font-weight:900;"
            "font-size:0.92rem;color:#4e6130;'> AI</span></div>",
            unsafe_allow_html=True,
        )

    for i, (icon, short, page_key) in enumerate(PAGES):
        with cols[i + 1]:
            cls = "nav-btn-active" if current == page_key else "nav-btn"
            st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
            if st.button(f"{icon} {short}", key=f"nav_{i}", use_container_width=True):
                st.session_state["current_page"] = page_key
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with cols[-1]:
        wl    = len(st.session_state.get("watchlist", []))
        uname = st.session_state.get("username", "")[:10]
        st.markdown(
            f"<div style='font-family:DM Mono,monospace;font-size:0.62rem;"
            f"color:#5a6a48;text-align:right;padding-top:0.15rem;'>"
            f"👤{uname} ⭐{wl}</div>",
            unsafe_allow_html=True,
        )
        if st.button("🚪 Logout", key="top_logout", use_container_width=True):
            st.session_state["logged_in"] = False
            st.session_state["current_page"] = "Market Snapshot"
            st.rerun()

    st.markdown("<hr style='margin:0 0 0.5rem 0;border-color:#dae0cb;'>",
                unsafe_allow_html=True)
    return current


def main():
    if not st.session_state["logged_in"]:
        page_login()
        return

    page = render_sidebar()
    render_market_header()
    st.divider()
    nse500 = fetch_nse500_list()

    if   page == "Market Snapshot":     page_market_snapshot(nse500)
    elif page == "Pivot Boss Analysis": page_pivot_boss(nse500)
    elif page == "CPR Scanner":         page_cpr_scanner(nse500)
    elif page == "Trade Signals":       page_trade_signals(nse500)
    elif page == "Paper Trading":       page_paper_trading()
    elif page == "Broker Settings":     page_broker_settings()
    elif page == "Watchlist":           page_watchlist()
    else:                               page_market_snapshot(nse500)


if __name__ == "__main__":
    main()