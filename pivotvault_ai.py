import streamlit as st
import pandas as pd
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False
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
try:
    from tv_chart import render_tv_chart, render_tv_screener_chart
    _TV_CHARTS = True
except ImportError:
    _TV_CHARTS = False

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
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

/* 1. Root variables */
:root {
    --bg:       #f0f4f8;
    --surface:  #ffffff;
    --border:   #dce3ed;
    --accent:   #1a6b3c;
    --danger:   #dc2626;
    --warn:     #d97706;
    --text:     #1a2332;
    --dim:      #5a6a80;
}

/* 2. Nuke ALL dark backgrounds Streamlit injects */
html, body { background: #f0f4f8 !important; }

.stApp                                          { background: #f0f4f8 !important; }
.stApp > div                                    { background: #f0f4f8 !important; }
.stApp [data-testid="stAppViewContainer"]       { background: #f0f4f8 !important; }
.stApp [data-testid="stMain"]                   { background: #f0f4f8 !important; }
.stApp [data-testid="stMainBlockContainer"]     { background: #f0f4f8 !important; }
.stApp [data-testid="stVerticalBlockBorderWrapper"]  { background: #f0f4f8 !important; }
[data-testid="stVerticalBlock"]                 { background: transparent !important; }
.block-container                                { background: #f0f4f8 !important; padding: 1.5rem 2rem 2rem; max-width: 1500px; }

/* Emotion cache class sweep (Streamlit changes these but !important overrides) */
[class^="css-"], [class*=" css-"] { background-color: inherit; }

/* 3. Global font & text */
html, body, .stApp, .stMarkdown, .stText,
p, span, label, li {
    font-family: 'IBM Plex Sans', sans-serif !important;
    color: #1a2332 !important;
}
/* Set div font/color without overriding button internals */
.stMarkdown div, .stText div, [data-testid="stVerticalBlock"] > div {
    font-family: 'IBM Plex Sans', sans-serif;
    color: #1a2332;
}

/* 4. Hide only chrome decorations — never touch sidebar */
#MainMenu { visibility: hidden !important; }
footer    { visibility: hidden !important; }

/* 5. SIDEBAR */
section[data-testid="stSidebar"] {
    background: #1e293b !important;
    min-width: 220px !important;
}
section[data-testid="stSidebar"] > div {
    background: #1e293b !important;
    padding-top: 1rem !important;
}
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] div {
    color: #cbd5e1 !important;
    font-family: 'IBM Plex Mono', monospace !important;
}
section[data-testid="stSidebar"] hr {
    border-color: #334155 !important;
}
/* Radio nav items */
section[data-testid="stSidebar"] .stRadio > div {
    gap: 4px !important;
}
section[data-testid="stSidebar"] .stRadio label {
    background: transparent !important;
    border-radius: 8px !important;
    padding: 0.6rem 0.9rem !important;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    color: #94a3b8 !important;
    cursor: pointer !important;
    width: 100% !important;
    transition: background 0.15s !important;
}
section[data-testid="stSidebar"] .stRadio label:hover {
    background: #334155 !important;
    color: #e2e8f0 !important;
}
/* Selected radio item */
section[data-testid="stSidebar"] .stRadio label[data-baseweb="radio"]:has(input:checked),
section[data-testid="stSidebar"] .stRadio [aria-checked="true"] ~ label {
    background: #1a6b3c !important;
    color: #ffffff !important;
}
/* Logout button */
section[data-testid="stSidebar"] .stButton > div > button {
    background: #334155 !important;
    border: 1px solid #475569 !important;
    color: #cbd5e1 !important;
    width: 100% !important;
}
section[data-testid="stSidebar"] .stButton > div > button:hover {
    background: #475569 !important;
    color: #ffffff !important;
}

/* 6. METRIC CARDS */
div[data-testid="metric-container"] {
    background: #ffffff !important;
    border: 1px solid #dce3ed !important;
    border-radius: 10px !important;
    padding: 1rem 1.25rem !important;
    box-shadow: 0 1px 6px rgba(0,0,0,0.06) !important;
}
div[data-testid="metric-container"] label  { color: #5a6a80 !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.72rem !important; letter-spacing: 0.08em !important; text-transform: uppercase !important; }
div[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #1a2332 !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.4rem !important; font-weight: 600 !important; }
div[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.78rem !important; }

/* 7. BUTTONS — suppress ALL wrapper styles that cause double-button appearance */
/* Kill the wrapper div completely */
.stButton { background: transparent !important; border: none !important; padding: 0 !important; }
.stButton > div,
.stButton [data-testid="baseButton-secondary"],
.stButton [data-testid="baseButton-primary"] {
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    box-shadow: none !important;
}
/* Style only the actual <button> element */
.stButton > button,
.stButton > div > button,
button[kind="secondary"],
button[kind="primary"] {
    background: #ffffff !important;
    border: 1.5px solid #1a6b3c !important;
    color: #1a6b3c !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.06em !important;
    border-radius: 6px !important;
    padding: 0.4rem 1rem !important;
    transition: background 0.2s, color 0.2s !important;
    outline: none !important;
    box-shadow: none !important;
    cursor: pointer !important;
    width: 100% !important;
}
.stButton > button:hover,
.stButton > div > button:hover {
    background: #1a6b3c !important;
    color: #ffffff !important;
    border-color: #1a6b3c !important;
    outline: none !important;
    box-shadow: none !important;
}
.stButton > button:focus,
.stButton > div > button:focus {
    outline: none !important;
    box-shadow: 0 0 0 3px rgba(26,107,60,0.15) !important;
}
.stButton > button:active,
.stButton > div > button:active {
    transform: scale(0.98) !important;
}
/* Sidebar buttons override */
section[data-testid="stSidebar"] .stButton > button,
section[data-testid="stSidebar"] .stButton > div > button {
    background: #2d4a60 !important;
    border: 1px solid #3d6080 !important;
    color: #c8d8e8 !important;
}
section[data-testid="stSidebar"] .stButton > button:hover,
section[data-testid="stSidebar"] .stButton > div > button:hover {
    background: #3d6080 !important;
    color: #ffffff !important;
}

/* 8. SELECTBOXES */
div[data-baseweb="select"] > div {
    background: #ffffff !important;
    border: 1.5px solid #c8d4e0 !important;
    border-radius: 6px !important;
    color: #1a2332 !important;
    font-family: 'IBM Plex Mono', monospace !important;
}
div[data-baseweb="select"] span,
div[data-baseweb="select"] div { color: #1a2332 !important; background: transparent !important; }
ul[data-baseweb="menu"] li,
div[data-baseweb="popover"] li {
    background: #ffffff !important;
    color: #1a2332 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.82rem !important;
}
ul[data-baseweb="menu"] li:hover  { background: #f0fdf4 !important; color: #1a6b3c !important; }
ul[data-baseweb="menu"],
div[data-baseweb="popover"] > div {
    background: #ffffff !important;
    border: 1.5px solid #c8d4e0 !important;
    border-radius: 8px !important;
    box-shadow: 0 8px 24px rgba(0,0,0,0.1) !important;
}

/* 9. TEXT INPUTS */
input[type="text"], input[type="password"],
.stTextInput input {
    background: #ffffff !important;
    border: 1.5px solid #c8d4e0 !important;
    border-radius: 6px !important;
    color: #1a2332 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.85rem !important;
}
input::placeholder { color: #8a9ab0 !important; }
input:focus {
    border-color: #1a6b3c !important;
    box-shadow: 0 0 0 3px rgba(26,107,60,0.15) !important;
    outline: none !important;
}

/* 10. DATAFRAMES */
.stDataFrame { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.8rem !important; }
[data-testid="stDataFrameContainer"]  { background: #ffffff !important; border: 1px solid #dce3ed !important; border-radius: 8px !important; }

/* 11. EXPANDER */
[data-testid="stExpander"] { background: #ffffff !important; border: 1px solid #dce3ed !important; border-radius: 8px !important; }
[data-testid="stExpander"] summary { color: #1a2332 !important; font-family: 'IBM Plex Mono', monospace !important; }
[data-testid="stExpander"] > div    { background: #ffffff !important; }

/* 12. CHECKBOX & RADIO */
.stCheckbox label, .stRadio label { color: #1a2332 !important; font-family: 'IBM Plex Mono', monospace !important; }

/* 13. ALERT / INFO / CAPTION */
.stCaption p, .stCaption { color: #5a6a80 !important; font-family: 'IBM Plex Mono', monospace !important; }
[data-testid="stInfo"]    { background: #eff6ff !important; color: #1e40af !important; border-left: 4px solid #3b82f6 !important; }
[data-testid="stWarning"] { background: #fffbeb !important; color: #92400e !important; border-left: 4px solid #f59e0b !important; }
[data-testid="stSuccess"] { background: #f0fdf4 !important; color: #166534 !important; border-left: 4px solid #22c55e !important; }
[data-testid="stError"]   { background: #fef2f2 !important; color: #991b1b !important; border-left: 4px solid #ef4444 !important; }

/* 14. DIVIDERS & HEADINGS */
h1, h2, h3 { font-family: 'IBM Plex Mono', monospace !important; letter-spacing: -0.02em !important; color: #1a2332 !important; }
hr { border-color: #dce3ed !important; margin: 1rem 0 !important; }

/* 15. COMPONENT CLASSES */
.wl-pill {
    display: inline-block;
    background: #f0fdf4; border: 1px solid #bbf7d0;
    color: #1a6b3c; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem; font-weight: 600;
    padding: 0.2rem 0.65rem; border-radius: 20px; margin: 0.2rem;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.live-dot {
    display: inline-block; width: 8px; height: 8px;
    background: #16a34a; border-radius: 50%;
    margin-right: 6px; animation: pulse 1.6s ease-in-out infinite;
}
.title-bar { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1.25rem; }
.title-bar h1 { margin: 0 !important; font-size: 1.4rem !important; color: #1a2332 !important; }
.title-bar .ts { margin-left: auto; font-family: 'IBM Plex Mono', monospace; font-size: 0.72rem; color: #5a6a80; }

.pb-card {
    background: #ffffff !important;
    border: 1px solid #dce3ed; border-radius: 10px;
    padding: 1rem 1.25rem; margin-bottom: 0.75rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.pb-card-title { font-family: 'IBM Plex Mono', monospace; font-size: 0.68rem; letter-spacing: 0.1em; text-transform: uppercase; color: #5a6a80; margin-bottom: 0.5rem; }
.pb-card-value { font-family: 'IBM Plex Mono', monospace; font-size: 1.1rem; font-weight: 600; }
.pb-bull { color: #16a34a !important; }
.pb-bear { color: #dc2626 !important; }
.pb-neut { color: #d97706 !important; }

.signal-badge {
    display: inline-block; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem; font-weight: 600; letter-spacing: 0.08em;
    text-transform: uppercase; padding: 0.25rem 0.75rem;
    border-radius: 4px; margin: 0.15rem 0.1rem;
}
.sig-bull { background: #f0fdf4; color: #16a34a; border: 1px solid #bbf7d0; }
.sig-bear { background: #fef2f2; color: #dc2626; border: 1px solid #fecaca; }
.sig-neut { background: #fffbeb; color: #d97706; border: 1px solid #fde68a; }

/* 16. MOBILE */
@media (max-width: 768px) {
    .block-container { padding: 0.75rem !important; padding-bottom: 4rem !important; max-width: 100vw !important; }
    section[data-testid="stSidebar"] { min-width: 80vw !important; max-width: 85vw !important; }
    section[data-testid="stSidebar"] .stRadio label { min-height: 48px !important; font-size: 0.95rem !important; }
}
/* Mobile nav handled inline in render_sidebar */
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────
defaults = {
    'watchlist': [],
    'cpr_scan_df':  None,
    'cpr_scan_15m': None,
    'cpr_scan_1h':  None,
    'cpr_scan_1d':  None,
    'cpr_scan_1wk': None,
    'cpr_scan_1mo': None,
    'logged_in':    False,
    'wl_data':      {},
    'wl_last_refresh': None,
    'smtp_cfg': {"host": "smtp.gmail.com", "port": 587, "sender": "", "password": ""},
    'auto_email_enabled': False,
    'auto_email_to': "",
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
    Priority: NSE API → yfinance fast_info → yfinance history (fallback)
    """
    # NSE API mapping
    NSE_MAP = {
        "^NSEI":    "NIFTY 50",
        "^BSESN":   None,          # BSE handled separately
        "^NSEBANK": "NIFTY BANK",
    }

    # ── Method 1: NSE India API (most accurate, real-time) ───────────────
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
            f"<span style='color:#94a3b8;'>{next_info}</span>"
            f"<span style='color:#94a3b8;margin-left:auto;'>"
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
def page_login():
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;
                justify-content:center;min-height:70vh;gap:1rem;">
        <div style="font-family:'IBM Plex Mono',monospace;font-size:2.8rem;
                    font-weight:600;letter-spacing:-0.03em;color:#1e293b;">
            🏦 PivotVault <span style="color:#16a34a;">AI</span>
        </div>
        <div style="font-family:'IBM Plex Mono',monospace;font-size:0.82rem;
                    color:#64748b;letter-spacing:0.1em;text-transform:uppercase;">
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
        '<div class="title-bar"><span class="live-dot"></span><h1 style="color:#1e293b;">Market Snapshot</h1>'
        f'<span class="ts" style="color:#64748b;">{datetime.now().strftime("%d %b %Y  %H:%M")}</span></div>',
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
            "letter-spacing:0.08em;text-transform:uppercase;color:#64748b;"
            "margin-bottom:0.4rem;'>"
            "<span class='live-dot'></span>"
            "Sectoral Heatmap · Nifty 500 · Colour = Avg 1-Day % Change · Click a sector for detail</div>",
            unsafe_allow_html=True,
        )
    with legend_col:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
            "color:#64748b;padding-top:0.1rem;line-height:1.9;'>"
            "<span style='color:#16a34a;'>■</span> Strong Gain<br>"
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
                    f"<span style='font-size:1rem;font-weight:700;color:#1e293b;'>"
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
                    f"letter-spacing:0.08em;text-transform:uppercase;color:#16a34a;"
                    f"margin-bottom:0.35rem;'>▲ Top 5 Gainers</div>",
                    unsafe_allow_html=True,
                )
                g_rows = [{"Symbol": r["Symbol"], "Change %": f"+{r['Change%']:.2f}%"}
                          for _, r in top_g.iterrows()]
                st.dataframe(pd.DataFrame(g_rows), use_container_width=True, hide_index=True)

            with d2:
                st.markdown(
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    f"letter-spacing:0.08em;text-transform:uppercase;color:#dc2626;"
                    f"margin-bottom:0.35rem;'>▼ Top 5 Losers</div>",
                    unsafe_allow_html=True,
                )
                l_rows = [{"Symbol": r["Symbol"], "Change %": f"{r['Change%']:.2f}%"}
                          for _, r in top_l.iterrows()]
                st.dataframe(pd.DataFrame(l_rows), use_container_width=True, hide_index=True)

    else:
        st.markdown(
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
            "color:#94a3b8;text-align:center;padding:0.55rem;"
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
    try:
        df = yf.Ticker(symbol + ".NS").history(period=period, interval=interval)
        df.index = df.index.tz_localize(None)
        return df
    except Exception:
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
        'color:#64748b;margin-left:0.5rem;">Frank Ochoa Methodology</span>'
        f'<span class="ts" style="color:#64748b;">{datetime.now().strftime("%d %b %Y  %H:%M")}</span></div>',
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
        "bull": ("#f0fdf4", "#00e5a0"),
        "bear": ("#fef2f2", "#ff4d6a"),
        "neut": ("#fffbeb", "#f5a623"),
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
    if _TV_CHARTS:
        render_tv_chart(df, symbol, tf_label, analysis, show_stoch=True, height=700)
    else:
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
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#64748b;"
            f"margin-top:0.4rem;'>{cpr_detail}</div></div>",
            unsafe_allow_html=True,
        )

    with cb:
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>3/10 Oscillator</div>"
            f"<div class='pb-card-value pb-{analysis['osc_col']}'>{analysis['osc_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#64748b;"
            f"margin-top:0.4rem;'>Ochoa's momentum gauge<br>3-MA minus 10-MA vs 16-Signal</div>"
            f"</div>", unsafe_allow_html=True,
        )

    with cc:
        st.markdown(
            f"<div class='pb-card'>"
            f"<div class='pb-card-title'>HMA(20) Trend</div>"
            f"<div class='pb-card-value pb-{analysis['hma_col']}'>{analysis['hma_sig']}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#64748b;"
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
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#64748b;"
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
            "letter-spacing:0.1em;text-transform:uppercase;color:#64748b;"
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
                        f"<b style='color:#1e293b;float:right;'>{val} "
                        f"<span style='color:#64748b;font-size:0.7rem;'>{arr}{abs(dist)}%</span>"
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
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#64748b;margin-top:0.4rem;'>"
            f"{'% of price: ' + str(analysis['atr_pct']) + '%' if analysis['atr_pct'] else ''}"
            f"</div></div>", unsafe_allow_html=True,
        )

    with v2:
        nl = analysis.get("nearest")
        st.markdown(
            f"<div class='pb-card'><div class='pb-card-title'>Nearest Pivot ★</div>"
            f"<div class='pb-card-value pb-neut'>"
            f"{nl[0] + '  ₹' + str(nl[1]) if nl else '—'}</div>"
            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.73rem;color:#64748b;margin-top:0.4rem;'>"
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
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;color:#64748b;"
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
        'color:#64748b;margin-left:0.75rem;">'
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
                    f"<div style='background:#f0fdf4;border:1px solid {col_fg}33;"
                    f"border-left:4px solid {col_fg};border-radius:8px;"
                    f"padding:1rem 1.5rem;margin:0.5rem 0;'>"

                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;"
                    f"letter-spacing:0.12em;text-transform:uppercase;color:{col_fg}88;'>"
                    f"{arrow} {auto_pat} Trade Plan  ·  {symbol}</div>"

                    f"<div style='display:flex;gap:2rem;margin-top:0.6rem;flex-wrap:wrap;'>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#64748b;text-transform:uppercase;'>Short Term (1-3d)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#1e293b;'>T: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{sh.get('target',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#dc2626;'>₹{sh.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{sh.get('rr',0)}x</b></div></div>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#64748b;text-transform:uppercase;'>Medium Term (1-4w)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#1e293b;'>T1: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{md.get('target1',0):,.2f}</span>"
                    f" T2: <span style='color:{col_fg};font-weight:700;'>₹{md.get('target2',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#dc2626;'>₹{md.get('sl',0):,.2f}</span>"
                    f" &nbsp; R:R <b>{md.get('rr',0)}x</b></div></div>"

                    f"<div><div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                    f"color:#64748b;text-transform:uppercase;'>Long Term (1-3m)</div>"
                    f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.9rem;"
                    f"color:#1e293b;'>T1: <span style='color:{col_fg};font-weight:700;'>"
                    f"₹{lg.get('target1',0):,.2f}</span>"
                    f" T2: <span style='color:{col_fg};font-weight:700;'>₹{lg.get('target2',0):,.2f}</span>"
                    f" T3: <span style='color:{col_fg};font-weight:700;'>₹{lg.get('target3',0):,.2f}</span>"
                    f" &nbsp; SL: <span style='color:#dc2626;'>₹{lg.get('sl',0):,.2f}</span>"
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
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#94a3b8;line-height:1.8;'>"
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
            "<div style='font-family:IBM Plex Mono,monospace;color:#64748b;font-size:0.85rem;"
            "padding:2rem 0;'>No stocks added yet. Use <b>Trade Signals</b> or <b>CPR Scanner</b> to find stocks.</div>",
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
                "color:#64748b;letter-spacing:0.06em;text-transform:uppercase;"
                "margin-bottom:0.5rem;'>Remove</div>", unsafe_allow_html=True)
    rm_cols = st.columns(min(len(wl), 6))
    for i, sym in enumerate(wl[:6]):
        with rm_cols[i]:
            if st.button(f"✕ {sym}", key=f"rm_{sym}"):
                st.session_state["watchlist"].remove(sym)
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
            f"<h2 style='color:#1a2332;'>PivotVault AI — CPR Scanner Auto-Report</h2>"
            f"<p style='color:#5a6a80;'>{tf_label} &nbsp;|&nbsp; {scan_time}</p>"
            f"<p>Please find the latest CPR Scanner report attached as PDF.</p>"
            f"<p style='color:#16a34a;'>Scan completed automatically. Top 10 Bullish + Top 10 Bearish stocks.</p>"
            f"<hr/>"
            f"<p style='color:#94a3b8;font-size:0.8em;'>For educational purposes only. Not financial advice.</p>"
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
        "📅 1 Day   — Swing Trading":   {"interval":"1d", "period":"90d", "tag":"1d", "refresh":14400, "color":"#1a6b3c","bg":"#f0fdf4","label":"Swing Trading",  "refresh_label":"4 hours"},
        "📆 1 Week  — Positional":      {"interval":"1wk","period":"2y",  "tag":"1wk","refresh":86400, "color":"#d97706","bg":"#fffbeb","label":"Positional",     "refresh_label":"24 hours"},
        "🗓️ 1 Month — Prime Trading":   {"interval":"1mo","period":"5y",  "tag":"1mo","refresh":86400, "color":"#dc2626","bg":"#fef2f2","label":"Prime Trading",  "refresh_label":"24 hours"},
    }

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("""
    <div style="display:flex;align-items:center;gap:14px;margin-bottom:1.25rem;
                padding:1.25rem 1.5rem;background:#ffffff;border:1px solid #dce3ed;
                border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.06);">
        <div style="font-size:2rem;">📡</div>
        <div style="flex:1;">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;
                        font-weight:700;color:#1a2332;">CPR Scanner</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:0.68rem;
                        color:#5a6a80;letter-spacing:0.08em;text-transform:uppercase;margin-top:2px;">
                Nifty 200 · All CPR Setups · Best 10 Bullish + 10 Bearish · Pivot-Based Targets
            </div>
        </div>
        <div id="countdown-wrap" style="text-align:right;font-family:'IBM Plex Mono',monospace;">
            <div style="font-size:0.62rem;color:#5a6a80;text-transform:uppercase;letter-spacing:0.07em;">Next refresh in</div>
            <div id="countdown" style="font-size:1.3rem;font-weight:700;color:#1a6b3c;">—</div>
        </div>
    </div>
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

    # ── Auto-Email Config ─────────────────────────────────────────────────────
    with st.expander("📧 Auto-Email Settings — send PDF on every scan refresh", expanded=False):
        ae1, ae2, ae3 = st.columns([1, 3, 2])
        with ae1:
            auto_on = st.toggle(
                "Auto-Send",
                value=st.session_state.get("auto_email_enabled", False),
                key="auto_email_toggle",
            )
            st.session_state["auto_email_enabled"] = auto_on
        with ae2:
            auto_to = st.text_input(
                "Send PDF to",
                value=st.session_state.get("auto_email_to", ""),
                placeholder="your@email.com",
                label_visibility="collapsed",
                key="auto_email_to_input",
            )
            st.session_state["auto_email_to"] = auto_to
        with ae3:
            smtp = st.session_state.get("smtp_cfg", {})
            if smtp.get("sender") and smtp.get("password"):
                st.markdown(
                    "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                    "color:#16a34a;padding-top:0.5rem;'>✅ SMTP configured</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                    "color:#dc2626;padding-top:0.5rem;'>⚠️ Configure SMTP below first</div>",
                    unsafe_allow_html=True,
                )
        if auto_on:
            st.markdown(
                f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.72rem;"
                f"color:#1a6b3c;padding:0.4rem 0;'>"
                f"🔄 PDF will auto-send to <b>{auto_to or '—'}</b> every time the scanner refreshes "
                f"(every <b>{cfg['refresh_label']}</b>)</div>",
                unsafe_allow_html=True,
            )

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

        # ── Auto-email PDF on every scan refresh ──────────────────────────────
        _auto_on  = st.session_state.get("auto_email_enabled", False)
        _auto_to  = st.session_state.get("auto_email_to", "").strip()
        _smtp_cfg = st.session_state.get("smtp_cfg", {})
        if _auto_on and _auto_to and _smtp_cfg.get("sender") and _smtp_cfg.get("password"):
            try:
                _scan_t = datetime.now().strftime("%d %b %Y  %H:%M")
                # Get top 10 from fresh result
                _bull = result[result["Pattern"]=="Bullish"].sort_values(["Strength%","CPR Width%"],ascending=[False,True]).head(10) if not result.empty else pd.DataFrame()
                _bear = result[result["Pattern"]=="Bearish"].sort_values(["Strength%","CPR Width%"],ascending=[False,True]).head(10) if not result.empty else pd.DataFrame()
                _pdf  = build_scanner_pdf(_bull, _bear, tf_choice, _scan_t)
                _ok, _msg = send_scanner_pdf_email(_pdf, _auto_to, tf_choice, _scan_t, _smtp_cfg)
                if _ok:
                    st.toast(f"📧 Auto-report sent to {_auto_to}", icon="✅")
                else:
                    st.toast(f"Auto-email failed: {_msg}", icon="⚠️")
            except Exception as _e:
                st.toast(f"Auto-email error: {_e}", icon="⚠️")

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
        f"font-family:IBM Plex Mono,monospace;font-size:0.72rem;color:#5a6a80;"
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
            f"<div style='text-align:center;padding:2rem;background:#f8fafc;"
            f"border:2px dashed #dce3ed;border-radius:10px;"
            f"font-family:IBM Plex Mono,monospace;font-size:0.82rem;color:#94a3b8;'>"
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
        hbg = "#f0fdf4" if is_bull else "#fef2f2"
        hbd = "#bbf7d0" if is_bull else "#fecaca"
        arr = "▲" if is_bull else "▼"

        if df.empty:
            return (f"<div style='padding:2rem;text-align:center;background:#f8fafc;"
                    f"border:2px dashed #dce3ed;border-radius:10px;"
                    f"font-family:IBM Plex Mono,monospace;font-size:0.78rem;color:#94a3b8;'>"
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
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.95rem;font-weight:700;color:#1a2332;">{row["Symbol"]}</div>'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#5a6a80;">'
                f'&#8377;{row["LTP"]:,.2f} &nbsp;·&nbsp; ATR &#8377;{row["ATR"]:,.2f} &nbsp;·&nbsp; {candle_icon} {candle}</div>'
                f'</div></div>'
                f'<div style="text-align:right;">'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:1rem;font-weight:700;color:{hc};">{prob}%</div>'
                f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.62rem;color:#5a6a80;">Strength</div>'
                f'</div></div>'
                f'<div style="background:#f1f5f9;border-radius:3px;height:5px;margin-bottom:0.5rem;">'
                f'<div style="background:{hc};width:{prob}%;height:100%;border-radius:3px;"></div></div>'
                f'<div style="display:flex;flex-wrap:wrap;gap:0.5rem;margin-bottom:0.45rem;'
                f'padding:0.4rem 0.6rem;background:#f8fafc;border-radius:6px;'
                f'font-family:IBM Plex Mono,monospace;font-size:0.68rem;">'
                f'<span style="color:#5a6a80;">Entry <b style="color:#1a2332;">&#8377;{row["Entry"]:,.2f}</b></span>'
                f'<span>|</span>'
                f'<span style="color:#5a6a80;">T1 <b style="color:{hc};">&#8377;{row["T1"]:,.2f}</b></span>'
                f'<span style="color:#5a6a80;">T2 <b style="color:{hc};">&#8377;{row["T2"]:,.2f}</b></span>'
                f'<span>|</span>'
                f'<span style="color:#5a6a80;">SL <b style="color:#dc2626;">&#8377;{row["SL"]:,.2f}</b></span>'
                f'<span>|</span>'
                f'<span style="color:#5a6a80;">R:R <b style="color:{rr_col};">{rr1}x / {rr2}x</b></span>'
                f'</div>'
                f'<div style="display:flex;flex-wrap:wrap;gap:0.3rem;">'
                f'<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a2332;">CPR {cpr_w:.3f}%</span>'
                f'<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a2332;">TC &#8377;{row["TC"]:,.2f} / BC &#8377;{row["BC"]:,.2f}</span>'
                f'<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:{hc};">HMA {row["HMA"]}</span>'
                f'<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:{rsi_c};">RSI {row["RSI"]}</span>'
                f'<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a2332;">Osc {osc}</span>'
                f'<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:0.15rem 0.45rem;font-family:IBM Plex Mono,monospace;font-size:0.67rem;color:#1a2332;">Vol {vol}</span>'
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
        "color:#1a2332;margin-bottom:0.75rem;'>📤  Send / Download Scanner Report</div>",
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
                return "<tr><td colspan='9' style='padding:8px;color:#94a3b8;font-style:italic;'>No qualifying stocks found.</td></tr>"
            hc = "#16a34a" if is_bull else "#dc2626"
            out = ""
            for _, r in df.iterrows():
                rr_c = "#16a34a" if r.get("RR1",0)>=2 else ("#d97706" if r.get("RR1",0)>=1.5 else "#dc2626")
                out += (
                    f"<tr style='border-bottom:1px solid #f1f5f9;'>"
                    f"<td style='padding:7px 5px;font-weight:700;font-family:Courier New,monospace;color:#1a2332;'>{r['Symbol']}</td>"
                    f"<td style='padding:7px 5px;font-size:0.83rem;'>Rs.{r['LTP']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:{hc};font-weight:700;'>{int(r['Strength%'])}%</td>"
                    f"<td style='padding:7px 5px;font-size:0.8rem;'>{r.get('Candle','—')}</td>"
                    f"<td style='padding:7px 5px;font-size:0.8rem;'>Rs.{r['Entry']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:{hc};'>Rs.{r['T1']:,.2f} / Rs.{r['T2']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:#dc2626;'>Rs.{r['SL']:,.2f}</td>"
                    f"<td style='padding:7px 5px;color:{rr_c};font-weight:700;'>{r.get('RR1',0)}x</td>"
                    f"<td style='padding:7px 5px;color:#5a6a80;'>{r['RSI']}</td>"
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
  <div style="font-family:Courier New,monospace;font-size:0.72rem;font-weight:700;color:#16a34a;border-left:4px solid #16a34a;padding-left:8px;margin-bottom:10px;text-transform:uppercase;letter-spacing:0.07em;">▲ BULLISH SETUPS</div>
  <table style="{TBLS}"><tr><th style="{TH}">Symbol</th><th style="{TH}">LTP</th><th style="{TH}">Score</th><th style="{TH}">Candle</th><th style="{TH}">Entry</th><th style="{TH}">T1 / T2</th><th style="{TH}">SL</th><th style="{TH}">R:R</th><th style="{TH}">RSI</th></tr>
  {_tbl_rows(bull_df, True)}</table>
  <div style="font-family:Courier New,monospace;font-size:0.72rem;font-weight:700;color:#dc2626;border-left:4px solid #dc2626;padding-left:8px;margin:18px 0 10px;text-transform:uppercase;letter-spacing:0.07em;">▼ BEARISH SETUPS</div>
  <table style="{TBLS}"><tr><th style="{TH}">Symbol</th><th style="{TH}">LTP</th><th style="{TH}">Score</th><th style="{TH}">Candle</th><th style="{TH}">Entry</th><th style="{TH}">T1 / T2</th><th style="{TH}">SL</th><th style="{TH}">R:R</th><th style="{TH}">RSI</th></tr>
  {_tbl_rows(bear_df, False)}</table>
</td></tr>
<tr><td style="padding:12px 22px 20px;"><div style="background:#f8fafc;border-radius:6px;padding:10px 14px;font-size:0.68rem;color:#94a3b8;line-height:1.6;font-family:Courier New,monospace;">⚠️ For educational purposes only. Not financial advice. Entry/Target/SL from Frank Ochoa Pivot Boss + ATR-14. Always use proper risk management.</div></td></tr>
</table></td></tr></table></body></html>"""

    rtab1, rtab2, rtab3 = st.tabs(["📧 Gmail / Email", "💬 WhatsApp", "⬇️ Download PDF"])

    with rtab1:
        st.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a80;margin-bottom:0.75rem;'>Send report to any Gmail or SMTP email inbox.</div>", unsafe_allow_html=True)
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
        st.markdown("<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a80;margin-bottom:0.75rem;'>Share scanner results via WhatsApp.</div>", unsafe_allow_html=True)
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
            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a80;"
            "margin-bottom:0.75rem;'>Download the scanner report as a PDF. "
            "Use <b>Auto-Email Settings</b> above to send automatically on every refresh.</div>",
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
    <div style="background:#f8fafc;border:1px solid #dce3ed;border-radius:10px;
                padding:0.9rem 1.1rem;margin-top:0.75rem;
                font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#5a6a80;line-height:1.9;">
    <b style="color:#1a2332;">Auto-Refresh Schedule</b><br>
    ⚡ 15 Min chart → refreshes every <b>15 minutes</b> &nbsp;|&nbsp;
    🕐 1 Hour chart → refreshes every <b>1 hour</b> &nbsp;|&nbsp;
    📅 1 Day chart → refreshes every <b>4 hours</b> &nbsp;|&nbsp;
    📆 1 Week / 🗓️ 1 Month → refresh every <b>24 hours</b><br>
    <b style="color:#1a2332;">Filter:</b> Narrow CPR &lt; 0.25% · Strength 85–100% · Top 10 per direction · Nifty 200 only
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
        "bull": ("#16a34a", "#f0fdf4", "#bbf7d0"),
        "bear": ("#dc2626", "#fef2f2", "#fecaca"),
        "neut": ("#d97706", "#fffbeb", "#fde68a"),
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
            f"<span style='color:#1a2332;'>{text}</span></div>"
        )

    return f"""
<div style="background:#ffffff;border:1px solid {bdr};border-top:4px solid {fc};
            border-radius:10px;padding:1rem 1.1rem;margin-bottom:1rem;
            box-shadow:0 2px 8px rgba(0,0,0,0.06);">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:0.6rem;">
    <div>
      <div style="font-family:IBM Plex Mono,monospace;font-size:1rem;font-weight:700;color:#1a2332;">
        {sig['symbol']}
      </div>
      <div style="font-family:IBM Plex Mono,monospace;font-size:0.75rem;color:#5a6a80;">
        ₹{sig['ltp']:,.2f} &nbsp;·&nbsp; ATR ₹{sig['atr']:,.2f}
      </div>
    </div>
    <div style="text-align:right;">
      <div style="background:{bg};border:1px solid {bdr};border-radius:6px;
                  padding:0.3rem 0.7rem;font-family:IBM Plex Mono,monospace;
                  font-size:0.78rem;font-weight:700;color:{fc};">{bias_label}</div>
      <div style="font-family:IBM Plex Mono,monospace;font-size:0.7rem;color:#5a6a80;margin-top:3px;">
        Confidence: {sig['confidence']}%
      </div>
    </div>
  </div>
  {sig_rows}
  <div style="display:flex;gap:1rem;margin-top:0.6rem;padding-top:0.5rem;
              border-top:1px solid #f1f5f9;flex-wrap:wrap;">
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a80;">Entry</span>
      <span style="color:#1a2332;font-weight:700;"> ₹{sig['entry']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a80;">T1</span>
      <span style="color:{fc};font-weight:700;"> ₹{sig['tgt1']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a80;">T2</span>
      <span style="color:{fc};font-weight:700;"> ₹{sig['tgt2']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a80;">SL</span>
      <span style="color:#dc2626;font-weight:700;"> ₹{sig['sl']:,.2f}</span>
    </div>
    <div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;">
      <span style="color:#5a6a80;">R:R</span>
      <span style="color:#1a2332;font-weight:700;"> {sig['rr']}x</span>
    </div>
  </div>
  <div style="margin-top:0.5rem;font-family:IBM Plex Mono,monospace;font-size:0.68rem;
              color:#8a9ab0;display:flex;flex-wrap:wrap;gap:0.5rem;">
    <span>P:{sig['P']:,.0f}</span>
    <span style="color:#dc2626;">R1:{sig['R1']:,.0f} R2:{sig['R2']:,.0f}</span>
    <span style="color:#16a34a;">S1:{sig['S1']:,.0f} S2:{sig['S2']:,.0f}</span>
    <span>RSI:{sig['rsi']}</span>
    <span>CPR:{sig['cpr_width']}%</span>
  </div>
</div>"""


def page_trade_signals(nse500: pd.DataFrame):
    """Live Trade Signals board with browser push notifications."""

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:1.25rem;
                padding:1.25rem 1.5rem;background:#ffffff;border:1px solid #dce3ed;
                border-radius:12px;box-shadow:0 1px 6px rgba(0,0,0,0.06);">
        <div style="font-size:2rem;">🔔</div>
        <div style="flex:1;">
            <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;
                        font-weight:700;color:#1a2332;">Trade Signal Board</div>
            <div style="font-family:'IBM Plex Mono',monospace;font-size:0.68rem;
                        color:#5a6a80;letter-spacing:0.08em;text-transform:uppercase;">
                Pivot Boss Signals · Push Alerts · Real-Time Analysis
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Push notification JS ───────────────────────────────────────────────────
    st.markdown("""
    <div id="notif-banner" style="display:none;background:#1a6b3c;color:#fff;
         padding:0.6rem 1rem;border-radius:8px;margin-bottom:1rem;
         font-family:IBM Plex Mono,monospace;font-size:0.8rem;
         display:flex;align-items:center;gap:8px;">
        🔔 <span id="notif-text">Notifications enabled</span>
    </div>
    <script>
    function enableNotifications() {
        if (!("Notification" in window)) {
            document.getElementById("notif-status").innerText = "Not supported in this browser";
            return;
        }
        Notification.requestPermission().then(function(perm) {
            var btn = document.getElementById("notif-btn");
            var status = document.getElementById("notif-status");
            if (perm === "granted") {
                btn.style.background = "#16a34a";
                btn.style.borderColor = "#16a34a";
                btn.innerText = "✅ Notifications ON";
                status.innerText = "Push alerts enabled — you will be notified on strong signals";
                status.style.color = "#16a34a";
                new Notification("PivotVault AI", {
                    body: "Trade signal notifications are now active!",
                    icon: "/static/icon-192.png"
                });
                window._pvNotifEnabled = true;
            } else {
                status.innerText = "Permission denied — enable notifications in browser settings";
                status.style.color = "#dc2626";
            }
        });
    }

    function sendSignalAlert(symbol, bias, entry, target, sl) {
        if (window._pvNotifEnabled && Notification.permission === "granted") {
            var emoji = bias.includes("BUY") ? "🚀" : "🔻";
            new Notification(emoji + " " + symbol + " — " + bias, {
                body: "Entry: ₹" + entry + "  |  Target: ₹" + target + "  |  SL: ₹" + sl,
                icon: "/static/icon-192.png",
                tag: symbol,
                requireInteraction: true,
            });
        }
    }
    window.sendSignalAlert = sendSignalAlert;
    </script>

    <div style="background:#ffffff;border:1px solid #dce3ed;border-radius:10px;
                padding:1rem 1.25rem;margin-bottom:1.25rem;
                font-family:IBM Plex Mono,monospace;">
        <div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.08em;
                    color:#5a6a80;margin-bottom:0.6rem;">🔔 Browser Push Notifications</div>
        <div style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap;">
            <button id="notif-btn" onclick="enableNotifications()"
                style="background:#ffffff;border:1.5px solid #1a6b3c;color:#1a6b3c;
                       font-family:IBM Plex Mono,monospace;font-size:0.78rem;font-weight:600;
                       padding:0.4rem 1.2rem;border-radius:6px;cursor:pointer;
                       transition:all 0.2s;">
                🔔 Enable Notifications
            </button>
            <div id="notif-status" style="font-size:0.75rem;color:#5a6a80;">
                Click to receive push alerts when strong signals are detected
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Controls ───────────────────────────────────────────────────────────────
    symbols = sorted(fetch_nifty200_list())

    col1, col2, col3, col4 = st.columns([3, 2, 1.5, 1])
    with col1:
        watch_syms = st.multiselect(
            "Symbols to monitor",
            symbols,
            default=["RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK"],
            key="sig_symbols",
            label_visibility="collapsed",
            max_selections=20,
            placeholder="Choose up to 20 symbols…",
        )
    with col2:
        sig_tf = st.selectbox(
            "Timeframe",
            ["15 Min (Fast Scalping)", "1 Hour (Swing Scalp)",
             "Daily (Swing Trade)", "Weekly (Positional)"],
            index=2,
            label_visibility="collapsed",
            key="sig_tf",
        )
    with col3:
        sig_filter = st.selectbox(
            "Show",
            ["All Signals", "Buy Only", "Sell Only", "Strong Only"],
            label_visibility="collapsed",
            key="sig_filter",
        )
    with col4:
        refresh_btn = st.button("🔄 Refresh", use_container_width=True, key="sig_refresh")

    SIG_TF_MAP = {
        "15 Min (Fast Scalping)":  ("15m", "5d"),
        "1 Hour (Swing Scalp)":    ("1h",  "30d"),
        "Daily (Swing Trade)":     ("1d",  "90d"),
        "Weekly (Positional)":     ("1wk", "2y"),
    }
    sig_interval, sig_period = SIG_TF_MAP[sig_tf]

    if not watch_syms:
        st.info("Select at least one symbol to analyse.")
        return

    # ── Compute signals ────────────────────────────────────────────────────────
    cache_key = f"signals_{sig_interval}_{','.join(sorted(watch_syms))}"
    if refresh_btn or cache_key not in st.session_state:
        results = {}
        prog = st.progress(0, text="Analysing symbols…")
        for i, sym in enumerate(watch_syms):
            results[sym] = compute_signals_for_symbol(sym, sig_interval, sig_period)
            prog.progress((i + 1) / len(watch_syms), text=f"Analysing {sym}…")
        prog.empty()
        st.session_state[cache_key] = results
    else:
        results = st.session_state[cache_key]

    # Filter
    valid = {s: r for s, r in results.items() if r}
    if sig_filter == "Buy Only":
        valid = {s: r for s, r in valid.items() if r["bias_col"] == "bull"}
    elif sig_filter == "Sell Only":
        valid = {s: r for s, r in valid.items() if r["bias_col"] == "bear"}
    elif sig_filter == "Strong Only":
        valid = {s: r for s, r in valid.items() if "STRONG" in r["bias"]}

    # Sort by |score| desc
    sorted_sigs = sorted(valid.items(), key=lambda x: abs(x[1]["score"]), reverse=True)

    if not sorted_sigs:
        st.info("No signals match the current filter.")
        return

    # ── Summary strip ──────────────────────────────────────────────────────────
    n_buy       = sum(1 for _, r in sorted_sigs if r["bias_col"] == "bull")
    n_sell      = sum(1 for _, r in sorted_sigs if r["bias_col"] == "bear")
    n_strong    = sum(1 for _, r in sorted_sigs if "STRONG" in r["bias"])
    n_neut      = sum(1 for _, r in sorted_sigs if r["bias_col"] == "neut")

    sm1, sm2, sm3, sm4 = st.columns(4)
    sm1.metric("🟢 Buy Signals",    n_buy)
    sm2.metric("🔴 Sell Signals",   n_sell)
    sm3.metric("🚀 Strong Signals", n_strong)
    sm4.metric("⚪ Neutral",        n_neut)

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    # ── Push notification triggers ─────────────────────────────────────────────
    # Inject JS calls for strong signals
    js_alerts = ""
    for sym, sig in sorted_sigs:
        if "STRONG" in sig["bias"]:
            js_alerts += (
                f"sendSignalAlert('{sym}','{sig['bias']}',"
                f"'{sig['entry']}','{sig['tgt1']}','{sig['sl']}');"
            )
    if js_alerts:
        st.markdown(f"<script>setTimeout(function(){{ {js_alerts} }}, 1500);</script>",
                    unsafe_allow_html=True)

    # ── Signal cards in 2-column grid ─────────────────────────────────────────
    cards_left  = ""
    cards_right = ""
    for i, (sym, sig) in enumerate(sorted_sigs):
        card = _signal_card(sig)
        if i % 2 == 0:
            cards_left  += card
        else:
            cards_right += card

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(cards_left,  unsafe_allow_html=True)
    with col_r:
        st.markdown(cards_right, unsafe_allow_html=True)

    # ── Pivot level table ──────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.8rem;"
        "font-weight:700;color:#1a2332;margin-bottom:0.5rem;'>📋 Pivot Level Reference</div>",
        unsafe_allow_html=True,
    )
    ref_rows = []
    for sym, sig in sorted_sigs:
        if not sig:
            continue
        ref_rows.append({
            "Symbol": sym,
            "LTP":    f"₹{sig['ltp']:,.2f}",
            "Signal": sig["bias"],
            "R2":     f"₹{sig['R2']:,.2f}",
            "R1":     f"₹{sig['R1']:,.2f}",
            "P":      f"₹{sig['P']:,.2f}",
            "S1":     f"₹{sig['S1']:,.2f}",
            "S2":     f"₹{sig['S2']:,.2f}",
            "RSI":    sig["rsi"],
            "CPR%":   f"{sig['cpr_width']}%",
        })
    if ref_rows:
        st.dataframe(pd.DataFrame(ref_rows), use_container_width=True, hide_index=True)

    st.markdown(
        "<div style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;color:#94a3b8;"
        "margin-top:0.75rem;'>⚠️ Signals based on Frank Ochoa Pivot Boss methodology. "
        "Not financial advice. Always use proper risk management.</div>",
        unsafe_allow_html=True,
    )


def render_sidebar():
    PAGES = ["Market Snapshot","Pivot Boss Analysis","CPR Scanner","Trade Signals","Watchlist"]
    ICONS = ["📊","📈","📡","🔔","⭐"]
    SHORT = ["Market","Pivot","Scanner","Signals","Watch"]

    # ── Read page from query params (set by mobile nav buttons) ──────────
    # Use a SEPARATE session state key for mobile nav — never touch widget key directly
    qp = st.query_params.get("page", "")
    if qp in PAGES:
        st.session_state["_mobile_nav"] = qp

    # Determine which page to show — mobile nav takes priority if set
    mobile_choice = st.session_state.get("_mobile_nav", "")
    cur_idx = PAGES.index(mobile_choice) if mobile_choice in PAGES else 0

    # ── Sidebar (desktop) ─────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(
            "<div style='padding:0.75rem 0.5rem 0.25rem;'>"
            "<span style='font-family:IBM Plex Mono,monospace;font-size:1.1rem;"
            "font-weight:700;color:#f1f5f9;'>🏦 PivotVault</span>"
            "<span style='font-family:IBM Plex Mono,monospace;font-size:1.1rem;"
            "font-weight:700;color:#16a34a;'> AI</span><br>"
            "<span style='font-family:IBM Plex Mono,monospace;font-size:0.6rem;"
            "color:#475569;letter-spacing:0.1em;text-transform:uppercase;'>"
            "Pivot Boss · Equity Terminal</span></div>",
            unsafe_allow_html=True,
        )
        st.divider()
        menu = st.radio(
            "Navigation", PAGES,
            index=cur_idx,
            label_visibility="collapsed",
            key="main_nav",
        )
        st.divider()
        wl_count = len(st.session_state.get("watchlist", []))
        st.caption(f"👤 {st.session_state.get('username','user')}  |  ⭐ {wl_count}")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state["logged_in"] = False
            st.query_params.clear()
            st.rerun()

    # Sync URL to current selection
    st.query_params["page"] = menu

    # ── Mobile bottom nav ─────────────────────────────────────────────────
    st.markdown("""
    <style>
    .pv-mobile-nav { display: none !important; }
    @media (max-width: 768px) {
        .pv-mobile-nav {
            display: flex !important;
            position: fixed;
            bottom: 0; left: 0; right: 0;
            height: 60px;
            background: #1e293b;
            border-top: 2px solid #334155;
            z-index: 99999;
            padding-bottom: env(safe-area-inset-bottom, 0);
        }
        .block-container { padding-bottom: 72px !important; }
        section[data-testid="stSidebar"],
        [data-testid="collapsedControl"],
        button[data-testid="baseButton-header"] { display: none !important; }
        /* Make mobile nav buttons look right */
        .pv-mobile-nav .stButton { flex: 1 !important; }
        .pv-mobile-nav .stButton > div { height: 100% !important; }
        .pv-mobile-nav .stButton > div > button {
            background: transparent !important;
            border: none !important;
            border-top: 3px solid transparent !important;
            box-shadow: none !important;
            border-radius: 0 !important;
            color: #64748b !important;
            font-size: 0.95rem !important;
            height: 60px !important;
            width: 100% !important;
            padding: 4px 0 0 0 !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="pv-mobile-nav">', unsafe_allow_html=True)
        cols = st.columns(len(PAGES))
        for i, (page, icon, short) in enumerate(zip(PAGES, ICONS, SHORT)):
            is_active = (menu == page)
            with cols[i]:
                # Show emoji icon only — clean mobile look
                if st.button(icon, key=f"nav_m_{i}", use_container_width=True, help=page):
                    # Set the non-widget state key — safe to modify
                    st.session_state["_mobile_nav"] = page
                    st.query_params["page"] = page
                    st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    return menu


def main():
    if not st.session_state["logged_in"]:
        page_login()
        return

    menu   = render_sidebar()
    render_market_header()
    st.divider()
    nse500 = fetch_nse500_list()



    if   menu == "Market Snapshot":     page_market_snapshot(nse500)
    elif menu == "Pivot Boss Analysis": page_pivot_boss(nse500)
    elif menu == "CPR Scanner":         page_cpr_scanner(nse500)
    elif menu == "Trade Signals":       page_trade_signals(nse500)
    elif menu == "Watchlist":           page_watchlist()


if __name__ == "__main__":
    main()
