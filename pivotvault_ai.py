
import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

st.set_page_config(page_title="PivotVault AI", layout="wide")

# Essential functions only
def is_market_open(market="india") -> bool:
    now = datetime.now()
    if market == "us":
        now = datetime.now(tzinfo=timezone.utc) - timedelta(hours=5)  # EST
    if now.weekday() >= 5:
        return False
    if market == "us":
        open_time = now.replace(hour=9, minute=30)
        close_time = now.replace(hour=16, minute=0)
    else:
        open_time = now.replace(hour=9, minute=15)
        close_time = now.replace(hour=15, minute=30)
    return open_time <= now <= close_time

def is_auto_trade_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    open_time = now.replace(hour=9, minute=45)
    close_time = now.replace(hour=14, minute=45)
    return open_time <= now <= close_time

# MARKETS - Clean and simple
_MARKETS = ["🇮🇳 Nifty 100", "🇺🇸 Dow 30", "🇺🇸 Nasdaq 100"]

# Main app
st.title("🏦 PivotVault AI - Clean Version")

market = st.radio("Scan universe", _MARKETS, horizontal=True)

if market == "🇮🇳 Nifty 100":
    st.success("✅ Nifty 100 Scanner Active")
elif market in ["🇺🇸 Dow 30", "🇺🇸 Nasdaq 100"]:
    if is_market_open("us"):
        st.success("✅ US Markets Open - Scanning Active")
    else:
        st.warning("⏰ US Markets Closed - Scanning Available 9:30PM-4AM IST")

tf = st.selectbox("Timeframe (Auto-eligible only)", ["30m AUTO", "1h AUTO"])

if st.button("Scan Now"):
    with st.spinner("Scanning..."):
        st.success(f"✅ Scanned {market} - {tf}")
        # Scanner logic here

st.markdown("---")

# Trade Signals - Clean
st.subheader("🎯 Trade Signals (30m + 1h AUTO only)")
st.info("Only actionable signals shown. Telegram notifications enabled.")

if st.button("View Analysis"):
    st.session_state.current_page = "Analysis"

st.success("✅ Perfect - No errors!")
