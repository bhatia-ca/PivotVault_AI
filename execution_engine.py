import secrets, datetime, requests, streamlit as st
from event_logger import EventLogger


class OrderExecutor:
    """
    Full order execution engine for PivotVault AI.
    Handles: placement → monitoring → SL/Target hit → event logging.
    """

    @staticmethod
    def place_order(symbol, side, entry, sl, t1, t2, qty, broker,
                    strategy_name="", order_id=None):
        """Place order and record in session_state active_trades."""
        trade = {
            "id":            secrets.token_hex(4),
            "symbol":        symbol,
            "side":          side,
            "entry":         entry,
            "sl":            sl,
            "t1":            t1,
            "t2":            t2,
            "qty":           qty,
            "broker":        broker,
            "status":        "OPEN",
            "placed_at":     datetime.datetime.now().strftime("%H:%M:%S"),
            "order_id":      order_id or "PAPER",
            "strategy_name": strategy_name,
            "ltp":           entry,
            "pnl":           0.0,
        }
        if "active_trades" not in st.session_state:
            st.session_state["active_trades"] = []
        st.session_state["active_trades"].append(trade)
        EventLogger.log_order_execution(trade)
        return trade

    @staticmethod
    def monitor_trades(ltp_map: dict):
        """
        Check all open trades against current LTP.
        ltp_map = {symbol: ltp}
        Returns list of event dicts for triggered SL/Targets.
        """
        events = []
        for trade in st.session_state.get("active_trades", []):
            if trade["status"] != "OPEN":
                continue
            sym = trade["symbol"]
            ltp = ltp_map.get(sym)
            if not ltp:
                continue
            trade["ltp"] = ltp
            mult = 1 if trade["side"] == "BUY" else -1
            pnl  = round((ltp - trade["entry"]) * mult * trade["qty"], 2)
            trade["pnl"] = pnl

            if trade["side"] == "BUY":
                if ltp <= trade["sl"]:
                    trade["status"] = "SL_HIT"
                    EventLogger.log_stop_loss_hit(trade)
                    events.append({"type": "SL_HIT", "trade": trade})
                elif trade["t2"] and ltp >= trade["t2"]:
                    trade["status"] = "T2_HIT"
                    EventLogger.log_target_achievement(trade)
                    events.append({"type": "T2_HIT", "trade": trade})
                elif trade["t1"] and ltp >= trade["t1"]:
                    trade["status"] = "T1_HIT"
                    EventLogger.log_target_achievement(trade)
                    events.append({"type": "T1_HIT", "trade": trade})
            else:  # SELL
                if ltp >= trade["sl"]:
                    trade["status"] = "SL_HIT"
                    EventLogger.log_stop_loss_hit(trade)
                    events.append({"type": "SL_HIT", "trade": trade})
                elif trade["t2"] and ltp <= trade["t2"]:
                    trade["status"] = "T2_HIT"
                    EventLogger.log_target_achievement(trade)
                    events.append({"type": "T2_HIT", "trade": trade})
                elif trade["t1"] and ltp <= trade["t1"]:
                    trade["status"] = "T1_HIT"
                    EventLogger.log_target_achievement(trade)
                    events.append({"type": "T1_HIT", "trade": trade})
        return events

    @staticmethod
    def exit_trade(trade_id, broker, ltp):
        """Manually exit a trade."""
        for trade in st.session_state.get("active_trades", []):
            if trade["id"] == trade_id and trade["status"] == "OPEN":
                trade["status"] = "EXITED"
                trade["ltp"]    = ltp
                mult = 1 if trade["side"] == "BUY" else -1
                trade["pnl"] = round((ltp - trade["entry"]) * mult * trade["qty"], 2)
                EventLogger.log_order_execution({**trade, "event_type": "EXIT"})
                return trade
        return None
