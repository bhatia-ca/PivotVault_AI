import logging, os, json, datetime

LOGGING_DIR = 'logs'
os.makedirs(LOGGING_DIR, exist_ok=True)

LOG_FILE    = os.path.join(LOGGING_DIR, 'event_logger.log')
EVENTS_FILE = os.path.join(LOGGING_DIR, 'trade_events.json')

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

class EventLogger:

    @staticmethod
    def _append_event(event_type: str, details: dict):
        """Append structured event to JSON events file."""
        record = {
            "timestamp":  datetime.datetime.now().isoformat(),
            "event_type": event_type,
            "details":    details if isinstance(details, dict) else {"raw": str(details)},
        }
        events = []
        if os.path.exists(EVENTS_FILE):
            try:
                with open(EVENTS_FILE) as f:
                    events = json.load(f)
            except Exception as e:
                logging.warning(f"Could not read events file: {e}")
                events = []
        events.append(record)
        try:
            with open(EVENTS_FILE, "w") as f:
                json.dump(events, f, indent=2, default=str)
        except Exception as e:
            logging.error(f"Could not write events file: {e}")
        return record

    @staticmethod
    def log_order_execution(order_details):
        logging.info(f'Order Executed: {order_details}')
        EventLogger._append_event("ORDER_EXECUTION", order_details)

    @staticmethod
    def log_stop_loss_hit(order_details):
        logging.warning(f'Stop Loss Hit: {order_details}')
        EventLogger._append_event("STOP_LOSS_HIT", order_details)

    @staticmethod
    def log_target_achievement(order_details):
        logging.info(f'Target Achieved: {order_details}')
        EventLogger._append_event("TARGET_ACHIEVED", order_details)

    @staticmethod
    def get_recent_events(n=50):
        """Return last n events from the JSON log."""
        if not os.path.exists(EVENTS_FILE):
            return []
        try:
            with open(EVENTS_FILE) as f:
                events = json.load(f)
            return events[-n:]
        except Exception:
            return []
