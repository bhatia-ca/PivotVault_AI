import logging
import os
import datetime

# Configure logging
LOGGING_DIR = 'logs'
if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR)

LOG_FILE = os.path.join(LOGGING_DIR, 'event_logger.log')

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

class EventLogger:
    @staticmethod
    def log_order_execution(order_details):
        logging.info(f'Order Executed: {order_details}')

    @staticmethod
    def log_stop_loss_hit(order_details):
        logging.warning(f'Stop Loss Hit: {order_details}')

    @staticmethod
    def log_target_achievement(order_details):
        logging.info(f'Target Achieved: {order_details}')  

# Example usage
# EventLogger.log_order_execution('Order ID: 12345, Symbol: AAPL, Quantity: 10')
# EventLogger.log_stop_loss_hit('Order ID: 12345, Symbol: AAPL, Quantity: 10')
# EventLogger.log_target_achievement('Order ID: 12345, Symbol: AAPL, Quantity: 10')
