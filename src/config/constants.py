from enum import Enum
import logging

class LoggerInfo(Enum):
    """
    Enum for logger information.
    """
    LOG_LEVEL = logging.INFO
    MAX_BYTES = 5 * 1024 * 1024  # 5 MB
    BACKUP_COUNT = 3
    STOCK_BATCH_LOG_FILE = "logs/stock_batch.log"

STOCK_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "INTC", "JPM", "V"
]

# Define stocks with initial prices
STOCKS_INITAIL = {
    'AAPL': 180.0,   # Apple
    'MSFT': 350.0,   # Microsoft
    'GOOGL': 130.0,  # Alphabet (Google)
    'AMZN': 130.0,   # Amazon
    'META': 300.0,   # Meta (Facebook)
    'TSLA': 200.0,   # Tesla
    'NVDA': 400.0,   # NVIDIA
    'INTC': 35.0,    # Intel
}