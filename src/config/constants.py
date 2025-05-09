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
