import os
import logging
from logging.handlers import RotatingFileHandler
from config import LoggerInfo

def setup_logger(
    log_file: str = LoggerInfo.STOCK_BATCH_LOG_FILE.value,
    level: int = LoggerInfo.LOG_LEVEL.value,
    max_bytes: int = LoggerInfo.MAX_BYTES.value,
    backup_count: int = LoggerInfo.BACKUP_COUNT.value
) -> logging.Logger:
    """
    Set up a rotating file logger.

    Parameters:
        name (str): Logger name. Use empty string "" for root logger.
        log_file (str): Path to the log file.
        level (int): Logging level (e.g., logging.DEBUG).
        max_bytes (int): Max file size in bytes before rotation.
        backup_count (int): Number of rotated log files to keep.

    Returns:
        logging.Logger: Configured logger instance.
    """

    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            delay=True
        )
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# Default logger for the app
logger = setup_logger()