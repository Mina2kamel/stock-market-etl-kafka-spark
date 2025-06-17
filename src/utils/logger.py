# import os
# import logging
# from logging.handlers import RotatingFileHandler
# from config import LoggerInfo

# def setup_logger(
#     log_file: str = LoggerInfo.STOCK_BATCH_LOG_FILE.value,
#     level: int = LoggerInfo.LOG_LEVEL.value,
#     max_bytes: int = LoggerInfo.MAX_BYTES.value,
#     backup_count: int = LoggerInfo.BACKUP_COUNT.value
# ) -> logging.Logger:
#     """
#     Set up a rotating file logger.

#     Parameters:
#         name (str): Logger name. Use empty string "" for root logger.
#         log_file (str): Path to the log file.
#         level (int): Logging level (e.g., logging.DEBUG).
#         max_bytes (int): Max file size in bytes before rotation.
#         backup_count (int): Number of rotated log files to keep.

#     Returns:
#         logging.Logger: Configured logger instance.
#     """

#     log_dir = os.path.dirname(log_file)
#     if log_dir and not os.path.exists(log_dir):
#         os.makedirs(log_dir)

#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#     handler = RotatingFileHandler(
#             filename=log_file,
#             maxBytes=max_bytes,
#             backupCount=backup_count,
#             delay=True
#         )
#     handler.setFormatter(formatter)

#     logger = logging.getLogger()
#     logger.setLevel(level)
#     logger.addHandler(handler)

#     return logger


# # Default logger for the app
# logger = setup_logger()


import logging
import sys

def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Set up a logger with the specified name and level.
    Logs to stdout with a consistent format.

    Args:
        name (str): Logger name, usually __name__.
        level (int): Logging level (e.g. logging.INFO).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    # To avoid adding multiple handlers in case of multiple imports/calls
    if not logger.hasHandlers():
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False  # Avoid double logging if root logger is configured

    return logger

