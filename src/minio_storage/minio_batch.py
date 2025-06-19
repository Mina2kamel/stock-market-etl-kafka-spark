import os
import time
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from collections import defaultdict
from src.utils import setup_logger

logger = setup_logger('airflow')

class MinioBatchManager:
    """
    batch manager that buffers records per symbol and uploads to MinIO.
    Flushes when batch size reached or after flush interval in seconds.
    """

    def __init__(self, endpoint, access_key, secret_key, bucket_name, batch_size=100, flush_interval=30):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Use True for production
        )
        self.bucket_name = bucket_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval  # seconds
        self.buffers = defaultdict(list)  # symbol -> list of records
        self.last_flush_time = time.time()
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket {self.bucket_name} already exists.")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")

    def add_record_to_buffer(self, record):
        """
        Add record to buffer. Flush if batch size or flush interval exceeded.
        """
        symbol = record['symbol']
        self.buffers[symbol].append(record)

        current_time = time.time()

        # Flush if batch size reached or flush interval passed (and buffer not empty)
        if (len(self.buffers[symbol]) >= self.batch_size) or \
           (current_time - self.last_flush_time >= self.flush_interval and len(self.buffers[symbol]) > 0):
            self._flush_symbol(symbol)
            self.last_flush_time = current_time

    def _flush_symbol(self, symbol):
        """
        Flush buffered records for a symbol to MinIO as parquet.
        """
        records = self.buffers[symbol]
        if not records:
            return

        df = pd.DataFrame(records)
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        os.makedirs('tmp', exist_ok=True)
        parquet_file = f"tmp/{symbol}_{timestamp}.parquet"
        object_name = f"raw/stream/year={now.year}/month={now.month:02d}/day={now.day:02d}/{symbol}_{timestamp}.parquet"

        try:
            df.to_parquet(parquet_file, index=False)
            self.client.fput_object(self.bucket_name, object_name, parquet_file)
            logger.info(f"Uploaded batch of {len(records)} records for {symbol} to s3://{self.bucket_name}/{object_name}")
        except Exception as e:
            logger.error(f"Failed to upload batch for {symbol}: {e}")
        finally:
            if os.path.exists(parquet_file):
                os.remove(parquet_file)
            self.buffers[symbol] = []

    def flush_all(self):
        """Flush all symbol buffers."""
        for symbol in list(self.buffers.keys()):
            self._flush_symbol(symbol)
