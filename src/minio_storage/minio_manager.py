import os
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from typing import List, Dict
from src.utils import setup_logger

logger = setup_logger('airflow')

class MinioManager:
    def __init__(self, endpoint: str, access_key: str, 
                 secret_key: str, bucket_name: str, batch_size: int = 100):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Set to True in production
        )
        self.bucket_name = bucket_name
        self.batch_size = batch_size
        self._ensure_bucket()

    def _ensure_bucket(self):
        """
        Ensure the bucket exists. If not, create it.
        """
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket {self.bucket_name} already exists.")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")


    def write_to_minio(self, record: dict):
        """
        Write a single record to MinIO in Parquet format.

        Args:
            records (dict): The record to write.
        """
        df = pd.DataFrame([record])
        symbol = record['symbol']
        date = record['date']
        year, month, day = date.split('-')

        # Ensure tmp directory exists
        os.makedirs('tmp', exist_ok=True)
        parquet_file = f"tmp/{symbol}_{date}.parquet"
        object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}_{datetime.now().strftime('%H%M%S')}.parquet"

        try:
            # Write to Parquet
            df.to_parquet(parquet_file, index=False)

            # Upload to MinIO
            self.client.fput_object(
                self.bucket_name,
                object_name,
                parquet_file
            )
            logger.info(f"Uploaded {symbol} data to s3://{self.bucket_name}/{object_name}")

        except Exception as e:
            logger.error(f"Failed to upload {symbol} data to MinIO: {e}")

        finally:
            # Clean up the local file
            if os.path.exists(parquet_file):
                os.remove(parquet_file)
                logger.info(f"Removed local file: {parquet_file}")

    def write_batch_to_minio(self, records: List[Dict]):
        """
        Write a batch of records to MinIO in Parquet format, grouped by symbol and date.
        Each (symbol, date) pair will be written to a separate file.

        Args:
            records (list): List of records (dicts).
        """
        if not records:
            return

        df = pd.DataFrame(records)

        grouped = df.groupby(['symbol', 'date'])
        os.makedirs('tmp', exist_ok=True)

        for (symbol, date), group_df in grouped:
            year, month, day = date.split('-')

            parquet_file = f"tmp/{symbol}_{date}.parquet"
            object_name = (
                f"raw/historical/year={year}/month={month}/day={day}/"
                f"{symbol}_{datetime.now().strftime('%H%M%S')}.parquet"
            )

            try:
                group_df.to_parquet(parquet_file, index=False)
                self.client.fput_object(self.bucket_name, object_name, parquet_file)
                logger.info(f"Uploaded batch of {len(group_df)} records for {symbol} on {date} to s3://{self.bucket_name}/{object_name}")
            except Exception as e:
                logger.error(f"Failed to upload batch for {symbol} on {date} to MinIO: {e}")
            finally:
                if os.path.exists(parquet_file):
                    os.remove(parquet_file)



