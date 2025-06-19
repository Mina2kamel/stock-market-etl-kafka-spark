from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, round as spark_round, max, min, first, last, sum
from spark_manager import SparkManager
from src.utils import setup_logger
from src.config import config

logger = setup_logger('airflow')

class StockBatchProcessor:
    """
    Class to handle reading, processing, and writing batch stock data with PySpark.
    """

    def __init__(self, spark_manager: SparkManager, bucket_name: str):
        self.bucket_name = bucket_name
        self.spark: SparkSession = spark_manager.get_spark()

    def read_data_from_s3(self, date: datetime = None) -> DataFrame:
        """
        Read raw Parquet files for the given date from MinIO S3.
        If no date is given, uses today's date.
        """
        if date is None:
            date = datetime.now()
        else:
            date = datetime.strptime(date, "%Y-%m-%d") if isinstance(date, str) else date
        input_path = f"s3a://{self.bucket_name}/raw/historical/year={date.year}/month={date.month:02d}/day={date.day:02d}/"
        logger.info(f"Reading data from: {input_path}")
        try:
            df = self.spark.read.parquet(input_path)
            logger.info(f"Read {df.show(5)} records from {input_path}")                             
            # print(f"Schema: {df.printSchema()}")
            return df
        except Exception as e:
            logger.error(f"Failed to read data from {input_path}: {e}")
            return None

    def process_stock_data(self, df: DataFrame) -> DataFrame:
        """
        Process stock data: compute % change from open and daily high/low range.
        """
        if df is None or df.count() == 0:
            logger.warning("No stock data to process.")
            return None
        
        try:
            logger.info("Starting stock data batch processing...")
            # Calculate daily high and low
            window_spec = Window.partitionBy("symbol", "date")
            df = df.withColumn("daily_open", first('open').over(window_spec)) \
                   .withColumn("daily_high", max("high").over(window_spec)) \
                   .withColumn("daily_low", min("low").over(window_spec)) \
                    .withColumn("daily_volume", sum("volume").over(window_spec)) \
                    .withColumn("daily_close", last("close").over(window_spec))

            # Calculate % change from open
            df = df.withColumn("daily_change", 
                               spark_round((col("daily_close") - col("daily_open")) / col("daily_open") * 100, 2))

            # Select relevant columns
            processed_df = df.select(
                "date", "symbol", "daily_open", "daily_high", "daily_low", "daily_volume", "daily_close", "daily_change"
            )

            logger.info(f"Processed {processed_df.count()} records.")
            return processed_df
        except Exception as e:
            logger.error(f"Error processing stock data: {e}")
            return None


    def write_to_s3(self, df: DataFrame, date: datetime = None):
        """
        Write processed data back to MinIO in a partitioned path based on date.
        """
        if df is None or df.rdd.isEmpty():
            logger.warning("No stock data to write to S3.")
            return None
        if date is None:
            date= datetime.now().strftime("%Y-%m-%d")
            
        output_path = f"s3a://{self.bucket_name}/processed/historical/date={date}"
        logger.info(f"Writing processed data to: {output_path}")

        try:
            df.write.mode("overwrite").partitionBy("symbol").parquet(output_path)
            logger.info(f"Successfully wrote processed data to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write data to {output_path}: {e}")
            return None

def main():
    """
    Main entrypoint to run the full batch processing.
    """
    spark_manager = SparkManager(
        minio_endpoint=f'http://{config.minio_endpoint}',
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key
    )
    processor = StockBatchProcessor(spark_manager=spark_manager, bucket_name=config.minio_bucket_name)
    try:
        raw_df = processor.read_data_from_s3(date="2025-06-13")
        logger.info(f"Raw DataFrame: {raw_df.show(5)}")

        if raw_df is not None:
            processed_df = processor.process_stock_data(raw_df)
            if processed_df is not None:
                logger.info("Writing processed data to S3...")
                processor.write_to_s3(processed_df)
        else:
            logger.error("No data to process. Exiting.")
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
    finally:
        spark_manager.get_spark().stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
