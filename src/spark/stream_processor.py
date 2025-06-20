import traceback
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
from spark_manager import SparkManager
from src.utils import setup_logger
from src.config import config

logger = setup_logger('airflow')


class StockStreamConsumer:
    def __init__(self, kafka_broker: str, topic: str, minio_bucket: str):
        self.spark =  SparkManager(
        minio_endpoint=f'http://{config.minio_endpoint}',
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key
    ).get_spark()
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.minio_bucket = minio_bucket
        self.output_path = f"s3a://{minio_bucket}/processed/stream/"
        self.checkpoint_path = f"s3a://{minio_bucket}/checkpoints/streaming_processor"

        self.schema = StructType() \
            .add("symbol", StringType()) \
            .add("price", DoubleType()) \
            .add("change", DoubleType()) \
            .add("percent_change", DoubleType()) \
            .add("volume", IntegerType()) \
            .add("timestamp", TimestampType())

    def read_stream(self) -> DataFrame:
        try:
            kafka_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_broker) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", 1000) \
                .option("kafka.group.id", "stock_stream_consumer") \
                .load()
            logger.info(f"Connected to Kafka topic: {self.topic}")
            return kafka_df
        except StreamingQueryException as sqe:
            logger.error(f"StreamingQueryException: {sqe}")
            raise
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise

    def parse_stream_data(self, df: DataFrame) -> DataFrame:
        try:
            parsed_df = (
                df.selectExpr("CAST(value AS STRING)")
                  .select(from_json(col("value"), self.schema).alias("data"))
                  .filter(col("data").isNotNull())
                  .select("data.*")
            )
            logger.info("Parsed stream data successfully.")
            return parsed_df
        except Exception as e:
            logger.error(f"Error parsing stream: {e}")
            raise

    def transform_stream_data(self, df: DataFrame) -> DataFrame:
        try:
            transformed_df = (
                df.withWatermark("timestamp", "1 minute")
                  .groupBy(
                      window(col("timestamp"), "1 minute", "30 seconds"),
                      col("symbol")
                  )
                  .agg({"price": "avg", "volume": "sum"})
                  .withColumnRenamed("avg(price)", "avg_price")
                  .withColumnRenamed("sum(volume)", "total_volume")
            )
            logger.info("Transformed stream data with sliding window.")
            return transformed_df
        except Exception as e:
            logger.error(f"Error transforming stream data: {e}")
            raise

    def process_and_write_batch(self, df: DataFrame, batch_id: int):
        try:
            count = df.count()
            logger.info(f"Processing batch {batch_id} with {count} rows")
            if count > 0:
                for row in df.collect():
                    logger.info(f"Row: {row.asDict()}")

                df.write \
                    .mode("append") \
                    .partitionBy("symbol") \
                    .parquet(self.output_path)
                logger.info(f"Batch {batch_id} written to {self.output_path}")
            else:
                logger.info(f"Batch {batch_id} is empty. Skipping write.")
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}")
            logger.error(traceback.format_exc())
            raise

    def write_stream_to_minio(self, df: DataFrame):
        try:
            logger.info(f"Writing stream to MinIO at: {self.output_path}")
            query = (
                df.writeStream
                  .foreachBatch(self.process_and_write_batch)
                  .trigger(processingTime="30 seconds")
                  .option("checkpointLocation", self.checkpoint_path)
                  .outputMode("append")
                  .start()
            )
            logger.info("Streaming query started.")
            return query
        except StreamingQueryException as sqe:
            logger.error(f"StreamingQueryException occurred: {sqe}")
            logger.error(traceback.format_exc())
            raise
        except Exception as e:
            logger.error(f"Error writing to MinIO: {e}")
            logger.error(traceback.format_exc())
            raise

def main():
    """Main function to process real-time stock data using Spark Streaming."""
    logger.info("\n=========================================")
    logger.info("STARTING STOCK MARKET STREAMING PROCESSOR")
    logger.info("=========================================\n")
    
    stock_stream_processor = StockStreamConsumer(
        kafka_broker=config.bootstrap_servers,
        topic=config.stream_topic_name,
        minio_bucket=config.minio_bucket_name
    )
    
    try:
        kafka_df = stock_stream_processor.read_stream()
        if kafka_df is not None:
            parsed_df = stock_stream_processor.parse_stream_data(kafka_df)
            processed_df = stock_stream_processor.transform_stream_data(parsed_df)
            if processed_df is not None:
                query = stock_stream_processor.write_stream_to_minio(processed_df)
                if query is not None:
                    logger.info("\nStreaming processor is running...")
                    query.awaitTermination()
                else:
                    logger.info("Failed to start streaming query.")
            else:
                logger.info("No processed streaming data to write.")
        else:
            logger.info("No Kafka streaming data to process.")
    except Exception as e:
        logger.error(f"\nError in streaming processing: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("\nStopping Spark session.")
        stock_stream_processor.spark.stop()
        logger.info("=========================================")
        logger.info("STREAMING PROCESSING COMPLETED")
        logger.info("=========================================")

if __name__ == "__main__":
    main()