from pyspark.sql import SparkSession
from src.utils import setup_logger

logger = setup_logger('airflow')

class SparkManager:
    def __init__(self, minio_endpoint: str, access_key: str, secret_key: str):
        self.endpoint = minio_endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        try:

            spark = (
                SparkSession.builder
                .appName("StockBatchProcessor")
                .config("spark.master", "local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.default.parallelism", "4")
                .config("spark.sql.adaptive.enabled", "true")
                .config(
                    "spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.11.901,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,"
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.2")
                .config("spark.hadoop.fs.s3a.endpoint", self.endpoint)
                .config("spark.hadoop.fs.s3a.access.key", self.access_key)
                .config("spark.hadoop.fs.s3a.secret.key", self.secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.credential.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .getOrCreate()
            )
            logger.info("Spark session created successfully.")
            return spark
        except Exception as e:
            logger.info(f"Error creating Spark session: {e}")
            raise

    def get_spark(self):
        return self.spark
