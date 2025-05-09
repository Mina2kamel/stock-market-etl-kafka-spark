import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load .env file
load_dotenv()

@dataclass
class Config:
    bootstrap_servers: str = os.getenv("BOOTSTRAP_SERVERS")
    batch_topic_name: str = os.getenv("BATCH_TOPIC_NAME")

    # MinIO configuration
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY")
    minio_bucket_name: str = os.getenv("MINIO_BUCKET")


# Global config instance
config = Config()