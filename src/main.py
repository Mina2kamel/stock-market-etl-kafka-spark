
from utils import logger
from config import config
from minio_storage import MinioManager, MinioBatchManager
from kafka_confluent import  StockStreamProducer
from kafka_confluent import StockStreamConsumer


# minio_manager = MinioManager(
#     endpoint=config.minio_endpoint,
#     access_key=config.minio_access_key,
#     secret_key=config.minio_secret_key,
#     bucket_name=config.minio_bucket_name
# )
# # Example usage
# if __name__ == "__main__":
#     producer = StockBatchProducer(
#         kafka_bootstrap_servers=config.bootstrap_servers,
#         kafka_topic=config.batch_topic_name
#     )
#     producer.collect_historical_data(period="1y")

#     consumer = StockBatchConsumer(
#         kafka_topic=config.batch_topic_name,
#         minio_manager=minio_manager
#     )
#     consumer.consume_messages(timeout=1.0)



minio_manager = MinioBatchManager(
    endpoint=config.minio_endpoint,
    access_key=config.minio_access_key,
    secret_key=config.minio_secret_key,
    bucket_name=config.minio_bucket_name
)
# Example usage
if __name__ == "__main__":

    stream_producer = StockStreamProducer(
        kafka_bootstrap_servers=config.bootstrap_servers,
        kafka_topic=config.stream_topic_name
    )
    stream_producer.stream_stock_data()

    stream_consumer = StockStreamConsumer(
        kafka_topic=config.stream_topic_name,
        minio_manager=minio_manager
    )
    stream_consumer.consume_stream(timeout=1.0)                         
    
