import json
from src.config import config
from confluent_kafka import Consumer, KafkaError
from src.minio_storage import MinioManager
from src.utils import setup_logger

logger = setup_logger('airflow')

class StockBatchConsumer:
    def __init__(self, kafka_topic: str, minio_manager: MinioManager):
        self.minio_manager = minio_manager
        self.consumer = Consumer({
            'bootstrap.servers': config.bootstrap_servers,
            'group.id': 'stock_batch_consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 300000,
        })
        self.consumer.subscribe([kafka_topic])

    def consume_messages(self, timeout=1.0, max_empty_polls=5):
        buffer = []
        empty_polls = 0

        while True:
            msg = self.consumer.poll(timeout)

            if msg is None:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    logger.info("No more messages to consume. Flushing remaining records...")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            empty_polls = 0  # reset since we got a message

            try:
                record = json.loads(msg.value().decode("utf-8"))
                buffer.append(record)

                if len(buffer) >= self.minio_manager.batch_size:
                    self.minio_manager.write_batch_to_minio(buffer)
                    buffer.clear()

                self.consumer.commit()
            except Exception as e:
                logger.exception(f"Error processing message: {e}")

        # Flush any remaining records
        if buffer:
            self.minio_manager.write_batch_to_minio(buffer)
            buffer.clear()

        self.consumer.close()



def main():
    """
    Main function to run the StockBatchConsumer.
    """
    logger.info("Starting StockBatchConsumer...")
    minio_manager = MinioManager(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        bucket_name=config.minio_bucket_name
    )

    consumer = StockBatchConsumer(
        kafka_topic=config.batch_topic_name,
        minio_manager=minio_manager
    )
    consumer.consume_messages(timeout=1.0)

if __name__ == "__main__":
    main()