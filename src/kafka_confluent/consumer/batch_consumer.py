import json
import logging
from config import config
from confluent_kafka import Consumer, KafkaError
from minio_storage import MinioManager

logger = logging.getLogger(__name__)

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

    def consume_messages(self, timeout=1.0):
        try:
            while True:
                msg = self.consumer.poll(timeout)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break

                record = json.loads(msg.value().decode("utf-8"))

                # Write to MinIO
                self.minio_manager.write_to_minio(record)


                self.consumer.commit()

        except KeyboardInterrupt:
            logger.info("Stopping consumer.")
        finally:
            self.consumer.close()

