import json
import logging
from config import config
from confluent_kafka import Consumer, KafkaError
from minio_storage import MinioBatchManager

logger = logging.getLogger(__name__)

class StockStreamConsumer:
    def __init__(self, kafka_topic: str, minio_manager: MinioBatchManager):
        self.minio_manager = minio_manager
        try:
            self.consumer = Consumer({
                'bootstrap.servers': config.bootstrap_servers,
                'group.id': 'stock_stream_consumer',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'session.timeout.ms': 10000,
                'max.poll.interval.ms': 300000,
            })
            self.consumer.subscribe([kafka_topic])
            logger.info(f"Kafka stream consumer created and subscribed to topic: {kafka_topic}")
        except Exception as e:
            logger.exception(f"Failed to create Kafka stream consumer: {e}")
            raise

    def consume_stream(self, timeout=1.0):
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
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    logger.info(f"Consumed record: {record['symbol']} at {record['timestamp']}")

                    # Write to MinIO
                    self.minio_manager.add_record_to_buffer(record)

                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Stopping stream consumer by user request.")
        finally:
            self.minio_manager.flush_all()
            self.consumer.close()
            logger.info("Kafka stream consumer closed.")

