import json
import logging
import random
import time
import pandas as pd
from datetime import datetime
from confluent_kafka import Producer
from typing import Optional
from config import STOCKS_INITAIL

logger = logging.getLogger(__name__)

class StockStreamProducer:
    def __init__(self, kafka_bootstrap_servers: str, kafka_topic: str, interval: Optional[int] = 30):
        self.kafka_topic = kafka_topic
        self.interval = interval
        self.current_stocks = STOCKS_INITAIL.copy()

        try:
            self.producer = Producer({
                'bootstrap.servers': kafka_bootstrap_servers,
                'acks': 'all',
                'linger.ms': 100,
                'enable.idempotence': False,
                'batch.num.messages': 1000,
                'compression.type': 'snappy'
            })
        except Exception as e:
            logger.exception(f"Failed to create Kafka stream producer: {e}")
            raise

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for record {msg.key()}: {err}")
        else:
            logger.info(
                f"Record successfully delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            
    def generate_stock_data(self, symbol: str) -> dict:
        """
        Generates synthetic stock data for a given symbol.
        Args:
            symbol (str): The stock symbol for which to generate data.
        Returns:
            dict: A dictionary containing the stock data.
        """

        current_price = self.current_stocks[symbol]

        market_factor = random.uniform(-0.005, 0.005) # market wide movemenet
        stock_factor =  random.uniform(-0.005, 0.005) # stock specific movement 

        change_pct = market_factor + stock_factor

        if random.random() < 0.05:
            stock_factor += random.uniform(-0.02, 0.02)
        
        new_price = round(current_price * (1 + change_pct), 2)

        self.current_stocks[symbol] = new_price

        #calcuate percent change
        price_change = round(new_price - current_price, 2)
        percent_change = round((price_change / current_price) * 100, 2)

        volume = random.randint(1000,100000)

        stock_data = {
            'symbol': symbol,
            'price': new_price,
            'change': price_change,
            'percent_change': percent_change,
            'volume': volume,
            "timestamp": datetime.now().isoformat()

        }

        return stock_data
    
    def stream_stock_data(self):
        """
        Streams stock data for predefined symbols at regular intervals.
        """
        try:
            while True:
                successful_symbols, failed_symbols = 0, 0
                for symbol in STOCKS_INITAIL.keys():
                    logger.info(f"Producing stock data for {symbol}")
                    # Generate synthetic stock data                 
                    stock_data = self.generate_stock_data(symbol)
                    try:
                        if stock_data:
                            self.producer.produce(
                                topic=self.kafka_topic,
                                key=symbol,
                                value=json.dumps(stock_data),
                                callback=self.delivery_report
                            )
                            successful_symbols += 1
                        else:
                            failed_symbols += 1
                    except BufferError:
                        logger.warning("Local producer queue is full, waiting...")
                        time.sleep(1)
                        failed_symbols += 1
                    except Exception as e:
                        logger.exception(f"Failed to produce record for {symbol}: {e}")
                        failed_symbols += 1
                self.producer.poll(0)  # triggers delivery report callbacks
                logger.info(f"Produced {successful_symbols} symbols with {failed_symbols} failures.")
                logger.info(f"Sleeping for {self.interval} seconds before next batch...")
                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Stream interrupted by user. Exiting...")
        except Exception as e:
            logger.exception(f"Failed to produce record for {symbol}: {e}")
        finally:
            self.producer.flush()
            logger.info("Flushed stream producer.")
                
