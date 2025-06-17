import json
import pandas as pd
from datetime import datetime
from confluent_kafka import Producer
from typing import Optional
from src.config import STOCK_SYMBOLS, config
import yfinance as yf
from src.utils import setup_logger

logger = setup_logger('airflow')

class StockBatchProducer:
    def __init__(self, kafka_bootstrap_servers: str, kafka_topic: str):
        self.kafka_topic = kafka_topic
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'acks': 'all',
            'linger.ms': 100,
            'enable.idempotence': False,
            'batch.num.messages': 1000,
            'compression.type': 'snappy'
        })

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for record {msg.key()}: {err}")
        else:
            logger.info(
                f"Record successfully delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            
    def fetch_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """
        Fetch historical stock data for a given symbol and period from Yahoo Finance.

        Args:
            symbol (str): Stock ticker symbol (e.g., 'AAPL').
            period (str): Period string compatible with yfinance (e.g., '1y', '6mo').

        Returns:
            Optional[pd.DataFrame]: Cleaned DataFrame with stock data or None if fetch fails.
        """
        try:
            logger.info(f"Fetching historical data for {symbol} over period: {period}")
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)

            if df.empty:
                logger.warning(f"No data returned for {symbol}")
                return None

            return self._clean_stock_data(df, symbol)

        except Exception as e:
            logger.exception(f"Error fetching data for {symbol}: {e}")
            return None

    def _clean_stock_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Cleans and standardizes stock data DataFrame.

        Args:
            df (pd.DataFrame): Raw DataFrame from yfinance.
            symbol (str): The stock symbol for tagging the data.

        Returns:
            pd.DataFrame: Cleaned DataFrame with renamed columns and date formatting.
        """
        df.reset_index(inplace=True)

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }, inplace=True)

        df["symbol"] = symbol
        df["date"] = df["date"].dt.strftime('%Y-%m-%d')

        return df[["date", "symbol", "open", "high", "low", "close", "volume"]]

    def producer_to_kafka(self, df: pd.DataFrame, symbol: str):

        df['batch_id'] = datetime.now().strftime('%Y%m%d%H%M%S')
        df['batch_date'] = datetime.now().strftime('%Y-%m-%d')
        successful_records, failed_records = 0, 0

        records = df.to_dict(orient='records')
        for record in records:
            try:
                self.producer.produce(
                    topic=self.kafka_topic,
                    key=symbol,
                    value=json.dumps(record),
                    callback=self.delivery_report
                )
                if successful_records % 100 == 0:
                    self.producer.poll(0)  # triggers delivery report callbacks
                successful_records += 1
            except BufferError:
                logger.warning("Local producer queue is full, waiting...")
                self.producer.flush()
            except Exception as e:
                logger.exception(f"Failed to produce record for {symbol}: {e}")
                failed_records += 1

        self.producer.flush()
        logger.info(f"Produced {successful_records} records for {symbol} with {failed_records} failures.")

    def collect_historical_data(self, period: str = "1y"):
        for symbol in STOCK_SYMBOLS:
            try:
                logger.info(f"Collecting historical data for: {symbol}")
                df = self.fetch_historical_data(symbol, period)
                if df is not None and not df.empty:
                    self.producer_to_kafka(df, symbol)
                else:
                    logger.warning(f"No data to produce for {symbol}")
                    continue
            except Exception as e:
                logger.exception(f"Error collecting historical data for symbol {symbol}: {e}")
                continue


def main():
    """
    Main entrypoint to run the batch producer.
    """

    producer = StockBatchProducer(
        kafka_bootstrap_servers=config.bootstrap_servers,
        kafka_topic=config.batch_topic_name
    )
    producer.collect_historical_data(period="1y")

if __name__ == "__main__":
    main()



