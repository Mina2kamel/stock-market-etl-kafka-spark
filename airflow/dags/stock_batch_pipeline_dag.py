from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 12),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_batch_pipeline',
    default_args=default_args,
    description='Batch pipeline with Kafka and PySpark',
    schedule_interval=None,
    catchup=False,
)

# 1. Produce batch stock data to Kafka
produce_task = BashOperator(
    task_id='stock_batch_producer',
    bash_command='python /opt/airflow/src/kafka_confluent/producer/batch_producer.py',
    dag=dag,
)

# 2. Consume from Kafka and store in MinIO
consume_task = BashOperator(
    task_id='stock_batch_consumer',
    bash_command='python /opt/airflow/src/kafka_confluent/consumer/batch_consumer.py',
    dag=dag,
)



# Define task dependencies
produce_task >> consume_task
