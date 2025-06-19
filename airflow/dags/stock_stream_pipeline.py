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
    description='Stream pipeline with Kafka and PySpark',
    schedule_interval=None,
    catchup=False,
)

# 1. Produce batch stock data to Kafka
produce_task = BashOperator(
    task_id='stock_stream_producer',
    bash_command='python /opt/airflow/src/kafka_confluent/producer/stream_producer.py',
    dag=dag,
)


# 2. Process data with PySpark
process_data = BashOperator(
    task_id="process_data",
    bash_command=(
        "spark-submit "
        "--jars /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar,"
        "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar,"
        "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.4.jar "
        "/opt/airflow/src/spark/stream_processor.py"
    ),
    dag=dag,
)

# Define task dependencies
produce_task 
process_data
