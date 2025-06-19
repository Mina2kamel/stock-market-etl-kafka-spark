#!/bin/bash

set -e  # Exit on any error

# 1. Set up environment variables
PROJECT_DIR="$(pwd)"
VENV_DIR="$PROJECT_DIR/venv"

# 2. Create virtual environment if not exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"

    echo "Installing required packages..."
    pip install -r requirements.txt

fi

# 5. Run the Kafka Stream Consumer (MinIO writer)
echo "Starting Kafka consumer..."
python3 src/main.py

# 6. Optionally run your PySpark batch job (example: daily processing)
echo "Running PySpark batch job..."
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  src/spark_jobs/batch_processor.py

docker exec stock-market-etl-kafka-spark-spark-master-1 \
      spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
        /opt/src/spark/batch_processor.py


docker exec stock-market-etl-kafka-spark-spark-master-1 \
      spark-submit \
        --master spark://spark-master:7077 \
        /opt/src/spark/batch_processor.py
# Done
echo "Pipeline completed."


chmod +x run_pipeline.sh
./run_pipeline.sh
