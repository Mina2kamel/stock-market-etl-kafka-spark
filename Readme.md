# 🚀 Stock Data Pipeline with Kafka, Spark & Airflow

This project is a **data pipeline** built with **Kafka, PySpark, Airflow, and MinIO** to handle both **batch** and **streaming** financial data from sources like Yahoo Finance and Alpha Vantage.

---

### 🗺️ Architecture

![Pipeline Architecture](./stock_batch_stream.jpeg)

---

### 🔍 Project Overview

This pipeline handles both **batch** and **real-time** data ingestion and processing for stock market data. Below are the key stages:

---

#### ✅ 1. **Batch Ingestion**

- A **Kafka batch producer** fetches historical data from APIs like Yahoo Finance or Alpha Vantage.
- The producer pushes the data into a Kafka topic.
- A **Spark batch processor** reads from Kafka, processes the data, and stores it in **MinIO** as partitioned **Parquet** files.

---

#### 🔄 2. **Streaming Ingestion**

- A Kafka **streaming producer** continuously sends synthetic or real-time stock price updates.
- **Spark Structured Streaming** reads the live Kafka stream, applies windowed transformations (e.g., sliding window aggregations), and writes results to MinIO in near real-time.

---

#### 📦 3. **Raw & Processed Storage Zones (MinIO)**

- Raw, unprocessed data is first dumped into a MinIO `raw/` folder.
- Spark jobs clean and transform this data, then save it into a `processed/` folder, all in **Parquet format** for optimized analytics.

---

#### 🧩 4. **Processing Layer with Spark**

- Spark is used for both **batch** and **streaming** processing.
- Batch jobs perform transformations like column pruning, schema enforcement, and timestamp parsing.
- Streaming jobs apply aggregations and temporal logic on real-time data using **Spark Structured Streaming** with **sliding windows**.

---

#### ⏱️ 5. **Orchestration with Airflow**

- **Apache Airflow** is used to automate the end-to-end batch pipeline.
- DAGs are defined to schedule the batch producers, monitor data arrival, and trigger Spark jobs.
- Airflow also allows future integration with external sinks like **Snowflake** for loading processed data.

---

> This modular architecture enables scalable, fault-tolerant data ingestion and processing across batch and real-time use cases using open-source tools in Docker.

