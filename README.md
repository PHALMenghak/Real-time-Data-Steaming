# Stock Data Streaming

End-to-end real-time stock market streaming pipeline built with Kafka, Spark Structured Streaming, PostgreSQL, Airflow, and Streamlit. The producer ingests live trade data from Finnhub, publishes it to Kafka, Spark processes the stream in windows, and the results are stored in PostgreSQL for downstream access and visualization.

## Architecture

1. `producer/producer.py` connects to Finnhub WebSocket, subscribes to a set of stock symbols, and publishes trade events to Kafka.
2. `spark/streaming_job.py` reads events from Kafka, parses and cleans the JSON payloads, computes 1-minute windowed aggregations, and writes the processed results to PostgreSQL.
3. `database/init.sql` creates the raw and processed tables used by the pipeline.
4. `dashboard/app.py` provides a Streamlit dashboard for viewing the processed data.
5. `airflow/dags/stock_pipeline_dag.py` is the Airflow DAG location for orchestrating the pipeline.

## Services

The project is designed to run with Docker Compose and starts these services:

- Zookeeper and Kafka for event streaming.
- PostgreSQL for raw and processed stock data.
- Spark master and worker for stream processing.
- Spark streaming job container for the Kafka to PostgreSQL pipeline.
- Airflow webserver, scheduler, and init container.
- Streamlit dashboard for data visualization.
- Producer container for live trade ingestion.

## Prerequisites

- Docker and Docker Compose
- A Finnhub API key for the producer
- Git, if you want to publish the project to GitHub

## Configuration

Create these environment files before running the stack:

- `.env` in the project root for PostgreSQL and Airflow settings
- `producer/.env` for Kafka and Finnhub settings

Example root `.env` values:

```env
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=stock-data

POSTGRES_DB=stocks
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

AIRFLOW_UID=50000
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://admin:admin@postgres:5432/stocks
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Example `producer/.env` values:

```env
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=stock-data
FINNHUB_API_KEY=your_finnhub_api_key
```

## Run Locally

1. Build and start the full stack:

   ```bash
   docker compose up --build
   ```

2. Wait for the services to initialize. The producer reconnects until Kafka is ready, and the Spark streaming job creates the Kafka topic if needed.

3. Open the available services in your browser:
   - Streamlit dashboard: `http://localhost:8501`
   - Airflow webserver: `http://localhost:8081`
   - Spark master UI: `http://localhost:8080`
   - PostgreSQL: `localhost:5432`

## Data Flow

- Finnhub trade events are collected by the producer.
- Events are serialized and pushed into Kafka on the `stock-data` topic.
- Spark consumes the stream, converts timestamps, applies watermarking, and computes metrics such as moving average price, volatility, total volume, price range, and volume spikes.
- Aggregated results are written to `processed_stock_data` in PostgreSQL.

## Database Tables

The PostgreSQL initialization script creates:

- `raw_stock_data` for incoming events
- `processed_stock_data` for windowed analytics

## Project Structure

```text
docker-compose.yml
airflow/
  dags/
    stock_pipeline_dag.py
dashboard/
  app.py
  Dockerfile
  requirements.txt
database/
  init.sql
producer/
  Dockerfile
  producer.py
  requirements.txt
spark/
  Dockerfile
  requirements.txt
  streaming_job.py
```

## Notes

- Do not commit API keys or local environment files.
- The repository includes a `.gitignore` to keep `.env` files, virtual environments, and cache directories out of Git.
- If you want, I can also add a more detailed setup section for GitHub, Airflow, or the Streamlit dashboard.
