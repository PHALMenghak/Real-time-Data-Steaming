FROM apache/spark:3.5.0

WORKDIR /app

COPY requirements.txt .
USER root
RUN pip install --no-cache-dir -r requirements.txt
USER spark
COPY streaming_job.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--conf", \
     "spark.jars.ivy=/tmp/.ivy2", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "/app/streaming_job.py"]