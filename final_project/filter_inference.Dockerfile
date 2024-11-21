FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt --no-cache-dir
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

# ENV SPARK_HOME=/opt/spark
ENV DRIVER_MEMORY=4G
ENV BROKER=localhost:9092
ENV TOPIC_IN=wikipedia_data
ENV TOPIC_OUT=wikipedia_filtered
ENV FILTER_PATH=./data/filter
ENV FILTER_RELOAD_PERIOD=3600

ENTRYPOINT ["bash", "-c", "$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --driver-memory $DRIVER_MEMORY \
  components/inference/filter_inference.py \
  --broker $BROKER \
  --topic_in $TOPIC_IN \
  --topic_out $TOPIC_OUT \
  --filter_path $FILTER_PATH \
  --filter_reload_period $FILTER_RELOAD_PERIOD"]