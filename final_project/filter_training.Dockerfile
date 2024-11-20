FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt --no-cache-dir
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

ENV DRIVER_MEMORY=4G
ENV BROKER=localhost:9092
ENV TOPIC=wikipedia_bots
ENV FILTER_PATH=./data/filter
ENV TRAIN_PERIOD=3600
ENV FORGET_PERIOD=604800

ENTRYPOINT ["bash", "-c", "$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --driver-memory $DRIVER_MEMORY \
  components/inference/filter_training.py \
  --broker $BROKER \
  --topic $TOPIC \
  --filter_path $FILTER_PATH \
  --train_period $TRAIN_PERIOD \
  --forget_period $FORGET_PERIOD"]