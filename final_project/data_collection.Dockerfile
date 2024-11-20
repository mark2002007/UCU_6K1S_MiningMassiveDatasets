FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt --no-cache-dir
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

ENV DRIVER_MEMORY=2G
ENV BROKER=localhost:9092
ENV TOPIC=wikipedia_data
ENV SAMPLING_FREQ=0.5
ENV DEST_FOLDER=./data/csv_stream

ENTRYPOINT ["bash", "-c", "$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --driver-memory $DRIVER_MEMORY \
  components/data_collection/data_collection.py \
  --broker $BROKER \
  --topic $TOPIC \
  --dest_folder $DEST_FOLDER \
  --sampling_freq $SAMPLING_FREQ"]
