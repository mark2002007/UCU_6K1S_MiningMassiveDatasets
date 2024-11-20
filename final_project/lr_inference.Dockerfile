FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt --no-cache-dir
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

ENV DRIVER_MEMORY=10G
ENV BROKER=localhost:9092
ENV TOPIC_IN=wikipedia_data
ENV TOPIC_OUT=wikipedia_bots
ENV MODEL_PATH=./data/logreg_small
ENV SAMPLING_FREQ=0.2

ENTRYPOINT ["bash", "-c", "$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1 \
  --driver-memory $DRIVER_MEMORY \
  components/inference/lr_inference.py \
  --broker $BROKER \
  --topic_in $TOPIC_IN \
  --topic_out $TOPIC_OUT \
  --model_path $MODEL_PATH \
  --sampling_freq $SAMPLING_FREQ"]