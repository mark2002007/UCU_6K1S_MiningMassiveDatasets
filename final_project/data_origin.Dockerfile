FROM python:3.9-slim

USER root

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt --no-cache-dir
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /app

ENV BROKER="localhost:9092"
ENV TOPIC="wikipedia_data"
ENV SERVER_NAME=""
ENV ACTION_TYPE=""
ENV VERBOSE="false"

CMD python components/input/data_origin.py \
    --broker $BROKER \
    --topic $TOPIC \
    $(if [ ! -z "$SERVER_NAME" ]; then echo --server_name $SERVER_NAME; fi) \
    $(if [ ! -z "$ACTION_TYPE" ]; then echo --action_type $ACTION_TYPE; fi) \
    $(if [ "$VERBOSE" = "true" ]; then echo --verbose; fi)
