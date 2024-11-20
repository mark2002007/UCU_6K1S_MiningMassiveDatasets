FROM python:3.9-slim

ENV BROKER="localhost:9092"
ENV TOPIC="wikipedia_data"
ENV SERVER_NAME=""
ENV ACTION_TYPE=""
ENV VERBOSE="false"

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD python components/input/data_origin.py \
    --broker $BROKER \
    --topic $TOPIC \
    $(if [ ! -z "$SERVER_NAME" ]; then echo --server_name $SERVER_NAME; fi) \
    $(if [ ! -z "$ACTION_TYPE" ]; then echo --action_type $ACTION_TYPE; fi) \
    $(if [ "$VERBOSE" = "true" ]; then echo --verbose; fi)
