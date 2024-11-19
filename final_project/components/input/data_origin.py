import argparse
import json

from confluent_kafka import Producer
from sseclient import SSEClient as EventSource


WIKI_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def stream_wikimedia_changes_to_kafka(kafka_broker: str, kafka_topic: str, server_name: str, action_type: str, verbose: bool = False):
    """Stream data from Wikimedia and send it to a Kafka topic."""
    kafka_producer = Producer({
        'bootstrap.servers': kafka_broker,
        'client.id': 'wikipedia-producer'
    })
    
    print(f"Starting to stream {server_name} {action_type}s to Kafka topic '{kafka_topic}'...")
    
    for event in EventSource(WIKI_URL):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                continue
            if change['meta']['domain'] == 'canary': # We are discarding monitoring events
                continue
            if server_name is not None and change['server_name'] != server_name:
                continue
            if action_type is not None and change['type'] != action_type:
                continue
            try:
                kafka_producer.produce(kafka_topic, value=event.data)
                kafka_producer.flush()  # Ensure the message is sent
                if verbose:
                    print(f"Message sent to Kafka: {event.data[:50]}...")
            except Exception as e:
                print(f"Error producing message to Kafka: {e}")
                break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Stream Wikipedia changes to a Kafka topic.")
    parser.add_argument('--broker', required=True, help="Kafka broker address (e.g., 'localhost:9092')")
    parser.add_argument('--topic', required=True, help="Kafka topic to send messages")
    parser.add_argument('--server_name', default='en.wikipedia.org', help="Wikipedia server name to stream from")
    parser.add_argument('--action_type', default='edit', help="Action type to stream")
    parser.add_argument('--verbose', 
                        action='store_true', 
                        help="Log streamed messages to the chat", 
                        default=False)
    args = parser.parse_args()

    stream_wikimedia_changes_to_kafka(
        args.broker, 
        args.topic, 
        args.server_name, 
        args.action_type, 
        args.verbose,
    )
