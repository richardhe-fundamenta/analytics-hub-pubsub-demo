import time
import uuid
import json

from google.cloud import pubsub_v1

# Set these variables
project_id = "practical-gcp-sandbox-1"
topic_id = "streaming-demo-topic"

# Initialize a Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def generate_unique_uuid4() -> str:
    """
    Generate a unique UUID4 and return it as a string.

    :return: A UUID4 string.
    """
    uuid4 = uuid.uuid4()
    return str(uuid4)


def publish_message(message):
    """Publishes a message to the Pub/Sub topic."""
    future = publisher.publish(topic_path, message.encode("utf-8"))
    print(f"Published message ID: {future.result()}")


def main():
    try:
        while True:
            for i in range(10):
                message_dict = {"id": generate_unique_uuid4(), "content": 'random content'}
                message = json.dumps(message_dict)
                publish_message(message)
                time.sleep(0.1)  # Send 10 messages a second (0.1 seconds interval)
            time.sleep(1)  # Ensure the loop maintains a 10 messages per second rate
    except KeyboardInterrupt:
        print("Publishing interrupted.")


if __name__ == "__main__":
    main()
