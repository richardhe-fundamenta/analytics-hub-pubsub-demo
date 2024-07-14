import time
from google.cloud import pubsub_v1

# Set your project ID and subscription ID
project_id = 'practical-gcp-sandbox-2'
subscription_id = 'streaming-demo-subscription-pull'

# Create a Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    if message.attributes:
        print("Attributes:")
        for key in message.attributes:
            value = message.attributes[key]
            print(f"{key}: {value}")
    message.ack()


def main():
    # Subscribe to the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Keep the main thread alive to listen for messages
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()


if __name__ == '__main__':
    main()
