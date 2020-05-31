import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/tinuade/Downloads/marvis-vdohmu-2777cc606436.json'
from google.cloud import pubsub_v1
import requests
import ast

project_id = "marvis-vdohmu"
subscription_name = "consumer"
timeout = 10.0  # "How long the subscriber should listen for messages in seconds"
subscriber = pubsub_v1.SubscriberClient()


def run(message):
    message.ack()
    message = message.data
    message = ast.literal_eval(message.decode('utf-8'))
    r = requests.post("http://localhost:5000/callback", json=message)
    print("message handled: ", r.text)


def consumer_listener():
    subscription_path = subscriber.subscription_path(
        project_id, subscription_name
    )
    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=run
    )

    print("Listening for messages on {}..\n".format(subscription_path))

    # result() in a future will block indefinitely if `timeout` is not set,
    # unless an exception is encountered first.
    try:
        message = streaming_pull_future.result(timeout=timeout)
        return message
    except:  # noqa
        streaming_pull_future.cancel()

while True:
    consumer_listener()
