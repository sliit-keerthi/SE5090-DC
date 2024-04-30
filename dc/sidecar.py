import paho.mqtt.client as mqtt
import json
import logging


# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Sidecar:
    def __init__(self, node_name, broker_address, broker_port=1883,
                 callback_api_version=mqtt.CallbackAPIVersion.VERSION2):
        self.node_name = node_name
        self.client = mqtt.Client(f"Sidecar_{node_name}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(broker_address,  port=broker_port, callback_api_version=callback_api_version)
        self.client.subscribe("nodes/#")
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        logging.info(f"Sidecar connected with result code {rc}")
        self.client.subscribe("nodes/announce")
        self.client.subscribe(f"nodes/data/{self.node_id}")

    def on_message(self, client, userdata, msg):
        logging.info(f"Received message on {msg.topic}: {msg.payload.decode()}")
        # Add message handling logic here

    def publish(self, topic, data):
        self.client.publish(topic, json.dumps(data))
        logging.info(f"Published to {topic}: {data}")
