import paho.mqtt.client as mqtt
import time
import json
from dc.constants import MessageType, enum_to_json


class Node:
    def __init__(self, node_id, broker_address, broker_port=1883):
        self.node_id = node_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client = mqtt.Client()

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Connect to the MQTT broker
        self.client.connect(self.broker_address, self.broker_port, 60)

        # Start the MQTT client loop in a separate thread
        self.client.loop_start()

        # Broadcast a message
        self.broadcast_message()

    # Signup
    def sign_up(self):
        print('calling signup message')
        message = {
            'node_name': self.node.name,
            'type': enum_to_json(MessageType.NEW_NODE_SIGNUP)
        }

        message = json.dumps(message)

        self.publish('node/signup', message)
        # Add message handling logic here

    def broadcast_message(self):
        # Publish a message to the 'broadcast' topic
        self.client.publish('broadcast', f'New node joined: {self.node_id}')

    def on_connect(self, client, userdata, flags, rc):
        print('Connected to MQTT broker with result code '+str(rc))
        # Subscribe to relevant topics
        client.subscribe('node/#')

    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode()
        print(f'Received message on topic '{topic}': {payload}')

if __name__ == '__main__':
    # Create a Node object and connect to the MQTT broker
    node = Node(node_id='Node1', broker_address='localhost', broker_port=6699)
    
    # Keep the program running to continue listening for messages
    while True:
        time.sleep(1)
