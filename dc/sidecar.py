import logging
import json


class Sidecar:
    def __init__(self, client, node_id):
        self.client = client
        self.node_id = node_id
        self.node_table = {}

    def broadcast_presence(self):
        data = {'name': self.client._client_id.decode(), 'id': self.node_id}
        self.client.publish("nodes/announce", json.dumps(data))

    def on_message(self, client, userdata, msg):
        logging.info(f"Message received on topic {msg.topic}")
        data = json.loads(msg.payload.decode())
        if msg.topic == "nodes/announce":
            self.handle_announcement(data)
        elif msg.topic.startswith(f"nodes/data/{self.node_id}"):
            self.handle_data(data)

    def handle_announcement(self, data):
        logging.info(f"Node {data['name']} with ID {data['id']} joined.")
        self.node_table[data['id']] = data['name']

    def handle_data(self, data):
        # This should implement the logic for handling data based on the node's role
        logging.info(f"Data handling for Node ID {self.node_id}")
