from dc.hasher import Hasher
from dc.receiver import Receiver
import random
import hashlib
import paho.mqtt.client as mqtt


class Node(Hasher, Receiver):
    type = None

    def __init__(self, node_id, node_name):
        self.node_id = node_id
        self.node_name = node_name
        self.node_table = {}
        self.value_table = {}
        self.receiver = False
        self.hasher = False

    def generate_random_name(self):
        letters = 'abcdefghijklmnopqrstuvwxyz'
        return ''.join(random.choice(letters) for _ in range(5))

    def hash_string(self, s):
        return hashlib.sha1(s.encode()).hexdigest()[:10]

    def send_broadcast_message(self, client):
        for node_id, node_name in self.node_table.items():
            client.publish(f"node/{node_id}", f"New node joined: {self.node_name} ({self.node_id})")

    def on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode()
        if topic.startswith("node/") and payload.startswith("New node joined:"):
            node_id = topic.split("/")[-1]
            self.node_table[node_id] = payload.split(": ")[1].split(" ")[0]
            self.send_peer_info(client, node_id)

    def send_peer_info(self, client, node_id):
        client.publish(f"node/{node_id}/info", f"Node info: {self.node_name} ({self.node_id})")

    def on_info_received(self, client, userdata, message):
        node_id = message.topic.split("/")[1]
        node_info = message.payload.decode().split(": ")[1]
        self.node_table[node_id] = node_info

    def join_cluster(self, client):
        self.node_name = self.generate_random_name()
        self.node_id = random.randint(0, 4)
        self.send_broadcast_message(client)

    def decide_role(self):
        num_nodes = len(self.node_table)
        if num_nodes % 2 == 0:
            self.hasher = True
        else:
            self.receiver = True

    def store_data(self, key, value):
        self.value_table[key] = value

    def relay_to_hasher(self, key):
        hasher_node_id = self.hash_string(key) % len(self.node_table)
        return hasher_node_id

    def retrieve_data(self, key, client):
        if self.receiver:
            hasher_node_id = self.relay_to_hasher(key)
            client.publish(f"node/{hasher_node_id}/retrieve", key)

    def on_retrieval_request(self, client, userdata, message):
        key = message.payload.decode()
        if self.hasher:
            if key in self.value_table:
                value = self.value_table[key]
                client.publish(f"node/{self.node_id}/reply", value)


def main():
    # Initialize MQTT client
    client = mqtt.Client()
    client.connect("localhost", 6699, 60)

    # Initialize node
    node = Node(0, "initial_node")
    node.join_cluster(client)
    client.subscribe("node/#")
    client.on_message = node.on_message
    client.message_callback_add("node/+/info", node.on_info_received)
    client.loop_start()

    # Additional logic to handle different roles and interactions between nodes
    node.decide_role()

    if node.receiver:
        # Implement fault tolerance and consistency algorithm for receivers
        pass

    if node.hasher:
        # Implement hasher logic
        pass

    client.disconnect()


if __name__ == "__main__":
    main()
