import configparser
import json
import threading

from dc.hasher import Hasher
from dc.receiver import Receiver
from dc.sidecar import Sidecar
from dc.constants import MessageType, enum_to_json
import random
import hashlib
import paho.mqtt.client as mqtt
import logging


config = {
    "broker": "localhost",
    "port": 6699,
    "keepalive": 60,
    "callback_api_version": mqtt.CallbackAPIVersion.VERSION2
}


class Node(Hasher, Receiver):
    def __init__(self):
        self.node_id = None
        self.node_name = Node.generate_random_name()
        self.leader_id = None

        self.sidecar = Sidecar(self.node_name, config['broker'], config['port'], config['callback_api_version'])
        self.heartbeat_timer = None

    @staticmethod
    def generate_random_name():
        letters = 'abcdefghijklmnopqrstuvwxyz'
        return ''.join(random.choice(letters) for _ in range(5))

    def sign_up(self):
        message = {
            "node_name": self.node_name,
            "type": enum_to_json(MessageType.NEW_NODE_SIGNUP)
        }

        message = json.dumps(message)

        self.publish("node/signup", message)

    def publish(self, topic, data):
        self.sidecar.publish(topic, data)

    def elect_leader(self):
        self.broadcast_presence()
        self.initiate_election()

    def broadcast_presence(self):
        data = {'name': f"Node_{self.node_id}", 'id': self.node_id}
        self.sidecar.publish("nodes/announce", data)

    def initiate_election(self):
        logging.info(f"Node {self.node_id} is initiating an election.")
        for id in range(self.node_id + 1, 5):  # Assuming 5 is the total number of nodes
            self.sidecar.publish(f"election/{id}", {'type': 'election', 'from': self.node_id})

    def handle_election_message(self, data):
        if data['type'] == 'election':
            logging.info(f"Node {self.node_id} received an election message from {data['from']}")
            # Respond to show this node is alive
            self.sidecar.publish(f"nodes/election/{data['from']}", {'type': 'response', 'from': self.node_id})
            # Take over the election process
            self.initiate_election()

    def handle_coordinator_message(self, data):
        self.leader_id = data['leader']
        logging.info(f"Node {self.node_id} recognizes Node {self.leader_id} as the leader.")

    def declare_as_leader(self):
        logging.info(f"Node {self.node_id} is declaring itself as the leader.")
        self.leader_id = self.node_id
        self.sidecar.publish("coordinator", {'leader': self.node_id})

    def check_responses(self, responses):
        if not responses:  # If no responses from higher nodes
            self.declare_as_leader()

    def start_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(10.0, self.heartbeat_expired)
        self.heartbeat_timer.start()

    def heartbeat_expired(self):
        logging.info(f"Heartbeat expired for leader {self.leader_id}")
        self.initiate_election()

    def handle_heartbeat(self, data):
        if data['leader'] == self.leader_id:
            logging.info(f"Heartbeat received from leader {self.leader_id}")
            self.start_heartbeat_timer()

    def send_heartbeat(self):
        if self.node_id == self.leader_id:
            self.sidecar.publish("nodes/heartbeat", {'leader': self.node_id})
            threading.Timer(5.0, self.send_heartbeat).start()


def main():
    node = Node()
    node.sign_up()


if __name__ == '__main__':
    main()
