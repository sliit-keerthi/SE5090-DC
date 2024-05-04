import configparser
import json
import threading
import time

from dc.hasher import Hasher
from dc.receiver import Receiver
from dc.sidecar import Sidecar
from dc.constants import MessageType, enum_to_json
from dc.leader import *

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
        self.id = None
        self.name = Node.generate_random_name()
        self.leader_id = None
        self.heartbeat_timer = None
        self.leader = None
        self.type = None

        self.nodes = list()

        self.sidecar = Sidecar(self, config['broker'], config['port'])

    @staticmethod
    def generate_random_name():
        letters = 'abcdefghijklmnopqrstuvwxyz'
        return ''.join(random.choice(letters) for _ in range(5))

    def get_own_data(self):
        return {
            "id": self.id,
            "name": self.name,
            "address": str(self.sidecar.client._client_id),
            "type": self.type,
            "is_leader": True if self.leader_id == self.id else False
        }

    # Signup
    def sign_up(self):
        print("calling signup message")

        print(self.sidecar.client._client_id)
        print(type(self.sidecar.client._client_id))

        message = {
            "type": enum_to_json(MessageType.NEW_NODE_SIGNUP),
            "node_data": {
                "id": None,
                "name": self.name,
                "address": str(self.sidecar.client._client_id),
                "type": self.type,
                "is_leader": False,
            },
        }

        message = json.dumps(message)

        self.sidecar.publish("node/signup", message)
        
        self.nodes.append(self.get_own_data())

    def on_sign_up_message(self, message):
        # skip if the request is from self
        if message['node_data']['name'] == self.name:
            return

        message_type = MessageType[message['type']]
        logging.info(f"Received message of type {message_type}")

        node_data = message['node_data']

        if message_type == MessageType.NEW_NODE_SIGNUP:
            # if the message is from another new node, publish own data as a response message
            own_data = self.get_own_data()

            message = {
                "type": enum_to_json(MessageType.NEW_NODE_SIGNUP_RESPONSE),
                "node_data": own_data
            }

            self.sidecar.publish("node/signup", json.dumps(message))
        elif message_type == MessageType.NEW_NODE_SIGNUP_RESPONSE:
            logging.info(F"Response Message : {node_data}")
            # if the message is from a leader update leader details
            if node_data['is_leader'] == True:
                self.leader_id = node_data['node_id']
            
        # update the nodes list
        existing_node = {
            "node_id": node_data['id'] if node_data["id"] else None,
            "node_name": node_data['name'],
            "node_address": node_data['address'] if 'address' in node_data else None,
            "node_type": node_data['type'],
            "is_leader": node_data['is_leader'],
        }
        
        self.nodes.append(existing_node)

    def on_data_save_request(self, data):
        # if the self is the leader, randomly select a hasher node from the nodes list
        # and send the data to the hasher node
        if self.leader_id == self.id:
            hasher_node = self.get_random_hasher()

            self.sidecar.publish(f"node/hash_data/{hasher_node['node_id']}", data)

    def on_data_retrieve_request(self, data):
        pass

    def find_node_by_name(self, name):
        for node in self.nodes:
            if node['node_name'] == name:
                return node

        return None

    def add_to_node_list(self, node_data):
        if self.find_node_by_name(node_data['name']) is None:
            self.nodes.append(node_data)

    def get_random_hasher(self):
        hashers = [node for node in self.nodes if node['node_type'] == "hasher"]
        
        if len(hashers) == 0:
            return None
        else:
            return random.choice(hashers)
               
    # Leader selection

    # Hasher

    # Receiver



def main():
    node = Node()
    node.sign_up()

    # write the code for set a timer timeout of 5 seconds
    # and check the number of nodes in the network
    # if the number of nodes is less than 2, declare self as the leader
    time.sleep(5)
    if len(node.nodes) < 2:
        node.id = 0
        node.leader = Leader()

    while True:
        time.sleep(1)


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("\nKeyboard interrupt detected.\nExiting gracefully...")
        exit()
