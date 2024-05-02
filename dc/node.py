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

        self.nodes = set()

        self.sidecar = Sidecar(self, config['broker'], config['port'])

    @staticmethod
    def generate_random_name():
        letters = 'abcdefghijklmnopqrstuvwxyz'
        return ''.join(random.choice(letters) for _ in range(5))

    def get_own_data(self):
        return {
            "node_id": self.id,
            "node_name": self.name,
            "node_address": str(self.sidecar.client._client_id),
            "node_type": "follower",
            "is_leader": False if self.leader_id is None else True
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
            },
        }

        message = json.dumps(message)

        self.sidecar.publish("node/signup", message)
        # Add message handling logic here

    def on_sign_up_message(self, message):
        # skip if the request is from self
        print("Signup request received")
        
        if message['node_data']['name'] == self.name:
            return

        node_data = message['node_data']

        # update the nodes list
        existing_node = {
            "node_id": node_data['node_id'] if 'node_id' in node_data else None,
            "node_name": node_data['node_name'],
            "node_address": node_data['node_address'] if 'node_address' in node_data else None,
            "node_type": node_data['node_type'],
        }
        
        self.nodes.add(existing_node)

        # if the message is from a leader update leader details
        if node_data['is_leader'] == True:
            self.leader_id = node_data['node_id']
            
        # if the message is from another new node, publish own data as a response message
        if node_data['type'] == MessageType.NEW_NODE_SIGNUP:
            own_data = self.get_own_data()

            message = {
                "type": enum_to_json(MessageType.NEW_NODE_SIGNUP_RESPONSE),
                "node_data": own_data
            }

            self.sidecar.publish("node/signup", json.dumps(message))

    def on_data_save_request(self, data):
        pass

    def on_data_retrieve_request(self, data):
        pass

    

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
