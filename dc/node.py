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
from threading import Thread, Timer


config = {
    'broker': 'localhost',
    'port': 6699,
    'keepalive': 60,
    'callback_api_version': mqtt.CallbackAPIVersion.VERSION2
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

        timer = Timer(5, self.init_complete)
        timer.start()

        self.sidecar = Sidecar(self, config['broker'], config['port'])

        # Heartbeat related variables for leader availability check
        self.last_heartbeat = time.time()
        self.heartbeat_timeout = 15  # seconds
    
    def monitor_heartbeat(self):
        def check_heartbeat():
            while True:
                if time.time() - self.last_heartbeat > self.heartbeat_timeout:
                    print('Heartbeat missed. Checking leader status or triggering election.')
                    self.handle_leader_absence()
                time.sleep(2)

        threading.Thread(target=check_heartbeat).start()
    
    # Leader selection
    def handle_leader_absence(self):
        # Implement leader election or other recovery mechanisms
        if not self.leader_id is None:
            return

        self.initiate_election()
    
    def initiate_election(self):
        print(f'Node {self.id} initiating election.')
        responded = False
        for node in self.nodes:
            if node['id'] > self.id:
                self.send_election_message(node['id'])
                responded = True
        
        if not responded:
            self.declare_victory()    

    def send_election_message(self, node_id):
        message = {
            'type': enum_to_json(MessageType.ELECTION), 
            'from_node_id': self.node_id,
            'to_node_id': node_id
        }

        self.sidecar.publish('node/election', message)

    def on_election_message(self, message):
        if message['from_node_id'] > self.node_id:
            self.send_ok_message(message['from_node_id'])

    def send_ok_message(self, to_node_id):
        message = {
            'type': 'ok', 
            'from_node_id': self.node_id,
            'to_node_id': to_node_id
        }

        self.sidecar.publish('node/election_ok', message)

    def on_election_ok_message(self, message):
        # Wait for either timeout or victory message from other nodes
        pass

    def declare_victory(self):
        self.is_leader = True
        for node in self.nodes:
            if node['id'] != self.d:
                self.send_victory_message(node['id'])

    def send_victory_message(self, node_id):
        message = {
            'type': 'victory', 
            'new_leader_id': self.node_id
        }

        self.sidecar.publish(f'node/{node_id}/victory', message)

    def on_victory_message(self, message):
        self.leader_id = message['new_leader_id']
        if self.node_id == message['new_leader_id']:
            self.is_leader = True
            self.leader = Leader()
        else:
            self.is_leader = False
    # End of leader selection

    def on_heartbeat_message(self, message):
        # Extend existing message handling to update last_heartbeat on receiving a heartbeat
        if message.type == 'heartbeat' and message.node_id == self.leader_id:
            self.last_heartbeat = time.time()

    @staticmethod
    def generate_random_name():
        letters = 'abcdefghijklmnopqrstuvwxyz'
        return ''.join(random.choice(letters) for _ in range(5))


    def init_complete(self):
        logging.info('Updating own id')

        # get all the ids of the nodes in self.nodes
        node_ids = [node['node_id'] for node in self.nodes if node['node_id'] is not None]
        # get the maximum id
        max_id = max(node_ids)
        self.id = max_id + 1

        logging.info(f'Own id updated to {self.id}')

        # if there are no nodes in the network, declare self as the leader
        if len(self.nodes) == 1:
            self.leader = Leader(self)
            self.leader_id = self.id
            self.type = 'hasher'

        # update the node id in nodes list
        for node in self.nodes:
            if node['node_name'] == self.name:
                node['id'] = self.id
                node['is_leader'] = True if self.leader_id == self.id else False
                node['type'] = self.type
        
        logging.info(f'Nodes list updated: {self.nodes}')

        # broadcast the updated node data to all the nodes
        message = {
            'type': enum_to_json(MessageType.NEW_NODE_SIGNUP_COMPLETE),
            'node_data': self.get_own_data()
        }
        self.sidecar.publish('node/signup_complete', message)


    def get_own_data(self):
        return {
            'id': self.id,
            'name': self.name,
            'address': str(self.sidecar.client._client_id),
            'type': self.type,
            'is_leader': True if self.leader_id == self.id else False
        }

    # Signup
    def sign_up(self):
        print('calling signup message')

        print(self.sidecar.client._client_id)
        print(type(self.sidecar.client._client_id))

        message = {
            'type': enum_to_json(MessageType.NEW_NODE_SIGNUP),
            'node_data': {
                'id': None,
                'name': self.name,
                'address': str(self.sidecar.client._client_id),
                'type': self.type,
                'is_leader': False,
            },
        }

        message = json.dumps(message)

        self.sidecar.publish('node/signup', message)
        
        self.nodes.append(self.get_own_data())

    def on_sign_up_message(self, message):
        # skip if the request is from self
        if message['node_data']['name'] == self.name:
            return

        message_type = MessageType[message['type']]
        logging.info(f'Received message of type {message_type}')

        node_data = message['node_data']

        if message_type == MessageType.NEW_NODE_SIGNUP:
            # if the message is from another new node, publish own data as a response message
            own_data = self.get_own_data()

            message = {
                'type': enum_to_json(MessageType.NEW_NODE_SIGNUP_RESPONSE),
                'node_data': own_data
            }

            self.sidecar.publish('node/signup', json.dumps(message))
        elif message_type == MessageType.NEW_NODE_SIGNUP_RESPONSE:
            logging.info(F'Response Message : {node_data}')
            # if the message is from a leader update leader details
            if node_data['is_leader'] == True:
                self.leader_id = node_data['node_id']
            
        # update the nodes list
        existing_node = {
            'id': node_data['id'] if node_data['id'] else None,
            'name': node_data['name'],
            'address': node_data['address'] if 'address' in node_data else None,
            'type': node_data['type'],
            'is_leader': node_data['is_leader'],
        }
        
        self.nodes.append(existing_node)

    def on_sign_up_complete_message(self, message):
        logging.info(f'Received message: {message}')

        node_data = message['node_data']

        # if the message is not from the self, update the node data
        if node_data['name'] != self.name:
            new_node = self.find_node_by_name(node_data['name'])
            new_node['id'] = node_data['id']
            new_node['address'] = node_data['address']
            new_node['type'] = node_data['type']
            new_node['is_leader'] = node_data['is_leader']

    def on_data_save_request(self, data):
        # if the self is the leader, randomly select a hasher node from the nodes list
        # and send the data to the hasher node
        if self.leader_id == self.id:
            hasher_node = self.get_random_hasher()

            self.sidecar.publish(f'node/hash_data/{hasher_node['node_id']}', data)

    def on_data_retrieve_request(self, data):
        pass

    def find_node_by_name(self, name):
        for node in self.nodes:
            if node['name'] == name:
                return node

        return None

    def add_to_node_list(self, node_data):
        if self.find_node_by_name(node_data['name']) is None:
            self.nodes.append(node_data)

    def get_random_hasher(self):
        hashers = [node for node in self.nodes if node['node_type'] == 'hasher']
        
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
        print('\nKeyboard interrupt detected.\nExiting gracefully...')
        exit()
