import paho.mqtt.client as mqtt
import json
import logging
from dc.constants import MessageType, enum_to_json

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Sidecar:
    def __init__(self, node, broker_address, broker_port=1883,
                 callback_api_version=mqtt.CallbackAPIVersion.VERSION2):
        self.node = node
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'NODE-{node.name}',)

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect(broker_address, port=broker_port, keepalive=60)

        self.client.loop_start()

        # self.sign_up()

    def on_connect(self, client, userdata, flags, rc, prop):
        logging.info(f"Sidecar connected with result code {rc}")
        client.subscribe("node/#")

    def on_message(self, client, userdata, msg):
        logging.info(f"On message: {msg.topic}")
        
        dpayload = msg.payload.decode('utf-8')

        jpayload = json.loads(dpayload)

        if type(jpayload) == str: # if the payload is a string, after decoding.
            jpayload = json.loads(jpayload)

        logging.info(F"JSON payload: [{type(jpayload)}] - {json.dumps(jpayload)}")    
        
        if(msg.topic == "node/signup"):
            logging.info("Signup request received")
            self.node.on_sign_up_message(jpayload)

            logging.info(F"Nodes table: {self.node.nodes}")
        elif(msg.topic == "node/save_data"):
            logging.info("Save data request received")
            self.node.on_save_data_message(jpayload)
        elif(msg.topic == "node/get_data"):
            logging.info("Get data request received")
            self.node.on_get_data_message(jpayload)
        elif(msg.topic == "node/hash_data"):
            logging.info("Hashed data request received")
            self.node.on_hashed_data_message(jpayload)

    def publish(self, topic, data):
        self.client.publish(topic, json.dumps(data))
        
        logging.info(f"Published to {topic}: {data}")
