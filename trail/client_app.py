import paho.mqtt.client as mqtt

config = {
    "broker": "localhost",
    "port": 6699,
    "keepalive": 60,
    "callback_api_version": mqtt.CallbackAPIVersion.VERSION2
}

# define text with a dummy text
TEXT = """LOREM IPSUM DOLOR SIT AMET, CONSECTETUR ADIPISCING ELIT. UT VIVERRA, 
LIGULA SIT AMET VARIUS VEHICULA, LIGULA LIGULA VARIUS LIGULA, VEL VARIUS.
"""

# write the code for requesting to save a text, which publishes to the leader
def save_data(content):
    client = mqtt.Client(callback_api_version=config['callback_api_version'])
    client.connect(config['broker'], config['port'], config['keepalive'])

    client.loop_start()
    
    # publish to the leader
    client.publish("node/save_data", content)

    client.loop_stop()

if __name__ == "__main__":
    save_data(TEXT)
