import paho.mqtt.client as mqtt


def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()}")


def main():
    # Initialize MQTT client
    # create an mqtt Client
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message

    # Connect to local Mosquitto broker
    client.connect("localhost", 6699, 60)

    # Subscribe to a topic
    client.subscribe("node/+")

    # Start the MQTT client loop
    client.loop_forever()


if __name__ == "__main__":
    main()
