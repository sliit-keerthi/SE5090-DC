import paho.mqtt.publish as publish


def main():
    # Publish a message to a topic
    publish.single("test/topic", "Hello, Mosquitto DC!", hostname="localhost", port=6699)


if __name__ == "__main__":
    main()
