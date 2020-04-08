from paho.mqtt.client import Client


class MqttClient(Client):
    def __init__(self, client_id):
        super().__init__(client_id=client_id, clean_session=True)
