from time import sleep
import json

from paho.mqtt import publish

from gateway.model.mqtt_client import MqttClient
from gateway.service.kafka_service import KafkaService
from service.mongo.mongo_service import MongoService
from utils.common import Common
from utils.logger import Logger


class MqttService(object):
    def __init__(self, client_id, log_obj):
        self.logger = log_obj.logger
        self.client_id = client_id
        self.client = MqttClient(client_id=client_id)

        self.config = Common.load_config('../../configs/mqtt_config.yaml')
        self.mqtt_configs = self.config['mqtt']
        self.__mqtt_host = self.mqtt_configs['host']
        self.__mqtt_port = self.mqtt_configs['port']
        self.__mqtt_topic = self.mqtt_configs['topic']
        self.mqtt_topics = []
        self.set_callbacks()

        self.kafka_config = Common.load_config('../../configs/kafka_configs.yaml')['kafka']
        self.__kafka_host = self.kafka_config['host']
        self.__kafka_topic = self.kafka_config['topic']

        self.mongo_config = Common.load_config('../../configs/mongo.yaml')['mongo']
        self.__mongo_host = self.mongo_config['host']
        self.__mongo_port = self.mongo_config['port']
        self.__mongo_database = self.mongo_config['database']
        self.__mongo_collection = self.mongo_config['collection']

        self.qos = int(self.mqtt_configs['qos'])
        self.kafka_service = KafkaService(self.__kafka_host, log_obj=log_obj)
        self.mongo_service = MongoService(self.__mongo_host, self.__mongo_port, log_obj=log_obj)

    def connect(self, keep_alive=60, bind_address='', reconnect=True, retry_freq=2):
        is_connected = False

        try:
            self.client.connect(host=self.__mqtt_host,
                                port=self.__mqtt_port,
                                keepalive=keep_alive,
                                bind_address=bind_address)

        except ConnectionError as e:

            if not reconnect:
                self.logger.error('Connection error without reconnecting: %s' % e)
                raise e

            else:

                while not is_connected:

                    try:
                        self.client.reconnect()

                    except ConnectionError:
                        sleep(retry_freq)

                else:
                    is_connected = True

    def on_connect(self, client, userdata, flags, rc):
        # logger.info('connected rc: %s' % str(rc))
        pass

    def on_message(self, client, obj, message):

        if message is not None:

            try:
                self.kafka_service.publish(kafka_topic=self.__kafka_topic, msg=message.payload)

            except Exception as e:
                raise e
        message = json.loads(message.payload)
        self.logger.info('Message: %s -> topic: %s: qos: %s from client: %s' %
                         (message, message.topic, str(message.qos), client))

    def on_publish(self, client, obj, mid):
        self.logger.info('mid: %s' % str(mid))

    def on_subscribe(self, client, obj, mid, granted_qos):
        self.logger.info('Subscribed: %s: \tgranted_qos: %s' %
                         (str(mid), str(granted_qos)))

    def set_callbacks(self):
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        self.client.on_connect = self.on_connect

    def publish_single(self, msg):

        try:
            self.logger.info('MQTT Published message [MESSAGE: %s] -> mqtt_topic [TOPIC: %s]' % (msg, self.__mqtt_topic))
            self.client.publish(topic=self.__mqtt_topic, payload=msg, qos=self.qos)

        except Exception as e:
            self.logger.error('Cannot publish message: %s -> topic: %s \tqos:%s [ERROR: %s]' %
                              (str(msg), self.__mqtt_topic, str(self.qos), e))

    def publish_multiple(self, msgs):
        # msgs = [{'topic': "paho/test/multiple", 'payload': "multiple 1"},
        #         ("paho/test/multiple", "multiple 2", 0, False)]
        # publish.multiple(msgs, hostname="mqtt.eclipse.org")

        try:
            publish.multiple(msgs=msgs, hostname=self.__mqtt_host,
                             port=self.__mqtt_port, client_id=self.client_id)

        except Exception as e:
            self.logger.error('Publish message to multiple topics fail: %s' % e)

    def subscribe(self):

        try:
            self.client.subscribe(topic=self.__mqtt_topic, qos=self.qos)
            self.mqtt_topics.append(self.__mqtt_topic)

        except Exception as e:
            self.logger.error('Cannot subscribe to topic: %s -> error: %s' % (self.__mqtt_topic, e))

    @property
    def mqtt_host(self):
        return self.__mqtt_host

    @mqtt_host.setter
    def mqtt_host(self, value):
        self.__mqtt_host = value

    @property
    def mqtt_port(self):
        return self.__mqtt_port

    @mqtt_port.setter
    def mqtt_port(self, value):
        self.__mqtt_port = value


if __name__ == "__main__":
    kafka = KafkaService('localhost:9092', Logger(__name__))
    # kafka.delete_topic(['test'])
    # kafka.create_topic(['iot_platform'])
    mqtt_service = MqttService('test_client', Logger(__name__))
    mqtt_service.connect()
    # mqtt_service.publish_single('mqtt/status', 'test222')

    for i in range(10):
        message_tpl = {
            'device_id': 'test' + str(i),
            'temperature': i
        }
        mqtt_service.publish_single(str(message_tpl))
    mqtt_service.subscribe()
    mqtt_service.client.loop_forever()
