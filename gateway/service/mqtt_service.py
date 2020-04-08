from time import sleep
from multiprocessing import Queue

from paho.mqtt import publish, subscribe

# from gateway.model.mqtt_client import MqttClient
from paho.mqtt.client import Client as MqttClient
from utils.common import Common
from utils.logger import logger, logging_config


class MqttService(object):
    def __init__(self, client_id):
        self.client_id = client_id
        self.client = MqttClient(client_id=client_id)
        self.config = Common.load_config('configs/mqtt_config.yaml')
        self.mqtt_configs = self.config['mqtt']
        self.__host = self.mqtt_configs['host']
        self.__port = self.mqtt_configs['port']
        self.qos = int(self.mqtt_configs['qos'])
        self.topics = []
        self.mqtt_queue = Queue()
        self.set_callbacks()

    def connect(self, keep_alive=60, bind_address='', reconnect=True, retry_freq=2):
        is_connected = False

        try:
            self.client.connect(host=self.__host,
                                port=self.__port,
                                keepalive=keep_alive,
                                bind_address=bind_address)

        except ConnectionError as e:

            if not reconnect:
                logger.error('Connection error without reconnecting: %s' % e)
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
                self.mqtt_queue.put(message)

            except Exception as e:
                raise e
        logger.info('Message: %s -> topic: %s: qos: %s from client: %s' %
                    (message.payload, message.topic, str(message.qos), client))

    def on_publish(self, client, obj, mid):
        logger.info('mid: %s' % str(mid))

    def on_subscribe(self, client, obj, mid, granted_qos):
        logger.info('Subscribed: %s: \tgranted_qos: %s' %
                    (str(mid), str(granted_qos)))

    def set_callbacks(self):
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        self.client.on_connect = self.on_connect

    def publish_single(self, topic, msg):

        try:
            self.client.publish(topic=topic, payload=msg, qos=self.qos)

        except Exception as e:
            logger.error('Cannot publish message: %s -> topic: %s \tqos:%s' %
                         (str(msg), topic, str(self.qos)))
            raise e

    def publish_multiple(self, msgs):
        # msgs = [{'topic': "paho/test/multiple", 'payload': "multiple 1"},
        #         ("paho/test/multiple", "multiple 2", 0, False)]
        # publish.multiple(msgs, hostname="mqtt.eclipse.org")

        try:
            publish.multiple(msgs=msgs, hostname=self.__host,
                             port=self.__port, client_id=self.client_id)

        except Exception as e:
            logger.error('Publish message to multiple topics fail: %s' % e)

    def subscribe(self, topic):

        try:
            self.client.subscribe(topic=topic, qos=self.qos)
            self.topics.append(topic)

        except Exception as e:
            logger.error('Cannot subscribe to topic: %s -> error: %s' % (topic, e))

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, value):
        self.__host = value

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, value):
        self.__port = value


if __name__ == "__main__":
    logging_config(__name__)
    mqtt_service = MqttService('test')
    mqtt_service.connect()
    mqtt_service.client.loop_start()
    print('asdasdas')
    for i in range(10):
        mqtt_service.publish_single('iot_platform', str(i))
    # print(mqtt_service.mqtt_queue.get())
    mqtt_service.subscribe('mqtt/status')
    # mqtt_service.client.loop_forever()
    print(mqtt_service.mqtt_queue.get())
