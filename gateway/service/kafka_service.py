from pykafka import KafkaClient
from utils.logger import logger


class KafkaService(object):
    def __init__(self, host, log_obj):
        self.logger = log_obj.logger
        self.__str_host = host
        self.kafka_client = KafkaClient(hosts=self.__str_host)

    def get_topic(self, kafka_topic_name):
        return self.kafka_client.topics[kafka_topic_name]

    def consumer(self, kafka_topic_name):
        return self.get_topic(kafka_topic_name=kafka_topic_name).get_simple_consumer()

    def publisher(self, kafka_topic):
        return self.get_topic(kafka_topic_name=kafka_topic).get_producer()

    def publish(self, kafka_topic, msg):

        try:
            self.logger.info('Published message [MESSAGE: %s] -> kafka_topic [TOPIC: %s]' % (msg, kafka_topic))
            self.publisher(kafka_topic=kafka_topic).produce(message=msg)

        except Exception as e:
            self.logger.error('Cannot publish message: [MESSAGE: %s] to topic: [TOPIC: %s], [ERROR: %s]' % (msg,
                                                                                                            kafka_topic,
                                                                                                            e))
