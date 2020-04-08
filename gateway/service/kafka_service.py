from pykafka import KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic


class KafkaService(object):
    def __init__(self, str_bootstrap_server, log_obj):
        self.logger = log_obj.logger
        self.__str_host = str_bootstrap_server
        self.kafka_client = KafkaClient(hosts=self.__str_host)
        self.__admin = KafkaAdminClient(bootstrap_servers=self.__str_host, client_id='iot_platform')

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

    def create_topic(self, list_topic_names, num_partitions=1, replication_factor=1):

        try:
            list_topics = []
            topics = self.get_all_topics()
            for name in list_topic_names:
                if name in topics:
                    return False
                topic = NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor)
                list_topics.append(topic)
            self.__admin.create_topics(new_topics=list_topics)
            self.logger.info('Create topics: [NAMES: %s]' % list_topic_names)

        except Exception as e:
            self.logger.error('Cannot create topic: [ERROR: %s]' % e)

    def delete_topic(self, topic_names):
        """

        :param topic_names: a list of string topic names
        :return:
        """

        try:
            self.__admin.delete_topics(topics=topic_names, timeout_ms=6000)
            self.logger.info('Deleted topic [TOPIC: %s]' % topic_names)

        except Exception as e:
            self.logger.error('Cannot delete topic [TOPIC: %s], [ERROR: %s]' % (topic_names, e))

    def get_all_topics(self):
        return self.__admin.list_topics()