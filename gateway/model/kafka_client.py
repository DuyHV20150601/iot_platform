from pykafka import KafkaClient

if __name__ == '__main__':
    client = KafkaClient('localhost:9092')
    topic = client.topics['test']

    print(topic.name)
    producer = topic.get_producer()
    consumer = topic.get_simple_consumer()
    producer.produce(b'message')

    for i in range(10):
        producer.produce(b'%d' % i)

    for msg in consumer:
        print('%s [key=%s, id=%s, offset=%s]' % (msg.value, msg.partition_key, msg.partition_id, msg.offset))