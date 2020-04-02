#!/usr/bin/env bash
# shellcheck disable=SC2164
# shellcheck disable=SC2046
sudo kill -l $(sudo lsof -t -i :2181)

cd ~/PycharmProjects/iot_analytic_platform/kafka_2.12-2.4.1/
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
cd ~/PycharmProjects/iot_analytic_platform/kafka_2.12-2.4.1/
bin/kafka-server-start.sh -daemon config/server.properties

## start consuming
#cd ~/PycharmProjects/iot_analytic_platform/kafka_2.12-2.4.1/
#bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning