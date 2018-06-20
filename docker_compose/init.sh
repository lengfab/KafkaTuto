#!/bin/bash

docker-compose exec broker \
    kafka-topics --create --topic httplog --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

curl -X POST -H "Content-Type:application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\":\"record\",\"name\":\"HttpLog\",\"namespace\":\"<empty>\",\"fields\":[{\"name\":\"agent\",\"type\":\"string\"},{\"name\":\"auth\",\"type\":\"string\"},{\"name\":\"ident\",\"type\":\"string\"},{\"name\":\"verb\",\"type\":\"string\"},{\"name\":\"message\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"response\",\"type\":\"int\"},{\"name\":\"bytes\",\"type\":\"int\"},{\"name\":\"clientip\",\"type\":\"string\"},{\"name\":\"httpversion\",\"type\":\"string\"},{\"name\":\"request\",\"type\":\"string\"},{\"name\":\"referrer\",\"type\":\"string\"},{\"name\":\"host\",\"type\":\"string\"}]}" }' \
    http://localhost:8081/subjects/HttpLog/versions


docker-compose exec broker \
    kafka-run-class -jar /tmp/streamingApp/scala-2.12/streaming-assembly-0.1.jar --bootstrap "http://broker:9092" --schema_registry "http://schema_registry:8081" --topic "httplog"  --zkurl zookeeper:2181 &


docker-compose exec logstash \
    cat /tmp/logs/nginx_access_light.log >> /tmp/access_log