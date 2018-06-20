FROM docker.elastic.co/logstash/logstash:6.0.1

RUN rm -f /usr/share/logstash/pipeline/logstash.conf
ADD pipeline/ /usr/share/logstash/pipeline/
ADD config/ /usr/share/logstash/config/
ADD logs/ /tmp/logs

RUN logstash-plugin install logstash-codec-avro_schema_registry
