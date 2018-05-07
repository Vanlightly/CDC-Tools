You can use the example docker-compose.yml in the /kafka folder to stand up a Kafka server, schema registry endpoint, REST proxy and Kafka Manager Web UI.

Note that in order for the CDC apps to be able to see Kafka, you must create a Docker network that will allow them to communicate. This network must be created before starting Kafka and any app.

Command to create a pre-exising network:
docker network create kafka-shared-net

If you have a Kafka cluster and schema registry already, then you can remove the references to this network in the various docker-compose.yml files.

This was all developed and tested on Windows 10 1803. In order to create connectivity to the Kafka service on Windows, you must add "kafkaserver" and "schema-registry" to your hosts file.

For example, I have:
192.168.1.33 kafkaserver
192.168.1.33 schema-registry

192.168.1.33 is my local IPv4 address.

