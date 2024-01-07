# Terminal 1:

`su - hadoop`

`start-all.sh`

# Terminal 2:

To start Nifi

`nifi-1.13.2/bin/nifi.sh start`

To start Kafka

`./kafkastart.sh`

To start spark job that consumes Kafka topics and produces predictions

`./modelstart.sh`
