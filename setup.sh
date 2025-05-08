sudo systemctl start kafka.

echo "Check kafka status"
sudo systemctl status kafka

~kafka/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic PSDTopic

echo "Hello, World" | ~kafka/kafka/bin/kafka-console-producer.sh --brokerlist localhost:9092 --topic PSDTopic > /dev/null

~kafka/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic PSDTopic --from-beginning

java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar kafdrop-3.29.0.jar --
kafka.brokerConnect=localhost:9092

# open kafka drop

# Flink
./bin/start-cluster.sh 
./bin/flink run examples/streaming/WordCount.jar

tail log/flink-psd-taskexecutor-0-ubuntu3.out

# open 8081 flink