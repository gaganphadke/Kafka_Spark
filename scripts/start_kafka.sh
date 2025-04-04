KAFKA_DIR="kafka_2.13-3.4.0"

# Start Zookeeper
echo "Starting Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties > zookeeper.log 2>&1 &
sleep 5

# Start Kafka
echo "Starting Kafka broker..."
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties > kafka.log 2>&1 &
sleep 5

echo "Kafka services started!"