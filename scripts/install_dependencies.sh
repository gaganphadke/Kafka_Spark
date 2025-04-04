echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Checking for Java installation (required for Kafka and Spark)..."
if ! command -v java &> /dev/null; then
    echo "Java is not installed. Installing OpenJDK..."
    sudo apt-get update
    sudo apt-get install -y openjdk-11-jdk
else
    echo "Java is already installed."
fi

echo "Installing Kafka..."
if [ ! -d "kafka_2.13-3.4.0" ]; then
    wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
    tar -xzf kafka_2.13-3.4.0.tgz
    rm kafka_2.13-3.4.0.tgz
else
    echo "Kafka is already downloaded."
fi

echo "Dependencies installed successfully!"