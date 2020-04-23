This document provides information on how to run the application

Step1: Download Kafka
Use the link below to download Kafka
https://www.apache.org/dyn/closer.cgi?path=/kafka/2.5.0/kafka_2.12-2.5.0.tgz

Untar using below:
tar -xzf kafka_2.12-2.5.0.tgz
cd kafka_2.12-2.5.0


Step 2: Start the zookeeper Server
bin/zookeeper-server-start.sh config/zookeeper.properties

Step 3:In a separate window Start the Kafka Server using below
bin/kafka-server-start.sh config/server.properties


Step4: Clone the git Repository
git clone https://github.com/Mohnishbajaj/pyspark_stream_udata.git

Step5:Setup the required directories
cd pyspark_stream_udata/src
sh setup.sh

Step6: Start the consumer in a new window:
cd pyspark_stream_udata/src
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark_kafka_userdata_consumer.py

Step7: Start the producer using the below command, This reads the JSON Data and steams a message every 5 seconds.
python json_producer.py
