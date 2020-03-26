# Topic properies
TOPIC=sf-3
REPLICATION=1
PARTITIONS=36

# Verifier settings
export SERVERS="localhost:9092"
export THROUGHPUT=225000
#export THROUGHPUT=-1
export RECORD_COUNT=10000000
export RECORD_SIZE=1024
export ACKS=-1

root_path=$(git rev-parse --show-toplevel)
java_path=${root_path}/build/java/bin/java
jar_path=${root_path}/build/java-build/kafka-verifier/kafka-verifier.jar

${java_path} -jar ${jar_path} \
  --topic ${TOPIC} \
  --replication-factor ${REPLICATION} \
  --partitions ${PARTITIONS} \
  --throughput ${THROUGHPUT} \
  --record-size ${RECORD_SIZE} \
  --num-records ${RECORD_COUNT} \
  --producer-props acks=${ACKS} \
  --broker ${SERVERS} \
  --random-consumer-group true
