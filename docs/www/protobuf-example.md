---
title: Running a Protobuf Schema Registry App in Redpanda in 15 min (or less)
order: 4
---

# Running a Protobuf Schema Registry App in Redpanda in 15 min (or less)

This tutorial shows in 15 minutes or less how you can build a Protobuf Schema Registry Application and run it in Redpanda.   

## Get your cluster ready

In order to understand this tutorial you must have already completed the previous ones, [Running a Scala App in Redpanda in 15 min](/docs/scala-example) 

This tutorial assumes that you already have a Redpanda cluster with three machines up and running in your development environment.

## Setting up the project 

We need to start a Docker container with Schema Registry.

From the command line, run the following commands:

`docker network create redpanda-sr`

`docker volume create redpanda-sr`

```bash
docker run \
    --pull=always \
    --name=redpanda-sr \
    --net=redpanda-sr \
    -v "redpanda-sr:/var/lib/redpanda/data" \
    -p 8081:8081 \
    -p 8082:8082 \
    -p 9092:9092 \
    --detach \ 
```
```bash
docker.vectorized.io/vectorized/redpanda start \
    --overprovisioned \
    --smp 1 \
    --memory 1G \
    --reserve-memory 0M \
    --node-id 0 \
    --check=false \
    --pandaproxy-addr 0.0.0.0:8082 \
    --advertise-pandaproxy-addr 127.0.0.1:8082 \
    --kafka-addr 0.0.0.0:9092 \
    --advertise-kafka-addr redpanda-sr:9092
```
Now Redpanda Schema Registry is running in `http://localhost:8081/`

The endpoints documentation is in `http://localhost:8081/v1` 

###Publish the Schema

Schemas are registered against a subject, typically in the form {topic}-key or {topic}-value.

Let’s register the example Person Protobuf schema which represents a Person for the value of the `protobuf-persons` topic

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{"schemaType":"PROTOBUF", "schema":"syntax = \"proto3\";option java_package = \"io.vectorized\";option java_outer_classname = \"Person\";message PersonMessage { string first_name = 1; string last_name = 2; int64 birth_date = 3; string city = 4; string ip_address = 5;}"}' \ 
  http://localhost:8081/subjects/protobuf-persons-value/versions
```
It is very important to indicate that `"schemaType":"PROTOBUF"` otherwise, Schema Registry will consider it as an Avro schema. 

The result should be like:
```bash
{
"id": 1
}
```
This `id` is unique for the schema in the whole Redpanda cluster.


Now, modify the *build.sbt* file with this content:

```scala
name := "redpanda-example"

version := "0.1"

scalaVersion := "2.13.7"

resolvers += "maven" at "https://packages.confluent.io/maven/"

libraryDependencies ++= List(
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-log4j12" % "1.7.32",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1",
  "io.confluent" % "kafka-protobuf-serializer" % "7.0.0",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "org.apache.kafka" % "kafka-streams" % "3.0.0"
)

enablePlugins(ProtobufPlugin)
```


For this example, we are adding the `protobuf-serializer` library to the project. 

Under the `/project` directory, create a new file called `plugins.sbt` with the following content:
```scala
addSbtPlugin("com.github.sbt" % "sbt-protobuf" % "0.7.1")
```
This plugin automatically generates the necessary Java files from the Protobuf Schema.

Under the `/resources` directory, create a new file called `Person.proto` with the following content:

```protobuf
syntax = "proto3";

option java_package = "io.vectorized";
option java_outer_classname = "Person";
option optimize_for = SPEED;

message PersonMessage {
  string first_name = 1;
  string last_name = 2;
  int64 birth_date = 3;
  string city = 4;
  string ip_address = 5;
}
```

It has to be the same Schema that we declared to Schema Registry with the curl command.

## Reading from Redpanda

Now that we have our project skeleton, remember that our event processor **reads** the events from a topic.

The specification for our **Processing Engine**  is to create a pipeline application which:
- Reads each message from a Redpanda topic called *persons*
- Calculates the age of each Person (each message), 
- Writes the age in a Redpanda topic called *ages*

These steps are detailed in this diagram:

![Processing Example](./images/processing_example.png)

As you can see, the first step in our pipeline is to read the input events.

Create the subdirectory `io/vectorized/withprotobuf` under the `scala` directory.

Inside the `withprotobuf` directory, create a file called `ProtobufProcessor` with the following content:

```scala
package io.vectorized.withprotobuf

import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufDeserializerConfig}
import io.vectorized.{Constants, Person}
import io.vectorized.Person.PersonMessage
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.{Duration, LocalDate, Period, ZoneId}
import java.util.{Collections, Date, Properties}

object ProtobufProcessor {
   // Our processor code here ...
   // 1. Consumer properties
   // 2. Producer properties
   // 3. process() method
   // 4. main() method
}
```
Here we have the necessary imports for our Processor. Note the import of the Protobuf Serdes and Deserializer Config.

Replace the comment `// Our processor code here ...` with the following code:

```scala
private final val brokers = "localhost:58383, localhost:58388, localhost:58389"
private final val schemaRegistryUrl = "http://localhost:8081"
```

Of course, here we put the `ipAddress:Port` where our Redpanda brokers are running.
Note how we added the SchemaRegistry URL, based on the Docker container we started before.

##The Consumer code

Replace the comment `// 1. Consumer properties` with the following code:

```scala
  private final val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "person-processor")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaProtobufDeserializer[PersonMessage]])
    consumerProps.put("schema.registry.url", schemaRegistryUrl)
    consumerProps.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, classOf[PersonMessage])
    private final val consumer = new KafkaConsumer[String, PersonMessage](consumerProps)
 ```

Here we have the Properties for our Consumer, brokers is a comma-separated String with the members of our cluster.

In this case, the `Key` is of type String so we use String Deserializers.

The `Value` is in Protobuf Format so we use the `KafkaProtobufDeserializer`

Replace the comment `// 3. process() method` with the following code:

```scala
  def process(pollDuration: Int): Unit = {
    consumer.subscribe(Collections.singletonList(Constants.getPersonsProtobufTopic))
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(pollDuration))
      records.forEach(r => {
        val person = new Person(
          r.getFirstName,
          r.getLastName,
          new Date(r.getBirthDate).asInstanceOf[Long])
        val birthDateLocal = person.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
        val age = Period.between(birthDateLocal, LocalDate.now).getYears
          // Here we write the events to the 'ages' topic
        })
      }
  }
```

This method receives a parameter that indicates how many times per second our app is going to be polling the topic for new messages.

For each record found, the Protobuf message is transformed to a Person class and then is calculated the age of the person.

Replace the comment `// 4. main() method` with the following code:
```scala
  def main(args: Array[String]): Unit = process(1)
```
Here we are saying to pool the topic `protobuf-persons` every second looking for new messages.

## Writting to Redpanda

Replace the comment `// 2. Producer properties` with the following code:

```scala
  private final val producerProps = new Properties
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  private final val producer = new KafkaProducer[String, String](producerProps)
```
It is important to mention that here the age could be considered an Integer, but for demonstration purposes we consider it a String.
As we can see, here both the `Key` and the `Value` are Strings, so we use String Serializers.

Replace the comment `// Here we write the events to the 'ages' topic` with the following code:  

```scala
  val future = producer.send(new ProducerRecord[String, String](
    Constants.getAgesTopic, person.firstName + ' ' + person.lastName, String.valueOf(age)))
  future.get
```
The Key of our 'Age' message is the "firstName lastName" string, the value is the age. 
As we can see, once the age has been calculated, we write it in the `ages` topic.
We use Scala futures, to avoid locks and guarantee thread safety.

##The Producer code

Inside the `withprotobuf` directory create a file called `ProtobufProducer` with the following content:

```scala
package io.vectorized.withprotobuf

import com.github.javafaker.Faker
import io.confluent.kafka.serializers.KafkaProtobufSerializer
import io.vectorized.{Constants, Person}
import io.vectorized.Person.PersonMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties

object ProtobufProducer {
   // Our producer code here ...
   // 1. Producer properties
   // 2. produce() method
   // 3. main() method
}
```
Here we import all the necessary classes for our Producer.

Replace the comment `// Our producer code here ...` with the following code:

```
private final val brokers = "localhost:58383, localhost:58388, localhost:58389"
private final val schemaRegistryUrl = "http://localhost:8081"
```
As mentioned, here we put the `ipAddress:Port` pairs indicating where our Redpanda brokers are running.
Note how we added the Schema Registry URL, based on the Docker container we started before.

Replace the comment `// 1. Producer properties` with the following code:

```scala
  private final val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaProtobufSerializer[PersonMessage]])
  props.put("schema.registry.url", schemaRegistryUrl)
  private final val producer = new KafkaProducer[String, PersonMessage](props)
```

In this case, the Key is a String and the Value is an Protobuf record containing the Person info.
Note how there is no need to specify the Schema in the Schema Registry, 
if the Schema is not in the Schema Registry, the first message registers its Schema on it.

Replace the comment `// 2. produce() method` with the following code:
```scala
  def produce(ratePerSecond: Int): Unit = {
  val waitTimeBetweenIterationsMs = 1000L / ratePerSecond
  val faker = new Faker
  while (true) {
    val person = PersonMessage.newBuilder
      .setFirstName(faker.name.firstName)
      .setLastName(faker.name.lastName)
      .setBirthDate(faker.date.birthday.getTime)
      .setCity(faker.address.city)
      .setIpAddress(faker.internet.ipV4Address)
      .build
    val futureResult =
      producer.send(new ProducerRecord[String, PersonMessage](Constants.getPersonsProtobufTopic, person))
    Thread.sleep(waitTimeBetweenIterationsMs)
    futureResult.get
  }
}
```

The ratePerSecond is Int that tells us how many records of type Person we are going to produce per second.

Here we use Java Faker to produce fictitious data, it is a tool that generates Birth Dates and IP addresses, all valid but fictitious. 
It can be customized to produce data according to a region or language.

Replace the `// 3. main() method` comment with the following code:
```scala
  def main(args: Array[String]): Unit = produce(2)
```

Here we are producing two records per second.

## Running our Processing Engine

To create the `protobuf-persons` topic in Redpanda, run:

```bash
rpk topic create protobuf-persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

To run a Redpanda consumer for the `protobuf-persons` topic:

```bash
rpk topic consume protobuf-persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

In a new console window, run a Redpanda consumer for the `ages` topic:

```bash
rpk topic consume ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

Finally, run first the main method of the `ProtobufProcessor` and then run the main method of the `ProtobufConsumer`.

The output for the command-line topic-consumer for the topic `protobuf-persons` should be like this:

```bash
{
  "topic": "protobuf-persons",
  "value": "\u0000\u0000\u0000\u0000\u0002\u000cAlexis\u0016Oberbrunner\ufffd\ufffd\ufffd\ufffd\ufffd\u000e\u0018South Santos\u001c37.163.213.126",
  "timestamp": 1640236336679,
  "partition": 0,
  "offset": 437
}
{
  "topic": "protobuf-persons",
  "value": "\u0000\u0000\u0000\u0000\u0002\u0008Ross\u0008Roob\ufffd\ufffd䘑\u0013\u001cNew Shantabury\u001e223.147.160.241",
  "timestamp": 1640236337183,
  "partition": 0,
  "offset": 438
}
```
Note how there is non readable characters, because the message is in Protobuf format.

The output for the command-line topic-consumer for topic `ages` should be similar to this:

```bash
{
  "topic": "ages",
  "key": "Alexis Oberbrunner",
  "value": "49",
  "timestamp": 1640236336679,
  "partition": 0,
  "offset": 553
}
{
  "topic": "ages",
  "key": "Ross Rob",
  "value": "35",
  "timestamp": 1640236337183,
  "partition": 0,
  "offset": 554
}
```

## Conclusions

- In 15 minutes we have coded a full Protobuf processor in Scala and ran it on Redpanda.
- As you can see, the migration of an actual Schema Registry application from Kafka to Redpanda is a really simple process.
- Redpanda exposes the *SAME* Kafka API, there is no need to change the code of our existing Kafka applications.
- Of course, all the source code of this tutorial is [here](https://github.com/vectorizedio/redpanda-examples/tree/main/clients/scala)

## What's next

Check the next part of the tutorial: 

 [Running a KStreams App with Schema Registry and Protobuf in Redpanda in 15 min (or less)](/docs/protobufstreams-example).
