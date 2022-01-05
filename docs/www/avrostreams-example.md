---
title: Running a KStreams App with Schema Registry in Redpanda in 15 min (or less)
order: 3
---

# Running a KStreams App with Schema Registry in Redpanda in 15 min (or less)

This tutorial shows in 15 minutes or less how you can build a KStreams Application with Schema Registry and run it in Redpanda.   

## Get your cluster ready

In order to understand this tutorial you must have already completed the previous one, [Running a Schema Registry App in Redpanda in 15 min](/docs/squemaRegistry-example) 

This tutorial assumes that you already have a Redpanda cluster with three machines up and running in your development environment.

## Setting up the project 

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
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",
  "io.confluent" % "kafka-avro-serializer" % "5.0.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.0.0",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "org.apache.kafka" % "kafka-streams" % "3.0.0"
)
```
For this example, we are adding the `kafka-streams-avro-serde` library to the project. 

## Reading from Redpanda

Now that we have our project skeleton, remember that our event processor **reads** the events from a topic.

The specification for our **Processing Engine**  is to create a pipeline application which:
- Reads each message from a Redpanda topic called *persons*
- Calculates the age of each Person (each message), 
- Writes the age in a Redpanda topic called *ages*

These steps are detailed in this diagram:

![Processing Example](./images/processing_example.png)

As you can see, the first step in our pipeline is to read the input events.

Inside the `withavro` directory, create a file called `AvroStreamsProcessor` with the following content:

```scala
package io.vectorized.withavro

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.vectorized.{Constants, Person}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, Produced}

import java.time.{LocalDate, Period, ZoneId}
import java.util.{Collections, Date, Properties}
  
object AvroStreamsProcessor {
  // Our processor code here ...
  // 1. Application properties
  // 2. process() method
  // 3. main() method
}
```
Here we have the necessary imports for our Processor.

Replace the comment `// Our processor code here ...` with the following code:

```scala
private final val brokers = "localhost:58383, localhost:58388, localhost:58389"
private final val schemaRegistryUrl = "http://localhost:8081"
```
Of course, here we put the `ipAddress:Port` where our Redpanda brokers are running.
Note how we added the SchemaRegistry URL, based on the Docker container we started previously.

##The Consumer code

Replace the comment `// 1. Application properties` with the following code:

```scala
private final val props = new Properties
props.put("bootstrap.servers", brokers)
props.put("application.id", "redpanda-example")
```

Here we have the Properties for our KStreams Application, the brokers is a comma-separated String with the members of our cluster.

Replace the comment `// 2. process() method` with the following code:

```scala
def process(): Unit = {
  val streamsBuilder = new StreamsBuilder
  val avroSerde = new GenericAvroSerde
  avroSerde.configure(Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false)
  val avroStream = streamsBuilder.stream(Constants.getPersonsAvroTopic, Consumed.`with`(Serdes.String, avroSerde))
  val personAvroStream = avroStream.mapValues((v: GenericRecord) =>
    new Person(
      v.get("firstName").toString,
      v.get("lastName").toString,
      new Date(v.get("birthDate").asInstanceOf[Long]),
      v.get("city").toString,
      v.get("ipAddress").toString))

  // 1. The writter code goes here
  // 2. Add the topology code here
}
```

This method uses a `StreamsBuilder` that reads from the `avro-persons` topic and uses a String Serde (SerializerDeserialized)
for the `Key` and a `GenericAvroSerde` for the `Value`.  

For each record received, the Avro message is transformed to a Person class.

Replace the comment `// 3. main() method` with the following code:
```scala
  def main(args: Array[String]): Unit = process()
```

## Writting to Redpanda

Replace the comment `// 1. The writter code goes here` with the following code:

```scala
  val ageStream = personAvroStream.map((_, v) => {
    val birthDateLocal = v.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
    val age = Period.between(birthDateLocal, LocalDate.now).getYears
    new KeyValue[String, String](v.firstName + ' ' + v.lastName, String.valueOf(age))
  })
  ageStream.to(Constants.getAgesTopic, Produced.`with`(Serdes.String, Serdes.String))
```
Here the age of each person is calculated.

It is important to mention that here the age could be considered an Integer, but for demonstration purposes we consider it a String.
The Key of our `Age` message is the "firstName lastName" String, the value is the age.
As we can see, here both the `Key` and the `Value` are Strings, so we use String Serdes.
Once the age has been calculated, we write it in the topic `ages`.

Replace the comment  `// 2. Add the topology code here` with the following code:  

```scala
    val topology = streamsBuilder.build
    val streams = new KafkaStreams(topology, props)
    streams.start()
```

And this is where the processing is made. If you see, we don't use Scala futures, we don't use while(true) cycles.
Here we start an elegant KStreams Topology running a non-obstructive process or CPU-consuming loop with an infinite while. 

## Running our Processing Engine

The `avro-persons` topic must be already created, if not, to create it in Redpanda run:

```bash
rpk topic create avro-persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

The `ages` topic must be already created, if not, to create it in Redpanda run:

```bash
rpk topic create ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

To run a Redpanda console-consumer for the `avro-persons` topic:

```bash
rpk topic consume avro-persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

In a new terminal window, run a Redpanda console-consumer for the `ages` topic:

```bash
rpk topic consume ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

Finally, run first the main method of the `AvroStreamsProcessor` and then run the main method of the `AvroProducer`.

The output for the command-line topic-consumer for the topic `avro-persons` should be like this:

```bash
{
  "topic": "avro-persons",
  "value": "\u0000\u0000\u0000\u0000\u0002\u000cAlexis\u0016Oberbrunner\ufffd\ufffd\ufffd\ufffd\ufffd\u000e\u0018South Santos\u001c37.163.213.126",
  "timestamp": 1640236336679,
  "partition": 0,
  "offset": 437
}
{
  "topic": "avro-persons",
  "value": "\u0000\u0000\u0000\u0000\u0002\u0008Ross\u0008Roob\ufffd\ufffd䘑\u0013\u001cNew Shantabury\u001e223.147.160.241",
  "timestamp": 1640236337183,
  "partition": 0,
  "offset": 438
}
```
Note how there is non readable characters, because the message is in Avro format.


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

- In 15 minutes we have coded a full KStreams processor with Schema Registry in Scala running on Redpanda.
- As you can see, the migration of an actual KStreams application from Kafka to Redpanda is a really simple process.
- Redpanda exposes the *SAME* Kafka API, there is no need to change the code of our existing Kafka applications.
- Of course, all the source code of this tutorial is [here](https://github.com/vectorizedio/redpanda-examples/tree/main/clients/scala)

## What's next

Check the next part of the tutorial: 

 [Running a Protobuf App in Redpanda in 15 min (or less)](/docs/protobuf-example).
