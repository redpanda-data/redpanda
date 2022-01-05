---
title: How to connect a KStreams App to Redpanda in 15 min (or less)
order: 1
---

# How to connect a KStreams App to Redpanda in 15 min (or less)

This tutorial shows in 15 minutes or less how to build a KStreams Application and connect it to Redpanda.   

## Get your cluster ready

In order to understand this tutorial you must have already completed the previous one, [How to connect a Scala App to Redpanda in 15 min](/docs/scala-example) 

This tutorial assumes that you already have a Redpanda cluster with three machines up and running in your development environment.

## Setting up the project 

Now, modify the *build.sbt* file with this content:

```scala
name := "redpanda-example"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies ++= List(
   "com.github.javafaker" % "javafaker" % "1.0.2",
   "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",
   "org.apache.kafka" % "kafka-clients" % "3.0.0",
   "org.apache.kafka" % "kafka-streams" % "3.0.0"
)
```
For this example, we are adding the `kafka-streams` library to the project. 

## Reading from Redpanda

Now that we have our project skeleton, remember that our event processor **reads** the events from a topic.

The specification for our **Processing Engine**  is to create a pipeline application which:
- Reads each message from a Redpanda topic called *persons*
- Calculates the age of each Person (each message), 
- Writes the age in a Redpanda topic called *ages*

These steps are detailed in this diagram:

![Processing Example](./images/processing_example.png)

As you can see, the first step in our pipeline is to read the input events.

Inside the `plainjson` directory, create a file called `JsonStreamsProcessor` with the following content:

```scala
package io.vectorized.plainjson

import io.vectorized.{Constants, Person}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{Consumed, Produced}

import java.time.{LocalDate, Period, ZoneId}
import java.util.Properties
  
object JsonStreamsProcessor {
  // Our processor code here ...
  // 1. Application properties
  // 2. process() method
  // 3. main() method
}
```
Here we have the necessary imports for our Processor.

Replace the comment `// Our processor code here ...` with the following code:

`private final val brokers = "localhost:58383, localhost:58388, localhost:58389"`

Of course, here we put the `ipAddress:Port` where our Redpanda brokers are running.

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
  val personJsonStream =
    streamsBuilder.stream(Constants.getPersonsTopic, Consumed.`with`(Serdes.String, Serdes.String))
  val personStream = personJsonStream.mapValues(v => Constants.getJsonMapper.readValue(v, classOf[Person]))

  // 1. The writter code goes here
  // 2. Add the topology code here
}
```

This method uses a `StreamsBuilder` that reads from the `persons` topic and uses a String Serde (Serializer Deserialized)
for both: the `Key` and the `Value`

For each record received, the Json message is transformed to a Person class.

Replace the comment `// 3. main() method` with the following code:
```scala
  def main(args: Array[String]): Unit = process()
```

## Writting to Redpanda

Replace the comment `// 1. The writter code goes here` with the following code:

```scala
    val ageStream = personStream.map((_, v) => {
      val startDateLocal = v.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
      val age = Period.between(startDateLocal, LocalDate.now).getYears
      new KeyValue[String, String](v.firstName + " " + v.lastName, String.valueOf(age))
    })
    ageStream.to(Constants.getAgesTopic, Produced.`with`(Serdes.String, Serdes.String))
```
Here the age of each person is calculated.

It is important to mention that here the age could be considered an Integer, but for demonstration purposes we consider it a String.
The Key of our 'Age' message is the "firstName lastName" String, the value is the age.
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

The `persons` topic must be already created, if not, to create it in Redpanda run:

```bash
rpk topic create persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

The `ages` topic must be already created, if not, to create it in Redpanda run:

```bash
rpk topic create ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

To run a Redpanda console-consumer for the `persons` topic:

```bash
rpk topic consume persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

In a new console window, run a Redpanda console-consumer for the `ages` topic:

```bash
rpk topic consume ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

Finally, run first the main method of the `JsonStreamsProcessor` and then run the main method of the `JsonProducer`.

The output for the command-line topic-consumer for the topic `persons` should be like this:

```bash
{
  "topic": "persons",
  "value": "{\"firstName\":\"Johnathan\",\"lastName\":\"Feest\",\"birthDate\":\"1972-08-01T22:24:36.523+00:00\",\"city\":\"North Harvey\",\"ipAddress\":\"233.24.49.159\"}",
  "timestamp": 1640235899919,
  "partition": 0,
  "offset": 543
}
{
  "topic": "persons",
  "value": "{\"firstName\":\"Tony\",\"lastName\":\"Frost\",\"birthDate\":\"1986-05-08T11:36:13.036+00:00\",\"city\":\"New Warrenbury\",\"ipAddress\":\"178.13.90.20\"}",
  "timestamp": 1640235900460,
  "partition": 0,
  "offset": 544
}
```

The output for the command-line topic-consumer for topic `ages` should be similar to this:

```bash
{
  "topic": "ages",
  "key": "Johnathan Feest",
  "value": "49",
  "timestamp": 1640235899919,
  "partition": 0,
  "offset": 543
}
{
  "topic": "ages",
  "key": "Tony Frost",
  "value": "35",
  "timestamp": 1640235900460,
  "partition": 0,
  "offset": 544
}
```

## Conclusions

- In 15 minutes we have coded a full KStreams processor in Scala and connected it to Redpanda.
- As you can see, the migration of an actual KStreams application from Kafka to Redpanda is a really simple process.
- Redpanda exposes the *SAME* Kafka API, there is no need to change the code of our existing Kafka applications.
- Of course, all the source code of this tutorial is [here](https://github.com/vectorizedio/redpanda/tree/dev/docs)

## What's next

Check the next part of the tutorial: 

 [Running a Schema Registry App in Redpanda in 15 min (or less)](/docs/schemaRegistry-example).
