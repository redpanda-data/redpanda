---
title: Running a Schema Registry App in Redpanda in 15 min (or less)
order: 2
---

# Running a Schema Registry App in Redpanda in 15 min (or less)

This tutorial shows in 15 minutes or less how you can build a Schema Registry Application and run it in Redpanda.   

## Get your cluster ready

In order to understand this tutorial you must have already completed the previous ones, [How to connect a Scala App to Redpanda in 15 min](/docs/scala-example) 

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

Now, modify the *build.sbt* file with this content:

```scala
name := "redpanda-example"

version := "0.1"

scalaVersion := "2.13.7"

resolvers += "maven" at "https://packages.confluent.io/maven/"

libraryDependencies ++= List(
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0", 
  "io.confluent" % "kafka-avro-serializer" % "5.0.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.0.0",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "org.apache.kafka" % "kafka-streams" % "3.0.0"
)
```
For this example, we are adding the `avro-serializer` library to the project. 

## Reading from Redpanda

Now that we have our project skeleton, remember that our event processor **reads** the events from a topic.

The specification for our **Processing Engine**  is to create a pipeline application which:
- Reads each message from a Redpanda topic called *persons*
- Calculates the age of each Person (each message), 
- Writes the age in a Redpanda topic called *ages*

These steps are detailed in this diagram:

![Processing Example](./images/processing_example.png)

As you can see, the first step in our pipeline is to read the input events.

Create the subdirectory `io/vectorized/withavro` under the `scala` directory.

Inside the `withavro` directory, create a file called `AvroProcessor` with the following content:

```scala
package io.vectorized.withavro

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.vectorized.{Constants, Person}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.{Duration, LocalDate, Period, ZoneId}
import java.util.{Collections, Date, Properties}

object AvroProcessor {
   // Our processor code here ...
   // 1. Consumer properties
   // 2. Producer properties
   // 3. process() method
   // 4. main() method
}
```
Here we have the necessary imports for our Processor. Note the import of the Avro Serializers.

Replace the comment `// Our processor code here ...` with the following code:

```
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
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
  consumerProps.put("schema.registry.url", schemaRegistryUrl)
  private final val consumer = new KafkaConsumer[String, GenericRecord](consumerProps)
```

Here we have the Properties for our Consumer, brokers is a comma-separated String with the members of our cluster.

In this case, the `Key` is of type String so we use String Deserializers.

The `Value` is in Avro Format so we use the `KafkaAvroDeserializer`

Replace the comment `// 3. process() method` with the following code:

```scala
  def process(pollDuration: Int): Unit = {
      consumer.subscribe(Collections.singletonList(Constants.getPersonsAvroTopic))
      while (true) {
        val records = consumer.poll(Duration.ofSeconds(pollDuration))
        records.forEach( r => {
          val personAvro = r.value
          val person = new Person(
            personAvro.get("firstName").toString,
            personAvro.get("lastName").toString,
            new Date(personAvro.get("birthDate").asInstanceOf[Long]))
          val birthDateLocal = person.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
          val age = Period.between(birthDateLocal, LocalDate.now).getYears
          // Here we write the events to the 'ages' topic
        })
      }
  }
```

This method receives a parameter that indicates how many times per second our app is going to be polling the topic for new messages.

For each record found, the Avro message is transformed to a Person class and then is calculated the age of the person.

Replace the comment `// 4. main() method` with the following code:
```scala
  def main(args: Array[String]): Unit = process(1)
```
Here we are saying to pool the topic `avro-persons` every second looking for new messages.

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

Inside the `withavro` directory create a file called `AvroProducer` with the following content:

```scala
package io.vectorized.withavro

import com.github.javafaker.Faker
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.vectorized.{Constants, Person}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig

import java.io.File
import java.util.Properties

object AvroProducer {
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
Note how we added the SchemaRegistry URL, based on the Docker container we started before.

Replace the comment `// 1. Producer properties` with the following code:

```scala
  private final val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", schemaRegistryUrl)
  private final val producer = new KafkaProducer[String, GenericRecord](props)
  private final val schema = (new Schema.Parser).parse(new File("src/main/resources/person.avsc"))
```

In this case, the Key is a String and the Value is an Avro record containing the Person info.
Note how in the last line there is no need to specify the Schema in the Schema Registry, 
if the Schema is not in the Schema Registry, the first message registers its Schema on it.


Replace the comment `// 2. produce() method` with the following code:
```scala
  def produce(ratePerSecond: Int): Unit = {
  val waitTimeBetweenIterationsMs = 1000L / ratePerSecond.toLong
  val faker = new Faker
  while (true) {
    val fakePerson = new Person(
      faker.name.firstName,
      faker.name.lastName,
      faker.date.birthday,
      faker.address.city,
      faker.internet.ipV4Address)
    val recordBuilder = new GenericRecordBuilder(schema)
    recordBuilder.set("firstName", fakePerson.firstName)
    recordBuilder.set("lastName", fakePerson.lastName)
    recordBuilder.set("birthDate", fakePerson.birthDate.getTime)
    recordBuilder.set("city", fakePerson.city)
    recordBuilder.set("ipAddress", fakePerson.ipAddress)
    val avroPerson = recordBuilder.build
    val futureResult =
      producer.send(new ProducerRecord[String, GenericRecord](
        Constants.getPersonsAvroTopic, avroPerson))
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

To create the `avro-persons` topic in Redpanda, run:

```bash
rpk topic create avro-persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

To run a Redpanda consumer for the `avro-persons` topic:

```bash
rpk topic consume avro-persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

In a new console window, run a Redpanda consumer for the `ages` topic:

```bash
rpk topic consume ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

Finally, run first the main method of the `AvroProcessor` and then run the main method of the `AvroConsumer`.

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

- In 15 minutes we have coded a full Redpanda processor in Scala and ran it on Redpanda.
- As you can see, the migration of an actual Schema Registry application from Kafka to Redpanda is a really simple process.
- Redpanda exposes the *SAME* Kafka API, there is no need to change the code of our existing Kafka applications.
- Of course, all the source code of this tutorial is [here](https://github.com/vectorizedio/redpanda/tree/dev/docs)

## What's next

Check the next part of the tutorial: 

 [Running a KStreams App with Schema Registry in Redpanda in 15 min (or less)](/docs/avrostreams-example).
