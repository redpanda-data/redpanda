---
title: How to connect a Scala appplication to Redpanda in 15 min (or less)
order: 0
---

# How to connect a Scala appplication to Redpanda in 15 min (or less)

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This tutorial demonstrates how to build a Scala application that streams messages to and from Redpanda 

## Get your cluster ready

This tutorial assumes that you already have a Redpanda cluster with three machines up and running in your development environment.

If you need some help, checkout these quick start guides:

- Do you develop on a Mac? Access the [Mac OS Quick Start Guide](/docs/quick-start-macos).
- Do you use Linux? Check the [Linux Quick Start Guide](/docs/quick-start-linux).
- A Windows user? No problem, go to the [Windows Quick Start Guide](/docs/quick-start-windows).

The goal is that the output of the status command should look like this:

```bash
BROKERS
=======
ID    HOST       PORT
0*    127.0.0.1  58383
1     127.0.0.1  58388
2     127.0.0.1  58389
```

> **_Note:_** For this tutorial the brokers are running on those ports, in your case they may be different because
> when you run Redpanda with Docker, if you don't explicitly specify the ports on which your brokers will run, 
> Redpanda will assign the ports automatically.   

## Event modeling

The first step in Event Modeling is to express the event in English language in the form:
Subject - Verb - Direct Object

For this example we are modeling the event *"Customer signs up in our page"*

We can represent our message in several message formats, in this case we will use JSON (JavaScript
Object Notation).
We could also use [Avro](/docs/scala-serviceregistry-example) or [Protocol Buffers](/docs/scala-protobuf-example), covered in their respective guides.

This is an example of *"Customer signs up in our page"* event in JSON format

```json
{
   "firstName":"Raul", 
   "lastName":"Strada", 
   "birthDate":"1976-07-04T00:00:00",
   "city":"Philadelphia",
   "ipAddress":"95.31.18.111"
}
```

Having modeled our event, we want to process it with Redpanda using the existing Kafka API compatible Scala library, 
for this example we are going to calculate the age of the people.

## Setting up the project 

Since our example is running in Scala, we are going to use *sbt* as the build tool

In Mac OS we can install *sbt* with *brew*:

`brew install sbt`

In Linux we can install *sbt* with the *apt-get* command:

`apt-get install sbt`

To generate an empty project execute the folowing command: `sbt new scala/hello-world.g8`
This pulls the ‘hello-world’ template from GitHub.

When prompted a name for the application type: **redpanda-example**. As expected, this will create a project called “redpanda-example”.

The generated directory should be similar to:

```bash
- redpanda-example
   - project    #sbt uses this to install and manage plugins and dependencies)
      - build.properties
   - src
      - main
         - scala   # Your scala code goes here
            - Main.scala    # Entry point of program
   - build.sbt   # sbt's build definition file
```

The sbt tool creates some archives from a template, one is Main.scala.
We can delete the file `redpanda-example/src/main/scala/Main.scala` because we will write our own application.

Now, modify the *build.sbt* file with this content:

```scala
name := "redpanda-example"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies ++= List(
   "com.github.javafaker" % "javafaker" % "1.0.2",
   "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1",
   "org.apache.kafka" % "kafka-clients" % "3.0.0"
)
```
In our example, we use javafaker to generate fictional information and jackson to parse JSON messages. 

Inside the subdirectory `io/vectorized` create the file `Constants.scala` with the following content:

```scala
package io.vectorized

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat

object Constants {
   private val jsonMapper = new ObjectMapper
   jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
   jsonMapper.setDateFormat(new StdDateFormat)
   
   def getPersonsTopic = "persons"
   def getPersonsAvroTopic = "avro-persons"
   def getPersonsProtobufTopic = "protobuf-persons"
   def getAgesTopic = "ages"
   def getJsonMapper: ObjectMapper = jsonMapper
}
```
This is a simple class of constants (yes, I know, a Java antipattern but useful for educational purposes). 


Inside the subdirectory `io/vectorized` create the file `Person.scala` with the following content:

```scala
package io.vectorized

import java.util.Date

case class Person(firstName: String,
   lastName: String,
   birthDate: Date,
   city: String,
   ipAddress: String)

object Person {
  def apply(firstName: String, lastName: String, birthDate: Date): Person = {
    Person(firstName, lastName, birthDate, "", "")
  }
}
```

This is a simple Person class (also known as POJO or Bean) that contains our Customer's data to model.

## Reading from Redpanda

Now that we have our project skeleton, remember that our event processor **reads** the events from a topic.

The specification for our **Processing Engine**  is to create a pipeline application which:
- Reads each message from a Redpanda topic called *persons*
- Calculates the age of each Person (each message), 
- Writes the age in a Redpanda topic called *ages*

These steps are detailed in this diagram:

![Processing Example](./images/processing_example.png)

As you can see, the first step in our pipeline is to read the input events.

Create the subdirectories `io/vectorized/plainjson` under the `scala` directory.

Inside the `plainjson` directory, create a file called `JsonProcessor` with the following content:

```scala
package io.vectorized.plainjson

import io.vectorized.{Constants, Person}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.{Duration, LocalDate, Period, ZoneId}
import java.util.{Collections, Properties}

object JsonProcessor {
   // Our processor code here ...
   // 1. Consumer properties
   // 2. Producer properties
   // 3. process() method
   // 4. main() method
}
```
Here we have the necessary imports for our Processor.

Replace the comment `// Our processor code here ...` with the following code:

`private final val brokers = "localhost:58383, localhost:58388, localhost:58389"`

Of course, here we put the `ipAddress:Port` where our Redpanda brokers are running.
In my case, I just asked Redpanda to produce a three broker cluster and it automatically assigned these port numbers.


##The Consumer code

Replace the comment `// 1. Consumer properties` with the following code:

```scala
  private final val consumerProps = new Properties
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "person-processor")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  private final val consumer = new KafkaConsumer[String, String](consumerProps)
```

Here we have the Properties for our Consumer, the brokers is a comma-separated String with the members of our cluster.

As the `Key` and the `Value` are both Strings, we use String Deserializers, we consider a JSON message as a String.

Replace the comment `// 3. process() method` with the following code:

```scala
  def process(pollRate: Int): Unit = {
   consumer.subscribe(Collections.singletonList(Constants.getPersonsTopic))
   while (true) {
      val records = consumer.poll(Duration.ofSeconds(pollRate))
      records.forEach(r => {
         val person = Constants.getJsonMapper.readValue(r.value(), classOf[Person])
         val birthDateLocal = person.birthDate.toInstant.atZone(ZoneId.systemDefault).toLocalDate
         val age = Period.between(birthDateLocal, LocalDate.now).getYears
         // Here we write the events to the 'ages' topic
      }
      )
   }
}
```
This method receives a parameter that indicates how many times per second our app is going to be polling the topic for new messages.

For each record found, the Json message is transformed to a Person class and then is calculated the age of the person.

Replace the comment `// 4. main() method` with the following code:
```scala
  def main(args: Array[String]): Unit = process(1)
```

Here we are saying to pool the topic `persons` every second looking for new messages.

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
   val future = producer.send(
    new ProducerRecord[String, String](
      Constants.getAgesTopic, person.firstName + ' ' + person.lastName, String.valueOf(age)))
   future.get
```
The Key of our 'Age' message is the "firstName lastName" string, the value is the age. 
As we can see, once the age has been calculated, we write it in the `ages` topic.
We use Scala futures, to avoid locks and guarantee thread safety.

##The Producer code

Inside the `plainjson` directory create a file called `JsonProducer` with the following content:

```scala
package io.vectorized.plainjson

import com.github.javafaker.Faker
import io.vectorized.{Constants, Person}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object JsonProducer {
   // Our producer code here ...
   // 1. Producer properties
   // 2. produce() method
   // 3. main() method
}
```
Here we import all the necessary classes for our Producer.

Replace the comment `// Our producer code here ...` with the following code:

`private final val brokers = "localhost:58383, localhost:58388, localhost:58389"`

As mentioned, here we put the `ipAddress:Port` pairs indicating where our Redpanda brokers are running.

Replace the comment `// 1. Producer properties` with the following code:

```scala
  private final val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  private final val producer = new KafkaProducer[String, String](props)
```

In this case, the Key is a String and the Value is a String containing the Person info.

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
      val fakePersonJson = Constants.getJsonMapper.writeValueAsString(fakePerson)
      val futureResult =
        producer.send(new ProducerRecord[String, String](Constants.getPersonsTopic, fakePersonJson))
      futureResult.get
      Thread.sleep(waitTimeBetweenIterationsMs)
    }
  }
```

The ratePerSecond is Int that tells us how many records of type Person we are going to produce per second.

Here we use Java Faker to produce fictitious data, it is a tool that generates birthdates and IP addresses, all valid but fictitious. 
It can be customized to produce data according to a region or language.

Replace the `// 3. main() method` comment with the following code:
```scala
  def main(args: Array[String]): Unit = produce(2)
```

Here we are producing two records per second.

## Running our Processing Engine

To create the `persons` topic in Redpanda, run:

```bash
rpk topic create persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

To create the `ages` topic in Redpanda, run:

```bash
rpk topic create ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

To run a Redpanda consumer for the `persons` topic:

```bash
rpk topic consume persons --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

In a new console window, run a Redpanda consumer for the `ages` topic:

```bash
rpk topic consume ages --brokers 127.0.0.1:58383,127.0.0.1:58388,127.0.0.1:58389
```

Finally, run first the main method of the `JsonProcessor` and then run the main method of the `JsonConsumer`.

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

- In 15 minutes we have coded a full Redpanda processor in Scala and ran it.
- As you can see, the migration of an actual application from Kafka to Redpanda is a really simple process.
- Redpanda exposes the *SAME* API as Kafka, there is no need to change the code of our existing Kafka applications.
- Of course, all the source code of this tutorial is [here](https://github.com/vectorizedio/redpanda-examples/tree/main/clients/scala)

## What's next

Check the next part of the tutorial: 

 [Running a KafkaStreams App in Redpanda in 15 min (or less)](/docs/kstreams-example).
