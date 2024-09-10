/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.redpanda;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 * Implementation of interface for AVRO message serialization/deserialization
 *
 * @see KafkaMessagingInterface
 */
public class AvroMessaging implements KafkaMessagingInterface {

  /**
   * {@inheritDoc}
   * <p>
   * Get producer configuration for AVRO serialization
   */
  @Override
  public Properties getProducerProperties(
      String brokers, String srAddr, SecuritySettings securitySettings,
      boolean autoRegisterSchema, boolean skipKnownTypes) {
    Properties prop = new Properties();
    prop.put("bootstrap.servers", brokers);
    prop.put("key.serializer", StringSerializer.class);
    prop.put("value.serializer", KafkaAvroSerializer.class);
    prop.put(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
        autoRegisterSchema);
    prop.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srAddr);
    if (securitySettings != null) {
      prop.putAll(securitySettings.toProperties());
    }

    return prop;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Get consumer configuration for AVRO deserialization
   */
  @Override
  public Properties getConsumerProperties(
      String brokers, String srAddr, SecuritySettings securitySettings,
      String consumerGroup) {
    Properties prop = new Properties();
    prop.put("bootstrap.servers", brokers);
    prop.put("key.deserializer", StringDeserializer.class);
    prop.put("value.deserializer", KafkaAvroDeserializer.class);
    prop.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srAddr);
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    if (securitySettings != null) {
      prop.putAll(securitySettings.toProperties());
    }

    return prop;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Produce AVRO serialized messages
   */
  @Override
  public void produce(Logger log, Properties props, String topic, int count) {
    Schema schema = new Schema.Parser().parse(
        "{\"type\": \"record\", \"name\": \"payload\", \"fields\": [{\"name\": "
        + "\"val\", \"type\": \"int\"}]}");

    AtomicInteger produced = new AtomicInteger();
    log.debug("Avro schema: " + schema);

    log.debug("Setting up producer properties: " + props);

    log.info(String.format(
        "Producing %d %s records to topic %s", count, "AVRO", topic));

    AtomicInteger acked = new AtomicInteger();
    try (
        final Producer<String, GenericRecord> producer
        = new KafkaProducer<>(props)) {
      IntStream.range(0, count).forEachOrdered(i -> {
        producer.send(
            new ProducerRecord<>(
                topic, UUID.randomUUID().toString(),
                createRecord(schema, "val", i)),
            (event, ex) -> {
              if (ex != null) {
                try {
                  throw ex;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              } else {
                acked.getAndIncrement();
              }
            });
        produced.getAndIncrement();
      });

      log.info("Flushing records...");
      producer.flush();
      log.info(String.format("Records flushed: %d", produced.get()));

      log.info(String.format("Records acked: %d", acked.get()));
    }

    if (acked.get() != produced.get()) {
      throw new RuntimeException(String.format(
          "Did not ack all produced messages: %d != %d", acked.get(),
          produced.get()));
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Consume and deserialize AVRO serialized messages
   */
  @Override
  public void consume(Logger log, Properties props, String topic, int count) {
    log.info(String.format(
        "Consuming %d AVRO records from topic %s with group %s", count, topic,
        props.get(ConsumerConfig.GROUP_ID_CONFIG)));

    log.debug("Setting up consumer properties: " + props);

    int consumed = 0;
    try (
        final Consumer<String, GenericRecord> consumer
        = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));

      while (consumed < count) {
        ConsumerRecords<String, GenericRecord> records
            = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, GenericRecord> record : records) {
          int val = (int)record.value().get("val");
          if (val != consumed) {
            throw new RuntimeException(String.format(
                "Received invalid val.  Expected %d received %d", consumed,
                val));
          }
          consumed++;
        }
      }
    }
  }

  /**
   * Creates a serialized AVRO record
   *
   * @param schema The schema to use
   * @param fieldName The name of the field to modify
   * @param val The value to set
   * @return The serialized AVRO message
   *
   *
   */
  private static GenericData.Record
  createRecord(Schema schema, String fieldName, int val) {
    GenericData.Record record = new GenericData.Record(schema);

    record.put(fieldName, val);

    return record;
  }
}
