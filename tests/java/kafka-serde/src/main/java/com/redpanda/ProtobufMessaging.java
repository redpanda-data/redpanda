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

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 * Tests serialization/deseralization of messages using Protobufs
 *
 * @see KafkaMessagingInterface
 */
public class ProtobufMessaging implements KafkaMessagingInterface {
  /**
   * {@inheritDoc}
   * <p>
   * Implementation to get producer properties for deserializing protobuf
   * messages
   */
  @Override
  public Properties getProducerProperties(
      String brokers, String srAddr, SecuritySettings securitySettings,
      boolean autoRegisterSchema, boolean skipKnownTypes) {
    Properties prop = new Properties();

    prop.put("bootstrap.servers", brokers);
    prop.put("key.serializer", StringSerializer.class);
    prop.put("value.serializer", KafkaProtobufSerializer.class);
    prop.put(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
        autoRegisterSchema);
    prop.put(
        KafkaProtobufSerializerConfig.SKIP_KNOWN_TYPES_CONFIG, skipKnownTypes);
    prop.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srAddr);
    if (securitySettings != null) {
      prop.putAll(securitySettings.toProperties());
    }

    return prop;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implementation to get consumer properties for Protobuf serializer
   */
  @Override
  public Properties getConsumerProperties(
      String brokers, String srAddr, SecuritySettings securitySettings,
      String consumerGroup) {
    Properties prop = new Properties();

    prop.put("bootstrap.servers", brokers);
    prop.put("key.deserializer", StringDeserializer.class);
    prop.put("value.deserializer", KafkaProtobufDeserializer.class);
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
   * Produces Protobuf serialized messages
   */
  @Override
  public void produce(Logger log, Properties props, String topic, int count) {

    log.debug("Setting up producer properties: " + props);

    log.info(String.format(
        "Producing %d %s records to topic %s", count, "PROTOBUF", topic));

    AtomicInteger produced = new AtomicInteger();
    AtomicInteger acked = new AtomicInteger();
    try (
        final Producer<String, Payload> producer = new KafkaProducer<>(props)) {
      IntStream.range(0, count).forEachOrdered(i -> {
        producer.send(
            new ProducerRecord<>(
                topic, UUID.randomUUID().toString(),
                Payload.newBuilder()
                    .setVal(i)
                    .setTimestamp(fromMillis(currentTimeMillis()))
                    .build()),
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
   * Consumes Protobuf serialized messages
   */
  @Override
  public void consume(Logger log, Properties props, String topic, int count) {
    log.info(String.format(
        "Consuming %d PROTOBUF records from topic %s with group %s", count,
        topic, props.get(ConsumerConfig.GROUP_ID_CONFIG)));

    int consumed = 0;

    log.debug("Setting up consumer properties: " + props);

    try (
        final Consumer<String, DynamicMessage> consumer
        = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));

      while (consumed < count) {
        ConsumerRecords<String, DynamicMessage> records
            = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, DynamicMessage> record : records) {
          int val = (int)record.value().getField(
              record.value().getDescriptorForType().findFieldByName("val"));
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
}
