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

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * Interface used to test serialization and deserialization of Kafka messages
 */
public interface KafkaMessagingInterface {
  /**
   * Generates properties for a Kafka producer
   *
   * @param brokers The comma separated list of broker addresses
   * @param srAddr The URL of the schema registry
   * @param securitySettings The security settings (may be null)
   * @param autoRegisterSchema True to automatically register schemas
   * @param skipKnownTypes Whether to skip known types when resolving schema
   *     dependencies
   * @return Instance of Properties
   *
   * @see java.util.Properties
   * @see SecuritySettings
   */
  Properties getProducerProperties(
      String brokers, String srAddr, SecuritySettings securitySettings,
      boolean autoRegisterSchema, boolean skipKnownTypes);

  /**
   * Generates properties for a Kafka consumer
   *
   * @param brokers The comma separated list of brokers
   * @param srAddr The URL of the schema registry
   * @param securitySettings The security settings (may be null)
   * @param consumerGroup The consumer group to use
   * @return Instance of Properties
   *
   * @see java.util.Properties
   * @see SecuritySettings
   */
  Properties getConsumerProperties(
      String brokers, String srAddr, SecuritySettings securitySettings,
      String consumerGroup);

  /**
   * Tests production of messages
   *
   * @param log The logger to use
   * @param props The producer configuration
   * @param topic The topic to communicate with
   * @param count The number of messages to send
   *
   * @see org.apache.log4j.Logger
   */
  void produce(Logger log, Properties props, String topic, int count);

  /**
   * Tests consumption of messages
   *
   * @param log The logger to use
   * @param props The consumer configuration
   * @param topic The topic to communicate with
   * @param count The number of messages to consume
   *
   * @see org.apache.log4j.Logger
   */
  void consume(Logger log, Properties props, String topic, int count);
}
