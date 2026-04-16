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

import org.apache.log4j.Logger;

/**
 * Class used to test the Java Kafka serde library
 */
public class JavaKafkaSerdeClient {

  /**
   * Supported protocols
   */
  public enum Protocol { AVRO, PROTOBUF }

  private final Protocol protocol;
  private final String brokers;
  private final String topic;
  private final String srAddr;
  private final String consumerGroup;
  private final SecuritySettings securitySettings;
  private final boolean skipKnownTypes;
  private final boolean useLatestVersion;
  private final String contextNameStrategy;
  private final String contextName;

  private final Logger log;

  /**
   * Creates a new instance of the testing client
   *
   * @param brokers Comma separated list of brokers
   * @param topic The topic to communicate with
   * @param srAddr The URL of the schema registry
   * @param consumerGroup The consumer group to use
   * @param protocol The protocol to use for serialization/deserialization
   * @param securitySettings The security settings
   * @param skipKnownTypes Whether to skip known types when resolving schema
   *     dependencies
   * @param useLatestVersion Whether to use the latest schema version for lookup
   * @param contextNameStrategy Fully qualified class name of the
   *     ContextNameStrategy to use (may be null)
   * @param contextName Context name to pass to the ContextNameStrategy
   *     (may be null)
   * @param log The logger to use
   *
   * @see SecuritySettings
   * @see org.apache.log4j.Logger
   */
  public JavaKafkaSerdeClient(
      String brokers, String topic, String srAddr, String consumerGroup,
      Protocol protocol, SecuritySettings securitySettings,
      boolean skipKnownTypes, boolean useLatestVersion,
      String contextNameStrategy, String contextName, Logger log) {
    this.brokers = brokers;
    this.topic = topic;
    this.srAddr = srAddr;
    this.consumerGroup = consumerGroup;
    this.protocol = protocol;
    this.securitySettings = securitySettings;
    this.skipKnownTypes = skipKnownTypes;
    this.useLatestVersion = useLatestVersion;
    this.contextNameStrategy = contextNameStrategy;
    this.contextName = contextName;
    this.log = log;
  }

  /**
   * Executes the test
   *
   * @param count Number of messages to produce and consume
   * @throws RuntimeException Exception thrown during run
   */
  public void run(int count) throws RuntimeException {
    KafkaMessagingInterface test_interface = null;

    switch (this.protocol) {

    case AVRO:
      test_interface = new AvroMessaging();
      break;
    case PROTOBUF:
      test_interface = new ProtobufMessaging();
      break;
    }

    log.info("Starting produce");
    test_interface.produce(
        this.log,
        test_interface.getProducerProperties(
            this.brokers, this.srAddr, this.securitySettings, true,
            this.skipKnownTypes, this.useLatestVersion,
            this.contextNameStrategy, this.contextName),
        this.topic, count);

    log.info("Starting consume");
    test_interface.consume(
        this.log,
        test_interface.getConsumerProperties(
            this.brokers, this.srAddr, this.securitySettings,
            this.consumerGroup, this.useLatestVersion, this.contextNameStrategy,
            this.contextName),
        this.topic, count);

    log.info("Done!");
  }
}
