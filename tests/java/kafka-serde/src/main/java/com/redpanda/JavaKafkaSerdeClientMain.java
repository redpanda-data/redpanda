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

import java.util.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.Logger;
import org.json.*;

/**
 * Main application class
 */
public class JavaKafkaSerdeClientMain {

  // Static logger
  static Logger log
      = Logger.getLogger(JavaKafkaSerdeClientMain.class.getName());

  /**
   * Main method
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    Namespace ns = null;
    var parser = createArgumentParser();
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    log.debug("Arguments: " + ns);

    SecuritySettings securitySettings = null;

    if (ns.getString("security") != null) {
      securitySettings = parseSecuritySettings(ns.getString("security"));
    }

    String brokers = ns.getString("brokers");
    String topic = ns.getString("topic");
    String srAdr = ns.getString("schema_registry");
    String consumerGroup = ns.getString("consumer_group");
    int count = ns.getInt("count");
    boolean skipKnownTypes = ns.getBoolean("skip_known_types");

    JavaKafkaSerdeClient.Protocol protocol = ns.get("protocol");

    JavaKafkaSerdeClient client = new JavaKafkaSerdeClient(
        brokers, topic, srAdr, consumerGroup, protocol, securitySettings,
        skipKnownTypes, log);

    try {
      client.run(count);
    } catch (RuntimeException e) {
      log.error("Failed during exection: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Creates the CLI argument parser
   *
   * @return Instance of `ArgumentParser`
   * @see net.sourceforge.argparse4j.inf.ArgumentParser
   */
  private static ArgumentParser createArgumentParser() {
    ArgumentParser parser = ArgumentParsers.newFor("Java Kafka Serde Client")
                                .build()
                                .defaultHelp(true)
                                .description("Java Kafka Serde Client");

    parser.addArgument("-b", "--brokers")
        .help("Comma separated list of brokers")
        .required(true);
    parser.addArgument("-t", "--topic")
        .help("Name of topic")
        .setDefault(UUID.randomUUID());
    parser.addArgument("-s", "--schema-registry")
        .help("URL of Schema Registry")
        .required(true);
    parser.addArgument("-g", "--consumer-group")
        .help("Consumer group")
        .setDefault(UUID.randomUUID());
    parser.addArgument("-p", "--protocol")
        .help("Protocol to use")
        .choices(Arrays.asList(JavaKafkaSerdeClient.Protocol.values()))
        .type(JavaKafkaSerdeClient.Protocol.class)
        .setDefault(JavaKafkaSerdeClient.Protocol.AVRO);
    parser.addArgument("-c", "--count")
        .help("Number of messages to send and consume")
        .setDefault(1)
        .type(Integer.class);
    parser.addArgument("--security").help("JSON formatted security string");
    parser.addArgument("--skip-known-types")
        .help("Whether to skip known types when resolving schema")
        .setDefault(false)
        .action(Arguments.storeTrue())
        .type(Boolean.class);

    return parser;
  }

  /**
   * Generates security settings from the supplied security options in JSON
   * format <p> The format of the provided string should be:
   *
   * <pre>{@code
   * {
   *     "sasl_mechanism": "mech",
   *     "security_protocol": "protocol",
   *     "sasl_plain_username": "username",
   *     "sasl_plain_password": "password"
   * }
   * }</pre>
   *
   * @param val The security settings in JSON format
   * @return Instance of SecuritySettings
   *
   * @see SecuritySettings
   */
  private static SecuritySettings parseSecuritySettings(String val) {
    JSONObject obj = new JSONObject(val);

    return SecuritySettings.fromJSON(obj);
  }
}
