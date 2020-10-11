package io.vectorized.kafka.configuration;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import io.vectorized.kafka.Mode;
import io.vectorized.kafka.configuration.ImmutableConsumerConfig;
import io.vectorized.kafka.configuration.ImmutableProducerConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.commons.lang3.RandomStringUtils;

public class Configuration {

  public Configuration() {
    this.parser = ArgumentParsers.newFor("kafka-compacted-topic-verifier")
                      .addHelp(true)
                      .build();
    this.subparsers
        = parser.addSubparsers()
              .title("modes")
              .description("valid modes")
              .help("Valid modes for kafka compacted topic verifiers")
              .dest("mode")
              .metavar("MODE");
    this.produceParser = subparsers.addParser("produce").help(
        "Creates topic and produces data, generates verifier file");
    this.consumeParser = subparsers.addParser("consume").help(
        "Consumes data from given topic, when available uses provided state file for verification");
    initParser();
  }

  private void initParser() {
    parser.addArgument("--security-properties")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("SECURITY_PROPS")
        .dest("securityProperties")
        .help("properties file containing securing configuration");

    parser.addArgument("--broker")
        .action(store())
        .required(true)
        .type(String.class)
        .metavar("BROKER")
        .help("broker address");

    parser.addArgument("--topic")
        .action(store())
        .required(true)
        .type(String.class)
        .metavar("TOPIC")
        .help("produce messages to this topic");

    parser.addArgument("--state-file")
        .action(store())
        .required(false)
        .type(String.class)
        .dest("stateFile")
        .metavar("STATE-FILE")
        .help("path to state file");

    produceParser.addArgument("--num-records")
        .action(store())
        .required(true)
        .type(Long.class)
        .metavar("NUM-RECORDS")
        .dest("numRecords")
        .help("number of messages to produce");

    produceParser.addArgument("--replication-factor")
        .action(store())
        .required(true)
        .type(Short.class)
        .metavar("R-G")
        .dest("replicationFactor")
        .help("topic replication factor");

    produceParser.addArgument("--partitions")
        .action(store())
        .required(true)
        .type(Integer.class)
        .metavar("PARTITIONS")
        .dest("partitions")
        .help("topic partition count");

    produceParser.addArgument("--segment-size")
        .action(store())
        .required(false)
        .type(Integer.class)
        .metavar("SEGMENT-SIZE")
        .dest("segmentSize")
        .help("segment size of created topic");

    produceParser.addArgument("--key-cardinality")
        .action(store())
        .required(false)
        .type(Integer.class)
        .setDefault(1024)
        .dest("keyCardinality")
        .help("number of keys");

    produceParser.addArgument("--payload-size")
        .action(store())
        .required(false)
        .type(Integer.class)
        .metavar("PAYLOAD-SIZE")
        .dest("payloadSize")
        .help("size of record values");

    produceParser.addArgument("--key-size")
        .action(store())
        .required(false)
        .type(Integer.class)
        .metavar("KEY-SIZE")
        .dest("keySize")
        .help("size of record keys");

    produceParser.addArgument("--compression")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("COMPRESSION")
        .dest("compression")
        .setDefault("none")
        .choices("none", "snappy", "gzip", "lz4", "zstd", "random")
        .help("compression used by producer");

    produceParser.addArgument("--producer-props")
        .nargs("+")
        .required(false)
        .metavar("PROP-NAME=PROP-VALUE")
        .type(String.class)
        .dest("producerConfig")
        .help(
            "kafka producer related configuration properties like "
            + "bootstrap.servers,client.id etc. ");
    consumeParser.addArgument("--consumer-props")
        .nargs("+")
        .required(false)
        .metavar("PROP-NAME=PROP-VALUE")
        .type(String.class)
        .dest("consumerConfig")
        .help(
            "kafka consumer related configuration properties like "
            + "bootstrap.servers,client.id etc. ");

    consumeParser.addArgument("--consumer-group")
        .action(store())
        .required(false)
        .type(String.class)
        .setDefault(false)
        .metavar("RANDOM-CONSUMER-GROUP")
        .dest("consumer-group")
        .help("Consumer group name, if not provided random name will be used");
  }

  public static Properties parseProperties(final List<String> properties) {
    final Properties props = new Properties();
    for (final String prop : properties) {
      final String[] pieces = prop.split("=");
      if (pieces.length != 2)
        throw new IllegalArgumentException("Invalid property: " + prop);
      props.put(pieces[0], pieces[1]);
    }

    return props;
  }

  public Mode getMode(final String[] args) throws ArgumentParserException {

    argsNs = parser.parseArgs(args);
    final String modeStr = argsNs.getString("mode");

    if (modeStr.equals("consume")) {
      return Mode.CONSUMER;
    }
    return Mode.PRODUCER;
  }

  private Properties readSecurityProperties(String file) {
    Path path = Paths.get(file);
    try (final InputStream is = Files.newInputStream(path)) {
      Properties properties = new Properties();
      properties.load(is);
      return properties;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public ProducerConfig getProducerConfig() {
    var compression = argsNs.getString("compression").equals("random")
                          ? getRandomCompression()
                          : argsNs.getString("compression");
    var builder
        = ImmutableProducerConfig.builder()
              .brokers(argsNs.getString("broker"))
              .topic(argsNs.getString("topic"))
              .statePath(argsNs.getString("stateFile"))
              .addAllProperties(
                  Optional.ofNullable(argsNs.<String>getList("producerConfig"))
                      .orElseGet(List::of))
              .replicationFactor(argsNs.getShort("replicationFactor"))
              .partitions(argsNs.getInt("partitions"))
              .recordsCount(argsNs.getLong("numRecords"))
              .payloadSize(argsNs.getInt("payloadSize"))
              .keySize(argsNs.getInt("keySize"))
              .keyCardinality(argsNs.getInt("keyCardinality"))
              .compression(compression)
              .segmentSize(argsNs.getInt("segmentSize"));

    Optional.ofNullable(argsNs.getString("securityProperties"))
        .map(this::readSecurityProperties)
        .ifPresent(builder::securityProperties);

    return builder.build();
  }

  public ConsumerConfig getConsumerConfig() {
    var builder
        = ImmutableConsumerConfig.builder()
              .brokers(argsNs.getString("broker"))
              .topic(argsNs.getString("topic"))

              .statePath(argsNs.getString("stateFile"))
              .consumerGroup(
                  Optional.ofNullable(argsNs.getString("consumerGroup"))
                      .orElseGet(
                          ()
                              -> "consumer-gr-"
                                     + RandomStringUtils.randomAlphabetic(6)))
              .addAllProperties(
                  Optional.ofNullable(argsNs.<String>getList("producerConfig"))
                      .orElseGet(List::of));

    Optional.ofNullable(argsNs.getString("securityProperties"))
        .map(this::readSecurityProperties)
        .ifPresent(builder::securityProperties);

    return builder.build();
  }

  public void handleError(String[] args, ArgumentParserException e) {
    if (args.length == 0) {
      parser.printHelp();
    } else {
      produceParser.handleError(e);
      parser.handleError(e);
    }
  }

  static String getRandomCompression() {
    String[] compression = {"snappy", "gzip", "lz4", "zstd"};
    return compression[ThreadLocalRandom.current().nextInt(compression.length)];
  }

  private final ArgumentParser parser;
  private final Subparsers subparsers;
  private final Subparser produceParser;
  private final Subparser consumeParser;
  private Namespace argsNs;
}
