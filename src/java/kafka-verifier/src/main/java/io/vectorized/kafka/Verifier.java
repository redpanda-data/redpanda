package io.vectorized.kafka;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Verifier {
  private static final Logger logger = LoggerFactory.getLogger(Verifier.class);

  /** Get the command-line argument parser. */
  private static ArgumentParser argParser() {
    final ArgumentParser parser
        = ArgumentParsers.newFor("kafka-verifier").addHelp(true).build();

    final MutuallyExclusiveGroup payloadOptions
        = parser.addMutuallyExclusiveGroup().required(true).description(
            "either --record-size or --payload-file must be specified"
            + " but not both.");

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
    parser.addArgument("--replication-factor")
        .action(store())
        .required(true)
        .type(Short.class)
        .metavar("R-G")
        .dest("replicationFactor")
        .help("topic replication factor");
    parser.addArgument("--partitions")
        .action(store())
        .required(true)
        .type(Integer.class)
        .metavar("PARTITIONS")
        .dest("partitions")
        .help("topic partition count");
    parser.addArgument("--num-records")
        .action(store())
        .required(true)
        .type(Long.class)
        .metavar("NUM-RECORDS")
        .dest("numRecords")
        .help("number of messages to produce");

    payloadOptions.addArgument("--record-size")
        .action(store())
        .required(false)
        .type(Integer.class)
        .metavar("RECORD-SIZE")
        .dest("recordSize")
        .help(
            "message size in bytes. Note that you must provide exactly one of"
            + " --record-size or --payload-file.");

    parser.addArgument("--throughput")
        .action(store())
        .required(true)
        .type(Integer.class)
        .metavar("THROUGHPUT")
        .help(
            "throttle maximum message throughput to *approximately* THROUGHPUT"
            + " messages/sec. Set this to -1 to disable throttling.");

    parser.addArgument("--producer-props")
        .nargs("+")
        .required(false)
        .metavar("PROP-NAME=PROP-VALUE")
        .type(String.class)
        .dest("producerConfig")
        .help(
            "kafka producer related configuration properties like "
            + "bootstrap.servers,client.id etc. "
            + "These configs take precedence over those passed via"
            + " --producer.config.");

    parser.addArgument("--producer.config")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("CONFIG-FILE")
        .dest("producerConfigFile")
        .help("producer config properties file.");

    parser.addArgument("--consumer-props")
        .nargs("+")
        .required(false)
        .metavar("PROP-NAME=PROP-VALUE")
        .type(String.class)
        .dest("consumerConfig")
        .help(
            "kafka consumer related configuration properties like "
            + "bootstrap.servers,client.id etc. "
            + "These configs take precedence over those passed via "
            + "--consumer.config.");

    parser.addArgument("--consumer.config")
        .action(store())
        .required(false)
        .type(String.class)
        .metavar("CONFIG-FILE")
        .dest("consumerConfigFile")
        .help("consumer config properties file.");

    parser.addArgument("--random-consumer-group")
        .action(store())
        .required(false)
        .type(Boolean.class)
        .metavar("RANDOM-CONSUMER-GROUP")
        .dest("randomConsGroup")
        .help("randomize consumer group name");
    return parser;
  }

  private static Properties createProperties(
      final String producerConfig, final List<String> producerProps)
      throws IOException {

    final Properties props = new Properties();
    if (producerConfig != null) {
      props.putAll(Utils.loadProps(producerConfig));
    }
    if (producerProps != null) {
      for (final String prop : producerProps) {
        final String[] pieces = prop.split("=");
        if (pieces.length != 2)
          throw new IllegalArgumentException("Invalid property: " + prop);
        props.put(pieces[0], pieces[1]);
      }
    }
    return props;
  }

  public static void main(final String[] args) throws Exception {
    final ArgumentParser parser = argParser();

    try {
      final Namespace res = parser.parseArgs(args);

      /* parse args */
      final String broker = res.getString("broker");
      final String topicName = res.getString("topic");
      final short replicationFactor = res.getShort("replicationFactor");
      final int partitions = res.getInt("partitions");
      final long numRecords = res.getLong("numRecords");
      final Integer recordSize = res.getInt("recordSize");
      final int throughput = res.getInt("throughput");
      final List<String> producerProps = res.getList("producerConfig");
      final List<String> consumerProps = res.getList("consumerConfig");
      final String producerConfig = res.getString("producerConfigFile");
      final String consumerConfig = res.getString("consumerConfigFile");
      final Boolean randomGroup = res.getBoolean("randomConsGroup");

      if (recordSize < 16) {
        throw new ArgumentParserException(
            "Smaller record size supported by verifier is 16 bytes", parser);
      }

      if (producerProps == null && producerConfig == null) {
        throw new ArgumentParserException(
            "Either --producer-props or --producer.config must be specified.",
            parser);
      }

      final Properties pProps = createProperties(producerConfig, producerProps);
      final Properties cProps = createProperties(consumerConfig, consumerProps);
      pProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
      cProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
      if (randomGroup) {
        String group
            = "verifier-group-" + RandomStringUtils.randomAlphabetic(8);
        cProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        logger.info("Using consumer group - {}", group);
      }
      final Properties adminProperties = new Properties();
      adminProperties.setProperty(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

      try (AdminClient admin = AdminClient.create(adminProperties)) {
        admin.listTopics()
            .names()
            .thenApply((topics) -> {
              if (topics.contains(topicName)) {
                logger.info("Topic '{}' already exists", topicName);
                return KafkaFuture.<Void>completedFuture(null);
              }
              NewTopic nTopic
                  = new NewTopic(topicName, partitions, replicationFactor);
              logger.info(
                  "Creating topic {} with RF: {} and {} partitions", topicName,
                  replicationFactor, partitions);
              return admin.createTopics(Collections.singletonList(nTopic));
            })
            .get();

        Set<String> topics = admin.listTopics().names().get();
        while (!topics.contains(topicName)) {
          topics = admin.listTopics().names().get();
        }
      }

      final Expectations ex = new Expectations();
      final ProducerStats pStats = new ProducerStats(numRecords, 2000, ex);
      final Producer producer = new Producer(
          topicName, numRecords, recordSize, throughput, pProps, pStats);
      final Consumer consumer = new Consumer(topicName, numRecords, cProps, ex);
      final CompletableFuture<Void> cf = consumer.startConsumer();
      final CompletableFuture<Void> pf = producer.startProducer();

      CompletableFuture.allOf(pf, cf)
          .thenRun(() -> { producer.finish(); })
          .join();
      ex.finish();

    } catch (final ArgumentParserException e) {
      if (args.length == 0) {
        parser.printHelp();
        Exit.exit(0);
      } else {
        parser.handleError(e);
        Exit.exit(1);
      }
    }
  }
}
