# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.services.compatibility.example_runner import ExampleRunner
import rptest.services.compatibility.kafka_streams_examples as KafkaStreamExamples
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig


class KafkaStreamsTest(RedpandaTest):
    """
    Base class for KafkaStreams tests that contains all
    shared objects between tests
    """

    # The example is the java program that uses Kafka Streams.
    # The java program is represented by a wrapper in KafkaStreamExamples.
    Example = None

    def __init__(self, test_context, pandaproxy_config: PandaproxyConfig,
                 schema_registry_config: SchemaRegistryConfig):
        super(KafkaStreamsTest,
              self).__init__(test_context=test_context,
                             pandaproxy_config=pandaproxy_config,
                             schema_registry_config=schema_registry_config)

        self._ctx = test_context

        # 300s timeout because sometimes the examples
        # take 4min+ to produce correct output
        self._timeout = 300

    def create_example(self):
        # This will raise TypeError if Example is undefined
        example_helper = self.Example(self.redpanda, False)
        example = ExampleRunner(self._ctx,
                                example_helper,
                                timeout_sec=self._timeout)

        return example


class KafkaStreamsDriverBase(KafkaStreamsTest):
    """
    Base class for KafkaStreams tests that explicitly use a
    driver program to generate data.
    """

    # The driver is also a java program that uses Kafka Streams.
    # The java program is also represented by the example's corresponding
    # wrapper in KafkaStreamExamples.
    Driver = None

    def __init__(self, test_context, pandaproxy_config: PandaproxyConfig,
                 schema_registry_config: SchemaRegistryConfig):
        super(KafkaStreamsDriverBase,
              self).__init__(test_context=test_context,
                             pandaproxy_config=pandaproxy_config,
                             schema_registry_config=schema_registry_config)

    @cluster(num_nodes=5)
    def test_kafka_streams(self):
        example = self.create_example()

        # This will raise TypeError if DriverHeler is undefined
        driver_helper = self.Driver(self.redpanda, True)
        driver = ExampleRunner(self._ctx,
                               driver_helper,
                               timeout_sec=self._timeout)

        # Start the example
        example.start()
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the driver
        driver.start()
        wait_until(driver.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class KafkaStreamsProdConsBase(KafkaStreamsTest):
    """
    Base class for KafkaStreams tests that use a producer
    and consumer
    """

    # The producer should be an extension to KafProducer
    PRODUCER = None

    def __init__(self, test_context, pandaproxy_config: PandaproxyConfig,
                 schema_registry_config: SchemaRegistryConfig):
        super(KafkaStreamsProdConsBase,
              self).__init__(test_context=test_context,
                             pandaproxy_config=pandaproxy_config,
                             schema_registry_config=schema_registry_config)

    def is_valid_msg(self, msg):
        raise NotImplementedError("is_valid_msg() undefined.")

    @cluster(num_nodes=6)
    def test_kafka_streams(self):
        example = self.create_example()

        # This will raise TypeError if PRODUCER is undefined
        producer = self.PRODUCER(self._ctx, self.redpanda, self.topics[0].name)
        consumer = RpkConsumer(self._ctx, self.redpanda, self.topics[1].name)

        # Start the example
        example.start()

        # Produce some data
        producer.start()
        producer.wait()

        # Consume the data
        consumer.start()

        def try_cons():
            i = 0
            msgs = consumer.messages
            while i < len(msgs) and not self.is_valid_msg(msgs[i]):
                i += 1

            return i < len(msgs)

        wait_until(
            try_cons,
            timeout_sec=self._timeout,
            backoff_sec=5,
            err_msg=f"kafka-streams {self._ctx.cls_name} consumer failed")

        consumer.stop()
        producer.stop()


class KafkaStreamsTopArticles(KafkaStreamsDriverBase):
    """
    Test KafkaStreams TopArticles which counts the top N articles
    from a stream of page views
    """
    topics = (
        TopicSpec(name="PageViews"),
        TopicSpec(name="TopNewsPerIndustry"),
    )

    Example = KafkaStreamExamples.KafkaStreamsTopArticles
    Driver = KafkaStreamExamples.KafkaStreamsTopArticles

    def __init__(self, test_context):
        super(KafkaStreamsTopArticles,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class KafkaStreamsSessionWindow(KafkaStreamsDriverBase):
    """
    Test KafkaStreams SessionWindow counts user activity within
    a specific interval of time
    """
    topics = (
        TopicSpec(name="play-events"),
        TopicSpec(name="play-events-per-session"),
    )

    Example = KafkaStreamExamples.KafkaStreamsSessionWindow
    Driver = KafkaStreamExamples.KafkaStreamsSessionWindow

    def __init__(self, test_context):
        super(KafkaStreamsSessionWindow,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class KafkaStreamsJsonToAvro(KafkaStreamsDriverBase):
    """
    Test KafkaStreams JsontoAvro example which converts records from JSON to AVRO
    using the schema registry.
    """
    topics = (
        TopicSpec(name="json-source"),
        TopicSpec(name="avro-sink"),
    )

    Example = KafkaStreamExamples.KafkaStreamsJsonToAvro
    Driver = KafkaStreamExamples.KafkaStreamsJsonToAvro

    def __init__(self, test_context):
        super(KafkaStreamsJsonToAvro,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class KafkaStreamsPageView(RedpandaTest):
    """
    Test KafkaStreams PageView example which performs a join between a 
    KStream and a KTable
    """
    topics = (
        TopicSpec(name="PageViews"),
        TopicSpec(name="UserProfiles"),
        TopicSpec(name="PageViewsByRegion"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsPageView,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

        self._timeout = 300

    @cluster(num_nodes=5)
    def test_kafka_streams_page_view(self):
        example_jar = KafkaStreamExamples.KafkaStreamsPageView(self.redpanda,
                                                               is_driver=False)
        example = ExampleRunner(self.test_context,
                                example_jar,
                                timeout_sec=self._timeout)

        driver_jar = KafkaStreamExamples.KafkaStreamsPageView(self.redpanda,
                                                              is_driver=True)
        driver = ExampleRunner(self.test_context,
                               driver_jar,
                               timeout_sec=self._timeout)

        # Start the example
        example.start()
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the driver
        driver.start()
        wait_until(driver.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class KafkaStreamsWikipedia(RedpandaTest):
    """
    Test KafkaStreams Wikipedia example which computes, for every minute the
    number of new user feeds from the Wikipedia feed irc stream.
    """
    topics = (
        TopicSpec(name="WikipediaFeed"),
        TopicSpec(name="WikipediaStats"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsWikipedia,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

        self._timeout = 300

    @cluster(num_nodes=5)
    def test_kafka_streams_wikipedia(self):
        example_jar = KafkaStreamExamples.KafkaStreamsWikipedia(
            self.redpanda, is_driver=False)
        example = ExampleRunner(self.test_context,
                                example_jar,
                                timeout_sec=self._timeout)

        driver_jar = KafkaStreamExamples.KafkaStreamsWikipedia(self.redpanda,
                                                               is_driver=True)
        driver = ExampleRunner(self.test_context,
                               driver_jar,
                               timeout_sec=self._timeout)

        # Start the example
        example.start()
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the driver
        driver.start()
        wait_until(driver.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class KafkaStreamsSumLambda(KafkaStreamsDriverBase):
    """
    Test KafkaStreams SumLambda example that sums odd numbers
    using reduce
    """
    topics = (
        TopicSpec(name="numbers-topic"),
        TopicSpec(name="sum-of-odd-numbers-topic"),
    )

    Example = KafkaStreamExamples.KafkaStreamsSumLambda
    Driver = KafkaStreamExamples.KafkaStreamsSumLambda

    def __init__(self, test_context):
        super(KafkaStreamsSumLambda,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class AnomalyProducer(KafProducer):
    def __init__(self, context, redpanda, topic):
        super(AnomalyProducer, self).__init__(context,
                                              redpanda,
                                              topic,
                                              num_records=10)

    def value_gen(self):
        return "record-$((1 + $RANDOM % 3))"


class KafkaStreamsAnomalyDetection(KafkaStreamsProdConsBase):
    """
    Test KafkaStreams SessionWindow example that counts the user clicks
    within a 1min window
    """
    topics = (
        TopicSpec(name="UserClicks"),
        TopicSpec(name="AnomalousUsers"),
    )

    Example = KafkaStreamExamples.KafkaStreamsAnomalyDetection
    PRODUCER = AnomalyProducer

    def __init__(self, test_context):
        super(KafkaStreamsAnomalyDetection,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        key = msg["key"]
        return "record-1" in key or "record-2" in key or "record-3" in key


class RegionProducer(KafProducer):
    def __init__(self, context, redpanda, topic):
        super(RegionProducer, self).__init__(context,
                                             redpanda,
                                             topic,
                                             num_records=10)

    def value_gen(self):
        return "region-$((1 + $RANDOM % 3))"


class KafkaStreamsUserRegion(KafkaStreamsProdConsBase):
    """
    Test KafkaStreams UserRegion example the demonstrates group-by ops and
    aggregations on a KTable
    """
    topics = (
        TopicSpec(name="UserRegions"),
        TopicSpec(name="LargeRegions"),
    )

    Example = KafkaStreamExamples.KafkaStreamsUserRegion
    PRODUCER = RegionProducer

    def __init__(self, test_context):
        super(KafkaStreamsUserRegion,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        key = msg["key"]
        return "region-1" in key or "region-2" in key or "region-3" in key


class WordProducer(KafProducer):
    def __init__(self, context, redpanda, topic):
        super(WordProducer, self).__init__(context,
                                           redpanda,
                                           topic,
                                           num_records=10)

    def value_gen(self):
        return "\"redpanda is fast\""


class KafkaStreamsWordCount(KafkaStreamsProdConsBase):
    """
    Test KafkaStreams WordCount example which does simple prod-cons with
    KStreams and computes a historgram for word occurence
    """

    topics = (
        TopicSpec(name="streams-plaintext-input"),
        TopicSpec(name="streams-wordcount-output"),
    )

    Example = KafkaStreamExamples.KafkaStreamsWordCount
    PRODUCER = WordProducer

    def __init__(self, test_context):
        super(KafkaStreamsWordCount,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        key = msg["key"]
        return "redpanda" in key


class KafkaStreamsMapFunction(KafkaStreamsProdConsBase):
    """
    Test KafkaStreams MapFunction example which does .upper() as a state-less
    transform on records
    """

    topics = (
        TopicSpec(name="TextLinesTopic"),
        TopicSpec(name="UppercasedTextLinesTopic"),
        TopicSpec(name="OriginalAndUppercasedTopic"),
    )

    Example = KafkaStreamExamples.KafkaStreamsMapFunc
    PRODUCER = WordProducer

    def __init__(self, test_context):
        super(KafkaStreamsMapFunction,
              self).__init__(test_context=test_context,
                             pandaproxy_config=PandaproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        value = msg["value"]
        return "REDPANDA IS FAST" in value
