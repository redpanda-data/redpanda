# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.services.compatibility.example_runner import ExampleRunner
import rptest.services.compatibility.kafka_streams_examples as KafkaStreamExamples
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
import time
import requests

# 600s timeout because sometimes the examples
# take ~10min to produce data
TIMEOUT = 600


class KafkaStreamsWikipedia(RedpandaTest):
    """
    Test KafkaStreams wikipedia example which computes the number of new
    users to a wikipedia page
    """
    topics = (
        TopicSpec(name="WikipediaFeed"),
        TopicSpec(name="WikipediaStats"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsWikipedia, self).__init__(test_context=test_context,
                                                    enable_pp=True,
                                                    enable_sr=True)
        self._ctx = test_context

    @cluster(num_nodes=5)
    def test_kafka_streams_wikipedia(self):
        example_jar = KafkaStreamExamples.WikipediaExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.WikipediaDriver(self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the example
        example.start()
        example.wait()

        # Start the driver
        driver.start()
        driver.wait()

        driver.stop()
        example.stop()


class KafkaStreamsTopArticles(RedpandaTest):
    """
    Test KafkaStreams TopArticles which counts the top N articles
    from a stream of page views
    """
    topics = (
        TopicSpec(name="PageViews"),
        TopicSpec(name="TopNewsPerIndustry"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsTopArticles,
              self).__init__(test_context=test_context,
                             enable_pp=True,
                             enable_sr=True)
        self._ctx = test_context

    @cluster(num_nodes=5)
    def test_kafka_streams_top_articles(self):
        example_jar = KafkaStreamExamples.TopArticlesExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.TopArticlesDriver(self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the example
        example.start()
        example.wait()

        # Start the driver
        driver.start()
        driver.wait()

        driver.stop()
        example.stop()


class KafkaStreamsSessionWindow(RedpandaTest):
    """
    Test KafkaStreams SessionWindow counts user activity within
    a specific interval of time
    """
    topics = (
        TopicSpec(name="play-events"),
        TopicSpec(name="play-events-per-session"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsSessionWindow,
              self).__init__(test_context=test_context,
                             enable_pp=True,
                             enable_sr=True)
        self._ctx = test_context

    @cluster(num_nodes=5)
    def test_kafka_streams_session_window(self):
        example_jar = KafkaStreamExamples.SessionWindowExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.SessionWindowDriver(self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the example
        example.start()
        example.wait()

        # Start the driver
        driver.start()
        driver.wait()

        driver.stop()
        example.stop()


class KafkaStreamsJsonToAvro(RedpandaTest):
    """
    Test KafkaStreams JsontoAvro example which converts records from JSON to AVRO
    using the schema registry.
    """
    topics = (
        TopicSpec(name="json-source"),
        TopicSpec(name="avro-sink"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsJsonToAvro, self).__init__(test_context=test_context,
                                                     enable_pp=True,
                                                     enable_sr=True)
        self._ctx = test_context

    @cluster(num_nodes=5)
    def test_kafka_streams_json_to_avro(self):
        example_jar = KafkaStreamExamples.JsonToAvroExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.JsonToAvroDriver(self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the example
        example.start()
        example.wait()

        # Start the driver
        driver.start()
        driver.wait()

        driver.stop()
        example.stop()


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
        super(KafkaStreamsPageView, self).__init__(test_context=test_context,
                                                   enable_pp=True,
                                                   enable_sr=True)
        self._ctx = test_context

    @cluster(num_nodes=5)
    def test_kafka_streams_page_view(self):
        example_jar = KafkaStreamExamples.PageViewExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.PageViewDriver(self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the example
        example.start()
        example.wait()

        # Start the driver
        driver.start()
        driver.wait()

        driver.stop()
        example.stop()


class KafkaStreamsSumLambda(RedpandaTest):
    """
    Test KafkaStreams SumLambda example that sums odd numbers
    using reduce
    """
    topics = (
        TopicSpec(name="numbers-topic"),
        TopicSpec(name="sum-of-odd-numbers-topic"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsSumLambda, self).__init__(test_context=test_context)
        self._ctx = test_context

    @cluster(num_nodes=5)
    def test_kafka_streams_page_view(self):
        example_jar = KafkaStreamExamples.SumLambdaExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.SumLambdaDriver(self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the example
        example.start()
        example.wait()

        # Start the driver
        driver.start()
        driver.wait()

        driver.stop()
        example.stop()


def consume(ctx, consumer, is_valid_msg, num_msgs):
    def try_cons():
        # The messages are read into memory as json
        # by RpkConsumer. Exit if there is no data to analyze.
        if len(consumer.messages) < 1:
            return False

        consumer.logger.debug(f'msg len: {len(consumer.messages)}')

        # Get the last N fetched messages
        msgs = []
        try:
            msgs = consumer.messages[-num_msgs:]
        except Exception as ex:
            consumer.logger.debug(ex)
            return False

        # Validate each message
        for msg in msgs:
            consumer.logger.debug(f'msg: {msg}')

            if not is_valid_msg(msg):
                return False

        return True

    wait_until(try_cons,
               timeout_sec=TIMEOUT,
               backoff_sec=5,
               err_msg=f"kafka-streams {ctx.cls_name} consumer failed")


class AnomalyProducer(KafProducer):
    def __init__(self, context, redpanda, topic, num_produces):
        super(AnomalyProducer, self).__init__(context,
                                              redpanda,
                                              topic,
                                              num_records=num_produces)

        # Change this number if the number of
        # unique users represented in value_gen changes.
        self.num_unique_users = 3

    def value_gen(self):
        # The below string is used num_produces times
        # The user name is read as: "user-X clicked"
        return '"user-1\\nuser-2\\nuser-3\\nuser-1"'


class KafkaStreamsAnomalyDetection(RedpandaTest):
    """
    Test KafkaStreams SessionWindow example that counts the user clicks
    within a 1min window
    """
    topics = (
        TopicSpec(name="UserClicks"),
        TopicSpec(name="AnomalousUsers"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsAnomalyDetection,
              self).__init__(test_context=test_context)
        self._ctx = test_context
        self._num_produces = 10 if self.scale.local else 1000

    def is_valid_msg(self, msg):
        username = msg["key"]
        count = int(msg["value"])

        if "user-1" in username:
            return count == (self._num_produces * 2)
        else:
            return count == self._num_produces

    @cluster(num_nodes=6)
    def test_kafka_streams_anomaly_detection(self):
        example_jar = KafkaStreamExamples.AnomalyDetectionExample(
            self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        producer = AnomalyProducer(self._ctx, self.redpanda,
                                   self.topics[0].name, self._num_produces)

        form = '\'%{"key":"%k", "value":"%v{unpack[>Q]}"%}\\n\''
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topics[1].name,
                               formatter=form)

        # Start the example
        example.start()
        example.wait()

        # Produce some data
        producer.start()
        producer.wait()

        # Consume the data
        consumer.start()

        # This example counts the number of user clicks (i.e., user names)
        # in 1 minute and publishes that count to the second topic, AnomalousUsers.
        # So, on the consumer side, there is one record per unique user name.

        # is_valid_msg is called on every message the consumer reads
        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=producer.num_unique_users)

        producer.stop()
        consumer.stop()

        # Resend after 1min
        time.sleep(60)
        producer.start()
        producer.wait()

        consumer.start()

        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=producer.num_unique_users)

        producer.stop()
        consumer.stop()
        example.stop()


class RegionProducer(KafProducer):
    def __init__(self, context, redpanda, topic, num_produces):
        super(RegionProducer, self).__init__(context,
                                             redpanda,
                                             topic,
                                             num_records=num_produces)

        # Change this number if the number of
        # unique regions represented in value_gen changes.
        self.num_unique_regions = 3

    def value_gen(self):
        # The below string is used num_produces times
        # Sending a region name is read as: "someone goes to region-X"
        return "region-$((1 + $RANDOM % 3))"


class KafkaStreamsUserRegion(RedpandaTest):
    """
    Test KafkaStreams UserRegion example the demonstrates group-by ops and
    aggregations on a KTable
    """
    topics = (
        TopicSpec(name="UserRegions"),
        TopicSpec(name="LargeRegions"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsUserRegion, self).__init__(test_context=test_context)
        self._ctx = test_context
        self._num_produces = 10 if self.scale.local else 1000

    # is_valid_msg is called on every message the consumer reads
    def is_valid_msg(self, msg):
        region = msg["key"]
        population_size = int(msg["value"])

        if region != "region-1" and region != "region-2" and region != "region-3":
            return False

        if population_size < 0 or population_size > self._num_produces:
            return False

        return True

    @cluster(num_nodes=6)
    def test_kafka_streams_user_region(self):
        example_jar = KafkaStreamExamples.UserRegionExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        producer = RegionProducer(self._ctx, self.redpanda,
                                  self.topics[0].name, self._num_produces)

        form = '\'%{"key":"%k", "value":"%v{unpack[>Q]}"%}\\n\''
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topics[1].name,
                               formatter=form)

        # Start the example
        example.start()
        example.wait()

        # Produce some data
        producer.start()
        producer.wait()

        # Consume the data
        consumer.start()
        # is_valid_msg is called on every message the consumer reads
        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=producer.num_unique_regions)

        consumer.stop()
        producer.stop()
        example.stop()


class PhraseProducer(KafProducer):
    def __init__(self, context, redpanda, topic, num_produces):
        super(PhraseProducer, self).__init__(context,
                                             redpanda,
                                             topic,
                                             num_records=num_produces)

        # Change this number if the number of
        # total words represented in value_gen changes.
        self.total_words = 6

    def value_gen(self):
        # The below string is used num_produces times.
        # Send "redpanda" 3 x num_produces times
        # Send "is" 2 x num_produces times
        # send "fast" num_produces times
        return '"redpanda is fast\\nredpanda is\\nredpanda"'


class KafkaStreamsWordCount(RedpandaTest):
    """
    Test KafkaStreams WordCount example which does simple prod-cons with
    KStreams and computes a historgram for word occurence
    """

    topics = (
        TopicSpec(name="streams-plaintext-input"),
        TopicSpec(name="streams-wordcount-output"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsWordCount, self).__init__(test_context=test_context)
        self._ctx = test_context
        self._num_produces = 10 if self.scale.local else 1000

    # is_valid_msg is called on every message the consumer reads
    def is_valid_msg(self, msg):
        word = msg["key"]
        count = int(msg["value"])

        if word == "redpanda":
            return 0 < count and count <= (self._num_produces * 3)
        elif word == "is":
            return 0 < count and count <= (self._num_produces * 2)
        elif word == "fast":
            return 0 < count and count <= self._num_produces
        else:
            return False

    @cluster(num_nodes=6)
    def test_kafka_streams_wordcount(self):
        example_jar = KafkaStreamExamples.WordCountExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        producer = PhraseProducer(self._ctx, self.redpanda,
                                  self.topics[0].name, self._num_produces)

        form = '\'%{"key":"%k", "value":"%v{unpack[>Q]}"%}\\n\''
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topics[1].name,
                               formatter=form)

        # Start the example
        example.start()
        example.wait()

        # Produce some data
        producer.start()
        producer.wait()

        # Consume the data
        consumer.start()
        # is_valid_msg is called on every message the consumer reads
        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=(producer.total_words * self._num_produces))

        consumer.stop()
        producer.stop()
        example.stop()


class TextProducer(KafProducer):
    def __init__(self, context, redpanda, topic, num_produces):
        super(TextProducer, self).__init__(context,
                                           redpanda,
                                           topic,
                                           num_records=num_produces)

        self.choices = [
            "redpanda is fast", "the fastest queue in the west",
            "bring streams to redpanda"
        ]

    def value_gen(self):
        # The below string is used num_produces times
        choices_str = '\\n'.join(self.choices)
        return f'"{choices_str}"'


class KafkaStreamsMapFunction(RedpandaTest):
    """
    Test KafkaStreams MapFunction example which does .upper() as a state-less
    transform on records
    """

    topics = (
        TopicSpec(name="TextLinesTopic"),
        TopicSpec(name="UppercasedTextLinesTopic"),
        TopicSpec(name="OriginalAndUppercasedTopic"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsMapFunction,
              self).__init__(test_context=test_context)
        self._ctx = test_context
        self._num_produces = 10 if self.scale.local else 1000
        self._producer = TextProducer(self._ctx, self.redpanda,
                                      self.topics[0].name, self._num_produces)

    # is_valid_msg is called on every message the consumer reads
    def is_valid_msg(self, msg):
        original = msg["key"]
        upper = msg["value"]

        return original in self._producer.choices and original == upper.lower()

    @cluster(num_nodes=6)
    def test_kafka_streams_map_function(self):
        example_jar = KafkaStreamExamples.MapFuncExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)

        consumer = RpkConsumer(self._ctx, self.redpanda, self.topics[2].name)

        # Start the example
        example.start()

        # Produce some data
        self._producer.start()
        self._producer.wait()

        # Consume the data
        consumer.start()
        # is_valid_msg is called on every message the consumer reads
        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=(len(self._producer.choices) * self._num_produces))

        consumer.stop()
        self._producer.stop()
        example.stop()
