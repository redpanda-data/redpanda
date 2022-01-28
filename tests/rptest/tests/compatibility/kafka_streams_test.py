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
import json
import re

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


class KafkaStreamsInteractiveQueries(RedpandaTest):
    """
    Test KafkaStreams InteractiveQueries app which locates and
    queries kafka streams state stores. The state stores used in this app are
    a KeyValue store and a Windowed Store.

    Outside of KStreams, the app does simple prod-cons so the used RP features and systems are:
    - topic creation
    - produce/consume on topic
    - src/v/kafka
    - src/v/raft
    - src/v/storage
    """

    topics = (TopicSpec(name="TextLinesTopic"), )

    def __init__(self, test_context):
        super(KafkaStreamsInteractiveQueries,
              self).__init__(test_context=test_context)
        self._ctx = test_context
        self._ports = [7070, 7071]
        self._store_names = ["windowed-word-count", "word-count"]

    def check_all_running_instances(self, res):
        # Example response looks like
        # [{'host': 'docker_n_4', 'port': 7070, 'storeNames': ['windowed-word-count']},
        # {'host': 'docker_n_5', 'port': 7071, 'storeNames': ['word-count']}]

        if len(res) < 2:
            return False

        for instance in res:
            if not re.match("^docker_n_\d+$", instance["host"]):
                return False
            if instance["port"] not in self._ports:
                return False
            if len(instance["storeNames"]) != 1:
                return False
            if instance["storeNames"][0] not in self._store_names:
                return False
        return True

    def check_instances_by_store_name(self, res):
        # Example response looks like
        # [{'host': 'docker_n_5', 'port': 7071, 'storeNames': ['word-count']}]

        if len(res) < 1:
            return False

        for instance in res:
            if not re.match("^docker_n_\d+$", instance["host"]):
                return False
            if instance["port"] not in self._ports:
                return False
            if len(instance["storeNames"]) != 1:
                return False
            if instance["storeNames"][0] not in self._store_names:
                return False

        return True

    def check_instance_for_word_in_store(self, res):
        # Example response looks like
        # {'host': 'docker_n_4', 'port': 7070, 'storeNames': ['word-count']}

        if not re.match("^docker_n_\d+$", res["host"]):
            return False

        if res["port"] not in self._ports:
            return False

        if len(res["storeNames"]) != 1:
            return False

        if res["storeNames"][0] not in self._store_names:
            return False

        return True

    def check_latest_value_for_word_in_store(self, res):
        # Example response looks like
        # {"key":"hello","value":3}

        if res["key"] not in KafkaStreamExamples.QUERIES_HIST:
            return False

        return res["value"] == KafkaStreamExamples.QUERIES_HIST[res["key"]]

    def check_all_records_from_word_count(self, res):
        # Example response looks like
        # [{"key":"hello","value":3}, {"key":"world","value":2}, ... ]

        if len(res) < 1:
            return False

        for kv in res:
            # The response for each key-value pair is equal
            # to the response for the latest value of that key
            if not self.check_latest_value_for_word_in_store(kv):
                return False

        return True

    def _http_get(self, host, route, cond):
        def try_get():
            res = requests.get(f"http://{host}:7070{route}")

            if not res.ok:
                return False

            self.logger.debug(f"Route: {route}")
            self.logger.debug(f"Text: {res.text}")

            # The data is a JSON object
            text_json = json.loads(res.text)

            return cond(text_json)

        wait_until(try_get,
                   timeout_sec=TIMEOUT,
                   backoff_sec=5,
                   err_msg=f"GET {route} failed")

    @cluster(num_nodes=6)
    def test_kafka_streams_interactive_queries(self):
        # Different routes will respond with the same content in
        # use cases with a single app instance. For example,
        # /state/instances and /state/instances/<store name>

        # Therefore, create two instances of the app on different ports
        example_jar1 = KafkaStreamExamples.InteractiveQueriesExample(
            self.redpanda, self._ports[0])
        example_jar2 = KafkaStreamExamples.InteractiveQueriesExample(
            self.redpanda, self._ports[1])
        example1 = ExampleRunner(self._ctx, example_jar1, timeout_sec=TIMEOUT)
        example2 = ExampleRunner(self._ctx, example_jar2, timeout_sec=TIMEOUT)

        driver_jar = KafkaStreamExamples.InteractiveQueriesDriver(
            self.redpanda)
        driver = ExampleRunner(self._ctx, driver_jar, timeout_sec=TIMEOUT)

        # Start the examples and wait for them to load
        example1.start()
        example2.start()
        example1.wait()
        example2.wait()

        # Start the driver and wait for it to load
        driver.start()
        driver.wait()

        host = example1.node.name

        # Do HTTP GET on the possible routes

        # /state/instances lists all running instances of this application
        self._http_get(host, "/state/instances",
                       self.check_all_running_instances)

        # /state/instances/<store name> lists instances that currently manage
        # the state store
        self._http_get(host, "/state/instances/word-count",
                       self.check_instances_by_store_name)
        self._http_get(host, "/state/instances/windowed-word-count",
                       self.check_instances_by_store_name)

        # /state/instance/<store name>/<word> finds the app instance that
        # contains the word (if it exists) for the state store
        for word in KafkaStreamExamples.QUERIES_HIST:
            self._http_get(host, f"/state/instance/word-count/{word}",
                           self.check_instance_for_word_in_store)
        for word in KafkaStreamExamples.QUERIES_HIST:
            self._http_get(host, f"/state/instance/windowed-word-count/{word}",
                           self.check_instance_for_word_in_store)

        # /state/keyvalue/word-count/<word> gets the latest value for the word
        # in the state store
        # The store windowed-word-count is not intended to work with this route
        for word in KafkaStreamExamples.QUERIES_HIST:
            self._http_get(host, f"/state/keyvalue/word-count/{word}",
                           self.check_latest_value_for_word_in_store)

        # /state/keyvalues/word-count/all gets all key-value records from the
        # word-count state store
        # The store windowed-word-count is not intended to work with this route
        # Don't test this route for now because there are problems in CI where the
        # response from the HTTP server is 500 error

        driver.stop()
        example2.stop()
        example1.stop()


class TokenProducer(KafProducer):
    def __init__(self, context, redpanda, topic, num_produces):
        super(TokenProducer, self).__init__(context,
                                            redpanda,
                                            topic,
                                            num_records=num_produces)

        # Change this number if the number of
        # unique tokens represented in value_gen changes.
        self.num_unique_tokens = 3

    def value_gen(self):
        # The below string is used num_produces times
        return "token-$((1 + $RANDOM % 3))"


class KafkaStreamsAppReset(RedpandaTest):
    """
    Test KafkaStreams ApplicationReset example that uses confluent's cleanUp API
    which resets offsets for all topics and deletes all internal/auto-created topics.
    The actual application, such as word-count, is irrelevant to the example and the
    test.

    The RP features and systems that the cleanUp API uses are:
    - topic deletion
    - resets consumer offsets

    More details on the cleanUp API are at
    https://kafka.apache.org/20/javadoc/org/apache/kafka/streams/KafkaStreams.html#cleanUp
    """
    topics = (
        TopicSpec(name="my-input-topic"),
        TopicSpec(name="my-output-topic"),
        TopicSpec(name="rekeyed-topic"),
    )

    def __init__(self, test_context):
        super(KafkaStreamsAppReset, self).__init__(test_context=test_context)
        self._ctx = test_context
        self._num_produces = 10 if self.scale.local else 1000

    def is_valid_msg(self, msg):
        token = msg["key"]
        count = int(msg["value"])

        if token != "token-1" and token != "token-2" and token != "token-3":
            return False

        if count < 0 or count > self._num_produces:
            return False

        return True

    @cluster(num_nodes=6)
    def test_kafka_streams_app_reset(self):
        example_jar = KafkaStreamExamples.AppResetExample(self.redpanda)
        example = ExampleRunner(self._ctx, example_jar, timeout_sec=TIMEOUT)
        producer = TokenProducer(self._ctx, self.redpanda, self.topics[0].name,
                                 self._num_produces)

        form = '\'%{"key":"%k", "value":"%v{unpack[>Q]}"%}\\n\''
        consumer = RpkConsumer(self._ctx,
                               self.redpanda,
                               self.topics[1].name,
                               formatter=form)

        example.start()
        example.wait()

        producer.start()
        producer.wait()
        producer.stop()

        consumer.start()
        # is_valid_msg is called on every message the consumer reads
        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=producer.num_unique_tokens)

        consumer.stop()
        consumer.clean()
        example.stop()
        example.clean()

        # Reset the app on the node where the example is running
        cmd = KafkaStreamExamples.reset_app_cmd(self.redpanda,
                                                "application-reset-demo",
                                                self.topics[0].name,
                                                self.topics[2].name)

        # Ducktape raises an error if the cmd has non-zero exist status
        example.node.account.ssh(cmd)

        # Restart example and consumer.
        # No need to produce new data because the app will
        # re-run on the old data
        example.start()
        example.wait()

        consumer.start()
        consume(self._ctx,
                consumer,
                self.is_valid_msg,
                num_msgs=producer.num_unique_tokens)

        consumer.stop()
        example.stop()