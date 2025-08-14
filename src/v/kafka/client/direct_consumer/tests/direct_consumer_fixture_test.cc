/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/tests/cluster_test_fixture.h"
#include "container/chunked_vector.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/client/direct_consumer/fetcher.h"
#include "kafka/protocol/types.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "redpanda/tests/fixture.h"

#include <seastar/util/defer.hh>

#include <fmt/format.h>

#include <map>
#include <unordered_map>
#include <vector>

using namespace kafka::client;

namespace {
ss::logger logger{"direct-consumer-test"};
}
class consumer_fixture : public cluster_test_fixture {
public:
    kafka::client::connection_configuration make_connection_config() {
        kafka::client::connection_configuration config;
        config.client_id = "test-client";
        config.initial_brokers
          = std::views::transform(
              instance_ids(),
              [this](model::node_id id) { return instance(id); })
            | std::views::transform(
              [](const redpanda_thread_fixture* f) { return f->kafka_port; })
            | std::views::transform([](int port) {
                  return net::unresolved_address("localhost", port);
              })
            | std::ranges::to<std::vector<net::unresolved_address>>();
        return config;
    }

    ss::future<std::unique_ptr<kafka::client::cluster>> create_client_cluster(
      std::optional<std::unique_ptr<kafka::client::broker_factory>>
        broker_factory
      = std::nullopt) {
        auto cluster = [&]() {
            if (broker_factory.has_value()) {
                vassert(
                  broker_factory.value() != nullptr,
                  "expected non-null broker factory if provided");
                return std::make_unique<kafka::client::cluster>(
                  make_connection_config(), std::move(broker_factory.value()));
            }
            return std::make_unique<kafka::client::cluster>(
              make_connection_config());
        }();
        co_await cluster->start();
        co_return cluster;
    }

    topic_assignment make_assignment(
      const model::topic& topic,
      std::vector<int> partitions,
      std::optional<kafka::offset> initial_offset = std::nullopt) {
        topic_assignment assignment;
        assignment.topic = topic;
        for (const auto& p : partitions) {
            partition_assignment part;
            part.partition_id = model::partition_id(p);
            part.next_offset = initial_offset;
            assignment.partitions.push_back(part);
        }
        return assignment;
    }

    model::node_id get_partition_leader(const model::ntp& ntp) {
        std::optional<model::node_id> leader_id;
        model::timeout_clock::time_point deadline = model::timeout_clock::now()
                                                    + 10s;
        while (!leader_id) {
            if (model::timeout_clock::now() > deadline) {
                throw std::runtime_error(
                  fmt::format("Timeout while waiting for leader for {}", ntp));
            }
            leader_id = instance(model::node_id{0})
                          ->app.controller->get_partition_leaders()
                          .local()
                          .get_leader(ntp);

            ss::sleep(100ms).get();
        }
        return leader_id.value();
    }

    void produce_to_partition(
      const model::topic& topic, int partition, size_t record_count) {
        model::ntp ntp(
          model::kafka_namespace, topic, model::partition_id(partition));

        auto leader_id = get_partition_leader(ntp);

        vlog(
          logger.debug,
          "[broker: {}] Produce {} records to {}",
          leader_id,
          record_count,
          ntp);

        tests::kafka_produce_transport producer(
          instance(leader_id)->make_kafka_client().get());
        producer.start().get();
        auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

        auto records = tests::kv_t::sequence(0, record_count, partition);
        producer
          .produce_to_partition(topic, model::partition_id(partition), records)
          .get();
    }

    chunked_hash_map<
      model::topic_partition,
      chunked_vector<model::record_batch>>
    fetch_until_empty(direct_consumer& consumer) {
        chunked_hash_map<
          model::topic_partition,
          chunked_vector<model::record_batch>>
          ret;

        while (true) {
            auto fetched = consumer.fetch_next(1000ms).get();

            if (fetched.value().empty()) {
                break;
            }
            for (auto& topic_data : fetched.value()) {
                for (auto& partition_data : topic_data.partitions) {
                    auto& batches = ret[model::topic_partition(
                      topic_data.topic, partition_data.partition_id)];

                    std::ranges::move(
                      partition_data.data, std::back_inserter(batches));
                }
            }
        }
        return ret;
    }

    void assign_partitions(topic_assignment assgn) {
        consumer
          ->assign_partitions(
            chunked_vector<topic_assignment>::single(std::move(assgn)))
          .get();
    }

    void unassign_partition(model::topic_partition tp) {
        consumer
          ->unassign_partitions(
            chunked_vector<model::topic_partition>::single(std::move(tp)))
          .get();
    }

    void unassign_topic(model::topic topic) {
        consumer
          ->unassign_topics(
            chunked_vector<model::topic>::single(std::move(topic)))
          .get();
    }

    redpanda_thread_fixture* rp;
    std::unique_ptr<kafka::client::cluster> cluster;
    std::unique_ptr<kafka::client::direct_consumer> consumer;
    model::topic topic{"test-topic"};
};

namespace {

enum class session_config : uint8_t {
    with_sessions,
    without_sessions,
    toggle_sessions,
};

[[maybe_unused]] auto format_as(session_config c) { return fmt::underlying(c); }

class basic_consume_fixture
  : public consumer_fixture
  , public testing::TestWithParam<session_config> {
public:
    void SetUp() override {
        create_node_application(model::node_id{0});
        create_node_application(model::node_id{1});
        create_node_application(model::node_id{2});
        auto* rp = instance(model::node_id{0});
        wait_for_all_members(3s).get();
        rp->add_topic({model::kafka_namespace, topic}, 3, std::nullopt, 3)
          .get();
        cluster = create_client_cluster().get();
        consumer = std::make_unique<kafka::client::direct_consumer>(
          *cluster,
          direct_consumer::configuration{
            .with_sessions = fetch_sessions_enabled{
              GetParam() == session_config::with_sessions}});
        consumer->start().get();
    }

    void TearDown() override {
        consumer->stop().get();
        cluster->stop().get();
    }

    void maybe_toggle_fetch_sessions() {
        if (GetParam() == session_config::toggle_sessions) {
            consumer->update_configuration(direct_consumer::configuration{
              .with_sessions = fetch_sessions_enabled::yes});
        }
    }
};

} // namespace

TEST_P(basic_consume_fixture, TestBasicConsumption) {
    assign_partitions(make_assignment(topic, {0, 1, 2}));

    // no data should be available immediately, as the topic is empty
    for (int i = 0; i < 10; ++i) {
        auto fetched = consumer->fetch_next(100ms).get();
        ASSERT_TRUE(fetched.value().empty());
    }

    // produce some data
    produce_to_partition(topic, 0, 1000);
    produce_to_partition(topic, 1, 400);
    produce_to_partition(topic, 2, 20);

    auto fetched = fetch_until_empty(*consumer);

    ASSERT_EQ(fetched.size(), 3);
    ASSERT_EQ(
      fetched[model::topic_partition(topic, model::partition_id(0))]
        .back()
        .last_offset(),
      model::offset(999));
    ASSERT_EQ(
      fetched[model::topic_partition(topic, model::partition_id(1))]
        .back()
        .last_offset(),
      model::offset(399));
    ASSERT_EQ(
      fetched[model::topic_partition(topic, model::partition_id(2))]
        .back()
        .last_offset(),
      model::offset(19));

    // produce again
    produce_to_partition(topic, 2, 1000);
    produce_to_partition(topic, 1, 400);
    produce_to_partition(topic, 0, 20);
    auto fetched_2 = fetch_until_empty(*consumer);
    ASSERT_EQ(fetched_2.size(), 3);
    ASSERT_EQ(
      fetched_2[model::topic_partition(topic, model::partition_id(0))]
        .back()
        .last_offset(),
      model::offset(1019));
    ASSERT_EQ(
      fetched_2[model::topic_partition(topic, model::partition_id(1))]
        .back()
        .last_offset(),
      model::offset(799));
    ASSERT_EQ(
      fetched_2[model::topic_partition(topic, model::partition_id(2))]
        .back()
        .last_offset(),
      model::offset(1019));
}

TEST_P(basic_consume_fixture, TestUnassignPartition) {
    assign_partitions(make_assignment(topic, {0, 1}));

    constexpr size_t n = 100;

    // produce some data
    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 2);
        ASSERT_EQ(
          fetched.find(model::topic_partition{topic, model::partition_id{2}}),
          fetched.end());
        for (auto id : std::array{0, 1}) {
            ASSERT_EQ(
              fetched[model::topic_partition(topic, model::partition_id(id))]
                .back()
                .last_offset(),
              model::offset(n - 1));
        }
    }

    unassign_partition(model::topic_partition{topic, model::partition_id{0}});

    // enable fetch sessions if the test is in toggle mode
    maybe_toggle_fetch_sessions();

    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);

        ASSERT_EQ(fetched.size(), 1);
        for (auto p : std::array{0, 2}) {
            ASSERT_EQ(
              fetched.find(
                model::topic_partition{topic, model::partition_id{p}}),
              fetched.end());
        }
        ASSERT_EQ(
          fetched[model::topic_partition(topic, model::partition_id(1))]
            .back()
            .last_offset(),
          model::offset(n * 2 - 1));
    }

    unassign_partition(model::topic_partition{topic, model::partition_id{1}});
    assign_partitions(make_assignment(topic, {0}));

    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);

        ASSERT_EQ(fetched.size(), 1);
        for (auto p : std::array{1, 2}) {
            ASSERT_EQ(
              fetched.find(
                model::topic_partition{topic, model::partition_id{p}}),
              fetched.end());
        }
        ASSERT_EQ(
          fetched[model::topic_partition(topic, model::partition_id(0))]
            .back()
            .last_offset(),
          model::offset(n * 3 - 1));
    }
}

TEST_P(basic_consume_fixture, TestUnassignTopic) {
    assign_partitions(make_assignment(topic, {0, 1, 2}));

    constexpr size_t n = 100;

    // produce some data
    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 3);
        for (auto id : std::array{0, 1, 2}) {
            ASSERT_EQ(
              fetched[model::topic_partition(topic, model::partition_id(id))]
                .back()
                .last_offset(),
              model::offset(n - 1));
        }
    }

    unassign_topic(topic);

    maybe_toggle_fetch_sessions();

    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 0);
    }
}

TEST_P(basic_consume_fixture, TestBogusPartitionIds) {
    // test that providing non-existent or ill formed partition IDs to the
    // consumer doesn't cause issues. we wouldn't expect this to happen in
    // practice.
    assign_partitions(make_assignment(topic, {0, 2, 5, 23, -1}));

    constexpr int n = 100;

    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 2);
        for (auto id : std::array{0, 2}) {
            ASSERT_EQ(
              fetched[model::topic_partition(topic, model::partition_id(id))]
                .back()
                .last_offset(),
              model::offset(n - 1));
        }
    }

    maybe_toggle_fetch_sessions();

    unassign_partition(model::topic_partition{topic, model::partition_id{5}});
    unassign_partition(model::topic_partition{topic, model::partition_id{42}});
    unassign_topic(model::topic{"noexist"});

    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 2);
        for (auto id : std::array{0, 2}) {
            ASSERT_EQ(
              fetched[model::topic_partition(topic, model::partition_id(id))]
                .back()
                .last_offset(),
              model::offset(2 * n - 1));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  test_with_basic_consume_fixture,
  basic_consume_fixture,
  testing::Values(
    session_config::with_sessions,
    session_config::without_sessions,
    session_config::toggle_sessions));

namespace {

constexpr model::offset forget_partition_placeholder = model::offset{-1};
constexpr int full_fetch_sid = kafka::fetch_session_id{0};

struct fetch_capture {
    using requests
      = std::unordered_map<model::topic_partition, std::vector<model::offset>>;
    std::map<kafka::fetch_session_id, requests> sessions;
    requests fetch_offsets;
    int total_requests{0};
    int empty_requests{0};
    fmt::iterator format_to(fmt::iterator it) const {
        fmt::format_to(it, "\ttotal requests: {}\n", total_requests);
        fmt::format_to(it, "\tempty requests: {}\n", empty_requests);
        for (const auto& [sid, reqs] : sessions) {
            if (sid == full_fetch_sid) {
                fmt::format_to(it, "\tfull fetch: \n");
            } else {
                fmt::format_to(it, "\tsession {}: \n", sid);
            }
            for (const auto& [tp, fos] : reqs) {
                fmt::format_to(
                  it,
                  "\t\t{} ({}): [{}]\n",
                  tp,
                  fos.size(),
                  fmt::join(
                    fos | std::views::transform([](const model::offset o) {
                        if (o == forget_partition_placeholder) {
                            return std::string{"F"};
                        } else {
                            return fmt::format("{}", o);
                        }
                    }),
                    ", "));
            }
        }

        return it;
    }
};

struct cluster_capture {
    std::unordered_map<model::node_id, fetch_capture> captured;
    fmt::iterator format_to(fmt::iterator it) const {
        it = fmt::format_to(it, "\n");
        for (const auto& [nid, fc] : captured) {
            // we don't care about the seed broker
            if (nid == unknown_node_id) {
                continue;
            }
            it = fmt::format_to(it, "node {}: {{\n{}}}\n", nid, fc);
        }
        return it;
    }
};

} // namespace

class request_capturing_remote_broker : public broker {
public:
    request_capturing_remote_broker(shared_broker_t remote, fetch_capture& cap)
      : _fetch_capture(&cap)
      , _broker(std::move(remote)) {}

    model::node_id id() const final { return _broker->id(); }

    ss::future<> stop() final { return _broker->stop(); }

    ss::future<std::optional<api_version_range>> get_supported_versions(
      kafka::api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt) final {
        return _broker->get_supported_versions(key, as);
    }

    const net::unresolved_address& get_address() const final {
        return _broker->get_address();
    }

    ss::future<response_t> dispatch(
      request_t req,
      kafka::api_version version,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt) final {
        ss::visit(
          req,
          [](const auto&) {},
          [this](const kafka::fetch_request& f) {
              auto partitions_v
                = f.data.topics
                  | std::views::transform([](const kafka::fetch_topic& t) {
                        return t.partitions
                               | std::views::transform(
                                 [](const kafka::fetch_partition& p) {
                                     return fmt::format(
                                       "{{{}, {}}}",
                                       p.partition,
                                       p.fetch_offset);
                                 });
                    })
                  | std::views::join;

              vlog(
                logger.debug,
                "[broker: {}] FETCH (session id: {} epoch: {}): [{}]",
                id(),
                f.data.session_id,
                f.data.session_epoch,
                fmt::join(partitions_v, ", "));

              // ignore full fetch requests that initialize a session. the
              // contents are less predictable and we only really care about
              // semantics w/in and across open sessions
              if (
                f.is_full_fetch_request()
                && f.data.session_epoch == kafka::initial_fetch_session_epoch) {
                  return;
              }

              _fetch_capture->total_requests++;
              if (
                f.data.topics.empty() && f.data.forgotten_topics_data.empty()) {
                  _fetch_capture->empty_requests++;
              }

              kafka::fetch_session_id session_id{f.data.session_id};

              for (const auto& t : f.data.topics) {
                  for (const auto& p : t.partitions) {
                      model::topic_partition tp{t.topic, p.partition};
                      _fetch_capture->sessions[session_id][tp].push_back(
                        p.fetch_offset);
                      _fetch_capture->fetch_offsets[tp].push_back(
                        p.fetch_offset);
                  }
              }
              for (const auto& t : f.data.forgotten_topics_data) {
                  for (const auto& p : t.partitions) {
                      model::topic_partition tp{
                        t.topic, model::partition_id{p}};
                      _fetch_capture->sessions[session_id][tp].push_back(
                        forget_partition_placeholder);
                      _fetch_capture->fetch_offsets[tp].push_back(
                        forget_partition_placeholder);
                  }
              }
          });
        return _broker->dispatch(std::move(req), version, as);
    }

private:
    fetch_capture* _fetch_capture;
    shared_broker_t _broker;
};

/**
 * Simple class used to create broker objects. Created broker objects use
 * configuration provided when creating the factory.
 */
struct request_capturing_broker_factory : public broker_factory {
    explicit request_capturing_broker_factory(
      connection_configuration config, cluster_capture& cap)
      : _capture(&cap)
      , _config(std::move(config))
      , _logger(logger, _config.client_id.value_or("kafka-client"))
      , _factory(std::make_unique<remote_broker_factory>(_config, _logger)) {}

    ss::future<shared_broker_t>
    create_broker(model::node_id id, net::unresolved_address addr) final {
        auto remote = co_await _factory->create_broker(id, addr);
        auto& cap = _capture->captured[id];
        co_return ss::make_shared<request_capturing_remote_broker>(
          std::move(remote), cap);
    }

private:
    cluster_capture* _capture;
    connection_configuration _config;
    prefix_logger _logger;
    std::unique_ptr<remote_broker_factory> _factory;
};

class fetch_session_fixture
  : public consumer_fixture
  , public testing::Test {
public:
    void wait_for_leadership() {
        for (auto i : std::views::iota(0, n_partitions)) {
            get_partition_leader(model::ntp{
              model::kafka_namespace, topic, model::partition_id{i}});
        }
    }

    void SetUp() override {
        create_node_application(model::node_id{0});
        create_node_application(model::node_id{1});
        create_node_application(model::node_id{2});
        auto* rp = instance(model::node_id{0});
        wait_for_all_members(3s).get();
        rp->add_topic(
            {model::kafka_namespace, topic}, n_partitions, std::nullopt, 3)
          .get();

        // the pattern of fetch requests is more predictable if we wait for
        // leadership before firing up the consumer
        wait_for_leadership();

        cluster = create_client_cluster(
                    std::make_unique<request_capturing_broker_factory>(
                      make_connection_config(), _capture))
                    .get();
        consumer = std::make_unique<kafka::client::direct_consumer>(
          *cluster,
          direct_consumer::configuration{
            .with_sessions = fetch_sessions_enabled::yes});
        consumer->start().get();
    }

    void TearDown() override {
        consumer->stop().get();
        cluster->stop().get();
    }

    void validate_sessions(std::function<void(
                             const model::topic_partition&,
                             const std::vector<model::offset>&)> validator) {
        for (const auto& [node, cap] : _capture.captured) {
            for (const auto& [sid, reqs] : cap.sessions) {
                ASSERT_NE(sid, kafka::invalid_fetch_session_id);
                for (const auto& [tp, fos] : reqs) {
                    validator(tp, fos);
                }
            }
        }
    }

    cluster_capture _capture{};
    constexpr static int n_partitions{10};
};

TEST_F(fetch_session_fixture, TestFetchRequestContents) {
    // This test
    //   - Assigns some partitions the consumer
    //   - Produce to all partitions and fetch until empty
    //   - Check captured fetch requests for each assigned partition with the
    //     expectation that it's included on when the fetch offset changes.
    //   - Produce to a subset of assigned partition and perform a similar
    //     check. Only those with new data should appear in subsequent fetches.

    auto all_partitions = std::views::iota(0, n_partitions);
    constexpr int p_assign = 7;
    auto initial_assignment = std::views::iota(0, p_assign);
    constexpr int p_second_produce = 4;
    auto second_produce = std::views::iota(0, p_second_produce);

    assign_partitions(make_assignment(
      topic, initial_assignment | std::ranges::to<std::vector<int>>()));

    for (int i = 0; i < 10; ++i) {
        auto fetched = consumer->fetch_next(100ms).get();
        ASSERT_TRUE(fetched.value().empty());
    }

    constexpr int64_t n = 100;

    for (auto i : all_partitions) {
        produce_to_partition(topic, i, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), initial_assignment.size());
    }

    std::unordered_map<model::partition_id, int> sessions_per_partition;

    validate_sessions([&](
                        const model::topic_partition& tp,
                        const std::vector<model::offset>& offsets) {
        sessions_per_partition[tp.partition]++;
        ASSERT_LT(tp.partition(), p_assign)
          << fmt::format("Unexpected request for {}", tp);
        ASSERT_EQ(
          offsets,
          std::vector<model::offset>({model::offset{0}, model::offset{n}}))
          << fmt::format("Unexpected offsets for {}: {}", tp, _capture);
    });

    for (auto i : all_partitions) {
        auto it = sessions_per_partition.find(model::partition_id{i});
        if (i < p_assign) {
            ASSERT_NE(it, sessions_per_partition.end());
            ASSERT_EQ(it->second, 1);
        } else {
            ASSERT_EQ(it, sessions_per_partition.end());
        }
    }

    for (auto i : second_produce) {
        produce_to_partition(topic, i, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), second_produce.size());
    }

    validate_sessions([&](
                        const model::topic_partition& tp,
                        const std::vector<model::offset>& offsets) {
        ASSERT_LT(tp.partition(), p_assign)
          << fmt::format("Unexpected request for {}", tp);

        if (tp.partition() < p_second_produce) {
            ASSERT_EQ(
              offsets,
              std::vector<model::offset>(
                {model::offset{0}, model::offset{n}, model::offset{n * 2}}))
              << fmt::format("Unexpected offsets for {}: {}", tp, _capture);
        } else {
            ASSERT_EQ(
              offsets,
              std::vector<model::offset>({model::offset{0}, model::offset{n}}))
              << fmt::format("Unexpected offsets for {}: {}", tp, _capture);
        }
    });

    vlog(logger.debug, "CAPTURE: {}", _capture);
}

TEST_F(fetch_session_fixture, TestFetchRequestUnassignContents) {
    // similar to the previous test, but this time forget partitions some
    // partitions and verify that this is reflected in the subsequent
    // incremental fetch request
    auto all_partitions = std::views::iota(0, n_partitions);
    constexpr int p_assign = 7;
    auto initial_assignment = std::views::iota(0, p_assign);
    constexpr int p_first_unassign = 4;
    auto to_unassign = std::views::iota(p_first_unassign, p_assign);

    wait_for_leadership();

    assign_partitions(make_assignment(
      topic, initial_assignment | std::ranges::to<std::vector<int>>()));

    for (int i = 0; i < 10; ++i) {
        auto fetched = consumer->fetch_next(100ms).get();
        ASSERT_TRUE(fetched.value().empty());
    }

    constexpr int64_t n = 100;

    for (auto i : all_partitions) {
        produce_to_partition(topic, i, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), initial_assignment.size());
    }

    validate_sessions([&](
                        const model::topic_partition& tp,
                        const std::vector<model::offset>& offsets) {
        ASSERT_LT(tp.partition(), p_assign)
          << fmt::format("Unexpected request for {}", tp);
        ASSERT_EQ(
          offsets,
          std::vector<model::offset>({model::offset{0}, model::offset{n}}))
          << fmt::format("Unexpected offsets for {}: {}", tp, _capture);
    });

    for (auto i : to_unassign) {
        unassign_partition(
          model::topic_partition{topic, model::partition_id{i}});
    }

    for (auto i : all_partitions) {
        produce_to_partition(topic, i, n);
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), p_first_unassign);
    }

    validate_sessions([&](
                        const model::topic_partition& tp,
                        const std::vector<model::offset>& offsets) {
        ASSERT_LT(tp.partition(), p_assign)
          << fmt::format("Unexpected request for {}", tp);
        if (tp.partition() < p_first_unassign) {
            ASSERT_EQ(
              offsets,
              std::vector<model::offset>(
                {model::offset{0}, model::offset{n}, model::offset{n * 2}}))
              << fmt::format("Unexpected offsets for {}: {}", tp, _capture);
        } else {
            ASSERT_EQ(
              offsets,
              std::vector<model::offset>(
                {model::offset{0},
                 model::offset{n},
                 forget_partition_placeholder}))
              << fmt::format("Unexpected offsets for {}: {}", tp, _capture);
        }
    });

    vlog(logger.debug, "CAPTURE: {}", _capture);
}

// TODO: inject errors?
