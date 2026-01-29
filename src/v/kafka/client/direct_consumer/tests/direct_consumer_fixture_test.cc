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

#include "kafka/client/direct_consumer/tests/direct_consumer_fixture.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"

#include <seastar/util/defer.hh>

#include <fmt/format.h>

#include <unordered_map>

using namespace kafka::client;
using ConsumerFixture = kafka::client::tests::consumer_fixture;
using BasicConsumerFixture = kafka::client::tests::basic_consumer_fixture;

namespace {
ss::logger logger{"direct-consumer-test"};
}

TEST_P(BasicConsumerFixture, TestBasicConsumption) {
    assign_partitions(make_assignment(topic, {0, 1, 2}));
    { // assert on intial updates, we should see each partition at most once,
      // but maybe not at all
        std::unordered_map<model::topic_partition, int> times_seen;
        for (int i = 0; i < 10; ++i) {
            auto fetched = consumer->fetch_next(100ms).get();
            ASSERT_TRUE(fetched.has_value())
              << "fetch should not have an error";

            for (auto& topic_data : fetched.value()) {
                for (auto& partition_data : topic_data.partitions) {
                    ASSERT_EQ(partition_data.size_bytes, 0);
                    ASSERT_EQ(partition_data.data.size(), 0);
                    ASSERT_EQ(partition_data.high_watermark, kafka::offset{0});
                    ASSERT_EQ(
                      partition_data.last_stable_offset, kafka::offset{0});
                    ASSERT_EQ(partition_data.start_offset, kafka::offset{0});
                    int& seen_counter = times_seen[model::topic_partition{
                      topic_data.topic, partition_data.partition_id}];
                    ++seen_counter;
                }
            }
        }
        ASSERT_TRUE(times_seen.size() <= 3);
        for (auto [tp, counter] : times_seen) {
            ASSERT_EQ(counter, 1);
        }
    }
    // produce some data
    produce_to_partition(topic, 0, 1000).get();
    produce_to_partition(topic, 1, 400).get();
    produce_to_partition(topic, 2, 20).get();

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
    produce_to_partition(topic, 2, 1000).get();
    produce_to_partition(topic, 1, 400).get();
    produce_to_partition(topic, 0, 20).get();
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
TEST_P(BasicConsumerFixture, TestBasicLeadershipTransfer) {
    // constants
    constexpr uint first_produce_count = 10;
    constexpr uint second_produce_count = 20;
    const auto test_partition_number = 0;
    const auto test_partition_id = model::partition_id(test_partition_number);
    const auto test_ntp = model::ntp(
      model::kafka_namespace, topic, test_partition_id);

    assign_partitions(make_assignment(topic, {test_partition_number}));

    produce_to_partition(topic, test_partition_number, first_produce_count)
      .get();

    { // fist fetch and assert
        auto fetched = fetch_until_empty(*consumer);

        ASSERT_EQ(
          fetched[model::topic_partition(topic, test_partition_id)]
            .back()
            .last_offset(),
          model::offset(first_produce_count - 1));
    }

    // kick off a leadership shuffle and wait for the effect to be noticable
    wait_for_visible_leadership_shuffle(test_ntp);

    produce_to_partition(topic, test_partition_number, second_produce_count)
      .get();

    { // second fetch and assert
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 1);

        ASSERT_EQ(
          fetched[model::topic_partition(topic, test_partition_id)]
            .back()
            .last_offset(),
          model::offset(first_produce_count + second_produce_count - 1));
    }
}

TEST_P(BasicConsumerFixture, TestBasicNodeRestart) {
    // constants
    constexpr uint first_produce_count = 10;
    constexpr uint second_produce_count = 20;
    const auto test_partition_number = 0;
    const auto test_partition_id = model::partition_id(test_partition_number);
    const auto test_ntp = model::ntp(
      model::kafka_namespace, topic, test_partition_id);

    assign_partitions(make_assignment(topic, {test_partition_number}));

    produce_to_partition(topic, test_partition_number, first_produce_count)
      .get();

    { // fist fetch and assert
        auto fetched = fetch_until_empty(*consumer);

        ASSERT_EQ(
          fetched[model::topic_partition(topic, test_partition_id)]
            .back()
            .last_offset(),
          model::offset(first_produce_count - 1));
    }

    auto leader = this->get_partition_leader(test_ntp);

    // restart the leader for the partition
    this->remove_node_application(leader);
    this->create_node_application(leader);

    auto current_leader = this->get_partition_leader(test_ntp);

    // the goal of this test is to check that consumption works on restart, make
    // sure leadership retargets the original leader
    this->assign_leader(test_ntp, current_leader, leader).get();

    // attempt to produce, with retry
    produce_to_partition(
      topic,
      test_partition_number,
      second_produce_count,
      std::chrono::seconds(10),
      std::chrono::seconds(1))
      .get();

    { // second fetch and assert
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 1);

        ASSERT_EQ(
          fetched[model::topic_partition(topic, test_partition_id)]
            .back()
            .last_offset(),
          model::offset(first_produce_count + second_produce_count - 1));
    }
}

TEST_P(BasicConsumerFixture, TestUnassignPartition) {
    assign_partitions(make_assignment(topic, {0, 1}));

    constexpr size_t n = 100;

    // produce some data
    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n).get();
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
        produce_to_partition(topic, p, n).get();
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
        produce_to_partition(topic, p, n).get();
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

TEST_P(BasicConsumerFixture, TestUnassignTopic) {
    assign_partitions(make_assignment(topic, {0, 1, 2}));

    constexpr size_t n = 100;

    // produce some data
    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n).get();
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
        produce_to_partition(topic, p, n).get();
    }

    {
        auto fetched = fetch_until_empty(*consumer);
        ASSERT_EQ(fetched.size(), 0);
    }
}

TEST_P(BasicConsumerFixture, TestBogusPartitionIds) {
    // test that providing non-existent or ill formed partition IDs to the
    // consumer doesn't cause issues. we wouldn't expect this to happen in
    // practice.
    assign_partitions(make_assignment(topic, {0, 2, 5, 23, -1}));

    constexpr int n = 100;

    for (auto p : std::array{0, 1, 2}) {
        produce_to_partition(topic, p, n).get();
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
        produce_to_partition(topic, p, n).get();
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

using session_config = kafka::client::tests::session_config;
INSTANTIATE_TEST_SUITE_P(
  test_with_basic_consume_fixture,
  BasicConsumerFixture,
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

class FetchSessionFixture
  : public ConsumerFixture
  , public ::testing::Test {
public:
    void wait_for_leadership() {
        for (auto i : std::views::iota(0, n_partitions)) {
            get_partition_leader(
              model::ntp{
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

    void validate_sessions(
      std::function<
        void(const model::topic_partition&, const std::vector<model::offset>&)>
        validator) {
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

TEST_F(FetchSessionFixture, TestFetchRequestContents) {
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

    constexpr int64_t n = 100;

    for (auto i : all_partitions) {
        produce_to_partition(topic, i, n).get();
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
        produce_to_partition(topic, i, n).get();
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

TEST_F(FetchSessionFixture, TestFetchRequestUnassignContents) {
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

    constexpr int64_t n = 100;

    for (auto i : all_partitions) {
        produce_to_partition(topic, i, n).get();
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
        produce_to_partition(topic, i, n).get();
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
