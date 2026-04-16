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
#include "kafka/client/cluster.h"
#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "kafka/client/direct_consumer/fetcher.h"
#include "kafka/client/test/cluster_mock.h"
#include "kafka/client/types.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>
using namespace kafka::client;
using namespace std::chrono_literals;
static ss::logger test_log("test-logger");
struct consumer_test_mock : public ::testing::Test {
    kafka::client::cluster create_client_cluster() {
        return kafka::client::cluster(
          kafka::client::connection_configuration{
            .initial_brokers = {net::unresolved_address{"localhost", 9092}},
            .client_id = "test-client",
            .max_metadata_age = 1s,
          },
          std::make_unique<kafka::client::broker_mock_factory>(&cluster_mock));
    }

    void setup_brokers() {
        cluster_mock.add_broker(
          model::node_id(0), net::unresolved_address{"localhost", 9092});
        cluster_mock.add_broker(
          model::node_id(1), net::unresolved_address{"localhost", 9093});
        cluster_mock.add_broker(
          model::node_id(2), net::unresolved_address{"localhost", 9094});
    }

    void prepare_cluster() {
        cluster_mock.register_default_handlers();
        setup_brokers();
        cluster_mock.register_handler(
          kafka::fetch_api::key,
          [this](model::node_id id, request_t req, kafka::api_version) {
              return handle_fetch(
                       id, std::get<kafka::fetch_request>(std::move(req)))
                .then([](kafka::fetch_response resp) {
                    return response_t{std::move(resp)};
                });
          });
        cluster_mock.register_handler(
          kafka::list_offsets_api::key,
          [this](model::node_id id, request_t req, kafka::api_version) {
              return handle_list_offsets(
                       id,
                       std::get<kafka::list_offsets_request>(std::move(req)))
                .then([](kafka::list_offsets_response resp) {
                    return response_t{std::move(resp)};
                });
          });
    }

    ss::future<kafka::list_offsets_response>
    handle_list_offsets(model::node_id id, kafka::list_offsets_request req) {
        kafka::list_offset_response_data resp_data;
        for (auto& topic : req.data.topics) {
            kafka::list_offset_topic_response topic_data;
            topic_data.name = topic.name;
            topic_data.partitions.reserve(topic.partitions.size());

            auto t_it = cluster_mock.get_topics().find(topic.name);
            for (auto& partition : topic.partitions) {
                kafka::list_offset_partition_response part_data;
                part_data.partition_index = partition.partition_index;
                if (t_it == cluster_mock.get_topics().end()) {
                    part_data.error_code
                      = kafka::error_code::unknown_topic_or_partition;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }
                auto p_it = t_it->second.partitions.find(
                  partition.partition_index);
                if (p_it == t_it->second.partitions.end()) {
                    part_data.error_code
                      = kafka::error_code::unknown_topic_or_partition;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }

                if (p_it->second.leader != id) {
                    part_data.error_code
                      = kafka::error_code::not_leader_for_partition;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }
                auto& state = p_state[topic.name][partition.partition_index];
                if (state.error_code != kafka::error_code::none) {
                    part_data.error_code = state.error_code;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }

                if (
                  partition.timestamp
                  == kafka::list_offsets_request::earliest_timestamp) {
                    part_data.offset
                      = p_state[topic.name][partition.partition_index]
                          .start_offset;
                } else if (
                  partition.timestamp
                  == kafka::list_offsets_request::latest_timestamp) {
                    part_data.offset
                      = p_state[topic.name][partition.partition_index]
                          .high_watermark;
                }
                topic_data.partitions.push_back(std::move(part_data));
            }
            resp_data.topics.push_back(std::move(topic_data));
        }

        co_return kafka::list_offsets_response{
          .data = std::move(resp_data),
        };
    }

    ss::future<kafka::fetch_response>
    handle_fetch(model::node_id id, kafka::fetch_request req) {
        kafka::fetch_response_data resp_data;
        resp_data.responses.reserve(req.data.topics.size());
        if (fetch_error.has_value()) {
            resp_data.error_code = fetch_error.value();
            fetch_error.reset();
            co_return kafka::fetch_response{.data = std::move(resp_data)};
        }

        for (auto& topic : req.data.topics) {
            kafka::fetchable_topic_response topic_data;
            topic_data.topic = topic.topic;
            topic_data.partitions.reserve(topic.partitions.size());

            for (auto& partition : topic.partitions) {
                auto t_it = cluster_mock.get_topics().find(topic.topic);
                kafka::partition_data part_data;
                part_data.partition_index = partition.partition;
                if (t_it == cluster_mock.get_topics().end()) {
                    part_data.error_code
                      = kafka::error_code::unknown_topic_or_partition;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }
                auto p_it = t_it->second.partitions.find(partition.partition);
                if (p_it == t_it->second.partitions.end()) {
                    part_data.error_code
                      = kafka::error_code::unknown_topic_or_partition;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }

                if (p_it->second.leader != id) {
                    part_data.error_code
                      = kafka::error_code::not_leader_for_partition;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }

                auto& state = p_state[topic.topic][partition.partition];
                vlog(
                  test_log.info,
                  "Handling fetch for broker: {}, topic: {}/{}, fetch_offset: "
                  "{}, start_offset: {}, high watermark: {}",
                  id,
                  topic.topic,
                  partition.partition,
                  partition.fetch_offset,
                  state.start_offset,
                  state.high_watermark);
                // handle error
                if (state.error_code != kafka::error_code::none) {
                    part_data.error_code = state.error_code;
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }
                if (partition.fetch_offset > state.high_watermark) {
                    part_data.error_code
                      = kafka::error_code::offset_out_of_range;
                    part_data.high_watermark = model::offset(-1);
                    topic_data.partitions.push_back(std::move(part_data));
                    continue;
                }
                auto batches_to_fetch = std::min<size_t>(
                  10, state.high_watermark - partition.fetch_offset);

                part_data.records = co_await generate_batches(
                  partition.fetch_offset, batches_to_fetch);
                auto& state_after = p_state[topic.topic][partition.partition];
                part_data.high_watermark = state_after.high_watermark;
                part_data.error_code = kafka::error_code::none;
                part_data.current_leader.leader_id = id;
                part_data.current_leader.leader_epoch = kafka::leader_epoch(0);

                topic_data.partitions.push_back(std::move(part_data));
            }
            resp_data.responses.push_back(std::move(topic_data));
        }

        co_return kafka::fetch_response{.data = std::move(resp_data)};
    }

    ss::future<kafka::batch_reader>
    generate_batches(model::offset first_offset, size_t batch_count) {
        iobuf record_set;
        auto writer{kafka::protocol::encoder(record_set)};
        auto batches = co_await model::test::make_random_batches(
          first_offset, batch_count, true, std::nullopt, 1);
        for (auto& batch : batches) {
            kafka::protocol::writer_serialize_batch(writer, std::move(batch));
        }

        co_return kafka::batch_reader{std::move(record_set)};
    }
    void set_partition_start_offset(
      model::topic topic, int partition, model::offset start_offset) {
        p_state[topic][model::partition_id(partition)].start_offset
          = start_offset;
    }

    void
    make_data_available(model::topic topic, int partition, size_t batch_cnt) {
        model::partition_id p_id(partition);
        auto& state = p_state[topic][p_id];
        auto prev = state.high_watermark == model::offset{}
                      ? state.start_offset
                      : state.high_watermark;

        state.high_watermark = prev + model::offset(batch_cnt);
    }

    ss::future<> fetch_and_append_to_map(
      topic_partition_map<chunked_vector<model::record_batch>>& batches,
      direct_consumer& consumer) {
        auto data = co_await consumer.fetch_next(std::chrono::seconds(4));
        for (auto& topic_data : data.value()) {
            for (auto& partition_data : topic_data.partitions) {
                auto& b_vector
                  = batches[topic_data.topic][partition_data.partition_id];
                vlog(
                  test_log.info,
                  "Fetched {} batches for topic: {}, partition: {}, "
                  "total_batches: {}",
                  partition_data.data.size(),
                  topic_data.topic,
                  partition_data.partition_id,
                  b_vector.size());
                std::ranges::move(
                  partition_data.data, std::back_inserter(b_vector));
            }
        }
    }

    ::testing::AssertionResult validate_offsets_monotonicity(
      const topic_partition_map<chunked_vector<model::record_batch>>& batches) {
        for (auto& [topic, partitions] : batches) {
            for (auto& [partition_id, batch_vector] : partitions) {
                if (batch_vector.empty()) {
                    continue;
                }
                model::offset last_end_offset;

                for (const auto& b : batch_vector) {
                    if (b.base_offset() <= last_end_offset) {
                        return ::testing::AssertionFailure()
                               << "Batch base offset is not greater than "
                                  "previous batch's last offset: "
                               << b.base_offset() << " <= " << last_end_offset;
                    }
                }
            }
        }
        return ::testing::AssertionSuccess();
    }

    chunked_vector<topic_assignment> make_assignment(
      const model::topic& topic, const std::vector<int>& partitions) {
        chunked_vector<topic_assignment> assignment;
        assignment.push_back({
          .topic = topic,
        });
        for (const auto& partition : partitions) {
            assignment.back().partitions.push_back(
              partition_assignment{
                .partition_id = model::partition_id(partition),
                .next_offset = kafka::offset(0),
              });
        }
        return assignment;
    }

    void
    set_error(const model::topic& topic, int partition, kafka::error_code ec) {
        auto& state = p_state[topic][model::partition_id(partition)];
        state.error_code = ec;
    }

    struct partition_state {
        model::offset start_offset{0};
        model::offset high_watermark{};
        kafka::error_code error_code{kafka::error_code::none};
    };
    topic_partition_map<partition_state> p_state;
    std::optional<kafka::error_code> fetch_error;

    cluster_mock cluster_mock;
};

TEST_F(consumer_test_mock, TestAssignUnassignPartitions) {
    prepare_cluster();
    model::topic test_topic("panda-test");
    cluster_mock.add_topic(test_topic, 16, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;
    // 10 batches available in partition 0, 5 in partition 2
    make_data_available(test_topic, 0, 10);
    make_data_available(test_topic, 2, 5);

    auto client_cluster = create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });

    consumer.assign_partitions(make_assignment(test_topic, {0})).get();

    fetch_and_append_to_map(all_batches, consumer).get();
    auto data = consumer.fetch_next(std::chrono::seconds(4)).get();
    ASSERT_EQ(all_batches.size(), 1);
    ASSERT_TRUE(all_batches.contains(test_topic));
    ASSERT_EQ(all_batches[test_topic].size(), 1);
    ASSERT_EQ(all_batches[test_topic][model::partition_id(0)].size(), 10);

    // add some more data to partition 0
    make_data_available(test_topic, 0, 3);

    consumer.assign_partitions(make_assignment(test_topic, {1, 2})).get();

    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return fetch_and_append_to_map(all_batches, consumer).then([&] {
            return all_batches[test_topic][model::partition_id(0)].size() == 13
                   && all_batches[test_topic][model::partition_id(2)].size()
                        == 5;
        });
    });
    // finally add some data to partition 1
    make_data_available(test_topic, 1, 17);
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return fetch_and_append_to_map(all_batches, consumer).then([&] {
            return all_batches[test_topic][model::partition_id(0)].size() == 13
                   && all_batches[test_topic][model::partition_id(1)].size()
                        == 17
                   && all_batches[test_topic][model::partition_id(2)].size()
                        == 5;
        });
    });
    ASSERT_TRUE(validate_offsets_monotonicity(all_batches));
    // unassign partition 0
    consumer
      .unassign_partitions(
        {model::topic_partition(test_topic, model::partition_id(0))})
      .get();

    // make more data available in partition 0, then shouldn't be fetched
    // because it is unassigned
    make_data_available(test_topic, 0, 100);
    // wait for a bit to ensure that no data is fetched
    // request next fetch, to ensure that all in progress fetches are
    // completed
    consumer.fetch_next(std::chrono::milliseconds(100)).get();
    for (int i = 0; i < 10; ++i) {
        auto data = consumer.fetch_next(std::chrono::milliseconds(100)).get();
        ASSERT_TRUE(data.value().empty());
    }
    // assign another topic
    model::topic test_topic_2("panda-test-2");
    cluster_mock.add_topic(test_topic_2, 2, 3);

    consumer.assign_partitions(make_assignment(test_topic_2, {0, 1})).get();

    make_data_available(test_topic_2, 0, 100);
    make_data_available(test_topic_2, 1, 100);

    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return fetch_and_append_to_map(all_batches, consumer).then([&] {
            return all_batches[test_topic][model::partition_id(0)].size() == 13
                   && all_batches[test_topic][model::partition_id(1)].size()
                        == 17
                   && all_batches[test_topic][model::partition_id(2)].size()
                        == 5
                   && all_batches[test_topic_2][model::partition_id(0)].size()
                        == 100
                   && all_batches[test_topic_2][model::partition_id(1)].size()
                        == 100;
        });
    });
}

TEST_F(consumer_test_mock, TestLeadershipChange) {
    prepare_cluster();
    model::topic test_topic("panda-test");
    cluster_mock.add_topic(test_topic, 1, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;
    // 10 batches available in partition 0
    make_data_available(test_topic, 0, 10);

    auto client_cluster = create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });
    chunked_vector<topic_assignment> assignment;
    assignment.push_back({
      .topic = test_topic,
    });
    // only partition 0 is assigned
    assignment.back().partitions.push_back(
      partition_assignment{
        .partition_id = model::partition_id(0),
        .next_offset = kafka::offset(0),
      });
    consumer.assign_partitions(std::move(assignment)).get();

    fetch_and_append_to_map(all_batches, consumer).get();
    auto data = consumer.fetch_next(std::chrono::seconds(4)).get();
    ASSERT_EQ(all_batches.size(), 1);
    ASSERT_TRUE(all_batches.contains(test_topic));
    ASSERT_EQ(all_batches[test_topic].size(), 1);
    ASSERT_EQ(all_batches[test_topic][model::partition_id(0)].size(), 10);

    // change leadership of partition 0 to another broker
    vlog(
      test_log.info,
      "Changing leadership from {} to 2",
      cluster_mock.get_topics()[test_topic]
        .partitions[model::partition_id(0)]
        .leader);
    cluster_mock.get_topics()[test_topic]
      .partitions[model::partition_id(0)]
      .leader = model::node_id(2);
    // add some more data to partition 0
    make_data_available(test_topic, 0, 23);

    // wait for all data to be fetched
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return fetch_and_append_to_map(all_batches, consumer).then([&] {
            return all_batches[test_topic][model::partition_id(0)].size() == 33;
        });
    });
}

namespace {
// types and functions specific to subscription epoch filtering
namespace epoch_filtering_ns {

enum reassign_direction : uint8_t { backward, same, forward };

std::pair<kafka::offset, kafka::offset>
init_offsets(reassign_direction direction) {
    switch (direction) {
    case backward:
        return {kafka::offset{20}, kafka::offset{10}};
    case same:
        return {kafka::offset{10}, kafka::offset{10}};
    case forward:
        return {kafka::offset{10}, kafka::offset{20}};
    }
}

/**
 * Checks stale subscription filtering.
 * 1. replicate batches of 10, offset 0 -> 49
 * 2. assign ntp to direct_consumer with assignment_offset
 * 3. let dc fetch data
 * 4. unassign ntp
 * 5. reassign ntp with reassignment_offset
 * 6. drain all fetches, check we see only 50 - reassignment offset
 */
void epoch_filtering_test(
  consumer_test_mock* self, reassign_direction direction) {
    self->prepare_cluster();
    model::topic test_topic("panda-test");
    self->cluster_mock.add_topic(test_topic, 1, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;

    static constexpr auto batch_count{5};
    static constexpr auto batch_size{10};

    // 1. replicate [0,9], [10,19], [20,29]... [40,49]
    for (auto index{0}; index < batch_count; ++index) {
        std::ignore = index;
        self->make_data_available(test_topic, 0, batch_size);
    }

    auto client_cluster = self->create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });

    auto [assignment_offset, reassignment_offset] = init_offsets(direction);

    auto get_assignment = [test_topic](kafka::offset begin) {
        chunked_vector<topic_assignment> assignment;
        assignment.push_back({
          .topic = test_topic,
        });
        // only partition 0 is assigned
        assignment.back().partitions.push_back(
          partition_assignment{
            .partition_id = model::partition_id(0),
            .next_offset = kafka::offset(begin),
          });
        return assignment;
    };

    // 2. assign ntp at assignment_offset to direct_consumer
    consumer.assign_partitions(get_assignment(assignment_offset)).get();

    // 3. let dc fetch data
    // TODO we need a way to force iterations to occur
    ss::sleep(2s).get();

    // 4&5. unassign & reassign with offset reassignment_offset instead
    consumer.unassign_topics({test_topic}).get();
    consumer.assign_partitions(get_assignment(reassignment_offset)).get();

    const size_t expected_size = (batch_count * batch_size)
                                 - static_cast<int64_t>(reassignment_offset);
    const model::offset final_offset{batch_count * batch_size - 1};

    // 6. drain all fetches, check we see only 50 - reassignment_offset
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return self->fetch_and_append_to_map(all_batches, consumer).then([&] {
            auto& ntp_batches = all_batches[test_topic][model::partition_id(0)];
            return ntp_batches.size() == expected_size
                   && ntp_batches.back().last_offset() == final_offset;
        });
    });
}
} // namespace epoch_filtering_ns
} // namespace

TEST_F(consumer_test_mock, TestEpochFilteringBackward) {
    epoch_filtering_ns::epoch_filtering_test(
      this, epoch_filtering_ns::backward);
}

TEST_F(consumer_test_mock, TestEpochFilteringSame) {
    epoch_filtering_ns::epoch_filtering_test(this, epoch_filtering_ns::same);
}

TEST_F(consumer_test_mock, TestEpochFilteringForward) {
    epoch_filtering_ns::epoch_filtering_test(this, epoch_filtering_ns::forward);
}

TEST_F(consumer_test_mock, TestOffsetResetPolicy) {
    prepare_cluster();
    model::topic test_topic("panda-test");
    cluster_mock.add_topic(test_topic, 2, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;
    // start from offset 1000
    set_partition_start_offset(test_topic, 0, model::offset(1000));
    // 10 batches available in partition 0
    make_data_available(test_topic, 0, 10);

    auto client_cluster = create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });
    chunked_vector<topic_assignment> assignment;
    assignment.push_back({
      .topic = test_topic,
    });
    // only partition 0 is assigned
    assignment.back().partitions.push_back(
      partition_assignment{
        .partition_id = model::partition_id(0),
        .next_offset = std::nullopt, // no offset set, should reset to earliest
      });
    consumer.assign_partitions(std::move(assignment)).get();

    fetch_and_append_to_map(all_batches, consumer).get();

    ASSERT_EQ(all_batches.size(), 1);
    ASSERT_TRUE(all_batches.contains(test_topic));
    ASSERT_EQ(all_batches[test_topic].size(), 1);
    ASSERT_EQ(all_batches[test_topic][model::partition_id(0)].size(), 10);

    ASSERT_EQ(
      all_batches[test_topic][model::partition_id(0)].front().base_offset(),
      model::offset(1000));
    // change offset reset policy to latest
    // this should reset the fetch offset to the latest offset available
    // in the partition.

    // Set start offset to 400, so that the latest offset is 430
    set_partition_start_offset(test_topic, 1, model::offset(400));
    make_data_available(test_topic, 1, 30);
    consumer.update_configuration(
      direct_consumer::configuration{
        .reset_policy = offset_reset_policy::latest});
    chunked_vector<topic_assignment> new_assignment;
    new_assignment.push_back({
      .topic = test_topic,
    });
    // only partition 0 is assigned
    new_assignment.back().partitions.push_back(
      partition_assignment{
        .partition_id = model::partition_id(1),
        .next_offset = std::nullopt, // no offset set, should reset to latest
      });
    consumer.assign_partitions(std::move(new_assignment)).get();
    // fetch will update offsets, but offer no data
    auto data = consumer.fetch_next(std::chrono::seconds(1)).get();
    for (auto& topic_data : data.value()) {
        ASSERT_EQ(topic_data.total_bytes, 0)
          << "should not find any batches on empty initial fetch";
    }
    // add more data to partition 1
    make_data_available(test_topic, 1, 6);
    auto& p1_batches = all_batches[test_topic][model::partition_id(1)];
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return fetch_and_append_to_map(all_batches, consumer).then([&] {
            return !p1_batches.empty();
        });
    });

    ASSERT_EQ(p1_batches.front().base_offset(), model::offset(430));
}
TEST_F(consumer_test_mock, TestFetchErrorPropagation) {
    prepare_cluster();
    model::topic test_topic("panda-test");
    cluster_mock.add_topic(test_topic, 2, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;

    // 10 batches available in partition 0
    make_data_available(test_topic, 0, 10);
    // first try to use retriable fetch error, this one should be retried and
    // not propagated to the consumer
    fetch_error = kafka::error_code::listener_not_found;
    auto client_cluster = create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });
    chunked_vector<topic_assignment> assignment;
    assignment.push_back({
      .topic = test_topic,
    });
    // only partition 0 is assigned
    assignment.back().partitions.push_back(
      partition_assignment{
        .partition_id = model::partition_id(0),
        .next_offset = std::nullopt, // no offset set, should reset to earliest
      });
    consumer.assign_partitions(std::move(assignment)).get();

    fetch_and_append_to_map(all_batches, consumer).get();

    ASSERT_EQ(all_batches.size(), 1);
    ASSERT_TRUE(all_batches.contains(test_topic));
    ASSERT_EQ(all_batches[test_topic].size(), 1);
    ASSERT_EQ(all_batches[test_topic][model::partition_id(0)].size(), 10);

    fetch_error = kafka::error_code::invalid_request;

    // now try to use non-retriable fetch error, this one should be propagated
    // to the consumer
    auto data = consumer.fetch_next(std::chrono::seconds(1)).get();
    ASSERT_TRUE(data.has_error());
}

TEST_F(consumer_test_mock, TestPartitionErrorPropagation) {
    prepare_cluster();
    model::topic test_topic("panda-test");
    cluster_mock.add_topic(test_topic, 2, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;

    // 10 batches available in partition 0
    make_data_available(test_topic, 0, 10);
    auto client_cluster = create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });
    chunked_vector<topic_assignment> assignment;
    assignment.push_back({
      .topic = test_topic,
    });
    // only partition 0 is assigned
    assignment.back().partitions.push_back(
      partition_assignment{
        .partition_id = model::partition_id(0),
        .next_offset = std::nullopt, // no offset set, should reset to earliest
      });
    // set retriable error for partition 0
    set_error(test_topic, 0, kafka::error_code::not_leader_for_partition);
    consumer.assign_partitions(std::move(assignment)).get();
    // try fo fetch data, no data should be fetched, but the error should not be
    // propagated to the consumer
    auto data = consumer.fetch_next(std::chrono::seconds(1)).get();
    ASSERT_TRUE(data.has_value());
    ASSERT_TRUE(data.value().empty());
    // reset the error to none, so that the next fetch will return data
    set_error(test_topic, 0, kafka::error_code::none);
    data = consumer.fetch_next(std::chrono::seconds(1)).get();
    ASSERT_TRUE(data.has_value());
    ASSERT_FALSE(data.value().empty());
    // set non-retriable error for partition 0, it should be propagated in the
    // partition reply
    set_error(test_topic, 0, kafka::error_code::unknown_server_error);
    auto expected_err = consumer.fetch_next(std::chrono::seconds(1)).get();
    ASSERT_TRUE(expected_err.has_value());
    ASSERT_EQ(
      expected_err.value()[0].partitions[0].error,
      kafka::error_code::unknown_server_error);
}

namespace {
prefix_logger test_pfx_logger(test_log, "session-state");
}

TEST(fetch_session_state_test, test_disabled) {
    // Test that fetch_session_state doesn't advance at all when disabled
    using fss = fetch_session_state::state;

    fetch_session_state state{
      unknown_node_id, test_pfx_logger, fetch_sessions_enabled::no};
    ASSERT_EQ(state.session_state, fss::none);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);

    state.reset();
    ASSERT_EQ(state.session_state, fss::none);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);

    state.update_fetch_session(kafka::fetch_session_id{1});
    ASSERT_EQ(state.session_state, fss::none);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);
}

TEST(fetch_session_state_test, test_toggle) {
    // Tests binding to config property and the effect of toggling sessions.
    using fss = fetch_session_state::state;

    fetch_session_state state{
      unknown_node_id, test_pfx_logger, fetch_sessions_enabled::no};
    ASSERT_EQ(state.session_state, fss::none);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);

    state.toggle(fetch_sessions_enabled::yes);

    ASSERT_EQ(state.session_state, fss::need_full_fetch);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::initial_fetch_session_epoch);

    state.toggle(fetch_sessions_enabled::no);

    ASSERT_EQ(state.session_state, fss::none);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);

    state.toggle(fetch_sessions_enabled::yes);

    kafka::fetch_session_id id{1};

    state.update_fetch_session(id);

    state.toggle(fetch_sessions_enabled::no);

    ASSERT_EQ(state.session_state, fss::needs_close);
    ASSERT_EQ(state.session_id, id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);
}

TEST(fetch_session_state_test, test_state_transitions) {
    using fss = fetch_session_state::state;

    fetch_session_state state{
      unknown_node_id, test_pfx_logger, fetch_sessions_enabled::yes};
    ASSERT_EQ(state.session_state, fss::need_full_fetch);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::initial_fetch_session_epoch);

    kafka::fetch_session_id id{1};

    state.update_fetch_session(id);
    ASSERT_EQ(state.session_state, fss::incremental_fetch);
    ASSERT_EQ(state.session_id, id);
    ASSERT_EQ(state.session_epoch, 1);

    state.update_fetch_session(id);
    ASSERT_EQ(state.session_state, fss::incremental_fetch);
    ASSERT_EQ(state.session_id, id);
    ASSERT_EQ(state.session_epoch, 2);

    state.update_fetch_session(id);
    ASSERT_EQ(state.session_state, fss::incremental_fetch);
    ASSERT_EQ(state.session_id, id);
    ASSERT_EQ(state.session_epoch, 3);

    state.update_fetch_session(id + 1);
    ASSERT_EQ(state.session_state, fss::needs_close);
    ASSERT_EQ(state.session_id, id);
    ASSERT_EQ(state.session_epoch, kafka::final_fetch_session_epoch);

    state.update_fetch_session(kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_state, fss::need_full_fetch);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::initial_fetch_session_epoch);

    state.update_fetch_session(kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_state, fss::need_full_fetch);
    ASSERT_EQ(state.session_id, kafka::invalid_fetch_session_id);
    ASSERT_EQ(state.session_epoch, kafka::initial_fetch_session_epoch);
}

TEST_F(consumer_test_mock, TestOffsetOutOfRangeHandling) {
    prepare_cluster();
    model::topic test_topic("panda-test");
    cluster_mock.add_topic(test_topic, 2, 3);
    topic_partition_map<chunked_vector<model::record_batch>> all_batches;

    // 10 batches available in partition 0
    make_data_available(test_topic, 0, 10);
    auto client_cluster = create_client_cluster();
    client_cluster.start().get();
    direct_consumer consumer(client_cluster, direct_consumer::configuration{});
    consumer.start().get();
    auto deferred_stop = ss::defer([&] {
        consumer.stop().get();
        client_cluster.stop().get();
    });
    chunked_vector<topic_assignment> assignment;
    assignment.push_back({
      .topic = test_topic,
    });
    // only partition 0 is assigned
    assignment.back().partitions.push_back(
      partition_assignment{
        .partition_id = model::partition_id(0),
        .next_offset = kafka::offset(10000),
      });
    consumer.assign_partitions(std::move(assignment)).get();
    fetch_and_append_to_map(all_batches, consumer).get();

    ASSERT_EQ(all_batches.size(), 1);
    ASSERT_TRUE(all_batches.contains(test_topic));
    ASSERT_EQ(all_batches[test_topic].size(), 1);
    ASSERT_EQ(all_batches[test_topic][model::partition_id(0)].size(), 10);
}
