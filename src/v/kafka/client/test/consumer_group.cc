/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/container/flat_hash_map.h"
#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/offset_fetch_response.h"
#include "kafka/server/group.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "redpanda/tests/fixture.h"
#include "ssx/future-util.h"
#include "test_utils/boost_fixture.h"

#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <iterator>
#include <vector>

namespace {

chunked_vector<kafka::offset_fetch_request_topic>
offset_request_from_assignment(kc::assignment assignment) {
    auto topics = chunked_vector<kafka::offset_fetch_request_topic>{};
    topics.reserve(assignment.size());
    std::transform(
      std::make_move_iterator(assignment.begin()),
      std::make_move_iterator(assignment.end()),
      std::back_inserter(topics),
      [](auto a) {
          return kafka::offset_fetch_request_topic{
            .name = std::move(a.first),
            .partition_indexes = std::move(a.second)};
      });
    return topics;
}

} // namespace

FIXTURE_TEST(consumer_group, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_connected_client();
    client.set_retry_base_backoff(10ms);
    client.set_max_retries(size_t(10));
    client.connect().get();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    info("Adding known topic");
    int partition_count = 3;
    int topic_count = 3;
    std::vector<model::topic_namespace> topics_namespaces;
    topics_namespaces.reserve(topic_count);
    for (int i = 0; i < topic_count; ++i) {
        topics_namespaces.push_back(create_topic(partition_count, i));
    }

    info("Waiting for topic");
    for (int t = 0; t < topic_count; ++t) {
        for (int p = 0; p < partition_count; ++p) {
            const auto& tp_ns = topics_namespaces[t];
            wait_for_lso(
              model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id{p}),
              model::offset{0})
              .get();
        }
    }
    // produce to topics
    for (int t = 0; t < topic_count; ++t) {
        for (auto b = 0; b < 10; ++b) {
            auto bat = make_batch(model::offset(0), 2);
            auto tp = model::topic_partition(
              topics_namespaces[t].tp,
              model::partition_id(b % partition_count));
            auto res = client.produce_record_batch(tp, std::move(bat)).get();
        }
    }

    kafka::group_id group_id{"test_group_id"};

    static auto find_coordinator_request_builder = [group_id]() mutable {
        return
          [group_id]() { return kafka::find_coordinator_request(group_id); };
    };

    static auto list_groups_request_builder = []() {
        return []() { return kafka::list_groups_request{}; };
    };

    static auto describe_group_request_builder = [group_id]() mutable {
        return [group_id]() {
            kafka::describe_groups_request req;
            req.data.groups.push_back(group_id);
            return req;
        };
    };

    info("Find coordinator for {}", group_id);
    auto find_res = client.dispatch(find_coordinator_request_builder()).get();
    info("Find coordinator res: {}", find_res);
    BOOST_REQUIRE_EQUAL(find_res.data.error_code, kafka::error_code::none);

    info("Waiting for group coordinator");
    kafka::describe_groups_response desc_res{};
    tests::cooperative_spin_wait_with_timeout(10s, [&client, &desc_res] {
        return client.dispatch(describe_group_request_builder())
          .then([&desc_res](kafka::describe_groups_response res) {
              desc_res = std::move(res);
              info("Describe group res: {}", desc_res);
              return desc_res.data.groups.size() == 1
                     && desc_res.data.groups[0].error_code
                          != kafka::error_code::not_coordinator;
          });
    }).get();

    auto check_group_response = [](
                                  const kafka::describe_groups_response& res,
                                  kafka::group_state state,
                                  size_t size) {
        BOOST_REQUIRE_EQUAL(res.data.groups.size(), 1);
        BOOST_REQUIRE_EQUAL(
          res.data.groups[0].error_code, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          res.data.groups[0].group_state,
          kafka::group_state_to_kafka_name(state));
        BOOST_REQUIRE_EQUAL(res.data.groups[0].members.size(), size);
    };

    BOOST_TEST_CONTEXT("Group not started") {
        check_group_response(desc_res, kafka::group_state::dead, 0);
    }

    std::vector<model::topic> topics;
    topics.reserve(topic_count);
    for (const auto& tp_ns : topics_namespaces) {
        topics.push_back(tp_ns.tp);
    }

    info("Joining Consumers: 0,1");
    std::vector<kafka::member_id> members;
    members.reserve(3);
    auto remove_consumers = ss::defer([&client, &group_id, &members]() {
        for (const auto& m_id : members) {
            client.remove_consumer(group_id, m_id)
              .handle_exception([](std::exception_ptr) {})
              .get();
        }
    });

    {
        auto [mem_0, mem_1] = ss::when_all_succeed(
                                client.create_consumer(group_id),
                                client.create_consumer(group_id))
                                .get();
        members.push_back(mem_0);
        members.push_back(mem_1);
    }
    info("Joined Consumers: 0,1");

    desc_res = client.dispatch(describe_group_request_builder()).get();
    BOOST_TEST_CONTEXT("Group size = 2") {
        check_group_response(desc_res, kafka::group_state::stable, 2);
    }

    // Check topic subscriptions - none expected
    for (auto& member : members) {
        auto consumer_topics = client.consumer_topics(group_id, member).get();
        BOOST_REQUIRE_EQUAL(consumer_topics.size(), 0);
    }

    info("Subscribing Consumers 0,1");
    ss::when_all_succeed(
      client.subscribe_consumer(group_id, members[0], {topics[0]}),
      client.subscribe_consumer(group_id, members[1], {topics[1]}))
      .get();

    desc_res = client.dispatch(describe_group_request_builder()).get();
    BOOST_TEST_CONTEXT("Group size = 2") {
        check_group_response(desc_res, kafka::group_state::stable, 2);
    }

    // Check topic subscriptions - one each expected
    for (size_t i = 0; i < members.size(); ++i) {
        auto consumer_topics
          = client.consumer_topics(group_id, members[i]).get();
        BOOST_REQUIRE_EQUAL(consumer_topics.size(), 1);
        BOOST_REQUIRE_EQUAL(consumer_topics[0], topics[i]);
    }

    info("Joining Consumer 2");
    auto mem_2 = client.create_consumer(group_id).get();
    members.push_back(mem_2);
    client.subscribe_consumer(group_id, mem_2, {topics[2]}).get();
    info("Joined Consumer 2");

    desc_res = client.dispatch(describe_group_request_builder()).get();
    BOOST_TEST_CONTEXT("Group size = 3") {
        check_group_response(desc_res, kafka::group_state::stable, 3);
    }

    auto list_res = client.dispatch(list_groups_request_builder()).get();
    info("list res: {}", list_res);

    // Check topic subscriptions - one each expected
    for (size_t i = 0; i < members.size(); ++i) {
        auto consumer_topics
          = client.consumer_topics(group_id, members[i]).get();
        BOOST_REQUIRE_EQUAL(consumer_topics.size(), 1);
        BOOST_REQUIRE_EQUAL(consumer_topics[0], topics[i]);
    }

    // Check member assignment and offsets
    // range_assignment is allocated according to sorted member ids
    auto sorted_members = members;
    std::sort(sorted_members.begin(), sorted_members.end());
    for (size_t i = 0; i < sorted_members.size(); ++i) {
        auto m_id = sorted_members[i];
        auto assignment = client.consumer_assignment(group_id, m_id).get();
        BOOST_REQUIRE_EQUAL(assignment.size(), 3);
        for (const auto& [topic, partitions] : assignment) {
            BOOST_REQUIRE_EQUAL(partitions.size(), 1);
            BOOST_REQUIRE_EQUAL(partitions[0](), i);
        }

        auto topics = offset_request_from_assignment(std::move(assignment));
        auto offsets
          = client.consumer_offset_fetch(group_id, m_id, std::move(topics))
              .get();
        BOOST_REQUIRE_EQUAL(offsets.data.error_code, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(offsets.data.topics.size(), 3);
        for (const auto& t : offsets.data.topics) {
            BOOST_REQUIRE_EQUAL(t.partitions.size(), 1);
            for (const auto& p : t.partitions) {
                BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
                BOOST_REQUIRE_EQUAL(p.partition_index(), i);
                BOOST_REQUIRE_EQUAL(p.committed_offset(), -1);
            }
        }
    }

    info("Consuming topic data");
    auto fetch_responses
      = ssx::parallel_transform(
          sorted_members.begin(),
          sorted_members.end(),
          [&](kafka::member_id m_id) {
              auto res
                = client.consumer_fetch(group_id, m_id, 200ms, 1_MiB).get();
              BOOST_REQUIRE_EQUAL(res.data.error_code, kafka::error_code::none);
              BOOST_REQUIRE_EQUAL(res.data.responses.size(), 3);
              for (const auto& p : res.data.responses) {
                  BOOST_REQUIRE_EQUAL(p.partitions.size(), 1);
                  const auto& res = p.partitions[0];
                  BOOST_REQUIRE_EQUAL(res.error_code, kafka::error_code::none);
                  BOOST_REQUIRE(!!res.records);
              }
              return res;
          })
          .get();

    // Commit 5 offsets, with metadata of the member id.
    for (size_t i = 0; i < sorted_members.size(); ++i) {
        auto m_id = sorted_members[i];
        auto t = chunked_vector<kafka::offset_commit_request_topic>{};
        t.reserve(3);
        std::transform(
          topics.begin(),
          topics.end(),
          std::back_inserter(t),
          [i, m_id](auto& topic) {
              return kafka::offset_commit_request_topic{
                .name = topic,
                .partitions = {
                  {.partition_index = model::partition_id{static_cast<int>(i)},
                   .committed_offset = model::offset{5},
                   .committed_metadata{m_id()}}}};
          });
        auto res
          = client.consumer_offset_commit(group_id, m_id, std::move(t)).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 3);
        for (const auto& t : res.data.topics) {
            BOOST_REQUIRE_EQUAL(t.partitions.size(), 1);
            auto& p = t.partitions[0];
            BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
            BOOST_REQUIRE_EQUAL(p.partition_index, i);
        }
    }

    // Check member assignment and offsets
    // range_assignment is allocated according to sorted member ids
    for (size_t i = 0; i < sorted_members.size(); ++i) {
        auto m_id = sorted_members[i];
        auto assignment = client.consumer_assignment(group_id, m_id).get();
        BOOST_REQUIRE_EQUAL(assignment.size(), 3);
        for (const auto& [topic, partitions] : assignment) {
            BOOST_REQUIRE_EQUAL(partitions.size(), 1);
            BOOST_REQUIRE_EQUAL(partitions[0](), i);
        }

        auto topics = offset_request_from_assignment(std::move(assignment));
        auto offsets
          = client.consumer_offset_fetch(group_id, m_id, std::move(topics))
              .get();
        BOOST_REQUIRE_EQUAL(offsets.data.error_code, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(offsets.data.topics.size(), 3);
        for (const auto& t : offsets.data.topics) {
            BOOST_REQUIRE_EQUAL(t.partitions.size(), 1);
            for (const auto& p : t.partitions) {
                BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
                BOOST_REQUIRE_EQUAL(p.partition_index(), i);
                BOOST_REQUIRE_EQUAL(p.committed_offset(), 5);
                BOOST_REQUIRE_EQUAL(p.metadata, m_id());
            }
        }
    }

    // Commit all offsets
    // empty list means commit all offsets
    for (size_t i = 0; i < sorted_members.size(); ++i) {
        auto m_id = sorted_members[i];
        auto t = chunked_vector<kafka::offset_commit_request_topic>{};
        auto res
          = client.consumer_offset_commit(group_id, m_id, std::move(t)).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 3);
        for (const auto& t : res.data.topics) {
            BOOST_REQUIRE_EQUAL(t.partitions.size(), 1);
            auto& p = t.partitions[0];
            BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
            BOOST_REQUIRE_EQUAL(p.partition_index, i);
        }
    }

    // Check comimtted offsets match the fetched offsets
    // range_assignment is allocated according to sorted member ids
    for (size_t i = 0; i < sorted_members.size(); ++i) {
        auto m_id = sorted_members[i];
        auto assignment = client.consumer_assignment(group_id, m_id).get();

        auto topics = offset_request_from_assignment(std::move(assignment));
        auto offsets
          = client.consumer_offset_fetch(group_id, m_id, std::move(topics))
              .get();
        BOOST_REQUIRE_EQUAL(offsets.data.error_code, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(offsets.data.topics.size(), 3);
        for (const auto& t : offsets.data.topics) {
            BOOST_REQUIRE_EQUAL(t.partitions.size(), 1);
            for (const auto& p : t.partitions) {
                BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
                BOOST_REQUIRE_EQUAL(p.partition_index(), i);
                auto part_it = std::find_if(
                  fetch_responses[i].begin(),
                  fetch_responses[i].end(),
                  [&](const auto& res) {
                      return res.partition->topic == t.name
                             && res.partition_response->partition_index
                                  == p.partition_index;
                  });
                BOOST_REQUIRE(part_it != fetch_responses[i].end());
                auto expected_offset
                  = part_it->partition_response->records->last_offset();
                BOOST_REQUIRE_EQUAL(p.committed_offset(), expected_offset);
            }
        }
    }
}

namespace {

// Collect every record offset carried by a consumer fetch response. A
// no-data response may either omit the partition or include it with an empty
// record set, so both shapes yield no offsets.
//
// The response is drained: reading a batch moves the buffer out of the record
// set, so a second call on the same response yields nothing.
std::vector<model::offset> consume_record_offsets(kafka::fetch_response& res) {
    std::vector<model::offset> offsets;
    for (const auto& part : res) {
        auto& p = *part.partition_response;
        BOOST_REQUIRE_EQUAL(p.error_code, kafka::error_code::none);
        if (!p.records) {
            continue;
        }
        while (!p.records->empty()) {
            auto adapter = p.records->consume_batch();
            BOOST_REQUIRE(adapter.batch.has_value());
            adapter.batch->for_each_record([&](const model::record& r) {
                offsets.push_back(
                  adapter.batch->base_offset()
                  + model::offset_delta{r.offset_delta()});
            });
        }
    }
    return offsets;
}

} // namespace

// A consumer is not safe for concurrent fetches, but pandaproxy can deliver
// them: an HTTP client (or load balancer) whose request timeout is shorter
// than the fetch long-poll retries while the original fetch is still in
// flight, and both requests land on the same consumer. Unserialized, two
// overlapping fetches on a fresh consumer would both dispatch
// session-creating (epoch 0) requests, making the broker mint two fetch
// sessions for a consumer that tracks only one per broker (see
// fetch_session::update_session_state).
//
// consumer::fetch() serializes concurrent callers instead: both fetches
// succeed, and each record is delivered to exactly one of them -- the
// serialized second fetch resumes from the first fetch's end position rather
// than re-reading the same records.
FIXTURE_TEST(consumer_group_concurrent_fetch, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_connected_client();
    client.set_retry_base_backoff(10ms);
    client.set_max_retries(size_t(10));
    client.connect().get();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    info("Adding known topic");
    auto tp_ns = create_topic(1);
    wait_for_lso(
      model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id{0}), model::offset{0})
      .get();

    info("Producing to topic");
    const auto tp = model::topic_partition(tp_ns.tp, model::partition_id{0});
    for (auto b = 0; b < 2; ++b) {
        auto res = client
                     .produce_record_batch(tp, make_batch(model::offset{0}, 2))
                     .get();
        BOOST_REQUIRE_EQUAL(res.error_code, kafka::error_code::none);
    }

    kafka::group_id group_id{"test_group_concurrent_fetch"};

    info("Joining consumer");
    auto member = client.create_consumer(group_id).get();
    auto remove_consumer = ss::defer([&]() {
        client.remove_consumer(group_id, member)
          .handle_exception([](std::exception_ptr) {})
          .get();
    });

    info("Subscribing consumer");
    client.subscribe_consumer(group_id, member, {tp_ns.tp}).get();
    tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return client.consumer_assignment(group_id, member)
          .then([](kc::assignment a) { return a.size() == 1; });
    }).get();

    info("Issuing two concurrent fetches");
    auto [res_a, res_b]
      = ss::when_all_succeed(
          client.consumer_fetch(group_id, member, 200ms, 1_MiB),
          client.consumer_fetch(group_id, member, 200ms, 1_MiB))
          .get();
    BOOST_REQUIRE_EQUAL(res_a.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(res_b.data.error_code, kafka::error_code::none);

    // Each record is delivered to exactly one of the two fetches: were they
    // not serialized, both would read from the same start position and
    // deliver the same records twice. Either fetch may deliver any part of
    // the log (a round need not drain everything), so assert the invariant
    // itself: together the fetches cover every produced record, and no
    // record appears in both.
    auto all_offsets = consume_record_offsets(res_a);
    all_offsets.append_range(consume_record_offsets(res_b));
    std::ranges::sort(all_offsets);
    const std::vector<model::offset> expected{
      model::offset{0}, model::offset{1}, model::offset{2}, model::offset{3}};
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      all_offsets.begin(), all_offsets.end(), expected.begin(), expected.end());

    // The consumer must remain usable afterwards.
    auto res_c = client.consumer_fetch(group_id, member, 200ms, 1_MiB).get();
    BOOST_REQUIRE_EQUAL(res_c.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE(consume_record_offsets(res_c).empty());
}
