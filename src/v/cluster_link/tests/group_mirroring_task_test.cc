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

#include "cluster_link/group_mirroring_task.h"
#include "cluster_link/tests/deps.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <gmock/gmock.h>

using namespace std::chrono_literals;

namespace cluster_link::tests {

using model::filter_pattern_type;
using model::filter_type;
using model::resource_name_filter_pattern;

namespace {
static const model::name_t link_name{"test_cluster_link"};
constexpr auto task_interval = 1s;
constexpr auto wait_interval = 2 * task_interval;

model::metadata get_default_metadata() {
    model::link_state link_state;
    model::metadata metadata{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = std::move(link_state)};
    metadata.configuration.consumer_groups_mirroring_cfg.task_interval
      = task_interval;

    metadata.configuration.consumer_groups_mirroring_cfg.filters.emplace_back(
      resource_name_filter_pattern{
        .pattern_type = filter_pattern_type::literal,
        .filter = filter_type::include,
        .pattern = resource_name_filter_pattern::wildcard});
    return metadata;
}

/**
 * Consumer groups metadata
 */
struct partition_offset_metadata {
    kafka::offset committed_offset;
};

struct consumer_group_metadata {
    kafka::group_id group_id;
    ::model::node_id coordinator_id;
    ss::sstring state = "stable";
    chunked_hash_map<
      ::model::topic,
      chunked_hash_map<::model::partition_id, partition_offset_metadata>>
      offsets;
};

} // namespace

/**
 * The test setup has the following assumptions:
 *
 *  - 4 consumer offsets topic partitions
 */
class group_mirroring_task_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;
    template<typename Api>
    void set_supported_version(::model::node_id id) {
        fixture()->get_cluster_mock().set_supported_versions(
          id, Api::key, {.min = Api::min_valid, .max = Api::max_valid});
    }
    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));

        co_await _clmtf->get_manager().invoke_on_all([](manager& m) {
            return m.register_task_factory<group_mirroring_task_factory>();
        });

        fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);

        // Set up __consumer_offsets topic
        auto consumer_offsets_cfg = cluster::topic_configuration(
          ::model::kafka_namespace,
          ::model::kafka_consumer_offsets_topic,
          4,  // partition count
          1); // replication factor
        fixture()->set_topic_config(consumer_offsets_cfg);
        fixture()->consumer_group_router()->partition_count = 4;
        fixture()->get_cluster_mock().register_handler(
          kafka::list_groups_api::key,
          [this](
            ::model::node_id id,
            kafka::client::request_t req,
            kafka::api_version version) {
              return handle_list_groups_request(id, std::move(req), version);
          });
        fixture()->get_cluster_mock().register_handler(
          kafka::find_coordinator_api::key,
          [this](
            ::model::node_id id,
            kafka::client::request_t req,
            kafka::api_version version) {
              return handle_find_coordinator_request(
                id, std::move(req), version);
          });
        fixture()->get_cluster_mock().register_handler(
          kafka::offset_fetch_api::key,
          [this](
            ::model::node_id id,
            kafka::client::request_t req,
            kafka::api_version version) {
              return handle_offset_fetch_request(id, std::move(req), version);
          });

        for (int i = 0; i < 3; ++i) {
            ::model::node_id id{i};
            set_supported_version<kafka::find_coordinator_api>(id);
            set_supported_version<kafka::list_groups_api>(id);
            set_supported_version<kafka::offset_fetch_api>(id);
        }
    }

    void make_current_node_leader_for(const std::vector<int>& partition_ids) {
        for (const auto& p_id : partition_ids) {
            ::model::ntp ntp(
              ::model::kafka_namespace,
              ::model::kafka_consumer_offsets_topic,
              p_id);
            fixture()->elect_leader(ntp, self(), ss::this_shard_id());
        }
    }

    void make_other_node_leader_for(const std::vector<int>& partition_ids) {
        for (const auto& p_id : partition_ids) {
            ::model::ntp ntp(
              ::model::kafka_namespace,
              ::model::kafka_consumer_offsets_topic,
              p_id);
            fixture()->elect_leader(ntp, ::model::node_id(1), std::nullopt);
        }
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }

    ::model::node_id self() { return ::model::node_id(0); }

    ss::future<bool> wait_for_task_state(model::task_state state) {
        return fixture()->wait_for_report_to_match(
          wait_interval,
          100ms,
          [state](const model::cluster_link_task_status_report& report) {
              auto link_it = report.link_reports.find(link_name);
              if (link_it == report.link_reports.end()) {
                  return false;
              }
              auto task_it = link_it->second.task_status_reports.find(
                group_mirroring_task::task_name);
              if (task_it == link_it->second.task_status_reports.end()) {
                  return false;
              }

              return task_it->second.task_state == state;
          });
    }
    template<typename Predicate>
    ss::future<>
    wait_for_group_state(const kafka::group_id& group, Predicate&& pred) {
        return ::tests::cooperative_spin_wait_with_timeout(
          3 * wait_interval,
          [this, pred = std::forward<Predicate>(pred), &group]() {
              auto& groups = fixture()->consumer_group_router()->groups;
              auto it = groups.find(group);
              if (it != groups.end()) {
                  return pred(it->second);
              }
              return false;
          });
    }

    ss::future<> wait_for_mirrored_offsets(
      const kafka::group_id& group,
      std::unordered_map<ss::sstring, std::unordered_map<int, int64_t>>
        offsets) {
        return wait_for_group_state(
          group,
          [offsets = std::move(offsets)](
            const test_consumer_group_router::group_state& g_state) {
              if (g_state.offsets.size() != offsets.size()) {
                  return false;
              }
              for (auto& [tp, partitions] : offsets) {
                  ::model::topic m_tp(tp);
                  auto it = g_state.offsets.find(m_tp);

                  if (it == g_state.offsets.end()) {
                      return false;
                  }
                  if (partitions.size() != it->second.size()) {
                      return false;
                  }
                  for (auto& [p, offset] : partitions) {
                      auto pit = it->second.find(::model::partition_id(p));
                      if (
                        pit == it->second.end()
                        || pit->second != kafka::offset(offset)) {
                          return false;
                      }
                  }
              }
              return true;
          });
    }

    void set_source_cluster_group_offsets(
      const kafka::group_id& group,
      std::unordered_map<ss::sstring, std::unordered_map<int, int64_t>>
        offsets) {
        auto& metadata = source_cluster_group[group];
        metadata.offsets.clear();

        for (auto& [topic, partitions] : offsets) {
            for (auto& [p, offset] : partitions) {
                metadata
                  .offsets[::model::topic(topic)][::model::partition_id(p)]
                  .committed_offset = kafka::offset(offset);
            }
        }
    }

    void set_source_cluster_high_watermark(
      std::unordered_map<ss::sstring, std::unordered_map<int, int64_t>>
        offsets) {
        auto p_md = fixture()->partition_metadata_provider();

        for (auto& [topic, partitions] : offsets) {
            for (auto& [p, offset] : partitions) {
                p_md->hwms[::model::topic_partition(
                  ::model::topic(topic), ::model::partition_id(p))]
                  = kafka::offset(offset);
            }
        }
    }

    ss::future<kafka::client::response_t> handle_list_groups_request(
      ::model::node_id id, kafka::client::request_t, kafka::api_version) {
        kafka::list_groups_response resp{};
        if (id == self()) {
            for (auto& [g, md] : source_cluster_group) {
                resp.data.groups.push_back(
                  kafka::listed_group{
                    .group_id = g,
                    .group_state = md.state,
                  });
            }
        }
        resp.data.error_code = kafka::error_code::none;
        co_return resp;
    }

    ss::future<kafka::client::response_t> handle_find_coordinator_request(
      ::model::node_id,
      kafka::client::request_t req,
      kafka::api_version version) {
        auto fc_req = std::get<kafka::find_coordinator_request>(std::move(req));
        if (fc_req.data.key_type != kafka::coordinator_type::group) {
            co_return kafka::find_coordinator_response{
              kafka::error_code::unsupported_version,
              ssx::sformat(
                "Unsupported coordinator type: {}", fc_req.data.key_type)};
        }
        // older versions only support single key
        if (version < kafka::api_version(4)) {
            auto coordinator = do_find_coordinator(
              kafka::group_id(std::move(fc_req.data.key)));
            co_return kafka::find_coordinator_response(
              coordinator.node_id, coordinator.host, coordinator.port);
        }

        kafka::find_coordinator_response resp;
        resp.data.error_code = kafka::error_code::none;
        for (auto& key : fc_req.data.coordinator_keys) {
            kafka::coordinator coordinator = do_find_coordinator(
              kafka::group_id(std::move(key)));
            resp.data.coordinators.push_back(std::move(coordinator));
        }
        co_return resp;
    }

    kafka::coordinator do_find_coordinator(const kafka::group_id& group) {
        auto it = source_cluster_group.find(group);
        if (it == source_cluster_group.end()) {
            throw std::runtime_error(ssx::sformat("Unknown group: {}", group));
        }

        return kafka::coordinator{
          .key = group,
          .node_id = it->second.coordinator_id,
          .host = "localhost",
          .port = 9092,
          .error_code = kafka::error_code::none,
          .error_message = std::nullopt};
    }

    ss::future<kafka::client::response_t> handle_offset_fetch_request(
      ::model::node_id id, kafka::client::request_t req, kafka::api_version) {
        auto of_req = std::get<kafka::offset_fetch_request>(std::move(req));

        auto it = source_cluster_group.find(of_req.data.group_id);
        if (it == source_cluster_group.end()) {
            co_return kafka::offset_fetch_response(
              std::move(of_req.data.topics));
        }

        if (it->second.coordinator_id != id) {
            co_return kafka::offset_fetch_response(
              kafka::error_code::not_coordinator);
        }

        kafka::offset_fetch_response resp;
        for (auto& [topic, partitions] : it->second.offsets) {
            kafka::offset_fetch_response_topic r_topic;
            r_topic.name = topic;
            for (auto& [p_id, offset_meta] : partitions) {
                r_topic.partitions.push_back(
                  kafka::offset_fetch_response_partition{
                    .partition_index = p_id,
                    .committed_offset = kafka::offset_cast(
                      offset_meta.committed_offset),
                    .error_code = kafka::error_code::none});
            }
            resp.data.topics.push_back(std::move(r_topic));
        }
        co_return resp;
    }

    chunked_hash_map<kafka::group_id, consumer_group_metadata>
      source_cluster_group;

    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_F(group_mirroring_task_test, check_if_task_follows_the_leader) {
    // Clear the consumer offsets leadership
    make_other_node_leader_for({0, 1, 2, 3});
    fixture()->upsert_link(get_default_metadata()).get();

    ss::sleep(wait_interval).get();

    bool is_stopped = wait_for_task_state(model::task_state::stopped).get();

    ASSERT_TRUE(is_stopped);

    // make current node a leader
    make_current_node_leader_for({0, 4});
    bool is_active = wait_for_task_state(model::task_state::active).get();
    ASSERT_TRUE(is_active);

    // make other node a leader
    make_other_node_leader_for({4});
    // wait for some time
    ss::sleep(wait_interval).get();
    // task should still be active as current node is still leader for one
    // of the cg partitions
    bool still_active = wait_for_task_state(model::task_state::active).get();
    ASSERT_TRUE(still_active);

    make_other_node_leader_for({0});
    // no leaders on current node, task should be stopped
    bool stopped = wait_for_task_state(model::task_state::stopped).get();
    ASSERT_TRUE(stopped);
}

/**
 * Simplest test validating consumer groups mirroring. This test mirrors
 * state of consumer group from `kafka::client::cluster_mock` representing
 * connection to the source cluster to the fixture `test_group_router` which
 * maintains the state of target cluster consumer groups.
 */
TEST_F(group_mirroring_task_test, test_happy_path_offsets_mirroring) {
    make_current_node_leader_for({0, 1, 2, 3});
    fixture()->upsert_link(get_default_metadata()).get();

    bool is_active = wait_for_task_state(model::task_state::active).get();
    ASSERT_TRUE(is_active);

    // set consumer group offsets

    kafka::group_id test_group("t-group");

    consumer_group_metadata metadata;
    metadata.group_id = test_group;
    metadata.coordinator_id = ::model::node_id(0);
    source_cluster_group[test_group] = std::move(metadata);

    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 100}, {4, 312}}}});
    set_source_cluster_high_watermark(
      {{"t-topic", {{0, 1000}, {1, 1000}, {4, 1000}}}});
    /**
     * Verify if offset is mirrored
     */
    wait_for_mirrored_offsets(test_group, {{"t-topic", {{0, 100}, {4, 312}}}})
      .get();
    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 0}, {4, 312}, {1, 10}}}});

    wait_for_mirrored_offsets(
      test_group, {{"t-topic", {{0, 0}, {4, 312}, {1, 10}}}})
      .get();
}

TEST_F(group_mirroring_task_test, test_find_coordinator_v3) {
    make_current_node_leader_for({0, 1, 2, 3});
    for (int i = 0; i < 3; ++i) {
        ::model::node_id id{i};
        fixture()->get_cluster_mock().set_supported_versions(
          id,
          kafka::find_coordinator_api::key,
          {.min = kafka::find_coordinator_api::min_valid,
           .max = kafka::api_version(3)});
    }
    fixture()->upsert_link(get_default_metadata()).get();

    bool is_active = wait_for_task_state(model::task_state::active).get();
    ASSERT_TRUE(is_active);

    // set consumer group offsets

    kafka::group_id test_group("t-group");

    consumer_group_metadata metadata;
    metadata.group_id = test_group;
    metadata.coordinator_id = ::model::node_id(0);
    source_cluster_group[test_group] = std::move(metadata);

    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 100}, {4, 312}}}});
    set_source_cluster_high_watermark(
      {{"t-topic", {{0, 1000}, {1, 1000}, {4, 1000}}}});
    /**
     * Verify if offset is mirrored
     */
    wait_for_mirrored_offsets(test_group, {{"t-topic", {{0, 100}, {4, 312}}}})
      .get();
    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 0}, {4, 312}, {1, 10}}}});

    wait_for_mirrored_offsets(
      test_group, {{"t-topic", {{0, 0}, {4, 312}, {1, 10}}}})
      .get();
}

TEST_F(group_mirroring_task_test, test_updating_source_cluster_coordinator) {
    make_current_node_leader_for({0, 1, 2, 3});
    fixture()->upsert_link(get_default_metadata()).get();

    bool is_active = wait_for_task_state(model::task_state::active).get();
    ASSERT_TRUE(is_active);

    kafka::group_id test_group("t-group");

    consumer_group_metadata metadata;
    metadata.group_id = test_group;
    metadata.coordinator_id = ::model::node_id(0);
    source_cluster_group[test_group] = std::move(metadata);
    set_source_cluster_high_watermark(
      {{"t-topic", {{0, 1000}, {1, 1000}, {4, 1025}, {12, 1000}}}});

    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 100}, {4, 312}}}});
    /**
     * Verify if offset is mirrored
     */
    wait_for_mirrored_offsets(test_group, {{"t-topic", {{0, 100}, {4, 312}}}})
      .get();
    // change coordinator
    source_cluster_group[test_group].coordinator_id = ::model::node_id(1);
    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 100}, {4, 1024}, {12, 10}}}});
    /**
     * Verify if offset is mirrored
     */
    wait_for_mirrored_offsets(
      test_group, {{"t-topic", {{0, 100}, {4, 1024}, {12, 10}}}})
      .get();
}

TEST_F(group_mirroring_task_test, test_offsets_trimming_to_hwm) {
    make_current_node_leader_for({0, 1, 2, 3});
    fixture()->upsert_link(get_default_metadata()).get();

    bool is_active = wait_for_task_state(model::task_state::active).get();
    ASSERT_TRUE(is_active);

    // set consumer group offsets
    kafka::group_id test_group("t-group");

    consumer_group_metadata metadata;
    metadata.group_id = test_group;
    metadata.coordinator_id = ::model::node_id(0);
    source_cluster_group[test_group] = std::move(metadata);

    set_source_cluster_group_offsets(
      test_group, {{"t-topic", {{0, 100}, {4, 312}}}});
    set_source_cluster_high_watermark({{"t-topic", {{0, 80}}}});
    /**
     * Verify if offset is mirrored
     */
    wait_for_mirrored_offsets(test_group, {{"t-topic", {{0, 80}}}}).get();
    // check that even after waiting for it, the offset for partition 0 is still
    // below the high watermark
    ASSERT_THROW(
      wait_for_mirrored_offsets(test_group, {{"t-topic", {{0, 81}}}}).get(),
      ss::timed_out_error);

    auto& groups = fixture()->consumer_group_router()->groups;
    auto it = groups.find(test_group);
    // check that only one partition is present, there is no hwm for partition 4
    // yet
    ASSERT_EQ(it->second.offsets[::model::topic("t-topic")].size(), 1);

    // update hwms
    set_source_cluster_high_watermark({{"t-topic", {{0, 82}, {4, 500}}}});

    wait_for_mirrored_offsets(test_group, {{"t-topic", {{0, 82}, {4, 312}}}})
      .get();
}

} // namespace cluster_link::tests
