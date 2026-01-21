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

#include "cluster_link/source_topic_syncer.h"
#include "cluster_link/tests/deps.h"
#include "kafka/server/handlers/topics/types.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

using namespace std::chrono_literals;

namespace cluster_link::tests {
using model::filter_pattern_type;
using model::filter_type;
using model::resource_name_filter_pattern;
namespace {
const ::model::topic test_topic{"test_topic"};
const model::name_t test_link_name{"test_link"};
model::metadata get_default_metadata(bool exclude_default_properties = false) {
    model::link_state link_state;
    model::link_configuration link_configuration;
    link_configuration.topic_metadata_mirroring_cfg.task_interval = 1s;
    link_configuration.topic_metadata_mirroring_cfg.topic_name_filters
      .emplace_back(
        resource_name_filter_pattern{
          .pattern_type = filter_pattern_type::literal,
          .filter = filter_type::include,
          .pattern = resource_name_filter_pattern::wildcard});
    link_configuration.topic_metadata_mirroring_cfg.exclude_default
      = exclude_default_properties;
    link_state.mirror_topics.emplace(
      test_topic,
      model::mirror_topic_metadata{
        .source_topic_name = test_topic,
        .partition_count = 1,
        .replication_factor = std::nullopt,
        .topic_configs = {
          {"max.message.bytes", "1048576"},
          {"cleanup.policy", "delete"},
          {"message.timestamp.type", "CreateTime"}}});
    model::metadata metadata{
      .name = test_link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = std::move(link_state),
      .configuration = std::move(link_configuration),
    };

    return metadata;
}

const absl::flat_hash_set<ss::sstring> expected_properties_to_be_present{
  ss::sstring{kafka::topic_property_max_message_bytes},
  ss::sstring{kafka::topic_property_cleanup_policy},
  ss::sstring{kafka::topic_property_timestamp_type},
  ss::sstring{kafka::topic_property_remote_allow_gaps},
};
} // namespace

class topic_properties_syncer_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;

    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));

        co_await _clmtf->get_manager().invoke_on_all([](manager& m) {
            return m.register_task_factory<source_topic_syncer_factory>();
        });

        fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);

        co_await fixture()->upsert_link(get_default_metadata(true));
        fixture()->get_cluster_mock().add_topic(
          test_topic, 1, 1, kafka::topic_authorized_operations(0x508));

        // Wait for all expected topic properties to be present in the mirror
        // topic
        RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
            auto link = fixture()->find_link_by_name(test_link_name);
            const auto& mirror_topics = link->state.mirror_topics;
            auto mirror_topic_it = mirror_topics.find(test_topic);
            if (mirror_topic_it == mirror_topics.end()) {
                return false;
            }
            return std::ranges::all_of(
              expected_properties_to_be_present, [&](const auto& property) {
                  return mirror_topic_it->second.topic_configs.contains(
                    property);
              });
        });
    }
    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }
    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }
    ::model::node_id self() const { return ::model::node_id{0}; }

private:
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_F_CORO(topic_properties_syncer_test, topic_properties_sync) {
    fixture()->get_cluster_mock().set_topic_partition_count(test_topic, 3);
    fixture()->get_cluster_mock().set_topic_replication_factor(test_topic, 3);
    ::cluster::topic_properties properties;
    properties.batch_max_bytes = 1024;
    fixture()->get_cluster_mock().set_topic_properties(
      test_topic, std::move(properties));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 3
                   && !topic_metadata.replication_factor.has_value()
                   && topic_metadata.topic_configs.at("max.message.bytes")
                        == "1024";
        }
        return false;
    });

    // Now remove the topic and add it back in with a partition count of 1 -
    // this will result in the mirror topic going into the failed state
    fixture()->get_cluster_mock().remove_topic(test_topic);
    fixture()->get_cluster_mock().add_topic(
      test_topic, 1, 1, kafka::topic_authorized_operations(0x508));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            return mirror_topic_it->second.status
                   == model::mirror_topic_status::failed;
        }
        return false;
    });

    // Now update the topic properties again, but there should be no change
    fixture()->get_cluster_mock().set_topic_partition_count(test_topic, 5);
    co_await ss::sleep(2s);
    auto link = fixture()->find_link_by_name(test_link_name);
    const auto& mirror_topics = link->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(test_topic);
    ASSERT_NE_CORO(mirror_topic_it, mirror_topics.end());
    EXPECT_EQ(mirror_topic_it->second.partition_count, 3);
    EXPECT_EQ(
      mirror_topic_it->second.status, model::mirror_topic_status::failed);
}

TEST_F_CORO(topic_properties_syncer_test, sync_rf) {
    auto md = get_default_metadata();
    md.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      .emplace("replication.factor");

    co_await fixture()->upsert_link(std::move(md));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 1
                   && topic_metadata.replication_factor == 1;
        }
        return false;
    });

    fixture()->get_cluster_mock().set_topic_replication_factor(test_topic, 3);

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 1
                   && topic_metadata.replication_factor == 3;
        }
        return false;
    });

    // Now add a new topic and ensure it gets the RF set correctly
    const ::model::topic new_topic{"new_topic"};
    fixture()->get_cluster_mock().add_topic(
      new_topic, 1, 3, kafka::topic_authorized_operations(0x508));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, new_topic] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(new_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 1
                   && topic_metadata.replication_factor == 3;
        }
        return false;
    });
}

class update_properties_invalid_describe_configs_test
  : public topic_properties_syncer_test {
public:
    void set_describe_configs_results(
      chunked_vector<kafka::describe_configs_result> results) {
        _describe_configs_results = std::move(results);
    }

    virtual ss::future<> SetUpAsync() override {
        co_await topic_properties_syncer_test::SetUpAsync();
        fixture()->get_cluster_mock().register_handler(
          kafka::describe_configs_api::key,
          [this](
            ::model::node_id id,
            kafka::client::request_t req,
            kafka::api_version v) {
              return handle_describe_configs(id, std::move(req), v);
          });
    }

    ss::future<kafka::client::response_t> handle_describe_configs(
      ::model::node_id id, kafka::client::request_t req, kafka::api_version v) {
        if (_describe_configs_results.empty()) {
            co_return co_await fixture()
              ->get_cluster_mock()
              .handle_describe_configs_request(id, std::move(req), v);
        } else {
            kafka::describe_configs_response resp;
            std::ranges::move(
              _describe_configs_results, std::back_inserter(resp.data.results));
            _describe_configs_results.clear();
            co_return resp;
        }
    }

private:
    chunked_vector<kafka::describe_configs_result> _describe_configs_results;
};

TEST_F_CORO(
  update_properties_invalid_describe_configs_test,
  do_not_return_topic_config_mod_partition_count) {
    chunked_vector<kafka::describe_configs_result> response;
    response.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::none,
        .resource_type = kafka::config_resource_type::topic,
        .resource_name = "no-such-topic",
      });
    set_describe_configs_results(std::move(response));
    fixture()->get_cluster_mock().set_topic_partition_count(test_topic, 3);

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 3;
        }
        return false;
    });
}

static constexpr std::string_view topic_properties_remote_allowgaps
  = "redpanda.remote.allowgaps";

TEST_F_CORO(
  update_properties_invalid_describe_configs_test,
  do_not_return_topic_config_no_mod) {
    auto properties = [this]() {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it == mirror_topics.end()) {
            throw std::runtime_error("Mirror topic not found in test setup");
        }
        return mirror_topic_it->second.copy();
    }();
    // this property is overridden by default in the source_topic_syncer
    // to allow gaps in replication

    properties.topic_configs[ss::sstring(topic_properties_remote_allowgaps)]
      = "true";

    chunked_vector<kafka::describe_configs_result> response;
    response.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::none,
        .resource_type = kafka::config_resource_type::topic,
        .resource_name = "no-such-topic",
      });
    set_describe_configs_results(std::move(response));

    // Nothing should change since no properties were changed
    co_await ss::sleep(2s);

    auto link = fixture()->find_link_by_name(test_link_name);
    const auto& mirror_topics = link->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(test_topic);
    ASSERT_NE_CORO(mirror_topic_it, mirror_topics.end());
    EXPECT_EQ(properties, mirror_topic_it->second)
      << "Properties should not have changed";
}

TEST_F_CORO(
  update_properties_invalid_describe_configs_test, return_error_with_mod) {
    auto properties = [this]() {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it == mirror_topics.end()) {
            throw std::runtime_error("Mirror topic not found in test setup");
        }
        return mirror_topic_it->second.copy();
    }();
    chunked_vector<kafka::describe_configs_result> response;
    response.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::topic_authorization_failed,
        .resource_type = kafka::config_resource_type::topic,
        .resource_name = test_topic,
        .configs = {kafka::describe_configs_resource_result{
          .name = "max.message.bytes", .value = "1024"}}});
    set_describe_configs_results(std::move(response));
    fixture()->get_cluster_mock().set_topic_partition_count(test_topic, 3);

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 3;
        }
        return false;
    });

    auto link = fixture()->find_link_by_name(test_link_name);
    const auto& mirror_topics = link->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(test_topic);
    ASSERT_NE_CORO(mirror_topic_it, mirror_topics.end());
    EXPECT_EQ(properties.topic_configs, mirror_topic_it->second.topic_configs)
      << "Properties should not have changed";
}

TEST_F_CORO(
  update_properties_invalid_describe_configs_test, return_wrong_resource_type) {
    auto properties = [this]() {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it == mirror_topics.end()) {
            throw std::runtime_error("Mirror topic not found in test setup");
        }
        return mirror_topic_it->second.copy();
    }();
    chunked_vector<kafka::describe_configs_result> response;
    response.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::none,
        .resource_type = kafka::config_resource_type::broker,
        .resource_name = test_topic,
        .configs = {kafka::describe_configs_resource_result{
          .name = "max.message.bytes", .value = "1024"}}});
    set_describe_configs_results(std::move(response));
    fixture()->get_cluster_mock().set_topic_partition_count(test_topic, 3);

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link = fixture()->find_link_by_name(test_link_name);
        const auto& mirror_topics = link->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(test_topic);
        if (mirror_topic_it != mirror_topics.end()) {
            const auto& topic_metadata = mirror_topic_it->second;
            return topic_metadata.partition_count == 3;
        }
        return false;
    });

    auto link = fixture()->find_link_by_name(test_link_name);
    const auto& mirror_topics = link->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(test_topic);
    ASSERT_NE_CORO(mirror_topic_it, mirror_topics.end());
    EXPECT_EQ(properties.topic_configs, mirror_topic_it->second.topic_configs)
      << "Properties should not have changed";
}

} // namespace cluster_link::tests
