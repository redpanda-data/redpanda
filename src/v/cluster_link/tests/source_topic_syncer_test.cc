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

#include "cluster_link/model/types.h"
#include "cluster_link/source_topic_syncer.h"
#include "cluster_link/tests/deps.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <algorithm>

using namespace std::chrono_literals;

namespace cluster_link::tests {

using model::filter_pattern_type;
using model::filter_type;
using model::resource_name_filter_pattern;

namespace {
model::metadata get_default_metadata(bool exclude_default_properties = false) {
    model::link_state link_state;
    model::metadata metadata{
      .name = model::name_t("test_link"),
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = std::move(link_state)};
    metadata.configuration.topic_metadata_mirroring_cfg.task_interval = 1s;
    metadata.configuration.topic_metadata_mirroring_cfg.topic_name_filters
      .emplace_back(
        resource_name_filter_pattern{
          .pattern_type = filter_pattern_type::literal,
          .filter = filter_type::include,
          .pattern = resource_name_filter_pattern::wildcard});
    metadata.configuration.topic_metadata_mirroring_cfg.exclude_default
      = exclude_default_properties;
    return metadata;
}
} // namespace

class source_topic_syncer_test : public seastar_test {
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
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }

    ::model::node_id self() { return ::model::node_id(0); }

private:
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_F_CORO(source_topic_syncer_test, create_auto_topic_sensor_task) {
    co_await fixture()->upsert_link(get_default_metadata());

    auto report = co_await fixture()->await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          auto link_it = report.link_reports.find(model::name_t("test_link"));
          if (link_it == report.link_reports.end()) {
              return false;
          }
          auto task_it = link_it->second.task_status_reports.find(
            source_topic_syncer::task_name);
          if (task_it == link_it->second.task_status_reports.end()) {
              return false;
          }

          return true;
      });

    ASSERT_TRUE_CORO(report.has_value()) << "Never received a task report";

    auto& task_report = report.value()
                          .link_reports.at(model::name_t("test_link"))
                          .task_status_reports.at(
                            source_topic_syncer::task_name);
    EXPECT_EQ(task_report.task_state, model::task_state::active);
}

TEST_F_CORO(source_topic_syncer_test, select_all_filter) {
    co_await fixture()->upsert_link(get_default_metadata(true));

    auto report = co_await fixture()->await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          auto link_it = report.link_reports.find(model::name_t("test_link"));
          if (link_it == report.link_reports.end()) {
              return false;
          }
          auto task_it = link_it->second.task_status_reports.find(
            source_topic_syncer::task_name);
          if (task_it == link_it->second.task_status_reports.end()) {
              return false;
          }

          return true;
      });

    ASSERT_TRUE_CORO(report.has_value()) << "Never received a task report";

    auto& task_report = report.value()
                          .link_reports.at(model::name_t("test_link"))
                          .task_status_reports.at(
                            source_topic_syncer::task_name);
    EXPECT_EQ(task_report.task_state, model::task_state::active);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    ASSERT_TRUE_CORO(link_metadata);

    EXPECT_TRUE(link_metadata->state.mirror_topics.empty());

    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x508));

    // Allow auto topic sensor to run
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(::model::topic("test_topic"));
        return mirror_topic_it != mirror_topics.end();
    });

    link_metadata = fixture()->find_link_by_name(model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(::model::topic("test_topic"));
    EXPECT_EQ(
      mirror_topic_it->second.source_topic_name, ::model::topic("test_topic"));
    EXPECT_EQ(mirror_topic_it->second.partition_count, 3);
    EXPECT_FALSE(mirror_topic_it->second.replication_factor.has_value());
    const auto& configs = mirror_topic_it->second.topic_configs;
    EXPECT_NE(configs.find("max.message.bytes"), configs.end());
    EXPECT_NE(configs.find("message.timestamp.type"), configs.end());
    EXPECT_NE(configs.find("cleanup.policy"), configs.end());
}

TEST_F_CORO(source_topic_syncer_test, select_all_with_exclude) {
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x508));
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("excluded-topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x508));

    auto md = get_default_metadata();
    md.configuration.topic_metadata_mirroring_cfg.topic_name_filters
      .emplace_back(
        resource_name_filter_pattern{
          .pattern_type = filter_pattern_type::literal,
          .filter = filter_type::exclude,
          .pattern = "excluded-topic"});
    co_await fixture()->upsert_link(std::move(md));

    // Allow auto topic sensor to run
    co_await ss::sleep(2s);
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(::model::topic("test_topic"));
        return mirror_topic_it != mirror_topics.end();
    });

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(::model::topic("excluded-topic"));
    EXPECT_EQ(mirror_topic_it, mirror_topics.end())
      << "Excluded topic should not be mirrored";
}

TEST_F_CORO(source_topic_syncer_test, schema_registry_test) {
    co_await fixture()->upsert_link(get_default_metadata(true));

    fixture()->get_cluster_mock().add_topic(
      ::model::schema_registry_internal_tp.topic,
      1,
      3,
      kafka::topic_authorized_operations(0x508));

    // sleep for 2 seconds to allow source topic syncer to run and then verify
    // that the topic was not added to the mirror topic state
    co_await ss::sleep(2s);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(
      ::model::schema_registry_internal_tp.topic);
    ASSERT_EQ_CORO(mirror_topic_it, mirror_topics.end())
      << "Should not have been able to find "
      << ::model::schema_registry_internal_tp.topic;

    // Now enable schema registry topic mirroring and ensure that it shows up
    auto update = co_await link_metadata->copy();
    update.configuration.schema_registry_sync_cfg
      .sync_schema_registry_topic_mode
      = model::schema_registry_sync_config::shadow_entire_schema_registry{};
    auto link_id = fixture()->find_link_id_by_name(model::name_t("test_link"));
    ASSERT_TRUE_CORO(link_id.has_value());
    co_await fixture()->update_link(*link_id, std::move(update));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(
          ::model::schema_registry_internal_tp.topic);
        return mirror_topic_it != mirror_topics.end();
    });
}

TEST_F_CORO(source_topic_syncer_test, schema_registry_exists_but_empty) {
    fixture()->get_cluster_mock().add_topic(
      ::model::schema_registry_internal_tp.topic,
      1,
      3,
      kafka::topic_authorized_operations(0x508));

    ASSERT_EQ_CORO(
      co_await fixture()->topic_creator().create_topic(
        {::model::kafka_namespace, ::model::schema_registry_internal_tp.topic},
        1,
        {},
        3),
      cluster::errc::success);

    auto md = get_default_metadata();
    md.configuration.schema_registry_sync_cfg.sync_schema_registry_topic_mode
      = model::schema_registry_sync_config::shadow_entire_schema_registry{};
    co_await fixture()->upsert_link(std::move(md));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(
          ::model::schema_registry_internal_tp.topic);
        return mirror_topic_it != mirror_topics.end();
    });
}

TEST_F_CORO(source_topic_syncer_test, schema_registry_exists_not_empty) {
    fixture()->get_cluster_mock().add_topic(
      ::model::schema_registry_internal_tp.topic,
      1,
      3,
      kafka::topic_authorized_operations(0x508));

    ASSERT_EQ_CORO(
      co_await fixture()->topic_creator().create_topic(
        {::model::kafka_namespace, ::model::schema_registry_internal_tp.topic},
        1,
        {},
        3),
      cluster::errc::success);

    fixture()->set_partition_hwm(
      ::model::schema_registry_internal_tp, kafka::offset(10));

    auto md = get_default_metadata();
    md.configuration.schema_registry_sync_cfg.sync_schema_registry_topic_mode
      = model::schema_registry_sync_config::shadow_entire_schema_registry{};
    co_await fixture()->upsert_link(std::move(md));

    // source topic syncer test runs every second, let it run a couple of times
    // and then verify that the schema registry topic was not added to the
    // mirror topic list
    co_await ss::sleep(3s);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(
      ::model::schema_registry_internal_tp.topic);
    ASSERT_EQ_CORO(mirror_topic_it, mirror_topics.end())
      << "Should not have been able to find "
      << ::model::schema_registry_internal_tp.topic;
}

TEST_F_CORO(source_topic_syncer_test, invalid_authorization) {
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x0));
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic2"),
      3,
      3,
      kafka::topic_authorized_operations_not_set);

    co_await fixture()->upsert_link(get_default_metadata());

    // Allow auto topic sensor to run
    co_await ss::sleep(2s);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    EXPECT_TRUE(mirror_topics.empty());
}

TEST_F_CORO(source_topic_syncer_test, no_controller_present) {
    fixture()->get_cluster_mock().set_controller_id(
      ::model::unassigned_node_id);

    co_await fixture()->upsert_link(get_default_metadata());

    auto report = co_await fixture()->await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          auto link_it = report.link_reports.find(model::name_t("test_link"));
          if (link_it == report.link_reports.end()) {
              return false;
          }
          auto task_it = link_it->second.task_status_reports.find(
            source_topic_syncer::task_name);
          if (task_it == link_it->second.task_status_reports.end()) {
              return false;
          }

          return true;
      });

    ASSERT_TRUE_CORO(report.has_value()) << "Never received a task report";

    auto& task_report = report.value()
                          .link_reports.at(model::name_t("test_link"))
                          .task_status_reports.at(
                            source_topic_syncer::task_name);
    EXPECT_EQ(task_report.task_state, model::task_state::link_unavailable);

    // Reset controller ID and ensure the task returns to active
    fixture()->get_cluster_mock().set_controller_id(std::nullopt);

    co_await ss::sleep(2s);
    report = co_await fixture()->await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          auto link_it = report.link_reports.find(model::name_t("test_link"));
          if (link_it == report.link_reports.end()) {
              return false;
          }
          auto task_it = link_it->second.task_status_reports.find(
            source_topic_syncer::task_name);
          if (task_it == link_it->second.task_status_reports.end()) {
              return false;
          }

          return true;
      });

    ASSERT_TRUE_CORO(report.has_value()) << "Never received a task report";
    {
        auto& task_report = report.value()
                              .link_reports.at(model::name_t("test_link"))
                              .task_status_reports.at(
                                source_topic_syncer::task_name);
        EXPECT_EQ(task_report.task_state, model::task_state::active);
    }
}

TEST_F_CORO(source_topic_syncer_test, topic_exists) {
    fixture()->set_topic_config(
      cluster::topic_configuration(
        ::model::kafka_namespace, ::model::topic{"test_topic"}, 3, 3));
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x508));

    co_await fixture()->upsert_link(get_default_metadata());

    // Allow auto topic sensor to run
    co_await ss::sleep(2s);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    EXPECT_TRUE(mirror_topics.empty());
}

TEST_F_CORO(source_topic_syncer_test, topic_with_no_partitions) {
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      0,
      3,
      kafka::topic_authorized_operations(0x508));

    co_await fixture()->upsert_link(get_default_metadata());

    // Allow auto topic sensor to run
    co_await ss::sleep(2s);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    EXPECT_TRUE(mirror_topics.empty());
}

TEST_F_CORO(source_topic_syncer_test, topic_id_changes) {
    auto topic_name = ::model::topic("test_topic");
    fixture()->get_cluster_mock().add_topic(
      topic_name, 3, 3, kafka::topic_authorized_operations(0x508));

    co_await fixture()->upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, topic_name] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        if (!link_metadata) {
            return false;
        }
        auto& mirror_topics = link_metadata->state.mirror_topics;
        return mirror_topics.contains(topic_name);
    });

    fixture()->get_cluster_mock().remove_topic(topic_name);
    // Re-create the topic which should change the topic ID
    fixture()->get_cluster_mock().add_topic(
      topic_name, 3, 3, kafka::topic_authorized_operations(0x508));

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, topic_name] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        if (!link_metadata) {
            return false;
        }
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto it = mirror_topics.find(topic_name);
        if (it == mirror_topics.end()) {
            return false;
        }
        return it->second.status == model::mirror_topic_status::failed;
    });
}

TEST_F_CORO(source_topic_syncer_test, topic_with_no_replicas) {
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      0,
      kafka::topic_authorized_operations(0x508));

    co_await fixture()->upsert_link(get_default_metadata());

    // Allow auto topic sensor to run
    co_await ss::sleep(2s);

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    EXPECT_TRUE(mirror_topics.empty());
}

TEST_F_CORO(source_topic_syncer_test, topic_with_high_rf) {
    fixture()->members_table_provider().set_node_count(1);
    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x508));

    co_await fixture()->upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        return link_metadata->state.mirror_topics.contains(
          ::model::topic("test_topic"));
    });

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(::model::topic("test_topic"));
    ASSERT_NE_CORO(mirror_topic_it, mirror_topics.end());
    EXPECT_EQ(mirror_topic_it->second.replication_factor, 1);

    fixture()->members_table_provider().set_node_count(3);
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(::model::topic("test_topic"));
        if (mirror_topic_it == mirror_topics.end()) {
            return false;
        }
        return mirror_topic_it->second.replication_factor == 3;
    });
}

class invalid_describe_configs_test : public source_topic_syncer_test {
public:
    virtual ss::future<> SetUpAsync() override {
        co_await source_topic_syncer_test::SetUpAsync();
        fixture()->get_cluster_mock().register_handler(
          kafka::describe_configs_api::key,
          [this](
            ::model::node_id id,
            kafka::client::request_t req,
            kafka::api_version v) {
              return fixture()
                ->get_cluster_mock()
                .handle_describe_configs_request(id, std::move(req), v)
                .then([this](kafka::client::response_t resp) {
                    return handle_describe_configs(std::move(resp));
                });
          });
    }

    ss::future<kafka::client::response_t>
    handle_describe_configs(kafka::client::response_t resp) {
        auto modified_resp = std::get<kafka::describe_configs_response>(
          std::move(resp));
        // Append _describe_configs_results to existing results
        std::ranges::move(
          _describe_configs_results,
          std::back_inserter(modified_resp.data.results));
        _describe_configs_results.clear();
        co_return modified_resp;
    }

protected:
    chunked_vector<kafka::describe_configs_result> _describe_configs_results;
};

TEST_F_CORO(invalid_describe_configs_test, bad_describe_config_response) {
    _describe_configs_results.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::topic_authorization_failed});

    _describe_configs_results.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::none,
        .resource_type = kafka::config_resource_type::broker,
      });

    _describe_configs_results.emplace_back(
      kafka::describe_configs_result{
        .error_code = kafka::error_code::none,
        .resource_type = kafka::config_resource_type::topic,
        .resource_name = "not_requested_topic",
      });

    fixture()->get_cluster_mock().add_topic(
      ::model::topic("test_topic"),
      3,
      3,
      kafka::topic_authorized_operations(0x508));

    co_await fixture()->upsert_link(get_default_metadata());

    // Allow auto topic sensor to run
    co_await ss::sleep(2s);
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        auto& mirror_topics = link_metadata->state.mirror_topics;
        auto mirror_topic_it = mirror_topics.find(::model::topic("test_topic"));
        return mirror_topic_it != mirror_topics.end();
    });

    auto link_metadata = fixture()->find_link_by_name(
      model::name_t("test_link"));
    auto& mirror_topics = link_metadata->state.mirror_topics;
    auto mirror_topic_it = mirror_topics.find(
      ::model::topic("not_requested_topic"));
    EXPECT_EQ(mirror_topic_it, mirror_topics.end())
      << "Excluded topic should not be mirrored";
}

class unsupported_describe_configs_test : public source_topic_syncer_test {
public:
    virtual ss::future<> SetUpAsync() override {
        co_await source_topic_syncer_test::SetUpAsync();
        fixture()->get_cluster_mock().register_handler(
          kafka::api_versions_api::key,
          [this](
            ::model::node_id id,
            kafka::client::request_t req,
            kafka::api_version v) {
              return fixture()
                ->get_cluster_mock()
                .handle_api_versions_request(id, std::move(req), v)
                .then([this](kafka::client::response_t resp) {
                    return handle_api_versions_request(std::move(resp));
                });
          });
    }

    ss::future<kafka::client::response_t>
    handle_api_versions_request(kafka::client::response_t resp) {
        auto modified_resp = std::get<kafka::api_versions_response>(
          std::move(resp));

        // Rearrange vector so the describe_configs api is at the end and
        // then pop it off
        std::ranges::partition(
          modified_resp.data.api_keys,
          [](const kafka::api_versions_response_key& key) {
              return key.api_key != kafka::describe_configs_api::key;
          });
        modified_resp.data.api_keys.pop_back_n(1);

        co_return modified_resp;
    }
};

TEST_F_CORO(unsupported_describe_configs_test, unsupported_describe_configs) {
    co_await fixture()->upsert_link(get_default_metadata());

    auto report = co_await fixture()->await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          auto link_it = report.link_reports.find(model::name_t("test_link"));
          if (link_it == report.link_reports.end()) {
              return false;
          }
          auto task_it = link_it->second.task_status_reports.find(
            source_topic_syncer::task_name);
          if (task_it == link_it->second.task_status_reports.end()) {
              return false;
          }

          return true;
      });

    ASSERT_TRUE_CORO(report.has_value()) << "Never received a task report";

    auto& task_report = report.value()
                          .link_reports.at(model::name_t("test_link"))
                          .task_status_reports.at(
                            source_topic_syncer::task_name);
    EXPECT_EQ(task_report.task_state, model::task_state::link_unavailable);
}
TEST_F_CORO(source_topic_syncer_test, cloud_topic_mirrored) {
    auto cloud_topic = ::model::topic("cloud-topic");
    auto normal_topic = ::model::topic("normal-topic");

    fixture()->get_cluster_mock().add_topic(
      cloud_topic, 3, 3, kafka::topic_authorized_operations(0x508));
    fixture()->get_cluster_mock().add_topic(
      normal_topic, 3, 3, kafka::topic_authorized_operations(0x508));

    ::cluster::topic_properties cloud_props;
    cloud_props.storage_mode = ::model::redpanda_storage_mode::cloud;
    fixture()->get_cluster_mock().set_topic_properties(
      cloud_topic, std::move(cloud_props));

    co_await fixture()->upsert_link(get_default_metadata());

    // Both topics should be mirrored
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, &normal_topic] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        return link_metadata->state.mirror_topics.contains(normal_topic);
    });

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, &cloud_topic] {
        auto link_metadata = fixture()->find_link_by_name(
          model::name_t("test_link"));
        return link_metadata->state.mirror_topics.contains(cloud_topic);
    });
}

} // namespace cluster_link::tests
