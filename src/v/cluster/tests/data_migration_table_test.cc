// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/commands.h"
#include "cluster/data_migration_table.h"
#include "cluster/data_migration_types.h"
#include "commands.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "data_migrated_resources.h"
#include "data_migration_types.h"
#include "errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "test_utils/test.h"
#include "topic_table.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <system_error>
namespace cluster::data_migrations::testing_details {
class data_migration_table_test_accessor {
public:
    explicit data_migration_table_test_accessor(
      cluster::data_migrations::migrations_table& t)
      : _t(t) {}

    data_migrations::id get_next_id() { return _t.get_next_id(); }

private:
    cluster::data_migrations::migrations_table& _t;
};
} // namespace cluster::data_migrations::testing_details

using namespace cluster;
template<typename Cmd, typename T>
model::record_batch create_cmd(T data) {
    return cluster::serde_serialize_cmd(Cmd(0, std::move(data)));
}

chunked_vector<model::topic_namespace>
create_topic_vector(std::vector<std::string_view> topics) {
    chunked_vector<model::topic_namespace> ret;
    ret.reserve(topics.size());
    for (auto& t : topics) {
        ret.emplace_back(model::kafka_namespace, model::topic(t));
    }

    return ret;
}

chunked_vector<cluster::data_migrations::inbound_topic>
create_inbound_topics(std::vector<std::string_view> topics) {
    chunked_vector<cluster::data_migrations::inbound_topic> ret;
    ret.reserve(topics.size());
    for (auto& t : topics) {
        ret.push_back(cluster::data_migrations::inbound_topic{
          .source_topic_name = model::topic_namespace(
            model::kafka_namespace, model::topic(t))});
    }
    return ret;
}

static chunked_vector<cluster::data_migrations::consumer_group>
create_groups(std::vector<std::string_view> strings) {
    chunked_vector<cluster::data_migrations::consumer_group> ret;
    std::ranges::transform(
      strings, std::back_inserter(ret), [](std::string_view s) {
          return cluster::data_migrations::consumer_group(s);
      });
    return ret;
}

struct data_migration_table_fixture : public seastar_test {
    ss::future<> SetUpAsync() override {
        // for all new topics to be created with it
        config::shard_local_cfg().cloud_storage_enable_remote_write.set_value(
          true);

        co_await resources.start();
        co_await topics.start(ss::sharded_parameter(
          [this] { return std::ref(resources.local()); }));
        table = std::make_unique<cluster::data_migrations::migrations_table>(
          resources, topics, true);
        table->register_notification([this](cluster::data_migrations::id id) {
            notifications.push_back(id);
        });
    }
    ss::future<> TearDownAsync() override {
        table->unregister_notification(notification_id);
        co_await topics.stop();
        co_await resources.stop();
    }

    data_migrations::id get_next_id() {
        return cluster::data_migrations::testing_details::
          data_migration_table_test_accessor(*table)
            .get_next_id();
    }

    ss::future<> populate_topic_table_with_topics(
      const chunked_vector<model::topic_namespace>& to_create) {
        for (const auto& tp_ns : to_create) {
            auto p_cnt = random_generators::get_int(1, 64);

            topic_configuration cfg(tp_ns.ns, tp_ns.tp, p_cnt, 3);
            cfg.properties.shadow_indexing = model::shadow_indexing_mode::full;
            ss::chunked_fifo<partition_assignment> assignments;
            for (auto i = 0; i < p_cnt; ++i) {
                assignments.push_back(
                  partition_assignment(group_id++, model::partition_id(i), {}));
            }
            topic_configuration_assignment cfg_a(
              std::move(cfg), std::move(assignments));

            create_topic_cmd cmd(tp_ns, std::move(cfg_a));

            co_await topics.local().apply(std::move(cmd), command_offset++);
        }
    }

    ss::future<std::error_code> apply_update_state(
      cluster::data_migrations::id id, cluster::data_migrations::state state) {
        return table->apply_update(
          create_cmd<cluster::update_data_migration_state_cmd>(
            cluster::data_migrations::update_migration_state_cmd_data{
              .id = id, .requested_state = state}));
    }

    ss::future<std::error_code>
    apply_remove_migration(cluster::data_migrations::id id) {
        return table->apply_update(
          create_cmd<cluster::remove_data_migration_cmd>(
            data_migrations::remove_migration_cmd_data{.id = id}));
    }

    ss::future<std::error_code> progress_through_states(
      cluster::data_migrations::id id,
      std::vector<cluster::data_migrations::state> states) {
        auto sz_before = notifications.size();
        for (auto s : states) {
            auto err = co_await apply_update_state(id, s);
            if (err) {
                co_return err;
            }
            EXPECT_EQ(notifications.size(), sz_before + 1);
            EXPECT_EQ(notifications.back(), id);
            sz_before = notifications.size();
        }
        co_return cluster::errc::success;
    }

    ss::future<result<data_migrations::id>>
    try_create_migration(data_migrations::data_migration m) {
        auto id = get_next_id();
        auto ec = co_await table->apply_update(
          create_cmd<cluster::create_data_migration_cmd>(
            cluster::data_migrations::create_migration_cmd_data{
              .id = id,
              .migration = std::move(m),
            }));
        if (ec) {
            co_return ec;
        }
        co_return id;
    }

    ss::future<cluster::data_migrations::id>
    create_migration(data_migrations::data_migration m) {
        auto r = co_await try_create_migration(std::move(m));
        EXPECT_TRUE(r.has_value());
        co_return r.value();
    }

    ss::future<cluster::data_migrations::id> create_simple_migration() {
        auto topics = create_topic_vector({"t-1", "t-2", "t-3"});
        co_await populate_topic_table_with_topics(topics);
        co_return co_await create_migration(
          cluster::data_migrations::outbound_migration{
            .topics = std::move(topics),
            .groups = create_groups({"g-1", "g-2"})});
    }

    void validate_group_resource_state(
      const std::vector<
        std::pair<ss::sstring, data_migrations::migrated_resource_state>>&
        expected_states) {
        for (auto& expected : expected_states) {
            EXPECT_EQ(
              resources.local().get_group_state(
                data_migrations::consumer_group{expected.first}),
              expected.second);
        }
    }

    void validate_topic_resource_state(
      const std::vector<
        std::pair<ss::sstring, data_migrations::migrated_resource_state>>&
        expected_states) {
        for (auto& expected : expected_states) {
            EXPECT_EQ(
              resources.local().get_topic_state(model::topic_namespace(
                model::kafka_namespace, model::topic(expected.first))),
              expected.second);
        }
    }

    cluster::data_migrations::migrations_table::notification_id notification_id;
    ss::sharded<cluster::topic_table> topics;
    ss::sharded<cluster::data_migrations::migrated_resources> resources;
    std::unique_ptr<cluster::data_migrations::migrations_table> table;

    std::vector<cluster::data_migrations::id> notifications;
    model::offset command_offset{0};
    raft::group_id group_id{0};
};

TEST_F_CORO(data_migration_table_fixture, test_crud_operations) {
    auto id_1 = get_next_id();
    auto topics = create_topic_vector({"t-1", "t-2", "t-3"});
    co_await populate_topic_table_with_topics(topics);
    validate_group_resource_state(
      {{"g-1", data_migrations::migrated_resource_state::non_restricted},
       {"g-2", data_migrations::migrated_resource_state::non_restricted}});

    validate_topic_resource_state(
      {{"t-1", data_migrations::migrated_resource_state::non_restricted},
       {"t-2", data_migrations::migrated_resource_state::non_restricted},
       {"t-3", data_migrations::migrated_resource_state::non_restricted}});

    auto r = co_await table->apply_update(
      create_cmd<cluster::create_data_migration_cmd>(
        cluster::data_migrations::create_migration_cmd_data{
          .id = id_1,
          .migration = cluster::data_migrations::outbound_migration{
            .topics = topics.copy(),
            .groups = create_groups({"g-1", "g-2"})}}));

    EXPECT_EQ(r, cluster::errc::success);
    EXPECT_EQ(notifications.back(), id_1);

    validate_group_resource_state(
      {{"g-1", data_migrations::migrated_resource_state::metadata_locked},
       {"g-2", data_migrations::migrated_resource_state::metadata_locked}});

    validate_topic_resource_state(
      {{"t-1", data_migrations::migrated_resource_state::metadata_locked},
       {"t-2", data_migrations::migrated_resource_state::metadata_locked},
       {"t-3", data_migrations::migrated_resource_state::metadata_locked}});

    // create migration with the same id, this should fail
    r = co_await table->apply_update(
      create_cmd<cluster::create_data_migration_cmd>(
        cluster::data_migrations::create_migration_cmd_data{
          .id = id_1,
          .migration = cluster::data_migrations::inbound_migration{
            .topics = {}, .groups = create_groups({"g-1", "g-2"})}}));

    EXPECT_EQ(r, cluster::errc::data_migration_already_exists);
    EXPECT_EQ(notifications.size(), 1);

    auto id_2 = get_next_id();
    /**
     * Create inbound migration with new id
     */
    r = co_await table->apply_update(
      create_cmd<cluster::create_data_migration_cmd>(
        cluster::data_migrations::create_migration_cmd_data{
          .id = id_2,
          .migration = cluster::data_migrations::inbound_migration{
            .topics = create_inbound_topics({"in-t-1"}),
            .groups = create_groups({"g-3", "g-4"})}}));

    validate_group_resource_state(
      {{"g-3", data_migrations::migrated_resource_state::metadata_locked},
       {"g-4", data_migrations::migrated_resource_state::metadata_locked}});

    validate_topic_resource_state(
      {{"in-t-1", data_migrations::migrated_resource_state::metadata_locked}});

    EXPECT_EQ(r, cluster::errc::success);
    EXPECT_EQ(notifications.back(), id_2);

    auto metadata = table->list_migrations();
    EXPECT_EQ(metadata.size(), 2);

    // delete the first migration
    r = co_await apply_remove_migration(id_1);

    EXPECT_EQ(r, cluster::errc::success);
    EXPECT_EQ(notifications.back(), id_1);
    validate_group_resource_state(
      {{"g-1", data_migrations::migrated_resource_state::non_restricted},
       {"g-2", data_migrations::migrated_resource_state::non_restricted}});

    validate_topic_resource_state(
      {{"t-1", data_migrations::migrated_resource_state::non_restricted},
       {"t-2", data_migrations::migrated_resource_state::non_restricted},
       {"t-3", data_migrations::migrated_resource_state::non_restricted}});

    // try deleting non existing migration
    r = co_await apply_remove_migration(cluster::data_migrations::id(10));
    EXPECT_EQ(r, cluster::errc::data_migration_not_exists);
    EXPECT_EQ(notifications.size(), 3);

    // try updating state of non existing migration
    r = co_await table->apply_update(
      create_cmd<cluster::update_data_migration_state_cmd>(
        cluster::data_migrations::update_migration_state_cmd_data{
          .id = id_1,
          .requested_state = cluster::data_migrations::state::preparing}));
    EXPECT_EQ(r, cluster::errc::data_migration_not_exists);
    EXPECT_EQ(notifications.size(), 3);

    // try updating state of migration that exists
    r = co_await table->apply_update(
      create_cmd<cluster::update_data_migration_state_cmd>(
        cluster::data_migrations::update_migration_state_cmd_data{
          .id = id_2,
          .requested_state = cluster::data_migrations::state::preparing}));
    EXPECT_EQ(r, cluster::errc::success);
    EXPECT_EQ(notifications.size(), 4);
    EXPECT_EQ(notifications.back(), id_2);

    EXPECT_EQ(
      table->get_migration(id_2)->get().state,
      cluster::data_migrations::state::preparing);
}

TEST_F_CORO(data_migration_table_fixture, test_migration_stm_happy_path) {
    auto id_1 = get_next_id();
    auto topics = create_topic_vector({"t-1", "t-2", "t-3"});

    co_await populate_topic_table_with_topics(topics);
    auto r = co_await table->apply_update(
      create_cmd<cluster::create_data_migration_cmd>(
        cluster::data_migrations::create_migration_cmd_data{
          .id = id_1,
          .migration = cluster::data_migrations::outbound_migration{
            .topics = std::move(topics),
            .groups = create_groups({"g-1", "g-2"})}}));

    EXPECT_EQ(r, cluster::errc::success);
    EXPECT_EQ(notifications.back(), id_1);

    r = co_await progress_through_states(
      id_1,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::prepared,
        cluster::data_migrations::state::executing,
        cluster::data_migrations::state::executed,
        cluster::data_migrations::state::cut_over,
        cluster::data_migrations::state::finished,
      });

    EXPECT_EQ(r, cluster::errc::success);
}

TEST_F_CORO(data_migration_table_fixture, test_invalid_transition) {
    auto id_1 = co_await create_simple_migration();

    // missing prepared state
    auto r = co_await progress_through_states(
      id_1,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::executing,
        cluster::data_migrations::state::executed,
        cluster::data_migrations::state::cut_over,
        cluster::data_migrations::state::finished,
      });

    EXPECT_EQ(r, cluster::errc::invalid_data_migration_state);
    // cancel and remove migration

    r = co_await progress_through_states(
      id_1,
      {cluster::data_migrations::state::canceling,
       cluster::data_migrations::state::cancelled});

    EXPECT_EQ(r, cluster::errc::success);
    co_await apply_remove_migration(id_1);

    auto id_2 = co_await create_simple_migration();

    // moving back to executed
    r = co_await progress_through_states(
      id_2,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::prepared,
        cluster::data_migrations::state::executing,
        cluster::data_migrations::state::executed,
        cluster::data_migrations::state::cut_over,
        cluster::data_migrations::state::finished,
        cluster::data_migrations::state::executed,
      });

    EXPECT_EQ(r, cluster::errc::invalid_data_migration_state);
}

TEST_F_CORO(data_migration_table_fixture, test_cancellation) {
    auto id = co_await create_simple_migration();

    // cancel after preparing
    auto r = co_await progress_through_states(
      id,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::canceling,
        cluster::data_migrations::state::cancelled,
      });
    EXPECT_EQ(r, cluster::errc::success);
    r = co_await apply_remove_migration(id);

    id = co_await create_simple_migration();

    // cancel after preparing
    r = co_await progress_through_states(
      id,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::prepared,
        cluster::data_migrations::state::canceling,
        cluster::data_migrations::state::cancelled,
      });

    EXPECT_EQ(r, cluster::errc::success);
    r = co_await apply_remove_migration(id);
    id = co_await create_simple_migration();
    // cancel after preparing
    r = co_await progress_through_states(
      id,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::prepared,
        cluster::data_migrations::state::executing,
        cluster::data_migrations::state::canceling,
        cluster::data_migrations::state::cancelled,
      });

    EXPECT_EQ(r, cluster::errc::success);
    /**
     * Can not cancel after finished
     */
    r = co_await apply_remove_migration(id);
    id = co_await create_simple_migration();
    r = co_await progress_through_states(
      id,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::prepared,
        cluster::data_migrations::state::executing,
        cluster::data_migrations::state::executed,
        cluster::data_migrations::state::cut_over,
        cluster::data_migrations::state::finished,
        cluster::data_migrations::state::canceling,
      });

    EXPECT_EQ(r, cluster::errc::invalid_data_migration_state);
    /**
     * Can not cancel after cut_over
     */
    r = co_await apply_remove_migration(id);
    id = co_await create_simple_migration();
    r = co_await progress_through_states(
      id,
      {
        cluster::data_migrations::state::preparing,
        cluster::data_migrations::state::prepared,
        cluster::data_migrations::state::executing,
        cluster::data_migrations::state::executed,
        cluster::data_migrations::state::cut_over,
        cluster::data_migrations::state::canceling,
      });

    EXPECT_EQ(r, cluster::errc::invalid_data_migration_state);
}

TEST_F_CORO(data_migration_table_fixture, test_resource_validation) {
    data_migrations::outbound_migration odm{
      .topics = create_topic_vector({"topic-1", "topic-2", "topic-3"}),
      .groups = create_groups({"gr-1", "gr-2", "gr-3"})};

    data_migrations::inbound_migration invalid_idm{
      .topics = create_inbound_topics({"topic-1"}),
      .groups = create_groups({"gr-4", "gr-5"})};

    auto inbound_topics = create_inbound_topics({"topic-1"});
    inbound_topics[0].alias = model::topic_namespace(
      model::kafka_namespace, model::topic("alias-of-topic-1"));
    data_migrations::inbound_migration idm_with_alias{
      .topics = inbound_topics.copy(),
      .groups = create_groups({"gr-4", "gr-5"})};

    /**
     * Requested topics do not exists, migration creation should fail
     */
    auto r = co_await try_create_migration(odm.copy());
    EXPECT_TRUE(r.has_error());
    EXPECT_EQ(r.error(), cluster::errc::data_migration_invalid_resources);

    // create topics, retry migration creation
    co_await populate_topic_table_with_topics(odm.topics);

    r = co_await try_create_migration(odm.copy());
    EXPECT_TRUE(r.has_value());

    validate_group_resource_state({
      {"gr-1", data_migrations::migrated_resource_state::metadata_locked},
      {"gr-2", data_migrations::migrated_resource_state::metadata_locked},
      {"gr-3", data_migrations::migrated_resource_state::metadata_locked},
    });

    validate_topic_resource_state({
      {"topic-1", data_migrations::migrated_resource_state::metadata_locked},
      {"topic-2", data_migrations::migrated_resource_state::metadata_locked},
      {"topic-3", data_migrations::migrated_resource_state::metadata_locked},
    });

    /**
     * Creation of invalid_idm should fail as the topic-1 is already in the
     * table
     */
    r = co_await try_create_migration(invalid_idm.copy());
    EXPECT_TRUE(r.has_error());
    EXPECT_EQ(r.error(), cluster::errc::data_migration_invalid_resources);
    // valid idm should be created
    r = co_await try_create_migration(idm_with_alias.copy());
    EXPECT_TRUE(r.has_value());
    validate_topic_resource_state({
      {"alias-of-topic-1",
       data_migrations::migrated_resource_state::metadata_locked},
    });
}
