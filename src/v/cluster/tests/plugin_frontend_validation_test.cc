/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/commands.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/fwd.h"
#include "cluster/plugin_frontend.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/transform.h"
#include "raft/fundamental.h"
#include "utils/uuid.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/range/irange.hpp>
#include <gtest/gtest.h>

#include <string_view>
#include <vector>

namespace cluster {

namespace {

using id = model::transform_id;
using name = model::transform_name;
using meta = model::transform_metadata;
constexpr size_t max_transforms = 5;

struct topic_config {
    int partition_count = 1;
    bool read_replica = false;
};

struct transform_config {
    ss::sstring name;
    ss::sstring src;
    std::vector<ss::sstring> sinks;
    absl::flat_hash_map<ss::sstring, ss::sstring> env;
};

ss::sstring make_string(size_t size) {
    ss::sstring str;
    for (size_t i = 0; i < size; ++i) {
        str.append("a", 1);
    }
    return str;
}

absl::flat_hash_map<ss::sstring, ss::sstring> make_env_map(size_t size) {
    absl::flat_hash_map<ss::sstring, ss::sstring> env;
    for (size_t i = 0; i < size; ++i) {
        env.insert({ss::format("var_{}", i), "bar"});
    }
    return env;
}

class PluginValidationTest : public testing::Test {
public:
    model::offset _latest_offset{0};
    model::transform_id _latest_id{0};

    plugin_table _plugin_table;
    data_migrations::migrated_resources _migrated_resources;
    topic_table _topic_table{_migrated_resources};
    plugin_frontend::validator _validator{
      &_topic_table,
      &_plugin_table,
      {model::topic("__internal_topic")},
      max_transforms};

    std::error_code
    create_topic(std::string_view name, topic_config config = {}) {
        topic_configuration topic_cfg(
          model::kafka_namespace,
          model::topic(name),
          config.partition_count,
          /*replication_factor=*/1);
        topic_cfg.properties.read_replica = config.read_replica;
        std::vector<model::broker_shard> broker_shards;
        broker_shards.push_back(model::broker_shard{
          .node_id = model::node_id(1),
          .shard = 0,
        });
        ss::chunked_fifo<partition_assignment> partition_assignments;
        for (int i = 1; i <= config.partition_count; ++i) {
            partition_assignments.push_back(partition_assignment(
              raft::group_id(), model::partition_id(i), broker_shards));
        }
        create_topic_cmd cmd(
          topic_cfg.tp_ns,
          topic_configuration_assignment(
            topic_cfg, std::move(partition_assignments)));
        return _topic_table.apply(std::move(cmd), ++_latest_offset).get();
    }
    errc upsert_transform(transform_config config) {
        std::vector<model::topic_namespace> outputs;
        outputs.reserve(config.sinks.size());
        for (const ss::sstring& topic : config.sinks) {
            outputs.emplace_back(model::kafka_namespace, model::topic(topic));
        }
        transform_update_cmd cmd{
          0,
          meta{
            .name = name(config.name),
            .input_topic = model::topic_namespace(
              model::kafka_namespace, model::topic(config.src)),
            .output_topics = outputs,
            .environment = config.env,
            .uuid = uuid_t::create(),
            .source_ptr = ++_latest_offset,
          }};
        auto ec = _validator.validate_mutation(cmd);
        if (ec == errc::success) {
            auto existing = _plugin_table.find_id_by_name(config.name);
            auto id = existing.value_or(++_latest_id);
            _plugin_table.upsert_transform(id, cmd.value);
        }
        return ec;
    }
    errc delete_transform(std::string_view transform_name) {
        transform_remove_cmd cmd{name(transform_name), 0};
        auto ec = _validator.validate_mutation(cmd);
        if (ec == errc::success) {
            _plugin_table.remove_transform(cmd.key);
        }
        return ec;
    }
    std::error_code set_topic_disabled(std::string_view name, bool disabled) {
        set_topic_partitions_disabled_cmd cmd(
          0,
          set_topic_partitions_disabled_cmd_data{
            .ns_tp = model::
              topic_namespace{model::kafka_namespace, model::topic{name}},
            .disabled = disabled});
        return _topic_table.apply(std::move(cmd), ++_latest_offset).get();
    }
};

} // namespace

TEST_F(PluginValidationTest, ValidateSuccess) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "qux",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::success);
}

TEST_F(PluginValidationTest, TooManyOutputTopics) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    for (int i : boost::irange(10)) {
        EXPECT_EQ(create_topic(ss::format("bar_{}", i)), errc::success);
    }
    EXPECT_EQ(
      upsert_transform({
        .name = "qux",
        .src = "foo",
        .sinks = {
          "bar_0",
          "bar_1",
          "bar_2",
          "bar_3",
          "bar_4",
          "bar_5",
          "bar_6",
          "bar_7",
          "bar_8",
          "bar_9",
        },
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(
      upsert_transform({
        .name = "qux",
        .src = "foo",
        .sinks = {
          "bar_0",
          "bar_1",
          "bar_2",
          "bar_3",
          "bar_4",
          "bar_5",
          "bar_6",
          "bar_7",
        },
      }),
      errc::success);
}

TEST_F(PluginValidationTest, MissingTopic) {
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "qux",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::topic_not_exists);
    EXPECT_EQ(
      upsert_transform({
        .name = "qux",
        .src = "bar",
        .sinks = {"foo"},
      }),
      errc::topic_not_exists);
}

TEST_F(PluginValidationTest, NoCycles) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(create_topic("baz"), errc::success);
    EXPECT_EQ(create_topic("qux"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "x1",
        .src = "foo",
        .sinks = {"foo"},
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(
      upsert_transform({
        .name = "x1",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "x2",
        .src = "bar",
        .sinks = {"foo"},
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(
      upsert_transform({
        .name = "x2",
        .src = "bar",
        .sinks = {"baz"},
      }),
      errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "x3",
        .src = "baz",
        .sinks = {"qux"},
      }),
      errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "x4",
        .src = "foo",
        .sinks = {"qux"},
      }),
      errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "x5",
        .src = "qux",
        .sinks = {"foo"},
      }),
      errc::transform_invalid_create);
}

TEST_F(PluginValidationTest, InvalidEnvironment) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    auto expect_invalid_env =
      [this](const absl::flat_hash_map<ss::sstring, ss::sstring>& env) {
          EXPECT_EQ(
            upsert_transform({
              .name = "qux",
              .src = "foo",
              .sinks = {"bar"},
              .env = env,
            }),
            errc::transform_invalid_environment)
            << "environment: " << env;
      };
    expect_invalid_env({
      {"REDPANDA_FOO", "bar"},
    });
    expect_invalid_env({
      // invalid UTF8
      {"FOO\xc3\x28", "bar"},
    });
    expect_invalid_env({
      // invalid UTF8
      {"FOO", "bar\xc3\x28"},
    });
    expect_invalid_env({
      // key too big
      {make_string(1_KiB), "alskdj"},
    });
    expect_invalid_env({
      // value too big
      {"foo", make_string(3_KiB)},
    });
    expect_invalid_env(
      // too many values
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
      make_env_map(130));
}

TEST_F(PluginValidationTest, NoReadReplicas) {
    EXPECT_EQ(create_topic("foo", {.read_replica = true}), errc::success);
    EXPECT_EQ(create_topic("bar", {.read_replica = true}), errc::success);
    EXPECT_EQ(create_topic("qux"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "no-read-replica",
        .src = "foo",
        .sinks = {"qux"},
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(
      upsert_transform({
        .name = "no-read-replica",
        .src = "qux",
        .sinks = {"bar"},
      }),
      errc::transform_invalid_create);
}

TEST_F(PluginValidationTest, RemoveMustExist) {
    EXPECT_EQ(delete_transform("bar2foo"), errc::transform_does_not_exist);
}

TEST_F(PluginValidationTest, DeleteWorks) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "foo2bar",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::success);
    EXPECT_EQ(delete_transform("foo2bar"), errc::success);
}

TEST_F(PluginValidationTest, UpdateCannotChangeTopicConfig) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(create_topic("qux"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "foo2bar",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "foo2bar",
        .src = "bar",
        .sinks = {"foo"},
      }),
      errc::transform_invalid_update);
    EXPECT_EQ(
      upsert_transform({
        .name = "foo2bar",
        .src = "foo",
        .sinks = {"qux"},
      }),
      errc::transform_invalid_update);
    EXPECT_EQ(
      upsert_transform({
        .name = "foo2bar",
        .src = "qux",
        .sinks = {"bar"},
      }),
      errc::transform_invalid_update);
    EXPECT_EQ(
      upsert_transform({
        .name = "foo2bar",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::success);
}

TEST_F(PluginValidationTest, InternalTopicsCannotBeDeployedToAsOutput) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(create_topic("__internal_topic"), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "not_so_fast",
        .src = "foo",
        .sinks = {"__internal_topic"},
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(
      upsert_transform({
        .name = "pass_go_and_collect_200_dollars",
        .src = "__internal_topic",
        .sinks = {"bar"},
      }),
      errc::success);
}

TEST_F(PluginValidationTest, NoDisabledTopics) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    EXPECT_EQ(set_topic_disabled("bar", true), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "baz",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(
      upsert_transform({
        .name = "baz",
        .src = "bar",
        .sinks = {"foo"},
      }),
      errc::transform_invalid_create);
    EXPECT_EQ(set_topic_disabled("bar", false), errc::success);
    EXPECT_EQ(
      upsert_transform({
        .name = "baz",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::success);
}

TEST_F(PluginValidationTest, TotalTransformsLimited) {
    EXPECT_EQ(create_topic("foo"), errc::success);
    EXPECT_EQ(create_topic("bar"), errc::success);
    for (size_t i = 0; i < max_transforms; ++i) {
        EXPECT_EQ(
          upsert_transform({
            .name = ss::format("baz_{}", i),
            .src = "foo",
            .sinks = {"bar"},
          }),
          errc::success);
    }
    EXPECT_EQ(
      upsert_transform({
        .name = "over-the-limit",
        .src = "foo",
        .sinks = {"bar"},
      }),
      errc::transform_count_limit_exceeded);
}

} // namespace cluster
