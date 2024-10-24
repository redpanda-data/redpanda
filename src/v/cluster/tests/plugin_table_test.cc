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

#include "cluster/plugin_table.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/transform.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <initializer_list>
#include <optional>
#include <stdexcept>

namespace cluster {
namespace {
using id = model::transform_id;
using meta = model::transform_metadata;
using name = model::transform_name;
using plugin_map = absl::btree_map<id, meta>;

struct metadata_spec {
    ss::sstring name;
    ss::sstring input;
    std::vector<ss::sstring> output;
    absl::flat_hash_map<ss::sstring, ss::sstring> env;
};
absl::flat_hash_set<id> ids(std::initializer_list<int> ids) {
    absl::flat_hash_set<id> output;
    for (int id : ids) {
        output.emplace(id);
    }
    return output;
}

model::topic_namespace topic(ss::sstring t) {
    return {model::kafka_namespace, model::topic(t)};
}
meta make_meta(const metadata_spec& spec) {
    std::vector<model::topic_namespace> outputs;
    outputs.reserve(spec.output.size());
    for (const auto& name : spec.output) {
        outputs.emplace_back(topic(name));
    }
    return {
      .name = name(spec.name),
      .input_topic = topic(spec.input),
      .output_topics = std::move(outputs),
      .environment = spec.env,
    };
}
} // namespace

TEST(PluginTable, Mutations) {
    cluster::plugin_table table;

    auto wheat_millwheel = make_meta(
      {.name = "millwheel", .input = "wheat", .output = {"flour"}});
    auto cow = make_meta({.name = "cow", .input = "grass", .output = {"milk"}});
    auto lawn = make_meta(
      {.name = "lawn", .input = "water", .output = {"grass", "weeds"}});
    auto corn_millwheel = make_meta(
      {.name = "millwheel", .input = "corn", .output = {"cornmeal"}});

    table.upsert_transform(id(1), wheat_millwheel);
    table.upsert_transform(id(2), cow);
    table.upsert_transform(id(3), lawn);
    // Names must be unique
    EXPECT_THROW(
      table.upsert_transform(id(4), corn_millwheel), std::logic_error);

    table.upsert_transform(id(1), corn_millwheel);

    plugin_map want{
      {id(1), corn_millwheel},
      {id(2), cow},
      {id(3), lawn},
    };
    EXPECT_EQ(table.all_transforms(), want);
    table.remove_transform(name("cow"));
    want.erase(id(2));
    EXPECT_EQ(table.all_transforms(), want);
    table.reset_transforms(want);
    EXPECT_EQ(table.all_transforms(), want);
    want.erase(id(1));
    auto goat = make_meta(
      {.name = "goat", .input = "grass", .output = {"milk"}});
    want.emplace(id(4), goat);
    table.reset_transforms(want);
    EXPECT_EQ(table.all_transforms(), want);
}

TEST(PluginTable, Notifications) {
    cluster::plugin_table table;
    absl::flat_hash_set<id> notifications;
    auto notif_id = table.register_for_updates([&](auto id) {
        auto [it, inserted] = notifications.emplace(id);
        // To ensure we don't get duplicate notifications, make sure there are
        // always imports and we just clear the notifications after assertions
        EXPECT_TRUE(inserted);
    });
    auto miracle = make_meta(
      {.name = "miracle", .input = "water", .output = {"wine"}});
    auto optimus_prime = make_meta(
      {.name = "optimus_prime", .input = "semi", .output = {"hero"}});
    auto optimus_prime2 = make_meta(
      {.name = "optimus_prime",
       .input = "semi",
       .output = {"hero"},
       .env = {{"allspark", "enabled"}}});
    auto parser = make_meta(
      {.name = "parser", .input = "text", .output = {"ast"}});
    auto compiler = make_meta(
      {.name = "compiler", .input = "awesomelang", .output = {"assembly"}});

    table.upsert_transform(id(1), miracle);
    EXPECT_EQ(notifications, ids({1}));
    notifications.clear();

    table.upsert_transform(id(2), optimus_prime);
    EXPECT_EQ(notifications, ids({2}));
    notifications.clear();

    table.remove_transform(miracle.name);
    EXPECT_EQ(notifications, ids({1}));
    notifications.clear();

    table.upsert_transform(id(3), miracle);
    table.upsert_transform(id(4), parser);
    EXPECT_EQ(notifications, ids({3, 4}));
    notifications.clear();

    table.reset_transforms(plugin_map({
      // changed
      {id(2), optimus_prime2},
      // unchanged
      {id(3), miracle},
      // removed id(4)
      // added 5
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
      {id(5), compiler},
    }));
    EXPECT_EQ(notifications, ids({2, 4, 5}));
    notifications.clear();

    table.unregister_for_updates(notif_id);
    table.remove_transform(miracle.name);
    EXPECT_TRUE(notifications.empty());
}

TEST(PluginTable, TopicIndexes) {
    static const std::optional<meta> missing = std::nullopt;
    static const std::optional<id> missing_id = std::nullopt;
    cluster::plugin_table table;

    auto millwheel = make_meta(
      {.name = "millwheel", .input = "wheat", .output = {"flour"}});
    table.upsert_transform(id(1), millwheel);
    auto cow = make_meta({.name = "cow", .input = "grass", .output = {"milk"}});
    table.upsert_transform(id(2), cow);
    auto lawn = make_meta(
      {.name = "lawn", .input = "water", .output = {"grass", "weeds"}});
    table.upsert_transform(id(3), lawn);
    auto miller = make_meta(
      {.name = "miller", .input = "corn", .output = {"flour", "cornmeal"}});
    table.upsert_transform(id(4), miller);
    EXPECT_EQ(table.find_by_name("lawn"), std::make_optional(lawn));
    EXPECT_EQ(table.find_id_by_name("miller"), std::make_optional(id(4)));
    EXPECT_EQ(table.find_by_name("law"), missing);
    EXPECT_EQ(table.find_id_by_name("miter"), missing_id);
    EXPECT_EQ(table.find_by_id(id(2)), std::make_optional(cow));
    EXPECT_EQ(table.find_by_id(id(99)), missing);
    EXPECT_EQ(
      table.find_by_input_topic(topic("grass")), plugin_map({{id(2), cow}}));
    EXPECT_EQ(
      table.find_by_output_topic(topic("flour")),
      plugin_map({{id(1), millwheel}, {id(4), miller}}));
    EXPECT_EQ(table.find_by_input_topic(topic("waldo")), plugin_map());
}
} // namespace cluster
