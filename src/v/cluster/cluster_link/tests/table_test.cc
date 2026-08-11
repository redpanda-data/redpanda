/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/table_utils.h"
#include "cluster/cluster_link/tests/utils.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/types.h"
#include "cluster_link/model/types.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

namespace cluster::cluster_link {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::connection_config;
using ::cluster_link::model::id_t;
using ::cluster_link::model::link_configuration;
using ::cluster_link::model::link_state;
using ::cluster_link::model::metadata;
using ::cluster_link::model::mirror_topic_metadata;
using ::cluster_link::model::mirror_topic_status;
using ::cluster_link::model::name_t;
using ::cluster_link::model::topic_metadata_mirroring_config;
using ::cluster_link::model::update_cluster_link_configuration_cmd;
using ::cluster_link::model::update_mirror_topic_properties_cmd;
using ::cluster_link::model::update_mirror_topic_status_cmd;
using ::cluster_link::model::uuid_t;

namespace {
bool snapshot_equals(
  const chunked_hash_map<id_t, metadata>& lhs, const table::map_t& rhs) {
    for (const auto& [id, md] : lhs) {
        auto it = rhs.find(id);
        if (it == rhs.end()) {
            return false;
        }
        EXPECT_EQ(*(it->second), md);
    }
    return true;
}
} // namespace

class cluster_link_table_test : public seastar_test {
public:
    virtual ss::future<> SetUpAsync() override { co_await _table.start(); }

    virtual ss::future<> TearDownAsync() override { co_await _table.stop(); }

protected:
    ss::sharded<table> _table;
};

TEST_F_CORO(cluster_link_table_test, empty) {
    ASSERT_EQ_CORO(_table.local().size(), 0);
}

TEST_F_CORO(cluster_link_table_test, reset_links) {
    table::map_t links;
    links.emplace(
      id_t(1),
      ss::make_lw_shared<metadata>(
        {.name = name_t("link1"),
         .uuid = uuid_t(::uuid_t::create()),
         .connection = connection_config{}}));
    links.emplace(
      id_t(2),
      ss::make_lw_shared<metadata>(
        {.name = name_t("link2"),
         .uuid = uuid_t(::uuid_t::create()),
         .connection = connection_config{}}));

    cluster::controller_snapshot snap;
    table::map_t links_copy;
    links_copy.reserve(links.size());
    for (const auto& [id, md_ptr] : links) {
        links_copy.emplace(id, md_ptr);
    }
    snap.cluster_links.links = co_await copy_links_for_snapshot(
      std::move(links_copy));
    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_snapshot(model::offset{}, snap));

    ASSERT_EQ_CORO(_table.local().size(), 2);
    cluster::controller_snapshot snap2;
    co_await _table.local().fill_snapshot(snap2);
    EXPECT_TRUE(snapshot_equals(snap2.cluster_links.links, links));

    chunked_vector<id_t> expected_ids = {id_t(1), id_t(2)};
    std::ranges::sort(expected_ids);
    auto all_ids = _table.local().get_all_link_ids();
    std::ranges::sort(all_ids);
    EXPECT_EQ(all_ids, expected_ids);
}

TEST_F_CORO(cluster_link_table_test, reset_links_duplicate_name) {
    table::map_t links;
    links.emplace(
      id_t(1),
      ss::make_lw_shared<metadata>(
        {.name = name_t("link1"),
         .uuid = uuid_t(::uuid_t::create()),
         .connection = connection_config{}}));
    links.emplace(
      id_t(2),
      ss::make_lw_shared<metadata>(
        {.name = name_t("link1"), // Duplicate name
         .uuid = uuid_t(::uuid_t::create()),
         .connection = connection_config{}}));

    cluster::controller_snapshot snap;
    snap.cluster_links.links = co_await copy_links_for_snapshot(
      std::move(links));
    EXPECT_THROW(
      co_await _table.local().apply_snapshot(model::offset{}, snap),
      std::logic_error);
}

TEST_F_CORO(cluster_link_table_test, upsert_success_test) {
    metadata link{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(
          model::offset{1}, co_await link.copy())));

    ASSERT_EQ_CORO(_table.local().size(), 1);

    auto found_link = _table.local().find_link_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_link);
    EXPECT_EQ(*found_link, link);
    auto found_id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_id);
    EXPECT_EQ(found_id.value(), id_t(1));
    found_link = _table.local().find_link_by_id(id_t(1));
    ASSERT_TRUE_CORO(found_link);
    EXPECT_EQ(*found_link, link);

    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_update(
        testing::create_remove_command(name_t("link1"), false)));
    found_link = _table.local().find_link_by_name(name_t("link1"));
    EXPECT_FALSE(found_link);
    found_link = _table.local().find_link_by_id(id_t(1));
    EXPECT_FALSE(found_link);
    found_id = _table.local().find_id_by_name(name_t("link1"));
    EXPECT_FALSE(found_id.has_value());
}

TEST_F_CORO(cluster_link_table_test, upsert_update) {
    auto first_uuid = uuid_t(::uuid_t::create());
    auto second_uuid = uuid_t(::uuid_t::create());
    metadata link{
      .name = name_t("link1"),
      .uuid = first_uuid,
      .connection = connection_config{}};
    metadata updated_link{
      .name = name_t("link1"),
      .uuid = second_uuid,
      .connection = connection_config{}};

    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(
          model::offset{1}, co_await link.copy())));
    ASSERT_EQ_CORO(_table.local().size(), 1);
    auto found_link = _table.local().find_link_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_link);
    EXPECT_EQ(*found_link, link);

    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(
          model::offset{2}, co_await updated_link.copy())));
    EXPECT_EQ(_table.local().size(), 1);
    found_link = _table.local().find_link_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(found_link);
    EXPECT_EQ(*found_link, updated_link);
}

TEST_F_CORO(cluster_link_table_test, upsert_duplicate_name) {
    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto ec = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link1.copy()));
    ASSERT_EQ_CORO(ec, errc::success);
    ec = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, co_await link1.copy()));
    ASSERT_EQ_CORO(ec, errc::success);
}

TEST_F_CORO(cluster_link_table_test, remove_non_existent_link) {
    EXPECT_EQ(_table.local().size(), 0);
    EXPECT_NO_THROW(
      co_await _table.local().apply_update(
        testing::create_remove_command(name_t("nonexistent"), false)));
    EXPECT_EQ(_table.local().size(), 0);
}

TEST_F_CORO(cluster_link_table_test, validate_batch_applicable) {
    auto upsert = testing::create_upsert_command(
      model::offset{1},
      metadata{
        .name = name_t("link1"),
        .uuid = uuid_t(::uuid_t::create()),
        .connection = connection_config{}});
    auto remove = testing::create_remove_command(name_t("link1"), false);
    EXPECT_TRUE(_table.local().is_batch_applicable(upsert));
    EXPECT_TRUE(_table.local().is_batch_applicable(remove));
    cluster::feature_update_license_update_cmd feature_update_cmd(
      cluster::feature_update_license_update_cmd_data{}, 0);
    auto batch = cluster::serde_serialize_cmd(std::move(feature_update_cmd));
    EXPECT_FALSE(_table.local().is_batch_applicable(batch));
    return ss::now();
}

TEST_F_CORO(cluster_link_table_test, callback_test) {
    bool was_called = false;
    id_t link_id{0};
    auto notification_id = _table.local().register_for_updates(
      [&was_called, &link_id](id_t id, model::revision_id) {
          was_called = true;
          link_id = id;
      });
    auto auto_remove = ss::defer([this, notification_id] {
        _table.local().unregister_for_updates(notification_id);
    });

    metadata link{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto ec = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link.copy()));
    ASSERT_EQ_CORO(ec, errc::success);

    EXPECT_TRUE(was_called);
    EXPECT_EQ(link_id, id_t(1));
}

TEST_F_CORO(cluster_link_table_test, callback_removal) {
    bool was_called = false;
    id_t link_id{0};

    metadata link{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link.copy()));
    auto notification_id = _table.local().register_for_updates(
      [&was_called, &link_id](id_t id, model::revision_id) {
          was_called = true;
          link_id = id;
      });
    auto auto_remove = ss::defer([this, notification_id] {
        _table.local().unregister_for_updates(notification_id);
    });
    co_await _table.local().apply_update(
      testing::create_remove_command(name_t("link1"), false));
    EXPECT_TRUE(was_called);
    EXPECT_EQ(link_id, id_t(1));
}

TEST_F_CORO(cluster_link_table_test, callback_snapshot) {
    absl::flat_hash_set<id_t> expected_ids{id_t(1), id_t(2), id_t(3)};
    absl::flat_hash_set<id_t> detected_ids{};

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link1.copy()));

    metadata link2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, co_await link2.copy()));

    table::map_t links;
    links.emplace(
      id_t(1),
      ss::make_lw_shared<metadata>(
        {.name = name_t("link1"),
         .uuid = uuid_t(::uuid_t::create()),
         .connection = connection_config{}}));
    links.emplace(
      id_t(3),
      ss::make_lw_shared<metadata>(
        {.name = name_t("link2"),
         .uuid = uuid_t(::uuid_t::create()),
         .connection = connection_config{
           .bootstrap_servers{net::unresolved_address{"localhost", 9092}}}}));

    auto notification_id = _table.local().register_for_updates(
      [&detected_ids](id_t id, model::revision_id) {
          detected_ids.insert(id);
      });
    auto auto_remove = ss::defer([this, notification_id] {
        _table.local().unregister_for_updates(notification_id);
    });

    cluster::controller_snapshot snap;
    snap.cluster_links.links = co_await copy_links_for_snapshot(
      std::move(links));
    co_await _table.local().apply_snapshot(model::offset{}, snap);

    ASSERT_EQ_CORO(_table.local().size(), 2);

    EXPECT_EQ(detected_ids, expected_ids);
}

TEST_F_CORO(cluster_link_table_test, with_mirror_topics) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;
    metadata link{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    testing::set_link_mirror_topics(link, test_topic, mirror_state, test_topic);

    ASSERT_NO_THROW_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(
          model::offset{1}, co_await link.copy())));

    ASSERT_EQ_CORO(_table.local().size(), 1);

    auto mirror_topic_state = _table.local().find_mirror_topic_status(
      test_topic);
    ASSERT_TRUE_CORO(mirror_topic_state.has_value());
    EXPECT_EQ(mirror_topic_state.value(), mirror_topic_status::active);

    auto link_id = _table.local().find_id_by_topic(test_topic);
    ASSERT_TRUE_CORO(link_id.has_value());
    EXPECT_EQ(link_id.value(), id_t(1));
}

TEST_F_CORO(cluster_link_table_test, with_multiple_links) {
    model::topic test_topic1("mirror-link1");
    model::topic test_topic2("mirror-link2");
    mirror_topic_status mirror_state1 = mirror_topic_status::active;
    mirror_topic_status mirror_state2 = mirror_topic_status::paused;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    testing::set_link_mirror_topics(
      link1, test_topic1, mirror_state1, test_topic1);

    metadata link2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    testing::set_link_mirror_topics(
      link2, test_topic2, mirror_state2, test_topic2);

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link1.copy()));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();
    res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, co_await link2.copy()));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link2: " << res.message();

    ASSERT_EQ_CORO(_table.local().size(), 2);

    auto mirror_topic_state1 = _table.local().find_mirror_topic_status(
      test_topic1);
    ASSERT_TRUE_CORO(mirror_topic_state1.has_value());
    EXPECT_EQ(mirror_topic_state1.value(), mirror_state1);

    auto link_id1 = _table.local().find_id_by_topic(test_topic1);
    ASSERT_TRUE_CORO(link_id1.has_value());
    EXPECT_EQ(link_id1.value(), id_t(1));

    auto mirror_topic_state2 = _table.local().find_mirror_topic_status(
      test_topic2);
    ASSERT_TRUE_CORO(mirror_topic_state2.has_value());
    EXPECT_EQ(mirror_topic_state2.value(), mirror_state2);

    auto link_id2 = _table.local().find_id_by_topic(test_topic2);
    ASSERT_TRUE_CORO(link_id2.has_value());
    EXPECT_EQ(link_id2.value(), id_t(2));

    res = co_await _table.local().apply_update(
      testing::create_remove_command(name_t("link1"), false));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to remove link1: " << res.message();

    link_id1 = _table.local().find_id_by_topic(test_topic1);
    EXPECT_FALSE(link_id1.has_value());
    mirror_topic_state1 = _table.local().find_mirror_topic_status(test_topic1);
    EXPECT_FALSE(mirror_topic_state1.has_value());

    link_id2 = _table.local().find_id_by_topic(test_topic2);
    ASSERT_TRUE_CORO(link_id2.has_value());
    EXPECT_EQ(link_id2.value(), id_t(2));
    mirror_topic_state2 = _table.local().find_mirror_topic_status(test_topic2);
    ASSERT_TRUE_CORO(mirror_topic_state2.has_value());
    EXPECT_EQ(mirror_topic_state2.value(), mirror_state2);
}

TEST_F_CORO(cluster_link_table_test, remove_mirror_topic) {
    model::topic test_topic1("mirror-link1");
    model::topic test_topic2("mirror-link2");
    mirror_topic_status mirror_state1 = mirror_topic_status::active;
    mirror_topic_status mirror_state2 = mirror_topic_status::paused;

    metadata link{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    ::cluster_link::model::link_state::mirror_topics_t mirror_topics;
    mirror_topics.emplace(
      test_topic1,
      testing::create_mirror_topic_metadata(mirror_state1, test_topic1));
    mirror_topics.emplace(
      test_topic2,
      testing::create_mirror_topic_metadata(mirror_state2, test_topic2));
    link.state.set_mirror_topics(std::move(mirror_topics));

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link.copy()));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link: " << res.message();

    link.state.mirror_topics.erase(test_topic1);
    res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link.copy()));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link: " << res.message();
    auto link_id = _table.local().find_id_by_topic(test_topic1);
    EXPECT_FALSE(link_id.has_value());
    auto topic_state = _table.local().find_mirror_topic_status(test_topic1);
    EXPECT_FALSE(topic_state.has_value());
    link_id = _table.local().find_id_by_topic(test_topic2);
    ASSERT_TRUE_CORO(link_id.has_value());
    EXPECT_EQ(link_id.value(), id_t(1));
    topic_state = _table.local().find_mirror_topic_status(test_topic2);
    ASSERT_TRUE_CORO(topic_state.has_value());
    EXPECT_EQ(topic_state.value(), mirror_state2);

    res = co_await _table.local().apply_update(
      testing::create_remove_command(name_t("link1"), false));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to remove link: " << res.message();
    link_id = _table.local().find_id_by_topic(test_topic2);
    EXPECT_FALSE(link_id.has_value());
    topic_state = _table.local().find_mirror_topic_status(test_topic2);
    EXPECT_FALSE(topic_state.has_value());
}

TEST_F_CORO(cluster_link_table_test, duplicate_topic) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    testing::set_link_mirror_topics(
      link1, test_topic, mirror_state, test_topic);

    metadata link2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    testing::set_link_mirror_topics(
      link2, test_topic, mirror_state, test_topic);

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link1.copy()));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, co_await link2.copy()));
    ASSERT_EQ_CORO(res.value(), int(errc::topic_being_mirrored_by_other_link))
      << "Expected error for duplicate topic, got: " << res.message();
}

TEST_F_CORO(cluster_link_table_test, reset_links_duplicate_topic) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;
    table::map_t links;
    ss::lw_shared_ptr<metadata> link1 = ss::make_lw_shared<metadata>();
    link1->name = name_t("link1");
    link1->uuid = uuid_t(::uuid_t::create());
    link1->connection = connection_config{};
    link1->state.mirror_topics.insert(
      {test_topic,
       testing::create_mirror_topic_metadata(
         mirror_state, test_topic)}); // Add the topic to the first link
    links.emplace(id_t(1), std::move(link1));
    auto link2 = ss::make_lw_shared<metadata>();
    link2->name = name_t("link2");
    link2->uuid = uuid_t(::uuid_t::create());
    link2->connection = connection_config{};
    link2->state.mirror_topics.insert(
      {test_topic,
       testing::create_mirror_topic_metadata(
         mirror_state, test_topic)}); // Add the topic to the second link
    links.emplace(id_t(2), std::move(link2));

    cluster::controller_snapshot snap;
    snap.cluster_links.links = co_await copy_links_for_snapshot(
      std::move(links));
    EXPECT_THROW(
      co_await _table.local().apply_snapshot(model::offset{}, snap),
      std::logic_error);
}

TEST_F_CORO(cluster_link_table_test, test_add_and_update_mirror_topic) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{1},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));

    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to add mirror topic: " << res.message();

    auto mirror_topic_state = _table.local().find_mirror_topic_status(
      test_topic);
    ASSERT_TRUE_CORO(mirror_topic_state.has_value());
    EXPECT_EQ(mirror_topic_state.value(), mirror_state);

    auto link_id = _table.local().find_id_by_topic(test_topic);
    ASSERT_TRUE_CORO(link_id.has_value());
    EXPECT_EQ(link_id.value(), id_t(1));

    mirror_state = mirror_topic_status::paused;
    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_status_command(
        id_t{1},
        update_mirror_topic_status_cmd{
          .topic = test_topic, .status = mirror_state}));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to update mirror topic state: " << res.message();
    mirror_topic_state = _table.local().find_mirror_topic_status(test_topic);
    ASSERT_TRUE_CORO(mirror_topic_state.has_value());
    EXPECT_EQ(mirror_topic_state.value(), mirror_state);
}

TEST_F_CORO(cluster_link_table_test, test_update_mirror_topic_state_not_found) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_status_command(
        id_t{1},
        update_mirror_topic_status_cmd{
          .topic = test_topic, .status = mirror_state}));
    EXPECT_EQ(res.value(), int(errc::topic_not_being_mirrored))
      << "Expected error for non-existent topic, got: " << res.message();
}

TEST_F_CORO(cluster_link_table_test, test_add_mirror_topic_no_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{2},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));
    EXPECT_EQ(res.value(), int(errc::does_not_exist))
      << "Expected error for non-existent topic, got: " << res.message();
}

TEST_F_CORO(
  cluster_link_table_test, test_add_mirror_topic_duplicate_same_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{1},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to add mirror topic: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{1},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));
    EXPECT_EQ(res.value(), int(errc::topic_already_being_mirrored))
      << res.message();
}

TEST_F_CORO(
  cluster_link_table_test, test_add_mirror_topic_duplicate_different_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    link1.state.mirror_topics.insert(
      {test_topic,
       testing::create_mirror_topic_metadata(mirror_state, test_topic)});

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    metadata link2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, std::move(link2)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link2: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{2},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));
    EXPECT_EQ(res.value(), int(errc::topic_being_mirrored_by_other_link))
      << "Expected error for duplicate topic, got: " << res.message();
}

TEST_F_CORO(cluster_link_table_test, update_state_non_existant_mirror_topic) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_status_command(
        id_t{1},
        update_mirror_topic_status_cmd{
          .topic = test_topic, .status = mirror_state}));
    EXPECT_EQ(res.value(), int(errc::topic_not_being_mirrored))
      << "Expected error for non-existent topic, got: " << res.message();
}

TEST_F_CORO(cluster_link_table_test, update_state_mirror_topic_other_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    metadata link2{
      .name = name_t("link2"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{2}, std::move(link2)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link2: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{1},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to add mirror topic: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_status_command(
        id_t{2},
        update_mirror_topic_status_cmd{
          .topic = test_topic, .status = mirror_state}));
    EXPECT_EQ(res.value(), int(errc::topic_being_mirrored_by_other_link))
      << "Expected error for topic being mirrored by other link, got: "
      << res.message();
}

TEST_F_CORO(
  cluster_link_table_test, test_add_and_update_mirror_topic_properties) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    res = co_await _table.local().apply_update(
      testing::create_add_mirror_topic_command(
        id_t{1},
        add_mirror_topic_cmd{
          .topic = test_topic,
          .metadata = testing::create_mirror_topic_metadata(
            mirror_state, test_topic)}));

    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to add mirror topic: " << res.message();

    auto link_id = _table.local().find_id_by_topic(test_topic);
    ASSERT_EQ_CORO(link_id, id_t(1));

    update_mirror_topic_properties_cmd cmd{
      .topic = test_topic,
      .partition_count = 5,
      .replication_factor = 5,
      .topic_configs = {{"key", "value"}}};

    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_properties_command(
        id_t{1}, std::move(cmd)));

    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to update mirror topic properties: " << res.message();

    auto it = _table.local().find_link_by_id(id_t{1})->state.mirror_topics.find(
      test_topic);
    EXPECT_EQ(it->second.partition_count, 5);
    EXPECT_EQ(it->second.replication_factor, 5);
    EXPECT_EQ(it->second.topic_configs.size(), 1);
    EXPECT_EQ(it->second.topic_configs.at("key"), "value");
}

TEST_F_CORO(
  cluster_link_table_test,
  test_update_mirror_topic_properties_topic_not_found) {
    model::topic test_topic("mirror-link1");

    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    update_mirror_topic_properties_cmd cmd{
      .topic = test_topic,
      .partition_count = 5,
      .replication_factor = 5,
      .topic_configs = {{"key", "value"}}};

    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_properties_command(
        id_t{1}, std::move(cmd)));

    EXPECT_EQ(res.value(), int(errc::topic_not_being_mirrored))
      << "Expected error for non-existent topic, got: " << res.message();
}

TEST_F_CORO(
  cluster_link_table_test, test_update_mirror_topic_properties_different_link) {
    model::topic test_topic("mirror-link1");
    metadata link1{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    link1.state.mirror_topics.insert(
      {test_topic,
       testing::create_mirror_topic_metadata(
         mirror_topic_status::active, test_topic)});

    auto res = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, std::move(link1)));
    ASSERT_EQ_CORO(res.value(), int(errc::success))
      << "Failed to upsert link1: " << res.message();

    update_mirror_topic_properties_cmd cmd{
      .topic = test_topic,
      .partition_count = 5,
      .replication_factor = 5,
      .topic_configs = {{"key", "value"}}};

    res = co_await _table.local().apply_update(
      testing::create_update_mirror_topic_properties_command(
        id_t{2}, std::move(cmd)));

    EXPECT_EQ(res.value(), int(errc::topic_being_mirrored_by_other_link))
      << "Expected error for mirrored by other link, got: " << res.message();
}

TEST_F_CORO(cluster_link_table_test, update_cluster_link_configuration) {
    chunked_hash_map<model::topic, mirror_topic_metadata> mirror_topics;
    mirror_topics.emplace(
      model::topic("test-topic"),
      mirror_topic_metadata{
        .source_topic_name = model::topic("test-topic"),
      });
    metadata link{
      .name = name_t{"link"},
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};

    link.state.set_mirror_topics(std::move(mirror_topics));

    auto ec = co_await _table.local().apply_update(
      testing::create_upsert_command(model::offset{1}, co_await link.copy()));
    ASSERT_FALSE_CORO(ec);

    update_cluster_link_configuration_cmd update_cmd {
        .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      },
      .link_config = link_configuration{
            .topic_metadata_mirroring_cfg = topic_metadata_mirroring_config {
                .task_interval = std::chrono::seconds(60)
            }
      }
    };

    ec = co_await _table.local().apply_update(
      testing::create_update_cluster_link_configuration_command(
        id_t{1}, update_cmd.copy()));
    ASSERT_EQ_CORO(ec.value(), int(errc::success));

    auto found_link = _table.local().find_link_by_id(id_t{1});
    ASSERT_TRUE_CORO(found_link);
    EXPECT_EQ(found_link->connection, update_cmd.connection);
    EXPECT_EQ(found_link->state, link.state);
    EXPECT_EQ(found_link->configuration, update_cmd.link_config);
}

TEST_F_CORO(cluster_link_table_test, update_non_existent_link) {
    update_cluster_link_configuration_cmd update_cmd {
        .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      },
      .link_config = link_configuration{
            .topic_metadata_mirroring_cfg = topic_metadata_mirroring_config {
                .task_interval = std::chrono::seconds(60)
            }
      }
    };

    auto ec = co_await _table.local().apply_update(
      testing::create_update_cluster_link_configuration_command(
        id_t{1}, update_cmd.copy()));

    EXPECT_EQ(static_cast<errc>(ec.value()), errc::does_not_exist);
}

} // namespace cluster::cluster_link
