/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/frontend.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/tests/utils.h"
#include "test_utils/test.h"
#include "utils/unresolved_address.h"

#include <gtest/gtest.h>

namespace cluster::cluster_link {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::connection_config;
using ::cluster_link::model::delete_mirror_topic_cmd;
using ::cluster_link::model::id_t;
using ::cluster_link::model::link_configuration;
using ::cluster_link::model::metadata;
using ::cluster_link::model::mirror_topic_status;
using ::cluster_link::model::name_t;
using ::cluster_link::model::scram_credentials;
using ::cluster_link::model::tls_file_path;
using ::cluster_link::model::tls_value;
using ::cluster_link::model::update_cluster_link_configuration_cmd;
using ::cluster_link::model::update_mirror_topic_properties_cmd;
using ::cluster_link::model::update_mirror_topic_status_cmd;
using ::cluster_link::model::uuid_t;

constexpr size_t max_links = 1;

namespace {
metadata create_base_metadata(
  name_t name = name_t("link1"), uuid_t uuid = uuid_t{::uuid_t::create()}) {
    return {
      .name = std::move(name),
      .uuid = uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
}
} // namespace

class frontend_validation_test : public seastar_test {
public:
    ss::sharded<table> _table;

    std::unique_ptr<frontend::validator> _validator{nullptr};

    ss::future<> SetUpAsync() override {
        co_await _table.start();
        _validator = std::make_unique<frontend::validator>(
          &_table.local(),
          max_links,
          absl::flat_hash_set<std::string_view>(
            ::cluster_link::model::disallowed_topic_properties.begin(),
            ::cluster_link::model::disallowed_topic_properties.end()));
    }
    ss::future<> TearDownAsync() override {
        _validator.reset(nullptr);
        co_await _table.stop();
    }

    ss::future<cluster::cluster_link::errc> upsert_cluster_link(metadata m) {
        cluster::cluster_link_upsert_cmd cmd{0, co_await m.copy()};
        auto ec = _validator->validate_mutation(std::move(cmd));
        if (ec == cluster::cluster_link::errc::success) {
            auto existing = _table.local().find_id_by_name(m.name);
            auto id = existing.value_or(++_latest_id);
            auto err = co_await _table.local().apply_update(
              testing::create_upsert_command(
                model::offset{id()}, std::move(m)));
            vassert(!err, "Failed to upsert link: {}", err.message());
        }

        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    delete_cluster_link(name_t m, bool force) {
        cluster::cluster_link_remove_cmd cmd{
          0, {.link_name = std::move(m), .force = force}};
        auto ec = _validator->validate_mutation(cmd);
        if (ec == cluster::cluster_link::errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_remove_command(
                cmd.value.link_name, cmd.value.force));
            vassert(!err, "Failed to remove link: {}", err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    add_mirror_topic(id_t id, add_mirror_topic_cmd cmd) {
        cluster::cluster_link_add_mirror_topic_cmd add_cmd{id, cmd.copy()};
        auto ec = _validator->validate_mutation(std::move(add_cmd));
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_add_mirror_topic_command(id, std::move(cmd)));
            vassert(!err, "Failed to add mirror topic: {}", err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    delete_mirror_topic(id_t id, delete_mirror_topic_cmd cmd) {
        cluster::cluster_link_delete_mirror_topic_cmd del_cmd{id, cmd};
        auto ec = _validator->validate_mutation(std::move(del_cmd));
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_delete_mirror_topic_command(id, std::move(cmd)));
            vassert(!err, "Failed to delete mirror topic: {}", err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    update_mirror_topic_status(id_t id, update_mirror_topic_status_cmd cmd) {
        cluster::cluster_link_update_mirror_topic_status_cmd update_cmd{
          id, std::move(cmd)};
        auto ec = _validator->validate_mutation(update_cmd);
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_update_mirror_topic_status_command(
                update_cmd.key, std::move(update_cmd.value)));
            vassert(
              !err, "Failed to update mirror topic state: {}", err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc> update_mirror_topic_properties(
      id_t id, update_mirror_topic_properties_cmd cmd) {
        cluster::cluster_link_update_mirror_topic_properties_cmd update_cmd{
          id, cmd.copy()};
        auto ec = _validator->validate_mutation(std::move(update_cmd));
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_update_mirror_topic_properties_command(
                id, std::move(cmd)));
            vassert(
              !err,
              "Failed to update mirror topic properties: {}",
              err.message());
        }
        co_return ec;
    }

    ss::future<cluster::cluster_link::errc> update_cluster_link_configuration(
      id_t id, update_cluster_link_configuration_cmd cmd) {
        cluster::cluster_link_update_cluster_link_configuration_cmd update_cmd{
          id, cmd.copy()};
        auto ec = _validator->validate_mutation(std::move(update_cmd));
        if (ec == errc::success) {
            auto err = co_await _table.local().apply_update(
              testing::create_update_cluster_link_configuration_command(
                id, std::move(cmd)));
            vassert(
              !err, "Failed to update link configuration: {}", err.message());
        }

        co_return ec;
    }

    ss::future<cluster::cluster_link::errc>
    failover_link_topic(id_t id, const model::topic& topic) {
        // first transition to failing over and then to failed over
        auto ec = co_await update_mirror_topic_status(
          id,
          update_mirror_topic_status_cmd{
            .topic = topic, .status = mirror_topic_status::failing_over});
        if (ec != errc::success) {
            co_return ec;
        }
        co_return co_await update_mirror_topic_status(
          id,
          update_mirror_topic_status_cmd{
            .topic = topic, .status = mirror_topic_status::failed_over});
    }

    id_t _latest_id{0};
};

TEST_F_CORO(frontend_validation_test, successful_upsert) {
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, too_many_links) {
    for (size_t i = 0; i < max_links; ++i) {
        EXPECT_EQ(
          co_await upsert_cluster_link(
            create_base_metadata(name_t(fmt::format("link{}", i + 1)))),
          cluster::cluster_link::errc::success);
    }
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata(name_t("toomany"))),
      cluster::cluster_link::errc::limit_exceeded);
}

TEST_F_CORO(frontend_validation_test, no_bootstrap_servers) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::bootstrap_servers_empty);
}

TEST_F_CORO(frontend_validation_test, name_too_long) {
    EXPECT_EQ(
      co_await upsert_cluster_link(
        create_base_metadata(name_t(std::string(129, 'a')))),
      cluster::cluster_link::errc::link_name_invalid);
}

TEST_F_CORO(frontend_validation_test, name_empty) {
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata(name_t(""))),
      cluster::cluster_link::errc::link_name_invalid);
}

TEST_F_CORO(frontend_validation_test, remote_non_existent) {
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("nonexistent"), false),
      cluster::cluster_link::errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, remove_existing) {
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, remove_empty_link) {
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::success);
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, remove_link_with_topics) {
    // create a link
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::success);
    // Add a topic to the link
    auto maybe_link_id = _table.local().find_id_by_name(name_t("link1"));
    EXPECT_TRUE(maybe_link_id.has_value())
      << "Unable to find link ID for link1";
    auto link_id = maybe_link_id.value();
    add_mirror_topic_cmd cmd0{
      .topic = model::topic("mirror-topic-0"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic-0"))};
    EXPECT_EQ(
      co_await add_mirror_topic(link_id, std::move(cmd0)),
      cluster::cluster_link::errc::success);
    // Try to delete the link, should fail
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::link_has_active_shadow_topics);
    // Add another topic and transition the first one to failed_over
    add_mirror_topic_cmd cmd1{
      .topic = model::topic("mirror-topic-1"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic-1"))};
    EXPECT_EQ(
      co_await add_mirror_topic(link_id, std::move(cmd1)),
      cluster::cluster_link::errc::success);
    // Try delete again, should still fail
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::link_has_active_shadow_topics);
    // Transition first topic to failed_over
    EXPECT_EQ(
      co_await failover_link_topic(link_id, model::topic("mirror-topic-0")),
      cluster::cluster_link::errc::success);
    // Try delete again, should still fail
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::link_has_active_shadow_topics);
    // Transition second topic to failed_over
    EXPECT_EQ(
      co_await failover_link_topic(link_id, model::topic("mirror-topic-1")),
      cluster::cluster_link::errc::success);
    // Now the delete should succeed
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, remove_link_with_topics_forced) {
    // create a link
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::success);
    // Add a topic to the link
    auto maybe_link_id = _table.local().find_id_by_name(name_t("link1"));
    EXPECT_TRUE(maybe_link_id.has_value())
      << "Unable to find link ID for link1";
    auto link_id = maybe_link_id.value();
    add_mirror_topic_cmd cmd0{
      .topic = model::topic("mirror-topic-0"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic-0"))};
    EXPECT_EQ(
      co_await add_mirror_topic(link_id, std::move(cmd0)),
      cluster::cluster_link::errc::success);
    // Try to delete the link, should fail
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), false),
      cluster::cluster_link::errc::link_has_active_shadow_topics);
    // Now the delete should succeed with force being true
    EXPECT_EQ(
      co_await delete_cluster_link(name_t("link1"), true),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, update_existing_bad_uuid) {
    metadata m{
      .name = name_t("link1"),
      .uuid = uuid_t(::uuid_t::create()),
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::success);

    // Create base metadata will generate a new UUID
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata()),
      cluster::cluster_link::errc::uuid_conflict);
}

TEST_F_CORO(frontend_validation_test, update_existing_good_uuid) {
    auto link_uuid = uuid_t(::uuid_t::create());

    EXPECT_EQ(
      co_await upsert_cluster_link(
        create_base_metadata(name_t("link1"), link_uuid)),
      cluster::cluster_link::errc::success);

    EXPECT_EQ(
      co_await upsert_cluster_link(
        create_base_metadata(name_t("link1"), link_uuid)),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, update_no_bootstrap_servers) {
    auto link_uuid = uuid_t(::uuid_t::create());
    metadata m{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9092}}}};
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
    metadata mupdate{
      .name = name_t("link1"),
      .uuid = link_uuid,
      .connection = connection_config{}};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(mupdate)),
      cluster::cluster_link::errc::bootstrap_servers_empty);
}

TEST_F_CORO(frontend_validation_test, invalid_utf8_in_name) {
    EXPECT_EQ(
      co_await upsert_cluster_link(
        create_base_metadata(name_t("\xFF\xFF\xFF"))),
      cluster::cluster_link::errc::link_name_invalid);
}

TEST_F_CORO(frontend_validation_test, control_character_in_name) {
    EXPECT_EQ(
      co_await upsert_cluster_link(create_base_metadata(name_t("link1\x0d"))),
      cluster::cluster_link::errc::link_name_invalid);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_missing_key) {
    auto m = create_base_metadata();
    m.connection.cert = tls_value("bah");
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::tls_configuration_invalid);
}

TEST_F_CORO(
  frontend_validation_test, add_mirror_topic_key_cert_types_different) {
    auto m = create_base_metadata();
    m.connection.cert = tls_value("bah");
    m.connection.key = tls_file_path("key.pem");
    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::tls_configuration_invalid);
}

TEST_F_CORO(frontend_validation_test, valid_scram_creds_256) {
    auto m = create_base_metadata();
    m.connection.authn_config = scram_credentials{
      .username = "user", .password = "password", .mechanism = "SCRAM-SHA-256"};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, valid_scram_creds_512) {
    auto m = create_base_metadata();
    m.connection.authn_config = scram_credentials{
      .username = "user", .password = "password", .mechanism = "SCRAM-SHA-512"};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m)),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, invalid_scram_creds) {
    {
        auto m = create_base_metadata();
        m.connection.authn_config = scram_credentials{
          .username = "", .password = "password", .mechanism = "SCRAM-SHA-256"};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m)),
          cluster::cluster_link::errc::scram_configuration_invalid);
    }
    {
        auto m = create_base_metadata();
        m.connection.authn_config = scram_credentials{
          .username = "user", .password = "", .mechanism = "SCRAM-SHA-256"};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m)),
          cluster::cluster_link::errc::scram_configuration_invalid);
    }
    {
        auto m = create_base_metadata();
        m.connection.authn_config = scram_credentials{
          .username = "user",
          .password = "pass",
          .mechanism = "SCRAM-SHA-256-NON_EXISTANT"};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m)),
          cluster::cluster_link::errc::scram_configuration_invalid);
    }
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_success) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_invalid_name) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("\xFF\xFF\xFF"), // Invalid UTF-8
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("\xFF\xFF\xFF"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)),
      errc::mirror_topic_name_invalid);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_no_link) {
    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id_t{5}, std::move(cmd)), errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_already_mirrored) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;
    auto m = create_base_metadata();
    testing::set_link_mirror_topics(m, test_topic, mirror_state, test_topic);
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = test_topic,
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, test_topic)};

    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)),
      errc::topic_already_being_mirrored);
}

TEST_F_CORO(frontend_validation_test, add_mirror_topic_mirrored_by_other_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;
    auto m1 = create_base_metadata();
    testing::set_link_mirror_topics(m1, test_topic, mirror_state, test_topic);

    auto m2 = create_base_metadata(name_t("link2"));

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{1}, std::move(m1))),
      errc::success);

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{2}, std::move(m2))),
      errc::success);

    add_mirror_topic_cmd cmd{
      .topic = test_topic,
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, test_topic)};

    EXPECT_EQ(
      co_await add_mirror_topic(id_t{2}, std::move(cmd)),
      errc::topic_being_mirrored_by_other_link);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_status_success) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    auto m = create_base_metadata();
    testing::set_link_mirror_topics(m, test_topic, mirror_state, test_topic);
    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_status_cmd update_cmd{
      .topic = test_topic, .status = mirror_topic_status::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_status(id.value(), std::move(update_cmd)),
      errc::success);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_status_invalid_name) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    auto m = create_base_metadata();
    testing::set_link_mirror_topics(m, test_topic, mirror_state, test_topic);

    ASSERT_EQ_CORO(co_await upsert_cluster_link(std::move(m)), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_status_cmd update_cmd{
      .topic = model::topic("\xFF\xFF\xFF"), // Invalid UTF-8
      .status = mirror_topic_status::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_status(id.value(), std::move(update_cmd)),
      errc::mirror_topic_name_invalid);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_non_existant_link) {
    update_mirror_topic_status_cmd update_cmd{
      .topic = model::topic("test-topic"),
      .status = mirror_topic_status::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_status(id_t{5}, std::move(update_cmd)),
      errc::does_not_exist);
}

TEST_F_CORO(
  frontend_validation_test, update_mirror_topic_mirror_topic_does_not_exist) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_status_cmd update_cmd{
      .topic = model::topic("test-topic"),
      .status = mirror_topic_status::paused};
    EXPECT_EQ(
      co_await update_mirror_topic_status(id.value(), std::move(update_cmd)),
      errc::topic_not_being_mirrored);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_mirrored_by_other) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    auto m1 = create_base_metadata();
    testing::set_link_mirror_topics(m1, test_topic, mirror_state, test_topic);

    auto m2 = create_base_metadata(name_t("link2"));

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{1}, std::move(m1))),
      errc::success);

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{2}, std::move(m2))),
      errc::success);

    update_mirror_topic_status_cmd update_cmd{
      .topic = test_topic, .status = mirror_topic_status::paused};

    EXPECT_EQ(
      co_await update_mirror_topic_status(id_t{2}, std::move(update_cmd)),
      errc::topic_being_mirrored_by_other_link);
}

TEST_F_CORO(frontend_validation_test, delete_mirror_topic_success) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    {
        add_mirror_topic_cmd cmd{
          .topic = model::topic("mirror-topic"),
          .metadata = testing::create_mirror_topic_metadata(
            mirror_topic_status::active, model::topic("mirror-topic"))};
        EXPECT_EQ(
          co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);
    }

    {
        delete_mirror_topic_cmd cmd{.topic = model::topic("mirror-topic")};
        EXPECT_EQ(
          co_await delete_mirror_topic(id.value(), std::move(cmd)),
          errc::success);
    }
}

TEST_F_CORO(frontend_validation_test, delete_mirror_topic_invalid_name) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    delete_mirror_topic_cmd cmd{.topic = model::topic("\xFF\xFF\xFF")};
    EXPECT_EQ(
      co_await delete_mirror_topic(id.value(), std::move(cmd)),
      errc::mirror_topic_name_invalid);
}

TEST_F_CORO(frontend_validation_test, delete_mirror_topic_no_link) {
    delete_mirror_topic_cmd cmd{.topic = model::topic("mirror-topic")};
    EXPECT_EQ(
      co_await delete_mirror_topic(id_t{5}, std::move(cmd)),
      errc::does_not_exist);
}

TEST_F_CORO(frontend_validation_test, delete_mirror_topic_not_found) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    delete_mirror_topic_cmd cmd{.topic = model::topic("mirror-topic")};
    EXPECT_EQ(
      co_await delete_mirror_topic(id.value(), std::move(cmd)),
      errc::topic_not_being_mirrored);
}

TEST_F_CORO(
  frontend_validation_test, delete_mirror_topic_mirrored_by_other_link) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;
    auto m1 = create_base_metadata();
    testing::set_link_mirror_topics(m1, test_topic, mirror_state, test_topic);

    auto m2 = create_base_metadata(name_t("link2"));

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{1}, std::move(m1))),
      errc::success);

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{2}, std::move(m2))),
      errc::success);

    delete_mirror_topic_cmd cmd{.topic = test_topic};

    EXPECT_EQ(
      co_await delete_mirror_topic(id_t{2}, std::move(cmd)),
      errc::topic_being_mirrored_by_other_link);
}

TEST_F_CORO(frontend_validation_test, test_mirror_properties) {
    auto m1 = create_base_metadata();
    m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      = ::cluster_link::model::topic_metadata_mirroring_config::properties_set{
        "segment.ms"};
    m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {
      {
        .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
        .filter = ::cluster_link::model::filter_type::include,
        .pattern
        = ::cluster_link::model::resource_name_filter_pattern::wildcard,
      },
      {
        .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
        .filter = ::cluster_link::model::filter_type::exclude,
        .pattern = "excluded-topic",
      }};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m1)),
      cluster::cluster_link::errc::success);
}

TEST_F_CORO(frontend_validation_test, test_mirror_properties_empty_pattern) {
    auto m1 = create_base_metadata();
    m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      = ::cluster_link::model::topic_metadata_mirroring_config::properties_set{
        "segment.ms"};
    m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {
      {
        .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
        .filter = ::cluster_link::model::filter_type::include,
        .pattern
        = ::cluster_link::model::resource_name_filter_pattern::wildcard,
      },
      {
        .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
        .filter = ::cluster_link::model::filter_type::exclude,
        .pattern = "",
      }};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m1)),
      cluster::cluster_link::errc::topic_filter_invalid);
}

TEST_F_CORO(frontend_validation_test, test_mirror_properties_invalid_wildcard) {
    auto m1 = create_base_metadata();
    m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      = ::cluster_link::model::topic_metadata_mirroring_config::properties_set{
        "segment.ms"};
    m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
      .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
      .filter = ::cluster_link::model::filter_type::include,
      .pattern = "*something",
    }};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m1)),
      cluster::cluster_link::errc::topic_filter_invalid);
}
TEST_F_CORO(
  frontend_validation_test, test_mirror_properties_wildcard_in_prefix) {
    auto m1 = create_base_metadata();
    m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      = ::cluster_link::model::topic_metadata_mirroring_config::properties_set{
        "segment.ms"};
    m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
      .pattern_type = ::cluster_link::model::filter_pattern_type::prefix,
      .filter = ::cluster_link::model::filter_type::include,
      .pattern = ::cluster_link::model::resource_name_filter_pattern::wildcard,
    }};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m1)),
      cluster::cluster_link::errc::topic_filter_invalid);
}

TEST_F_CORO(
  frontend_validation_test, test_mirror_properties_invalid_characters) {
    auto m1 = create_base_metadata();
    m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
      = ::cluster_link::model::topic_metadata_mirroring_config::properties_set{
        "segment.ms"};
    m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
      .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
      .filter = ::cluster_link::model::filter_type::include,
      .pattern = "\xFF",
    }};

    EXPECT_EQ(
      co_await upsert_cluster_link(std::move(m1)),
      cluster::cluster_link::errc::topic_filter_invalid);
}

TEST_F_CORO(
  frontend_validation_test, test_mirror_properties_invalid_topic_name) {
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::prefix,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "__redpanda",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_filter_invalid);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::prefix,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "_redpanda",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_filter_invalid);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "__consumer_offsets",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_filter_invalid);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "_schemas",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_filter_invalid);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "_redpanda.audit_log",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_filter_invalid);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::literal,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "_redpanda.mine",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::success);

        ASSERT_EQ_CORO(
          co_await delete_cluster_link(name_t("link1"), true), errc::success);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{"segment.ms"};
        m1.configuration.topic_metadata_mirroring_cfg.topic_name_filters = {{
          .pattern_type = ::cluster_link::model::filter_pattern_type::prefix,
          .filter = ::cluster_link::model::filter_type::include,
          .pattern = "_redpanda.mine",
        }};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::success);
    }
}

TEST_F_CORO(
  frontend_validation_test, test_mirror_properties_invalid_topic_property) {
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{ss::sstring{kafka::topic_property_read_replica}};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_property_excluded_from_mirroring);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{ss::sstring{kafka::topic_property_recovery}};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_property_excluded_from_mirroring);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{
              ss::sstring{kafka::topic_property_remote_allow_gaps}};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_property_excluded_from_mirroring);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{
              ss::sstring{kafka::topic_property_mpx_virtual_cluster_id}};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_property_excluded_from_mirroring);
    }
    {
        auto m1 = create_base_metadata();
        m1.configuration.topic_metadata_mirroring_cfg.topic_properties_to_mirror
          = ::cluster_link::model::topic_metadata_mirroring_config::
            properties_set{
              ss::sstring{kafka::topic_property_leaders_preference}};

        EXPECT_EQ(
          co_await upsert_cluster_link(std::move(m1)),
          cluster::cluster_link::errc::topic_property_excluded_from_mirroring);
    }
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_properties_success) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);

    update_mirror_topic_properties_cmd update_cmd{
      .topic = model::topic("mirror-topic"),
      .partition_count = 3,
      .replication_factor = 3,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(
        id.value(), std::move(update_cmd)),
      errc::success);
}

TEST_F_CORO(
  frontend_validation_test, update_mirror_topic_properties_invalid_name) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);

    update_mirror_topic_properties_cmd update_cmd{
      .topic = model::topic("\xFF\xFF\xFF"), // Invalid UTF-8
      .partition_count = 3,
      .replication_factor = 3,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(
        id.value(), std::move(update_cmd)),
      errc::mirror_topic_name_invalid);
}

TEST_F_CORO(frontend_validation_test, update_mirror_topic_properties_no_link) {
    update_mirror_topic_properties_cmd update_cmd{
      .topic = model::topic("test-topic"),
      .partition_count = 3,
      .replication_factor = 3,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(id_t{5}, std::move(update_cmd)),
      errc::does_not_exist);
}

TEST_F_CORO(
  frontend_validation_test, update_mirror_topic_properties_mirrored_by_other) {
    model::topic test_topic("mirror-link1");
    mirror_topic_status mirror_state = mirror_topic_status::active;

    auto m1 = create_base_metadata();
    testing::set_link_mirror_topics(m1, test_topic, mirror_state, test_topic);

    auto m2 = create_base_metadata(name_t("link2"));

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{1}, std::move(m1))),
      errc::success);

    ASSERT_EQ_CORO(
      co_await _table.local().apply_update(
        testing::create_upsert_command(model::offset{2}, std::move(m2))),
      errc::success);

    update_mirror_topic_properties_cmd update_cmd{
      .topic = test_topic,
      .partition_count = 3,
      .replication_factor = 3,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(id_t{2}, std::move(update_cmd)),
      errc::topic_being_mirrored_by_other_link);
}

TEST_F_CORO(
  frontend_validation_test, update_mirror_topic_properties_not_being_mirrored) {
    model::topic test_topic("mirror-link1");
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_mirror_topic_properties_cmd update_cmd{
      .topic = test_topic,
      .partition_count = 3,
      .replication_factor = 3,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(id_t{1}, std::move(update_cmd)),
      errc::topic_not_being_mirrored);
}

TEST_F_CORO(
  frontend_validation_test,
  update_mirror_topic_properties_invalid_partition_count) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);

    update_mirror_topic_properties_cmd update_cmd{
      .topic = model::topic("mirror-topic"),
      .partition_count = -1,
      .replication_factor = 3,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(
        id.value(), std::move(update_cmd)),
      errc::invalid_update);
}

TEST_F_CORO(
  frontend_validation_test,
  update_mirror_topic_properties_invalid_replication_factor) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    add_mirror_topic_cmd cmd{
      .topic = model::topic("mirror-topic"),
      .metadata = testing::create_mirror_topic_metadata(
        mirror_topic_status::active, model::topic("mirror-topic"))};
    EXPECT_EQ(
      co_await add_mirror_topic(id.value(), std::move(cmd)), errc::success);

    update_mirror_topic_properties_cmd update_cmd{
      .topic = model::topic("mirror-topic"),
      .partition_count = 3,
      .replication_factor = -1,
    };

    EXPECT_EQ(
      co_await update_mirror_topic_properties(
        id.value(), std::move(update_cmd)),
      errc::invalid_update);
}

TEST_F_CORO(frontend_validation_test, update_cluster_link_configuration) {
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());

    update_cluster_link_configuration_cmd update_cmd{
      .connection = connection_config{
        .bootstrap_servers = {net::unresolved_address{"localhost", 9093}}}};

    update_cmd.link_config.topic_metadata_mirroring_cfg.task_interval = 60s;

    ASSERT_EQ_CORO(
      co_await update_cluster_link_configuration(*id, update_cmd.copy()),
      errc::success);

    auto meta = _table.local().find_link_by_id(*id);
    ASSERT_TRUE_CORO(meta);
    EXPECT_EQ(meta->connection, update_cmd.connection);
    EXPECT_EQ(meta->configuration, update_cmd.link_config);
}

TEST_F_CORO(
  frontend_validation_test, update_cluster_link_configuration_errors) {
    // First try updating without a link present
    {
        update_cluster_link_configuration_cmd update_cmd{
          .connection = connection_config{
            .bootstrap_servers = {net::unresolved_address{"localhost", 9093}}}};

        update_cmd.link_config.topic_metadata_mirroring_cfg.task_interval = 60s;
        EXPECT_EQ(
          co_await update_cluster_link_configuration(
            id_t{1}, update_cmd.copy()),
          errc::does_not_exist);
    }
    // Now create a link
    ASSERT_EQ_CORO(
      co_await upsert_cluster_link(create_base_metadata()), errc::success);
    auto id = _table.local().find_id_by_name(name_t("link1"));
    ASSERT_TRUE_CORO(id.has_value());
    // Update with no bootstrap address provided
    {
        update_cluster_link_configuration_cmd update_cmd{
          .connection = connection_config{}};

        update_cmd.link_config.topic_metadata_mirroring_cfg.task_interval = 60s;
        EXPECT_EQ(
          co_await update_cluster_link_configuration(*id, update_cmd.copy()),
          errc::bootstrap_servers_empty);
    }
    // Update with invalid topic properties
    {
        update_cluster_link_configuration_cmd update_cmd{
          .connection = connection_config{
            .bootstrap_servers = {net::unresolved_address{"localhost", 9093}}}};

        update_cmd.link_config.topic_metadata_mirroring_cfg
          .topic_properties_to_mirror = ::cluster_link::model::
          topic_metadata_mirroring_config::properties_set{
            "redpanda.remote.readreplica"};

        EXPECT_EQ(
          co_await update_cluster_link_configuration(*id, update_cmd.copy()),
          errc::topic_property_excluded_from_mirroring);
    }
}

} // namespace cluster::cluster_link
