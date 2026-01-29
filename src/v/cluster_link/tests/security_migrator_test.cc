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

#include "cluster_link/security_migrator.h"
#include "cluster_link/tests/deps.h"
#include "kafka/server/handlers/details/security.h"
#include "security/acl_store.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <algorithm>

using namespace std::chrono_literals;

namespace cluster_link::tests {
namespace {
static const auto link_name = model::name_t("test_link");
model::metadata get_default_metadata() {
    model::link_state link_state;
    model::security_settings_sync_config security_sync_config;
    security_sync_config.task_interval = 1s;
    security_sync_config.acl_filters
      = { model::acl_filter{
        .resource_filter = {
          .resource_type = model::acl_resource::any,
          .pattern_type = model::acl_pattern::any,
        },
        .access_filter = {
          .operation = model::acl_operation::any,
          .permission_type = model::acl_permission_type::any
        },
      },
      model::acl_filter{.resource_filter = {
          .resource_type = model::acl_resource::schema_registry_any,
          .pattern_type = model::acl_pattern::any,
        },
        .access_filter = {
          .operation = model::acl_operation::any,
          .permission_type = model::acl_permission_type::any
        },},
    };

    model::metadata md{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::
        connection_config{.bootstrap_servers = {net::unresolved_address("localhost", 9092)}},
      .state = std::move(link_state)};
    md.configuration.security_settings_sync_cfg = std::move(
      security_sync_config);

    return md;
}
} // namespace

class security_migrator_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;

    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));

        _clmtf->get_cluster_mock().set_cluster_authorized_operations(
          kafka::cluster_authorized_operations(0x100));

        co_await _clmtf->get_manager().invoke_on_all([](manager& m) {
            return m.register_task_factory<security_migrator_factory>();
        });

        fixture().elect_leader(::model::controller_ntp, self(), std::nullopt);
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    ::model::node_id self() { return ::model::node_id(0); }

    cluster_link_manager_test_fixture& fixture() { return *_clmtf; }

private:
private:
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;

    std::optional<kafka::describe_acls_response> _describe_acls_response;
};

TEST_F_CORO(security_migrator_test, migrate_all_acls) {
    // This test will create an SR and a Kafka resource ACL and fetch both
    security::resource_pattern topic_resource{
      security::resource_type::topic,
      "test-topic",
      security::pattern_type::literal};
    security::resource_pattern subject_resource{
      security::resource_type::sr_subject,
      "test-topic-value",
      security::pattern_type::literal};
    fixture().get_cluster_mock().acl_store().add_bindings(
      {security::acl_binding(
         topic_resource,
         security::acl_entry{
           security::acl_principal::from_string("User:test-user"),
           security::acl_host::wildcard_host(),
           security::acl_operation::read,
           security::acl_permission::allow}),
       security::acl_binding(
         subject_resource,
         security::acl_entry{
           security::acl_principal::from_string("User:test-user"),
           security::acl_host::wildcard_host(),
           security::acl_operation::read,
           security::acl_permission::allow})});

    co_await fixture().upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 2; })

    const auto& local_acls = fixture().security_service().acls();

    EXPECT_EQ(local_acls.size(), 2);

    const auto topic_it = local_acls.find(topic_resource);
    ASSERT_NE_CORO(topic_it, local_acls.end())
      << "Failed to find topic resource " << topic_resource;
    ASSERT_FALSE_CORO(topic_it->second.empty())
      << "Topic resource contains no acls";

    EXPECT_TRUE(topic_it->second.contains(
      security::acl_operation::read,
      security::acl_principal::from_string("User:test-user"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow));

    const auto sr_it = local_acls.find(subject_resource);
    ASSERT_NE_CORO(sr_it, local_acls.end())
      << "Failed to find subject resource " << subject_resource;
    ASSERT_FALSE_CORO(sr_it->second.empty())
      << "Subject resource contains no acls";

    EXPECT_TRUE(sr_it->second.contains(
      security::acl_operation::read,
      security::acl_principal::from_string("User:test-user"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow));
}

TEST_F_CORO(security_migrator_test, migrate_group_acls) {
    // This test verifies that Group: principal ACLs are properly migrated
    security::resource_pattern topic_resource{
      security::resource_type::topic,
      "test-topic",
      security::pattern_type::literal};
    security::resource_pattern cluster_resource{
      security::resource_type::cluster,
      security::resource_pattern::wildcard,
      security::pattern_type::literal};

    fixture().get_cluster_mock().acl_store().add_bindings(
      {// Group ACL on topic resource
       security::acl_binding(
         topic_resource,
         security::acl_entry{
           security::acl_principal::from_string("Group:test-group"),
           security::acl_host::wildcard_host(),
           security::acl_operation::read,
           security::acl_permission::allow}),
       // Group ACL on cluster resource
       security::acl_binding(
         cluster_resource,
         security::acl_entry{
           security::acl_principal::from_string("Group:test-group"),
           security::acl_host::wildcard_host(),
           security::acl_operation::describe,
           security::acl_permission::allow})});

    co_await fixture().upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 2; })

    const auto& local_acls = fixture().security_service().acls();

    EXPECT_EQ(local_acls.size(), 2);

    const auto topic_it = local_acls.find(topic_resource);
    ASSERT_NE_CORO(topic_it, local_acls.end())
      << "Failed to find topic resource " << topic_resource;
    ASSERT_FALSE_CORO(topic_it->second.empty())
      << "Topic resource contains no acls";

    EXPECT_TRUE(topic_it->second.contains(
      security::acl_operation::read,
      security::acl_principal::from_string("Group:test-group"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow));

    const auto cluster_it = local_acls.find(cluster_resource);
    ASSERT_NE_CORO(cluster_it, local_acls.end())
      << "Failed to find cluster resource " << cluster_resource;
    ASSERT_FALSE_CORO(cluster_it->second.empty())
      << "Cluster resource contains no acls";

    EXPECT_TRUE(cluster_it->second.contains(
      security::acl_operation::describe,
      security::acl_principal::from_string("Group:test-group"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow));
}

TEST_F_CORO(security_migrator_test, migrate_mixed_principal_acls) {
    // This test verifies that a mix of User, Role, and Group ACLs are migrated
    security::resource_pattern topic_resource{
      security::resource_type::topic,
      "test-topic",
      security::pattern_type::literal};

    fixture().get_cluster_mock().acl_store().add_bindings(
      {// User ACL
       security::acl_binding(
         topic_resource,
         security::acl_entry{
           security::acl_principal::from_string("User:test-user"),
           security::acl_host::wildcard_host(),
           security::acl_operation::read,
           security::acl_permission::allow}),
       // Role ACL
       security::acl_binding(
         topic_resource,
         security::acl_entry{
           security::acl_principal::from_string("RedpandaRole:test-role"),
           security::acl_host::wildcard_host(),
           security::acl_operation::write,
           security::acl_permission::allow}),
       // Group ACL
       security::acl_binding(
         topic_resource,
         security::acl_entry{
           security::acl_principal::from_string("Group:test-group"),
           security::acl_host::wildcard_host(),
           security::acl_operation::describe,
           security::acl_permission::allow})});

    co_await fixture().upsert_link(get_default_metadata());

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 1; })

    const auto& local_acls = fixture().security_service().acls();

    EXPECT_EQ(local_acls.size(), 1);

    const auto topic_it = local_acls.find(topic_resource);
    ASSERT_NE_CORO(topic_it, local_acls.end())
      << "Failed to find topic resource " << topic_resource;

    // Verify User ACL
    EXPECT_TRUE(topic_it->second.contains(
      security::acl_operation::read,
      security::acl_principal::from_string("User:test-user"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow))
      << "User ACL not found";

    // Verify Role ACL
    EXPECT_TRUE(topic_it->second.contains(
      security::acl_operation::write,
      security::acl_principal::from_string("RedpandaRole:test-role"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow))
      << "Role ACL not found";

    // Verify Group ACL
    EXPECT_TRUE(topic_it->second.contains(
      security::acl_operation::describe,
      security::acl_principal::from_string("Group:test-group"),
      security::acl_host::wildcard_host(),
      security::acl_permission::allow))
      << "Group ACL not found";
}
} // namespace cluster_link::tests
