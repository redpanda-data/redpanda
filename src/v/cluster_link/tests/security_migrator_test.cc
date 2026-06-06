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
#include "security/acl.h"
#include "security/acl_store.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

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

TEST_F_CORO(security_migrator_test, deletes_acls_removed_from_source) {
    // Verifies that when an ACL is removed from the source cluster, the
    // migrator removes it from the target as well, without disturbing the
    // ACLs that still exist on the source.
    security::resource_pattern foo_resource{
      security::resource_type::topic, "foo", security::pattern_type::literal};
    security::resource_pattern bar_resource{
      security::resource_type::topic, "bar", security::pattern_type::literal};

    auto foo_binding = security::acl_binding(
      foo_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:test-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::read,
        security::acl_permission::allow});
    auto bar_binding = security::acl_binding(
      bar_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:other-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::write,
        security::acl_permission::allow});

    fixture().get_cluster_mock().acl_store().add_bindings(
      {foo_binding, bar_binding});

    auto md = get_default_metadata();
    md.configuration.security_settings_sync_cfg.sync_deletions = true;
    co_await fixture().upsert_link(std::move(md));

    // Both ACLs sync to the target.
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 2; })

    // Remove one ACL from the source.
    fixture().get_cluster_mock().acl_store().remove_bindings(
      {security::acl_binding_filter(
        security::resource_pattern_filter(foo_resource),
        security::acl_entry_filter(foo_binding.entry()))});

    // The deletion propagates to the target: foo is removed while bar remains.
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, foo_resource, bar_resource] {
        const auto& acls = fixture().security_service().acls();
        return acls.find(foo_resource) == acls.end()
               && acls.find(bar_resource) != acls.end();
    })
}

TEST_F_CORO(
  security_migrator_test, reconciles_acls_matching_a_concrete_filter) {
    // Drives the concrete (non-wildcard) branches of the model -> security
    // filter conversion: with a literal-topic filter the migrator must still
    // list the matching ACL on the target so that a source-side deletion is
    // reconciled.
    security::resource_pattern foo_resource{
      security::resource_type::topic, "foo", security::pattern_type::literal};
    auto foo_binding = security::acl_binding(
      foo_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:test-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::read,
        security::acl_permission::allow});

    fixture().get_cluster_mock().acl_store().add_bindings({foo_binding});

    auto md = get_default_metadata();
    auto& sync_cfg = md.configuration.security_settings_sync_cfg;
    sync_cfg.sync_deletions = true;
    sync_cfg.acl_filters.clear();
    sync_cfg.acl_filters.push_back(model::acl_filter{
      .resource_filter = {
        .resource_type = model::acl_resource::topic,
        .pattern_type = model::acl_pattern::literal,
      },
      .access_filter = {
        .operation = model::acl_operation::any,
        .permission_type = model::acl_permission_type::any,
      },
    });
    co_await fixture().upsert_link(std::move(md));

    // The literal-topic filter matches and syncs the ACL.
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 1; })

    // Deleting it on the source removes it from the target, which only works
    // if the concrete filter correctly lists the target's ACL.
    fixture().get_cluster_mock().acl_store().remove_bindings(
      {security::acl_binding_filter(
        security::resource_pattern_filter(foo_resource),
        security::acl_entry_filter(foo_binding.entry()))});

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().empty(); })
}

TEST_F_CORO(security_migrator_test, deletes_all_acls_when_source_emptied) {
    // Removing every ACL from the source must fully clear the target: an empty
    // source is reconciled as "delete everything", not treated as a no-op.
    security::resource_pattern foo_resource{
      security::resource_type::topic, "foo", security::pattern_type::literal};
    security::resource_pattern bar_resource{
      security::resource_type::topic, "bar", security::pattern_type::literal};
    auto foo_binding = security::acl_binding(
      foo_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:test-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::read,
        security::acl_permission::allow});
    auto bar_binding = security::acl_binding(
      bar_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:other-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::write,
        security::acl_permission::allow});

    fixture().get_cluster_mock().acl_store().add_bindings(
      {foo_binding, bar_binding});

    auto md = get_default_metadata();
    md.configuration.security_settings_sync_cfg.sync_deletions = true;
    co_await fixture().upsert_link(std::move(md));

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 2; })

    // Remove every ACL from the source.
    fixture().get_cluster_mock().acl_store().remove_bindings(
      {security::acl_binding_filter(
         security::resource_pattern_filter(foo_resource),
         security::acl_entry_filter(foo_binding.entry())),
       security::acl_binding_filter(
         security::resource_pattern_filter(bar_resource),
         security::acl_entry_filter(bar_binding.entry()))});

    // The target is cleared, not left holding the stale ACLs.
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().empty(); })
}

TEST_F_CORO(security_migrator_test, deletes_sr_acl_removed_from_source) {
    // A schema-registry ACL deletion must also reconcile. The per-binding
    // delete filter has to use the schema_registry subsystem; defaulting to
    // kafka would never match an sr_subject resource, leaving it stale.
    security::resource_pattern subject_resource{
      security::resource_type::sr_subject,
      "test-subject",
      security::pattern_type::literal};
    auto sr_binding = security::acl_binding(
      subject_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:test-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::read,
        security::acl_permission::allow});

    fixture().get_cluster_mock().acl_store().add_bindings({sr_binding});

    auto md = get_default_metadata();
    md.configuration.security_settings_sync_cfg.sync_deletions = true;
    co_await fixture().upsert_link(std::move(md));

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().size() >= 1; })

    // Remove the SR ACL from the source. The removal filter must use the
    // schema_registry subsystem to match an sr_subject resource.
    fixture().get_cluster_mock().acl_store().remove_bindings(
      {security::acl_binding_filter(
        security::resource_pattern_filter(
          subject_resource.resource(),
          subject_resource.name(),
          subject_resource.pattern(),
          security::resource_pattern_filter::resource_subsystem::
            schema_registry),
        security::acl_entry_filter(sr_binding.entry()))});

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      5s, [this] { return fixture().security_service().acls().empty(); })
}

TEST_F_CORO(
  security_migrator_test, does_not_delete_when_sync_deletions_disabled) {
    // sync_deletions defaults to false, so a default-configured link is
    // additive-only: an ACL removed on the source is NOT removed from the
    // target. Adding a second ACL after the removal proves a reconciliation
    // cycle ran without deleting the first.
    security::resource_pattern foo_resource{
      security::resource_type::topic, "foo", security::pattern_type::literal};
    security::resource_pattern bar_resource{
      security::resource_type::topic, "bar", security::pattern_type::literal};
    auto foo_binding = security::acl_binding(
      foo_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:test-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::read,
        security::acl_permission::allow});
    auto bar_binding = security::acl_binding(
      bar_resource,
      security::acl_entry{
        security::acl_principal::from_string("User:other-user"),
        security::acl_host::wildcard_host(),
        security::acl_operation::write,
        security::acl_permission::allow});

    fixture().get_cluster_mock().acl_store().add_bindings({foo_binding});

    co_await fixture().upsert_link(get_default_metadata());

    // foo syncs to the target.
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, foo_resource] {
        const auto& acls = fixture().security_service().acls();
        return acls.find(foo_resource) != acls.end();
    })

    // Remove foo on the source and add bar. bar appearing on the target proves
    // a reconciliation cycle ran after foo's removal.
    fixture().get_cluster_mock().acl_store().remove_bindings(
      {security::acl_binding_filter(
        security::resource_pattern_filter(foo_resource),
        security::acl_entry_filter(foo_binding.entry()))});
    fixture().get_cluster_mock().acl_store().add_bindings({bar_binding});

    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, bar_resource] {
        const auto& acls = fixture().security_service().acls();
        return acls.find(bar_resource) != acls.end();
    })

    // Additive-only: foo was not deleted from the target despite its removal on
    // the source.
    const auto& acls = fixture().security_service().acls();
    EXPECT_NE(acls.find(foo_resource), acls.end())
      << "foo should be retained when sync_deletions is disabled";
}
} // namespace cluster_link::tests
