// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_snapshot.h"
#include "cluster/security_manager.h"
#include "config/mock_property.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/role.h"
#include "security/role_store.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <fmt/format.h>

#include <vector>

namespace cluster {

namespace {

const security::acl_host any_host = security::acl_host::wildcard_host();

security::acl_binding literal_read_allow(
  std::string_view topic, const security::acl_principal& principal) {
    return security::acl_binding(
      security::resource_pattern(
        security::resource_type::topic,
        ss::sstring{topic},
        security::pattern_type::literal),
      security::acl_entry(
        principal,
        any_host,
        security::acl_operation::read,
        security::acl_permission::allow));
}

} // namespace

// Applying a controller snapshot must publish this shard's ACL store and role
// store together.
//
// The authorizer resolves role-granted ACLs by consulting the role store inline
// on every check, so the two stores are jointly load-bearing. Staging them
// yields, and authorization runs on this shard in those gaps -- if either store
// becomes visible before the other, a principal whose grant comes via a role is
// transiently denied even though both the role and the ACL are present before
// and after.
//
// The role_store half also has to be a replacement rather than a merge: a role
// deleted in a log range that the snapshot subsumes is never removed otherwise,
// so it keeps granting access on any node that installs that snapshot.
TEST_CORO(security_snapshot_apply, publishes_acls_and_roles_together) {
    // The snapshot rotates the grant from one role to another: both the ACL and
    // the role membership change. That is what makes the ordering observable --
    // publishing the ACLs first leaves a state where the new ACL names a role
    // the live role store does not have yet, and publishing the roles first
    // leaves the old ACL naming a role that is already gone. Either way alice
    // is denied, even though she is authorized both before and after.
    const security::role_name old_role{"old-role"};
    const security::role_name new_role{"new-role"};
    const security::role_name stale_role{"stale-role"};
    const security::acl_principal alice(
      security::principal_type::user, "alice");
    const security::acl_principal old_role_principal(
      security::principal_type::role, old_role());
    const security::acl_principal new_role_principal(
      security::principal_type::role, new_role());
    const model::topic granted_topic{"granted-topic"};

    security::role_store roles;
    security::credential_store credentials;
    config::mock_property<std::vector<ss::sstring>> superusers(
      std::vector<ss::sstring>{});
    security::authorizer auth(superusers.bind(), &roles);

    const security::role alice_role{
      {security::role_member::from_principal(alice)}};

    // alice can read granted-topic only by virtue of her role membership.
    // stale-role is granted nothing; it exists to show that a role the snapshot
    // omits is actually removed rather than merged through.
    ASSERT_TRUE_CORO(roles.put(old_role, alice_role));
    ASSERT_TRUE_CORO(roles.put(stale_role, alice_role));
    chunked_vector<security::acl_binding> initial;
    initial.push_back(literal_read_allow(granted_topic(), old_role_principal));
    auth.add_bindings(initial);

    auto allowed = [&] {
        return bool(auth.authorized(
          granted_topic,
          security::acl_operation::read,
          alice,
          any_host,
          security::superuser_required::no,
          {}));
    };
    ASSERT_TRUE_CORO(allowed());

    // The snapshot carries the same grant, plus enough filler that staging is
    // certain to exceed the task quota and yield. The grant under test is last
    // so a partially staged store cannot happen to contain it.
    constexpr size_t filler = 50'000;
    controller_snapshot_parts::security_t snapshot;
    snapshot.acls.reserve(filler + 1);
    for (size_t i = 0; i < filler; ++i) {
        snapshot.acls.push_back(literal_read_allow(
          fmt::format("filler-{:06}", i), new_role_principal));
    }
    snapshot.acls.push_back(
      literal_read_allow(granted_topic(), new_role_principal));
    snapshot.roles.emplace_back(new_role, alice_role);

    bool apply_complete = false;
    auto apply = apply_security_snapshot_to_shard(
                   credentials, auth, roles, snapshot)
                   .then([&apply_complete] { apply_complete = true; });

    // Interleave authorization checks with the staging.
    size_t checks = 0;
    size_t denials = 0;
    while (!apply_complete) {
        if (!allowed()) {
            ++denials;
        }
        ++checks;
        co_await ss::yield();
    }
    co_await std::move(apply);

    // Guards against the test passing trivially: if staging never yielded,
    // nothing interleaved and neither race was exercised.
    ASSERT_GT_CORO(checks, 1);
    ASSERT_EQ_CORO(denials, 0);

    ASSERT_TRUE_CORO(allowed());

    // The snapshot is authoritative for roles: the ones it omits are gone,
    // rather than merged through and left granting access indefinitely.
    ASSERT_TRUE_CORO(roles.contains(new_role));
    ASSERT_FALSE_CORO(roles.contains(old_role));
    ASSERT_FALSE_CORO(roles.contains(stale_role));
}

} // namespace cluster
