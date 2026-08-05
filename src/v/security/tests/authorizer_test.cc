// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "absl/container/flat_hash_set.h"
#include "config/mock_property.h"
#include "container/chunked_vector.h"
#include "pandaproxy/schema_registry/types.h"
#include "random/generators.h"
#include "security/acl.h"
#include "security/acl_store.h"
#include "security/authorizer.h"
#include "security/role.h"
#include "security/role_store.h"
#include "test_utils/test.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/defer.hh>

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <gtest/gtest.h>

#include <string_view>
#include <vector>

namespace security {

static const acl_entry allow_read_acl(
  acl_wildcard_user,
  acl_wildcard_host,
  acl_operation::read,
  acl_permission::allow);

static const acl_entry allow_write_acl(
  acl_wildcard_user,
  acl_wildcard_host,
  acl_operation::write,
  acl_permission::allow);

static const acl_entry deny_read_acl(
  acl_wildcard_user,
  acl_wildcard_host,
  acl_operation::read,
  acl_permission::deny);

static const resource_pattern default_resource(
  resource_type::topic, "foo-3lkjfklwe", pattern_type::literal);

static const resource_pattern wildcard_resource(
  resource_type::topic, resource_pattern::wildcard, pattern_type::literal);

static const resource_pattern
  prefixed_resource(resource_type::topic, "foo", pattern_type::prefixed);

static auto get_acls(authorizer& auth, resource_pattern resource) {
    absl::flat_hash_set<acl_entry> found;
    auto acls = auth.acls(acl_binding_filter(
      resource_pattern_filter(resource), acl_entry_filter::any()));
    for (auto acl : acls) {
        found.emplace(acl.entry());
    }
    return found;
}

static auto get_acls(authorizer& auth, acl_principal principal) {
    absl::flat_hash_set<acl_entry> found;
    auto acls = auth.acls(acl_binding_filter(
      resource_pattern_filter::any(),
      acl_entry_filter(principal, std::nullopt, std::nullopt, std::nullopt)));
    for (auto acl : acls) {
        found.emplace(acl.entry());
    }
    return found;
}

static auto get_acls(authorizer& auth, const acl_binding_filter& filter) {
    absl::flat_hash_set<acl_entry> found;
    auto acls = auth.acls(filter);
    for (const auto& acl : acls) {
        found.emplace(acl.entry());
    }
    return found;
}

static authorizer make_test_instance(
  authorizer::allow_empty_matches allow = authorizer::allow_empty_matches::no,
  std::optional<const role_store*> roles = std::nullopt) {
    static role_store _roles;
    auto b = config::mock_binding<std::vector<ss::sstring>>(
      std::vector<ss::sstring>{});

    return {allow, std::move(b), roles.value_or(&_roles)};
}

class authorizer_test : public ::testing::Test {};

TEST(AUTHORIZER_TEST, authz_resource_type_auto) {
    ASSERT_EQ(
      get_resource_type<model::topic>(), security::resource_type::topic);
    ASSERT_EQ(
      get_resource_type<kafka::group_id>(), security::resource_type::group);
    ASSERT_EQ(
      get_resource_type<security::acl_cluster_name>(),
      security::resource_type::cluster);
    ASSERT_EQ(
      get_resource_type<kafka::transactional_id>(),
      security::resource_type::transactional_id);
    ASSERT_EQ(
      get_resource_type<pandaproxy::schema_registry::context_subject>(),
      security::resource_type::sr_subject);
    ASSERT_EQ(
      get_resource_type<pandaproxy::schema_registry::registry_resource>(),
      security::resource_type::sr_registry);

    ASSERT_EQ(
      get_resource_type<model::topic>(), get_resource_type<model::topic>());
    ASSERT_NE(
      get_resource_type<model::topic>(), get_resource_type<kafka::group_id>());
}

TEST(AUTHORIZER_TEST, authz_empty_resource_name) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance();

    ASSERT_FALSE(auth.authorized(
      kafka::group_id(""),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {}));

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::group, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      kafka::group_id(""),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::group);
    ASSERT_EQ(result.resource_name, "");
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

static const model::topic default_topic("topic1");

TEST(AUTHORIZER_TEST, authz_deny_applies_first) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.2.1");

    auto auth = make_test_instance();

    acl_entry allow(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::all,
      acl_permission::allow);

    acl_entry deny(user, host, acl_operation::all, acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, allow);
    bindings.emplace_back(resource, deny);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, deny);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_allow_all) {
    acl_principal user(principal_type::user, "random");
    acl_host host("192.0.4.4");

    auto auth = make_test_instance();

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::all,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_super_user_allow) {
    acl_principal user1(principal_type::user, "superuser1");
    acl_principal user2(principal_type::user, "superuser2");
    acl_host host("192.0.4.4");

    config::mock_property<std::vector<ss::sstring>> superuser_config_prop(
      std::vector<ss::sstring>{});
    role_store roles;
    authorizer auth(superuser_config_prop.bind(), &roles);

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::all,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host,
      security::superuser_required::no,
      {});

    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user2,
      host,
      security::superuser_required::no,
      {});

    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user2);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    // Adding superusers
    superuser_config_prop.update({"superuser1", "superuser2"});

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_TRUE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user2,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user2);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_TRUE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    // Revoking a superuser
    superuser_config_prop.update({"superuser2"});

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host,
      security::superuser_required::no,
      {});

    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user2,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user2);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_TRUE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_wildcards) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance();

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_FALSE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_TRUE(result.empty_matches);

    acl_principal user1(principal_type::user, "alice");
    acl_host host1("192.168.3.1");

    acl_entry read_acl(
      user1, host1, acl_operation::read, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern wildcard_resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(wildcard_resource, read_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_EQ(result.acl, read_acl);
    ASSERT_EQ(result.resource_pattern, wildcard_resource);
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host1);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    acl_entry write_acl(
      user1, host1, acl_operation::write, acl_permission::allow);

    bindings.clear();
    resource_pattern resource1(
      resource_type::topic, "topic1", pattern_type::literal);
    bindings.emplace_back(resource1, write_acl);
    auth.add_bindings(bindings);

    acl_entry deny_write_acl(
      user1, host1, acl_operation::write, acl_permission::deny);

    bindings.clear();
    bindings.emplace_back(wildcard_resource, deny_write_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(
      default_topic,
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, deny_write_acl);
    ASSERT_EQ(result.resource_pattern, wildcard_resource);
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host1);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_no_acls_deny) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance();

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_FALSE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_TRUE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_no_acls_allow) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance(authorizer::allow_empty_matches::yes);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_TRUE(result.empty_matches);
}

static void do_implied_acls(
  const acl_principal& bind_principal,
  std::optional<role_store*> roles = std::nullopt,
  chunked_vector<acl_principal> groups = {}) {
    auto test_allow = [&bind_principal, &roles, &groups](
                        acl_operation op, std::set<acl_operation> allowed) {
        acl_principal user(principal_type::user, "alice");
        ASSERT_TRUE(
          user == bind_principal
          || bind_principal.type() == principal_type::role
          || bind_principal.type() == principal_type::group);

        acl_host host("192.168.3.1");

        acl_entry acl(
          bind_principal, acl_wildcard_host, op, acl_permission::allow);

        auto auth = make_test_instance(
          authorizer::allow_empty_matches::no, roles);

        chunked_vector<acl_binding> bindings;
        resource_pattern resource(
          resource_type::cluster, default_cluster_name, pattern_type::literal);
        bindings.emplace_back(resource, acl);
        auth.add_bindings(bindings);

        // unknown and any are not valid ops. should they be removed?
        for (auto test_op : {// acl_operation::unknown,
                             // acl_operation::any,
                             acl_operation::all,
                             acl_operation::read,
                             acl_operation::write,
                             acl_operation::create,
                             acl_operation::remove,
                             acl_operation::alter,
                             acl_operation::describe,
                             acl_operation::cluster_action,
                             acl_operation::describe_configs,
                             acl_operation::alter_configs,
                             acl_operation::idempotent_write}) {
            auto ok = auth.authorized(
              default_cluster_name,
              test_op,
              user,
              host,
              security::superuser_required::no,
              groups);
            if (allowed.contains(test_op) || test_op == op) {
                ASSERT_TRUE(ok.authorized);
                ASSERT_EQ(ok.acl, acl);
                ASSERT_EQ(ok.resource_pattern, resource);
                ASSERT_FALSE(ok.empty_matches);
            } else {
                ASSERT_FALSE(ok.authorized);
                ASSERT_FALSE(ok.acl.has_value());
                ASSERT_FALSE(ok.resource_pattern.has_value());
                ASSERT_TRUE(ok.empty_matches);
            }

            ASSERT_EQ(ok.principal, user);
            ASSERT_EQ(ok.host, host);
            ASSERT_FALSE(ok.is_superuser);
            ASSERT_EQ(ok.resource_type, security::resource_type::cluster);
            ASSERT_EQ(ok.resource_name, default_cluster_name());
        }
    };

    auto test_deny = [&bind_principal, &roles, &groups](
                       acl_operation op, std::set<acl_operation> denied) {
        acl_principal user(principal_type::user, "alice");
        ASSERT_TRUE(
          user == bind_principal
          || bind_principal.type() == principal_type::role
          || bind_principal.type() == principal_type::group);

        acl_host host("192.168.3.1");

        acl_entry deny(
          bind_principal, acl_wildcard_host, op, acl_permission::deny);

        acl_entry allow(
          bind_principal,
          acl_wildcard_host,
          acl_operation::all,
          acl_permission::allow);

        auto auth = make_test_instance(
          authorizer::allow_empty_matches::no, roles);

        chunked_vector<acl_binding> bindings;
        resource_pattern resource(
          resource_type::cluster, default_cluster_name, pattern_type::literal);
        bindings.emplace_back(resource, deny);
        bindings.emplace_back(resource, allow);
        auth.add_bindings(bindings);

        // unknown and any are not valid ops. should they be removed?
        for (auto test_op : {// acl_operation::unknown,
                             // acl_operation::any,
                             acl_operation::all,
                             acl_operation::read,
                             acl_operation::write,
                             acl_operation::create,
                             acl_operation::remove,
                             acl_operation::alter,
                             acl_operation::describe,
                             acl_operation::cluster_action,
                             acl_operation::describe_configs,
                             acl_operation::alter_configs,
                             acl_operation::idempotent_write}) {
            auto ok = auth.authorized(
              default_cluster_name,
              test_op,
              user,
              host,
              security::superuser_required::no,
              groups);
            if (denied.contains(test_op) || test_op == op) {
                ASSERT_FALSE(ok.authorized);
                ASSERT_EQ(ok.acl, deny);
            } else {
                ASSERT_TRUE(ok.authorized);
                ASSERT_EQ(ok.acl, allow);
            }

            ASSERT_EQ(ok.resource_pattern, resource);
            ASSERT_EQ(ok.principal, user);
            ASSERT_EQ(ok.host, host);
            ASSERT_FALSE(ok.is_superuser);
            ASSERT_FALSE(ok.empty_matches);
            ASSERT_EQ(ok.resource_type, security::resource_type::cluster);
            ASSERT_EQ(ok.resource_name, default_cluster_name());
        }
    };

    test_allow(
      acl_operation::all,
      {
        acl_operation::read,
        acl_operation::write,
        acl_operation::create,
        acl_operation::remove,
        acl_operation::alter,
        acl_operation::describe,
        acl_operation::cluster_action,
        acl_operation::describe_configs,
        acl_operation::alter_configs,
        acl_operation::idempotent_write,
      });

    test_deny(
      acl_operation::all,
      {
        acl_operation::read,
        acl_operation::write,
        acl_operation::create,
        acl_operation::remove,
        acl_operation::alter,
        acl_operation::describe,
        acl_operation::cluster_action,
        acl_operation::describe_configs,
        acl_operation::alter_configs,
        acl_operation::idempotent_write,
      });

    test_allow(acl_operation::read, {acl_operation::describe});
    test_allow(acl_operation::write, {acl_operation::describe});
    test_allow(acl_operation::remove, {acl_operation::describe});
    test_allow(acl_operation::alter, {acl_operation::describe});
    test_deny(acl_operation::describe, {});
    test_allow(acl_operation::alter_configs, {acl_operation::describe_configs});
    test_deny(acl_operation::describe_configs, {});
}

TEST(AUTHORIZER_TEST, authz_implied_acls) {
    do_implied_acls(acl_principal{principal_type::user, "alice"});
}

TEST(AUTHORIZER_TEST, authz_allow_for_all_wildcard_resource) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.3.1");

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::read,
      acl_permission::allow);

    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);

    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_remove_acl_wildcard_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    bindings.emplace_back(wildcard_resource, allow_write_acl);
    auth.add_bindings(bindings);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(wildcard_resource, allow_read_acl);
        auth.remove_bindings(filters);
    }

    absl::flat_hash_set<acl_entry> expected{allow_write_acl};
    ASSERT_EQ(get_acls(auth, wildcard_resource), expected);
}

TEST(AUTHORIZER_TEST, authz_remove_all_acl_wildcard_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    auth.add_bindings(bindings);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(wildcard_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    ASSERT_EQ(
      get_acls(auth, acl_binding_filter::any()),
      absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_allow_for_all_prefixed_resource) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.3.1");

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::read,
      acl_permission::allow);

    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, "foo", pattern_type::prefixed);
    bindings.emplace_back(resource, acl);

    auth.add_bindings(bindings);

    auto result = auth.authorized(
      model::topic("foo-3uk3rlkj"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, "foo-3uk3rlkj");
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_remove_acl_prefixed_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    bindings.emplace_back(prefixed_resource, allow_write_acl);
    auth.add_bindings(bindings);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(prefixed_resource, allow_read_acl);
        auth.remove_bindings(filters);
    }

    absl::flat_hash_set<acl_entry> expected{allow_write_acl};
    ASSERT_EQ(get_acls(auth, prefixed_resource), expected);
}

TEST(AUTHORIZER_TEST, authz_remove_all_acl_prefixed_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    auth.add_bindings(bindings);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(prefixed_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    ASSERT_EQ(
      get_acls(auth, acl_binding_filter::any()),
      absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_acls_on_literal_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(default_resource, allow_read_acl);
    bindings.emplace_back(default_resource, allow_write_acl);
    auth.add_bindings(bindings);

    bindings.clear();
    bindings.emplace_back(default_resource, allow_write_acl);
    bindings.emplace_back(default_resource, deny_read_acl);
    auth.add_bindings(bindings);

    auto get_acls = [](authorizer& auth, resource_pattern resource) {
        absl::flat_hash_set<acl_entry> found;
        auto acls = auth.acls(acl_binding_filter(
          resource_pattern_filter(resource), acl_entry_filter::any()));
        for (auto acl : acls) {
            found.emplace(acl.entry());
        }
        return found;
    };

    absl::flat_hash_set<acl_entry> expected{
      allow_read_acl,
      allow_write_acl,
      deny_read_acl,
    };

    ASSERT_EQ(expected, get_acls(auth, default_resource));
    ASSERT_EQ(
      get_acls(auth, wildcard_resource), absl::flat_hash_set<acl_entry>{});
    ASSERT_EQ(
      get_acls(auth, prefixed_resource), absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_acls_on_wildcard_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    bindings.emplace_back(wildcard_resource, allow_write_acl);
    auth.add_bindings(bindings);

    bindings.clear();
    bindings.emplace_back(wildcard_resource, allow_write_acl);
    bindings.emplace_back(wildcard_resource, deny_read_acl);
    auth.add_bindings(bindings);

    absl::flat_hash_set<acl_entry> expected{
      allow_read_acl,
      allow_write_acl,
      deny_read_acl,
    };

    ASSERT_EQ(expected, get_acls(auth, wildcard_resource));
    ASSERT_EQ(
      get_acls(auth, default_resource), absl::flat_hash_set<acl_entry>{});
    ASSERT_EQ(
      get_acls(auth, prefixed_resource), absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_acls_on_prefixed_resource) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    bindings.emplace_back(prefixed_resource, allow_write_acl);
    auth.add_bindings(bindings);

    bindings.clear();
    bindings.emplace_back(prefixed_resource, allow_write_acl);
    bindings.emplace_back(prefixed_resource, deny_read_acl);
    auth.add_bindings(bindings);

    absl::flat_hash_set<acl_entry> expected{
      allow_read_acl,
      allow_write_acl,
      deny_read_acl,
    };

    ASSERT_EQ(expected, get_acls(auth, prefixed_resource));
    ASSERT_EQ(
      get_acls(auth, wildcard_resource), absl::flat_hash_set<acl_entry>{});
    ASSERT_EQ(
      get_acls(auth, default_resource), absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_auth_prefix_resource) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.3.1");

    auto auth = make_test_instance();

    auto add_acl = [&auth](ss::sstring name, pattern_type type) {
        chunked_vector<acl_binding> bindings;
        bindings.emplace_back(
          resource_pattern(resource_type::topic, name, type), deny_read_acl);
        auth.add_bindings(bindings);
    };

    auto add_lit = [&add_acl](ss::sstring name) {
        add_acl(name, pattern_type::literal);
    };

    auto add_pre = [&add_acl](ss::sstring name) {
        add_acl(name, pattern_type::prefixed);
    };

    add_lit("a_other");
    add_pre("a_other");
    add_pre("foo-" + random_generators::gen_alphanum_string(10));
    add_pre("foo-" + random_generators::gen_alphanum_string(10));
    add_pre("foo-" + random_generators::gen_alphanum_string(10) + "-zzz");
    add_pre("fooo-" + random_generators::gen_alphanum_string(10));
    add_pre("fo-" + random_generators::gen_alphanum_string(10));
    add_pre("fop-" + random_generators::gen_alphanum_string(10));
    add_pre("fon-" + random_generators::gen_alphanum_string(10));
    add_pre("fon-");
    add_pre("z_other");
    add_lit("z_other");

    auto result = auth.authorized(
      model::topic(default_resource.name()),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_FALSE(result.is_superuser);
    ASSERT_TRUE(result.empty_matches);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_resource.name());

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(
      model::topic(default_resource.name()),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    ASSERT_TRUE(result.authorized);
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, prefixed_resource);
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_resource.name());
}

TEST(AUTHORIZER_TEST, authz_single_char) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.3.1");

    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    resource_pattern resource{resource_type::topic, "f", pattern_type::literal};
    bindings.emplace_back(resource, allow_read_acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      model::topic("f"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      model::topic("foo"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    bindings.clear();
    resource_pattern resource1(
      resource_type::topic, "_", pattern_type::prefixed);
    bindings.emplace_back(resource1, allow_read_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(
      model::topic("_foo"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, resource1);

    result = auth.authorized(
      model::topic("_"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, resource1);

    result = auth.authorized(
      model::topic("foo_"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_TRUE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_get_acls_principal) {
    acl_principal user(principal_type::user, "alice");

    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(
      default_resource,
      acl_entry(
        user, acl_wildcard_host, acl_operation::write, acl_permission::allow));
    auth.add_bindings(bindings);

    ASSERT_EQ(
      get_acls(auth, acl_principal(principal_type::user, "*")),
      absl::flat_hash_set<acl_entry>{});
    ASSERT_EQ(get_acls(auth, user).size(), 1);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(default_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    bindings.clear();
    bindings.emplace_back(
      default_resource,
      acl_entry(
        acl_wildcard_user,
        acl_wildcard_host,
        acl_operation::write,
        acl_permission::allow));
    auth.add_bindings(bindings);

    ASSERT_EQ(
      get_acls(auth, acl_principal(principal_type::user, "*")).size(), 1);
    ASSERT_EQ(get_acls(auth, user), absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_acl_filter) {
    acl_principal user(principal_type::user, "alice");

    resource_pattern resource1(
      resource_type::topic, "foo-39ufksxyz", pattern_type::literal);
    resource_pattern resource2(
      resource_type::topic, "bar-2309fnvkl", pattern_type::literal);
    resource_pattern prefixed_resource(
      resource_type::topic, "bar-", pattern_type::prefixed);

    acl_binding acl1(
      resource1,
      acl_entry(
        user, acl_wildcard_host, acl_operation::read, acl_permission::allow));

    acl_binding acl2(
      resource1,
      acl_entry(
        user,
        acl_host("192.168.0.1"),
        acl_operation::write,
        acl_permission::allow));

    acl_binding acl3(
      resource2,
      acl_entry(
        user,
        acl_wildcard_host,
        acl_operation::describe,
        acl_permission::allow));

    acl_binding acl4(
      prefixed_resource,
      acl_entry(
        acl_wildcard_user,
        acl_wildcard_host,
        acl_operation::read,
        acl_permission::allow));

    auto auth = make_test_instance();
    auth.add_bindings({acl1, acl2, acl3, acl4});

    auto to_set = [](chunked_vector<acl_binding> bindings) {
        absl::flat_hash_set<acl_binding> ret(bindings.begin(), bindings.end());
        return ret;
    };

    ASSERT_EQ(
      (absl::flat_hash_set<acl_binding>{acl1, acl2, acl3, acl4}),
      to_set(auth.acls(acl_binding_filter::any())));

    ASSERT_EQ(
      (absl::flat_hash_set<acl_binding>{acl1, acl2}),
      to_set(
        auth.acls(acl_binding_filter(resource1, acl_entry_filter::any()))));

    ASSERT_EQ(
      (absl::flat_hash_set<acl_binding>{acl4}),
      to_set(auth.acls(
        acl_binding_filter(prefixed_resource, acl_entry_filter::any()))));

    ASSERT_EQ(
      (absl::flat_hash_set<acl_binding>{acl3, acl4}),
      to_set(auth.acls(acl_binding_filter(
        resource_pattern_filter(
          std::nullopt,
          resource2.name(),
          resource_pattern_filter::pattern_match{}),
        acl_entry_filter::any()))));
}

TEST(AUTHORIZER_TEST, authz_topic_acl) {
    acl_principal user1(principal_type::user, "alice");
    acl_principal user2(principal_type::user, "rob");
    acl_principal user3(principal_type::user, "batman");
    acl_host host1("192.168.1.1");
    acl_host host2("192.168.1.2");

    // user1 -> allow read from host{1,2}
    acl_entry acl1(user1, host1, acl_operation::read, acl_permission::allow);

    acl_entry acl2(user1, host2, acl_operation::read, acl_permission::allow);

    // user1 -> deny read from host1
    acl_entry acl3(user1, host1, acl_operation::read, acl_permission::deny);

    // user1 -> allow write from host1
    acl_entry acl4(user1, host1, acl_operation::write, acl_permission::allow);

    // user1 -> allow describe
    acl_entry acl5(
      user1, acl_wildcard_host, acl_operation::describe, acl_permission::allow);

    // user2 -> allow read
    acl_entry acl6(
      user2, acl_wildcard_host, acl_operation::read, acl_permission::allow);

    // user3 -> allow write
    acl_entry acl7(
      user3, acl_wildcard_host, acl_operation::write, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, "topic1", pattern_type::literal);
    bindings.emplace_back(resource, acl1);
    bindings.emplace_back(resource, acl2);
    bindings.emplace_back(resource, acl3);
    bindings.emplace_back(resource, acl4);
    bindings.emplace_back(resource, acl5);
    bindings.emplace_back(resource, acl6);
    bindings.emplace_back(resource, acl7);

    auto auth = make_test_instance();
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host2,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl2);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result);
    ASSERT_EQ(result.acl, acl3);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl4);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::write,
      user1,
      host2,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    result = auth.authorized(
      default_topic,
      acl_operation::describe,
      user1,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl5);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::describe,
      user1,
      host2,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl5);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::alter,
      user1,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    result = auth.authorized(
      default_topic,
      acl_operation::alter,
      user1,
      host2,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    result = auth.authorized(
      default_topic,
      acl_operation::describe,
      user2,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl6);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::describe,
      user3,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl7);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user2,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl6);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic,
      acl_operation::write,
      user3,
      host1,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl7);
    ASSERT_EQ(result.resource_pattern, resource);
}

// a bug had allowed a topic with read permissions (prefix) to authorize a group
// for read permissions when the topic name was a prefix of the group name
TEST(AUTHORIZER_TEST, authz_topic_group_same_name) {
    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;

    resource_pattern resource(
      resource_type::topic, "topic-foo", pattern_type::prefixed);
    bindings.emplace_back(resource, allow_read_acl);

    auth.add_bindings(bindings);

    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    ASSERT_FALSE(auth.authorized(
      kafka::group_id("topic-foo"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {}));
    ASSERT_FALSE(auth.authorized(
      kafka::group_id("topic-foo-xxx"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {}));
}

TEST(AUTHORIZER_TEST, role_authz_principal_view) {
    ss::sstring s1{"foor"};
    ss::sstring s2{"bar"};

    ASSERT_NE(s1, s2);
    acl_principal p1{principal_type::user, s1};
    acl_principal p2{principal_type::user, s2};
    acl_principal_view pv1{p1};

    EXPECT_NE(p1, p2);
    EXPECT_EQ(p1, pv1);
    EXPECT_NE(p2, pv1);

    acl_principal p3{principal_type::role, s1};
    acl_principal p4{principal_type::role, s2};
    acl_principal_view pv3{p3};

    EXPECT_NE(p3, p4);
    EXPECT_EQ(p3, pv3);
    EXPECT_NE(p4, pv3);

    // respects principal type
    EXPECT_NE(p1, p3);
    EXPECT_NE(p2, p4);
    EXPECT_NE(p1, pv3);
    EXPECT_NE(pv1, pv3);
}

TEST(AUTHORIZER_TEST, role_authz_simple_allow) {
    acl_principal user1(principal_type::user, "phyllis");
    acl_principal user2(principal_type::user, "lola");
    acl_principal user3(principal_type::user, "dietrichson");
    acl_principal user4(principal_type::user, "walter");
    role_name role_name1{"dietrichsons"};
    acl_principal role1 = role::to_principal(role_name1());

    acl_host host1("192.168.1.2");
    auto host_any = acl_host::wildcard_host();
    const model::topic topic1("topic1");

    role the_dietrichsons{{
      /* members */
      role_member::from_principal(user1),
      role_member::from_principal(user2),
    }};

    // role1 -> allow read
    acl_entry acl1(role1, host_any, acl_operation::read, acl_permission::allow);

    // user3 -> allow describe
    acl_entry acl2(
      user3, host_any, acl_operation::describe, acl_permission::allow);

    // user4 -> allow write
    acl_entry acl3(
      user4, host_any, acl_operation::write, acl_permission::allow);

    chunked_vector<acl_binding> bindings;

    resource_pattern resource(
      resource_type::topic, topic1(), pattern_type::literal);
    bindings.emplace_back(resource, acl1);
    bindings.emplace_back(resource, acl3);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // We haven't populated the role store yet
    auto result = auth.authorized(
      topic1,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_FALSE(bool(result));
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.resource_pattern.has_value());
    EXPECT_FALSE(result.role.has_value());

    // Add the role to the store
    EXPECT_TRUE(roles.put(role_name1, the_dietrichsons));

    // check authZ for both role members
    for (const auto& user : {user1, user2}) {
        auto result = auth.authorized(
          topic1,
          acl_operation::read,
          user,
          host1,
          security::superuser_required::no,
          {});
        EXPECT_TRUE(bool(result));
        EXPECT_EQ(result.acl, acl1);
        EXPECT_EQ(result.resource_pattern, resource);
        EXPECT_EQ(result.role, role_name1);
    }

    // confirm that the non-member user3 was not granted read access
    result = auth.authorized(
      topic1,
      acl_operation::read,
      user3,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_FALSE(bool(result));
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.resource_pattern.has_value());
    EXPECT_FALSE(result.role.has_value());

    // check that the non-member user4 does have write access
    result = auth.authorized(
      topic1,
      acl_operation::write,
      user4,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_TRUE(bool(result));
    EXPECT_EQ(result.acl, acl3);
    EXPECT_EQ(result.resource_pattern, resource);
    EXPECT_FALSE(result.role.has_value());

    // And confirm that role members do NOT have write access
    for (const auto& user : {user1, user2}) {
        auto result = auth.authorized(
          topic1,
          acl_operation::write,
          user,
          host1,
          security::superuser_required::no,
          {});
        EXPECT_FALSE(bool(result));
        EXPECT_FALSE(result.acl.has_value());
        EXPECT_FALSE(result.resource_pattern.has_value());
        // No role here, becuase there was no match
        EXPECT_FALSE(result.role.has_value());
    }

    // Add user3 to the role
    auto r = roles.get(role_name1);
    EXPECT_TRUE(roles.remove(role_name1));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r.value(), the_dietrichsons);
    auto r1_members = std::move(r.value()).members();
    r1_members.insert(role_member::from_principal(user3));
    roles.put(role_name1, r1_members);

    // user3 should now have read permissions
    result = auth.authorized(
      topic1,
      acl_operation::read,
      user3,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_TRUE(bool(result));
    EXPECT_EQ(result.acl, acl1);
    EXPECT_EQ(result.resource_pattern, resource);
    EXPECT_EQ(result.role, role_name1);
}

TEST(AUTHORIZER_TEST, role_authz_user_deny_applies_first) {
    acl_principal user1(principal_type::user, "user1");
    role_name role_name1("role1");
    acl_principal role_principal1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    acl_entry deny_user(
      user1,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::deny);

    acl_entry allow_role(
      role_principal1,
      acl_host::wildcard_host(),
      acl_operation::all,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, deny_user);
    bindings.emplace_back(resource, allow_role);

    role_store roles;
    roles.put(role_name1, role{});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // user1 should be denied write access to the topic

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          {});
        EXPECT_FALSE(bool(result));
        if (op == acl_operation::write) {
            EXPECT_EQ(result.acl, deny_user);
            EXPECT_EQ(result.resource_pattern, resource);
        } else {
            EXPECT_FALSE(result.acl.has_value());
            EXPECT_FALSE(result.resource_pattern.has_value());
        }
        EXPECT_FALSE(result.role.has_value());
    }

    // now add user1 to the role, which is already bound to the allow all acl

    EXPECT_TRUE(roles.remove(role_name1));
    EXPECT_TRUE(roles.put(
      role_name1,
      std::vector<role_member>{role_member::from_principal(user1)}));

    // user1 should now have read and describe access, but the deny acl should
    // still take precedence for writes
    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          {});
        if (op == acl_operation::write) {
            EXPECT_FALSE(bool(result));
            EXPECT_EQ(result.acl, deny_user);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
        } else {
            EXPECT_TRUE(bool(result));
            EXPECT_EQ(result.acl, allow_role);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_EQ(result.role, role_name1);
        }
    }
}

TEST(AUTHORIZER_TEST, role_authz_role_deny_applies_first) {
    acl_principal user1(principal_type::user, "user1");
    role_name role_name1("role1");
    acl_principal role_principal1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    acl_entry allow_user(
      user1,
      acl_host::wildcard_host(),
      acl_operation::all,
      acl_permission::allow);

    acl_entry deny_role(
      role_principal1,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_user);
    bindings.emplace_back(resource, deny_role);

    role_store roles;
    roles.put(role_name1, role{});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // user1 should be have full access to the topic

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          {});
        EXPECT_TRUE(bool(result));
        EXPECT_EQ(result.acl, allow_user);
        EXPECT_EQ(result.resource_pattern, resource);
        EXPECT_FALSE(result.role.has_value());
    }

    // now add user1 to the role, which is already bound to the deny write acl

    EXPECT_TRUE(roles.remove(role_name1));
    EXPECT_TRUE(roles.put(
      role_name1,
      std::vector<role_member>{role_member::from_principal(user1)}));

    // user1 should still have read and describe access, but the deny acl from
    // the role should take precedence

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          {});
        if (op == acl_operation::write) {
            EXPECT_FALSE(bool(result));
            EXPECT_EQ(result.acl, deny_role);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_EQ(result.role, role_name1);
        } else {
            EXPECT_TRUE(bool(result));
            EXPECT_EQ(result.acl, allow_user);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
        }
    }
}

TEST(AUTHORIZER_TEST, role_authz_get_acls_principal) {
    acl_principal role(principal_type::role, "admins");

    // NOTE(oren): Wildcard roles are rejected at the Kafka layer, but we
    // can verify acl_store behavior nevertheless.
    acl_principal wildcard_role(principal_type::role, "*");

    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(
      default_resource,
      acl_entry(
        role, acl_wildcard_host, acl_operation::write, acl_permission::allow));
    auth.add_bindings(bindings);

    ASSERT_EQ(get_acls(auth, wildcard_role), absl::flat_hash_set<acl_entry>{});
    ASSERT_EQ(get_acls(auth, role).size(), 1);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(default_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    bindings.clear();
    bindings.emplace_back(
      default_resource,
      acl_entry(
        wildcard_role,
        acl_wildcard_host,
        acl_operation::write,
        acl_permission::allow));
    auth.add_bindings(bindings);

    ASSERT_EQ(get_acls(auth, wildcard_role).size(), 1);
    ASSERT_EQ(get_acls(auth, role), absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, role_authz_wildcard_no_auth) {
    acl_principal user1(principal_type::user, "alice");
    acl_principal role1(principal_type::role, "admins");
    acl_principal wildcard_role(principal_type::role, "*");
    acl_host host1("192.168.3.1");

    role_store roles;
    roles.put(
      role_name{role1.name()}, role{{role_member::from_principal(user1)}});
    roles.put(
      role_name{wildcard_role.name()},
      role{{role_member::from_principal(user1)}});

    auto auth = make_test_instance();

    // NOTE(oren): again, note that this usage would be rejected at Kafka layer,
    // but it's probably a good idea to codify expected behavior somewhere.
    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(
      resource_pattern{
        resource_type::topic, default_topic(), pattern_type::literal},
      acl_entry(
        wildcard_role,
        acl_wildcard_host,
        acl_operation::write,
        acl_permission::allow));
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {});

    EXPECT_FALSE(result.authorized);
}

TEST(AUTHORIZER_TEST, role_authz_implied_acls) {
    role_store roles;
    role_name role_name1{"admin"};
    role_member mem1{role_member_type::user, "alice"};
    roles.put(role_name1, role{{mem1}});
    do_implied_acls(role::to_principal(role_name1()), &roles);
}

TEST(AUTHORIZER_TEST, role_authz_user_same_name) {
    role_store roles;
    role_name role_name1{"admin"};
    acl_principal user1{principal_type::user, "admin"};
    acl_principal user2{principal_type::user, "alice"};
    roles.put(role_name1, role{{role_member::from_principal(user2)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    acl_host host1("192.168.1.1");

    acl_entry allow_user(
      user1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    acl_entry deny_role(
      role::to_principal(role_name1()),
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_user);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_user);
    EXPECT_EQ(result.principal, user1);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user2,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_FALSE(result.authorized);
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_EQ(result.principal, user2);
    EXPECT_FALSE(result.role.has_value());

    bindings.clear();
    bindings.emplace_back(resource, deny_role);
    auth.add_bindings(bindings);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_user);
    EXPECT_EQ(result.principal, user1);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user2,
      host1,
      security::superuser_required::no,
      {});
    EXPECT_FALSE(result.authorized);
    EXPECT_EQ(result.acl, deny_role);
    EXPECT_EQ(result.principal, user2);
    EXPECT_EQ(result.role, role_name1);
}

TEST(AUTHORIZER_TEST, role_authz_remove_binding_multiple_match) {
    role_name role_n{"role"};
    acl_principal role_p = role::to_principal(role_n());
    acl_principal user_p{principal_type::user, "user"};

    role_store roles;
    roles.put(role_n, std::vector<role_member>{});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);

    static const acl_entry allow_read_acl(
      role_p, acl_wildcard_host, acl_operation::read, acl_permission::allow);

    static const acl_entry allow_write_acl(
      user_p, acl_wildcard_host, acl_operation::write, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    bindings.emplace_back(wildcard_resource, allow_write_acl);
    auth.add_bindings(bindings);

    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(wildcard_resource, allow_read_acl);
        filters.emplace_back(wildcard_resource, allow_write_acl);
        auth.remove_bindings(filters);
    }

    absl::flat_hash_set<acl_entry> expected{};
    ASSERT_EQ(get_acls(auth, wildcard_resource), expected);
}

TEST(AUTHORIZER_TEST, authz_filter_out_non_kafka_resources) {
    namespace ppsr = pandaproxy::schema_registry;
    ppsr::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { ppsr::enable_qualified_subjects::reset_local(); });

    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.2.1");

    auto auth = make_test_instance();

    const acl_entry allow_all(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::all,
      acl_permission::allow);

    const acl_entry allow_read(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::read,
      acl_permission::allow);

    const acl_entry allow_describe(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::describe,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern subject_resource(
      resource_type::sr_subject, "model-", pattern_type::prefixed);
    resource_pattern registry_resource(
      resource_type::sr_registry,
      resource_pattern::wildcard,
      pattern_type::literal);
    resource_pattern topic_resource(
      resource_type::topic, "model", pattern_type::literal);
    bindings.emplace_back(subject_resource, allow_read);
    bindings.emplace_back(registry_resource, allow_describe);
    bindings.emplace_back(topic_resource, allow_all);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      model::topic("model"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(result.is_authorized());

    auto kafka_acls = get_acls(auth, acl_binding_filter::any());
    ASSERT_EQ(kafka_acls.size(), 1);

    auto sr_acls = get_acls(
      auth,
      acl_binding_filter::any(
        resource_pattern_filter::resource_subsystem::schema_registry));
    ASSERT_EQ(sr_acls.size(), 2);

    // Check prefix match for schema registry subject
    result = auth.authorized(
      ppsr::context_subject::from_string("model-value"),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(result.is_authorized());

    // Check read implies describe
    result = auth.authorized(
      ppsr::context_subject::from_string("model-key"),
      acl_operation::describe,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(result.is_authorized());

    // Check read does not imply write
    result = auth.authorized(
      ppsr::context_subject::from_string("model-key"),
      acl_operation::write,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result.is_authorized());

    // Check global resource
    result = auth.authorized(
      ppsr::registry_resource(),
      acl_operation::describe,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_TRUE(result.is_authorized());

    // Check that describe does not imply read
    result = auth.authorized(
      ppsr::registry_resource(),
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    ASSERT_FALSE(result.is_authorized());
}

TEST(AUTHORIZER_TEST, authz_superuser_required) {
    acl_principal superuser(principal_type::user, "superuser1");
    acl_principal normaluser(principal_type::user, "normaluser");
    acl_host host("192.0.4.4");

    config::mock_property<std::vector<ss::sstring>> superuser_config_prop(
      std::vector<ss::sstring>{"superuser1"});
    role_store roles;
    authorizer auth(superuser_config_prop.bind(), &roles);

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::all,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      normaluser,
      host,
      security::superuser_required::no,
      {});

    // Verify that the normal user is authorized still
    EXPECT_TRUE(result.is_authorized());
    EXPECT_FALSE(result.required_superuser);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      normaluser,
      host,
      security::superuser_required::yes,
      {});
    EXPECT_FALSE(result.is_authorized());
    EXPECT_TRUE(result.required_superuser);

    result = auth.authorized(
      default_topic,
      acl_operation::read,
      superuser,
      host,
      security::superuser_required::yes,
      {});
    EXPECT_TRUE(result.is_authorized());
    EXPECT_TRUE(result.is_superuser);
}

TEST(AUTHORIZER_TEST, group_authz_principal_view) {
    ss::sstring s1{"foor"};
    ss::sstring s2{"bar"};

    ASSERT_NE(s1, s2);
    acl_principal p1{principal_type::user, s1};
    acl_principal p2{principal_type::user, s2};
    acl_principal_view pv1{p1};

    EXPECT_NE(p1, p2);
    EXPECT_EQ(p1, pv1);
    EXPECT_NE(p2, pv1);

    acl_principal p3{principal_type::group, s1};
    acl_principal p4{principal_type::group, s2};
    acl_principal_view pv3{p3};

    EXPECT_NE(p3, p4);
    EXPECT_EQ(p3, pv3);
    EXPECT_NE(p4, pv3);

    // respects principal type
    EXPECT_NE(p1, p3);
    EXPECT_NE(p2, p4);
    EXPECT_NE(p1, pv3);
    EXPECT_NE(pv1, pv3);
}

TEST(AUTHORIZER_TEST, group_authz_simple_allow) {
    acl_principal user1(principal_type::user, "phyllis");
    acl_principal group1(principal_type::group, "group-lola");

    acl_host host1("192.168.1.2");
    auto host_any = acl_host::wildcard_host();
    const model::topic topic1("topic1");

    acl_entry acl1(
      group1, host_any, acl_operation::read, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, topic1(), pattern_type::literal);

    bindings.emplace_back(resource, acl1);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      topic1,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(bool(result));
    EXPECT_EQ(result.acl, acl1);
    EXPECT_EQ(result.group, group1);
    EXPECT_EQ(result.resource_pattern, resource);
    EXPECT_FALSE(result.role.has_value());
}

TEST(AUTHORIZER_TEST, group_authz_user_deny_applies_first) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");

    acl_host host1("192.168.1.2");
    acl_entry deny_user(
      user1,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::deny);

    acl_entry allow_group(
      group1,
      acl_host::wildcard_host(),
      acl_operation::all,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, deny_user);
    bindings.emplace_back(resource, allow_group);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // user1 should be denied write access to the topic

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          {group1});
        if (op == acl_operation::write) {
            EXPECT_FALSE(bool(result));
            EXPECT_EQ(result.acl, deny_user);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_FALSE(result.group.has_value());
        } else {
            EXPECT_TRUE(bool(result));
            EXPECT_EQ(result.acl, allow_group);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_EQ(result.group, group1);
        }
    }
}

TEST(AUTHORIZER_TEST, group_authz_group_deny_applies_first) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");

    acl_host host1("192.168.1.2");

    acl_entry allow_user(
      user1,
      acl_host::wildcard_host(),
      acl_operation::all,
      acl_permission::allow);

    acl_entry deny_group(
      group1,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_user);
    bindings.emplace_back(resource, deny_group);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // user1 should still have read and describe access, but the deny acl from
    // the group should take precedence
    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          {group1});
        if (op == acl_operation::write) {
            EXPECT_FALSE(bool(result));
            EXPECT_EQ(result.acl, deny_group);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_EQ(result.group, group1);
        } else {
            EXPECT_TRUE(bool(result));
            EXPECT_EQ(result.acl, allow_user);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_FALSE(result.group.has_value());
        }
    }
}

TEST(AUTHORIZER_TEST, group_authz_multiple_groups_deny_precedence) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group_allow(principal_type::group, "group_allow");
    acl_principal group_deny(principal_type::group, "group_deny");

    acl_host host1("192.168.1.2");

    acl_entry allow_all_group(
      group_allow,
      acl_host::wildcard_host(),
      acl_operation::all,
      acl_permission::allow);

    acl_entry deny_write_group(
      group_deny,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_all_group);
    bindings.emplace_back(resource, deny_write_group);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Test with both groups - deny should take precedence for write operations
    chunked_vector<acl_principal> both_groups{group_allow, group_deny};

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          both_groups);

        if (op == acl_operation::write) {
            // Write should be denied due to group_deny ACL taking precedence
            EXPECT_FALSE(bool(result));
            EXPECT_EQ(result.acl, deny_write_group);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_EQ(result.group, group_deny);
        } else {
            // Read and describe should be allowed via group_allow ACL
            EXPECT_TRUE(bool(result));
            EXPECT_EQ(result.acl, allow_all_group);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_EQ(result.group, group_allow);
        }
    }

    // Test with only the allow group - all operations should succeed
    chunked_vector<acl_principal> allow_group_only{group_allow};

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          allow_group_only);

        EXPECT_TRUE(bool(result));
        EXPECT_EQ(result.acl, allow_all_group);
        EXPECT_EQ(result.resource_pattern, resource);
        EXPECT_FALSE(result.role.has_value());
        EXPECT_EQ(result.group, group_allow);
    }

    // Test with only the deny group - only write should be denied
    chunked_vector<acl_principal> deny_group_only{group_deny};

    for (auto op :
         {acl_operation::read, acl_operation::write, acl_operation::describe}) {
        auto result = auth.authorized(
          default_topic,
          op,
          user1,
          host1,
          security::superuser_required::no,
          deny_group_only);

        if (op == acl_operation::write) {
            EXPECT_FALSE(bool(result));
            EXPECT_EQ(result.acl, deny_write_group);
            EXPECT_EQ(result.resource_pattern, resource);
            EXPECT_FALSE(result.role.has_value());
            EXPECT_EQ(result.group, group_deny);
        } else {
            // No ACL allows read/describe for group_deny, so should fail
            EXPECT_FALSE(bool(result));
            EXPECT_FALSE(result.acl.has_value());
            EXPECT_FALSE(result.resource_pattern.has_value());
            EXPECT_FALSE(result.role.has_value());
            EXPECT_FALSE(result.group.has_value());
            EXPECT_TRUE(result.empty_matches);
        }
    }
}

TEST(AUTHORIZER_TEST, group_authz_implied_acls) {
    acl_principal group1(principal_type::group, "group-admins");

    do_implied_acls(group1, std::nullopt, {group1});
}

TEST(AUTHORIZER_TEST, group_authz_empty_groups_no_auth) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    acl_host host1("192.168.1.2");

    acl_entry allow_group(
      group1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_group);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User with empty groups should not be authorized
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {});

    EXPECT_FALSE(result.authorized);
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.group.has_value());
    EXPECT_TRUE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, group_authz_host_specific) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    acl_host host1("192.168.1.2");
    acl_host host2("192.168.1.3");

    acl_entry allow_group_host1(
      group1, host1, acl_operation::read, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_group_host1);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Should be authorized from correct host
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_group_host1);
    EXPECT_EQ(result.group, group1);

    // Should NOT be authorized from different host
    result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host2,
      security::superuser_required::no,
      {group1});

    EXPECT_FALSE(result.authorized);
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.group.has_value());
}

TEST(AUTHORIZER_TEST, group_authz_roles_and_groups_priority) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // Role allows read
    acl_entry allow_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    // Group denies read
    acl_entry deny_group(
      group1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_role);
    bindings.emplace_back(resource, deny_group);

    role_store roles;
    roles.put(role_name1, role{{role_member::from_principal(user1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Group deny should take precedence over role allow (deny always wins)
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_FALSE(result.authorized);
    EXPECT_EQ(result.acl, deny_group);
    EXPECT_EQ(result.group, group1);
    EXPECT_FALSE(result.role.has_value());
}

TEST(AUTHORIZER_TEST, group_authz_different_resource_types) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    acl_host host1("192.168.1.2");

    // Group has read access to topics
    acl_entry allow_topic_read(
      group1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    // Group has write access to consumer groups
    acl_entry allow_group_write(
      group1,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern topic_resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    resource_pattern group_resource(
      resource_type::group, "consumer-group", pattern_type::literal);

    bindings.emplace_back(topic_resource, allow_topic_read);
    bindings.emplace_back(group_resource, allow_group_write);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Should have read access to topic
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_topic_read);
    EXPECT_EQ(result.group, group1);

    // Should have write access to consumer group
    result = auth.authorized(
      kafka::group_id("consumer-group"),
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_group_write);
    EXPECT_EQ(result.group, group1);

    // Should NOT have write access to topic
    result = auth.authorized(
      default_topic,
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_FALSE(result.authorized);
    EXPECT_FALSE(result.group.has_value());
}

TEST(AUTHORIZER_TEST, group_authz_prefixed_and_wildcard_resources) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    acl_host host1("192.168.1.2");

    acl_entry allow_prefixed(
      group1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    acl_entry allow_wildcard(
      group1,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern prefixed_resource(
      resource_type::topic, "test-", pattern_type::prefixed);
    resource_pattern wildcard_resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);

    bindings.emplace_back(prefixed_resource, allow_prefixed);
    bindings.emplace_back(wildcard_resource, allow_wildcard);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Should match prefixed resource
    auto result = auth.authorized(
      model::topic("test-topic"),
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_prefixed);
    EXPECT_EQ(result.resource_pattern, prefixed_resource);
    EXPECT_EQ(result.group, group1);

    // Should match wildcard resource
    result = auth.authorized(
      model::topic("any-topic"),
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_wildcard);
    EXPECT_EQ(result.resource_pattern, wildcard_resource);
    EXPECT_EQ(result.group, group1);
}

TEST(AUTHORIZER_TEST, group_authz_get_acls_by_group_principal) {
    acl_principal group1(principal_type::group, "group1");
    acl_principal group2(principal_type::group, "group2");

    auto auth = make_test_instance();

    acl_entry group1_acl(
      group1, acl_wildcard_host, acl_operation::read, acl_permission::allow);
    acl_entry group2_acl(
      group2, acl_wildcard_host, acl_operation::write, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(default_resource, group1_acl);
    bindings.emplace_back(default_resource, group2_acl);
    auth.add_bindings(bindings);

    // Should find ACLs for specific group
    auto group1_acls = get_acls(auth, group1);
    ASSERT_EQ(group1_acls.size(), 1);
    ASSERT_TRUE(group1_acls.contains(group1_acl));

    auto group2_acls = get_acls(auth, group2);
    ASSERT_EQ(group2_acls.size(), 1);
    ASSERT_TRUE(group2_acls.contains(group2_acl));

    // Should not cross-contaminate
    ASSERT_FALSE(group1_acls.contains(group2_acl));
    ASSERT_FALSE(group2_acls.contains(group1_acl));
}

TEST(AUTHORIZER_TEST, group_authz_superuser_overrides_group_deny) {
    acl_principal superuser(principal_type::user, "superuser1");
    acl_principal group1(principal_type::group, "group1");
    acl_host host("192.0.4.4");

    config::mock_property<std::vector<ss::sstring>> superuser_config_prop(
      std::vector<ss::sstring>{"superuser1"});
    role_store roles;
    authorizer auth(superuser_config_prop.bind(), &roles);

    acl_entry deny_group(
      group1, acl_wildcard_host, acl_operation::all, acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, deny_group);
    auth.add_bindings(bindings);

    // Superuser should be authorized despite group deny ACL
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      superuser,
      host,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_TRUE(result.is_superuser);
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.group.has_value());
}

TEST(AUTHORIZER_TEST, group_authz_remove_bindings_with_groups) {
    acl_principal group1(principal_type::group, "group1");
    acl_principal group2(principal_type::group, "group2");

    auto auth = make_test_instance();

    acl_entry group1_read(
      group1, acl_wildcard_host, acl_operation::read, acl_permission::allow);
    acl_entry group1_write(
      group1, acl_wildcard_host, acl_operation::write, acl_permission::allow);
    acl_entry group2_read(
      group2, acl_wildcard_host, acl_operation::read, acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(default_resource, group1_read);
    bindings.emplace_back(default_resource, group1_write);
    bindings.emplace_back(default_resource, group2_read);
    auth.add_bindings(bindings);

    // Remove only group1's read permission
    {
        chunked_vector<acl_binding_filter> filters;
        filters.emplace_back(default_resource, group1_read);
        auth.remove_bindings(filters);
    }

    auto remaining_acls = get_acls(auth, default_resource);
    absl::flat_hash_set<acl_entry> expected{group1_write, group2_read};
    ASSERT_EQ(remaining_acls, expected);
}

TEST(AUTHORIZER_TEST, group_authz_large_number_of_groups) {
    acl_principal user1(principal_type::user, "user1");
    acl_host host1("192.168.1.2");

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);

    // Create many groups (simulate realistic scenario)
    chunked_vector<acl_principal> many_groups;
    chunked_vector<acl_binding> bindings;

    for (int i = 0; i < 50; ++i) {
        acl_principal group(principal_type::group, fmt::format("group{}", i));
        many_groups.push_back(group);

        // Only group42 has permissions
        if (i == 42) {
            acl_entry allow_read(
              group,
              acl_host::wildcard_host(),
              acl_operation::read,
              acl_permission::allow);

            resource_pattern resource(
              resource_type::topic, default_topic(), pattern_type::literal);
            bindings.emplace_back(resource, allow_read);
        }
    }

    auth.add_bindings(bindings);

    // Should find the one group with permissions
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      many_groups);

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.group.value().name(), "group42");
}

// Tests for group-role lookup: groups can be members of roles
TEST(AUTHORIZER_TEST, group_role_authz_simple_allow) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // Role has read permission
    acl_entry allow_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_role);

    role_store roles;
    // Add group1 as a member of role1
    roles.put(role_name1, role{{role_member::from_principal(group1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User in group1 should be authorized via group -> role path
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_role);
    EXPECT_EQ(result.role, role_name1);
}

TEST(AUTHORIZER_TEST, group_role_authz_deny_takes_precedence) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // Role has deny permission
    acl_entry deny_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::deny);

    // Group has allow permission
    acl_entry allow_group(
      group1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, deny_role);
    bindings.emplace_back(resource, allow_group);

    role_store roles;
    // Add group1 as a member of role1
    roles.put(role_name1, role{{role_member::from_principal(group1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Role deny should take precedence over group allow
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_FALSE(result.authorized);
    EXPECT_EQ(result.acl, deny_role);
    EXPECT_EQ(result.role, role_name1);
}

TEST(AUTHORIZER_TEST, group_role_authz_multiple_groups_one_in_role) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    acl_principal group2(principal_type::group, "group2");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // Role has read permission
    acl_entry allow_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_role);

    role_store roles;
    // Only group2 is a member of role1
    roles.put(role_name1, role{{role_member::from_principal(group2)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User in both groups should be authorized via group2 -> role path
    chunked_vector<acl_principal> groups{group1, group2};
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      groups);

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_role);
    EXPECT_EQ(result.role, role_name1);
}

TEST(AUTHORIZER_TEST, group_role_authz_group_not_in_role) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    acl_principal group2(principal_type::group, "group2");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // Role has read permission
    acl_entry allow_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_role);

    role_store roles;
    // group2 is a member of role1, but user is only in group1
    roles.put(role_name1, role{{role_member::from_principal(group2)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User in group1 should NOT be authorized since group1 is not in the role
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_FALSE(result.authorized);
    EXPECT_FALSE(result.role.has_value());
}

TEST(AUTHORIZER_TEST, group_role_authz_user_and_group_in_different_roles) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name user_role_name("user_role");
    role_name group_role_name("group_role");
    acl_principal user_role = role::to_principal(user_role_name());
    acl_principal group_role = role::to_principal(group_role_name());
    acl_host host1("192.168.1.2");

    // User role denies read
    acl_entry deny_user_role(
      user_role,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::deny);

    // Group role allows read
    acl_entry allow_group_role(
      group_role,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, deny_user_role);
    bindings.emplace_back(resource, allow_group_role);

    role_store roles;
    // User is directly in user_role, group is in group_role
    roles.put(user_role_name, role{{role_member::from_principal(user1)}});
    roles.put(group_role_name, role{{role_member::from_principal(group1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User role deny should be checked first and take precedence
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_FALSE(result.authorized);
    EXPECT_EQ(result.acl, deny_user_role);
    EXPECT_EQ(result.role, user_role_name);
}

TEST(AUTHORIZER_TEST, group_role_authz_group_deny_via_role) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // User has direct allow
    acl_entry allow_user(
      user1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    // Role (containing group) has deny
    acl_entry deny_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::deny);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_user);
    bindings.emplace_back(resource, deny_role);

    role_store roles;
    // group1 is a member of role1
    roles.put(role_name1, role{{role_member::from_principal(group1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Group's role deny should be checked and deny takes precedence
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    // The deny via group's role should take precedence over user's direct allow
    EXPECT_FALSE(result.authorized);
    EXPECT_EQ(result.acl, deny_role);
    EXPECT_EQ(result.role, role_name1);
}

TEST(AUTHORIZER_TEST, group_role_authz_multiple_roles_for_group) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name role_name1("role1");
    role_name role_name2("role2");
    acl_principal role1 = role::to_principal(role_name1());
    acl_principal role2 = role::to_principal(role_name2());
    acl_host host1("192.168.1.2");

    // Role1 allows read
    acl_entry allow_role1(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    // Role2 allows write
    acl_entry allow_role2(
      role2,
      acl_host::wildcard_host(),
      acl_operation::write,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_role1);
    bindings.emplace_back(resource, allow_role2);

    role_store roles;
    // group1 is a member of both roles
    roles.put(role_name1, role{{role_member::from_principal(group1)}});
    roles.put(role_name2, role{{role_member::from_principal(group1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User should have read access via role1
    auto result = auth.authorized(
      default_topic,
      acl_operation::read,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_role1);
    EXPECT_EQ(result.role, role_name1);

    // User should have write access via role2
    result = auth.authorized(
      default_topic,
      acl_operation::write,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_role2);
    EXPECT_EQ(result.role, role_name2);
}

TEST(AUTHORIZER_TEST, group_role_authz_implied_operations) {
    acl_principal user1(principal_type::user, "user1");
    acl_principal group1(principal_type::group, "group1");
    role_name role_name1("role1");
    acl_principal role1 = role::to_principal(role_name1());
    acl_host host1("192.168.1.2");

    // Role allows read, which implies describe
    acl_entry allow_role(
      role1,
      acl_host::wildcard_host(),
      acl_operation::read,
      acl_permission::allow);

    chunked_vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_role);

    role_store roles;
    roles.put(role_name1, role{{role_member::from_principal(group1)}});
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // User should have describe access (implied by read) via group -> role
    auto result = auth.authorized(
      default_topic,
      acl_operation::describe,
      user1,
      host1,
      security::superuser_required::no,
      {group1});

    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_role);
    EXPECT_EQ(result.role, role_name1);
}

// Test ACL pattern types (prefix, literal, wildcard) for context subjects.
TEST(AUTHORIZER_TEST, authz_sr_context_subject_patterns) {
    namespace ppsr = pandaproxy::schema_registry;
    ppsr::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { ppsr::enable_qualified_subjects::reset_local(); });

    auto user = acl_principal{principal_type::user, "user"};
    auto host = acl_host{"192.168.2.1"};

    auto check_access = [&](authorizer& auth, std::string_view subject) {
        return auth
          .authorized(
            ppsr::context_subject::from_string(subject),
            acl_operation::read,
            user,
            host,
            security::superuser_required::no,
            {})
          .is_authorized();
    };

    // Prefix ACL: ":.staging:" should match all subjects in .staging context
    {
        auto auth = make_test_instance();
        auth.add_bindings(
          {{resource_pattern{
              resource_type::sr_subject, ":.staging:", pattern_type::prefixed},
            allow_read_acl}});

        EXPECT_TRUE(check_access(auth, ":.staging:topic-1"));
        EXPECT_TRUE(check_access(auth, ":.staging:topic-2"));
        EXPECT_FALSE(check_access(auth, ":.prod:topic-1"));
        EXPECT_FALSE(check_access(auth, "topic-1"));
        EXPECT_FALSE(check_access(auth, ":.:topic-1"));
    }

    // Literal ACL: should match only exact subject
    {
        auto auth = make_test_instance();
        auth.add_bindings(
          {{resource_pattern{
              resource_type::sr_subject,
              ":.staging:topic-1",
              pattern_type::literal},
            allow_read_acl}});

        EXPECT_TRUE(check_access(auth, ":.staging:topic-1"));
        EXPECT_FALSE(check_access(auth, ":.staging:topic-2"));
        EXPECT_FALSE(check_access(auth, ":.prod:topic-1"));
        EXPECT_FALSE(check_access(auth, "topic-1"));
        EXPECT_FALSE(check_access(auth, ":.:topic-1"));
    }

    // Wildcard ACL: should match all subjects
    {
        auto auth = make_test_instance();
        auth.add_bindings(
          {{resource_pattern{
              resource_type::sr_subject,
              resource_pattern::wildcard,
              pattern_type::literal},
            allow_read_acl}});

        EXPECT_TRUE(check_access(auth, ":.staging:topic-1"));
        EXPECT_TRUE(check_access(auth, ":.prod:topic-1"));
        EXPECT_TRUE(check_access(auth, "topic-1"));
        EXPECT_TRUE(check_access(auth, ":.:topic-1"));
    }
}

// Tests for Group principal parsing and validation
TEST(AUTHORIZER_TEST, parse_group_principal_from_string_view) {
    // Test the fixed from_string_view<principal_type> function
    auto user_type = from_string_view<principal_type>("user");
    ASSERT_TRUE(user_type.has_value());
    EXPECT_EQ(*user_type, principal_type::user);

    auto group_type = from_string_view<principal_type>("group");
    ASSERT_TRUE(group_type.has_value());
    EXPECT_EQ(*group_type, principal_type::group);

    auto role_type = from_string_view<principal_type>("role");
    ASSERT_TRUE(role_type.has_value());
    EXPECT_EQ(*role_type, principal_type::role);

    auto ephemeral_type = from_string_view<principal_type>("ephemeral user");
    ASSERT_TRUE(ephemeral_type.has_value());
    EXPECT_EQ(*ephemeral_type, principal_type::ephemeral_user);

    // Test invalid type
    auto invalid_type = from_string_view<principal_type>("invalid");
    EXPECT_FALSE(invalid_type.has_value());
}

TEST(AUTHORIZER_TEST, parse_group_principal_from_string) {
    // Test basic Group principal parsing
    auto group1 = acl_principal::from_string("Group:developers");
    EXPECT_EQ(group1.type(), principal_type::group);
    EXPECT_EQ(group1.name(), "developers");

    // Test Group with hyphens
    auto group2 = acl_principal::from_string("Group:my-team");
    EXPECT_EQ(group2.type(), principal_type::group);
    EXPECT_EQ(group2.name(), "my-team");

    // Test Group with underscores
    auto group3 = acl_principal::from_string("Group:team_123");
    EXPECT_EQ(group3.type(), principal_type::group);
    EXPECT_EQ(group3.name(), "team_123");

    // Test Group with mixed case
    auto group4 = acl_principal::from_string("Group:TeamLead");
    EXPECT_EQ(group4.type(), principal_type::group);
    EXPECT_EQ(group4.name(), "TeamLead");

    // Test that wildcard groups throw exception (only users can be wildcards)
    EXPECT_THROW(acl_principal::from_string("Group:*"), acl_conversion_error);

    // Test empty group name throws
    EXPECT_THROW(acl_principal::from_string("Group:"), std::exception);

    // Verify User principal with wildcard still works
    auto wildcard_user = acl_principal::from_string("User:*");
    EXPECT_EQ(wildcard_user.type(), principal_type::user);
    EXPECT_TRUE(wildcard_user.wildcard());

    // Verify User principal without wildcard
    auto user1 = acl_principal::from_string("User:alice");
    EXPECT_EQ(user1.type(), principal_type::user);
    EXPECT_EQ(user1.name(), "alice");
    EXPECT_FALSE(user1.wildcard());
}

TEST(AUTHORIZER_TEST, authorize_with_group_principal_acls) {
    // Create test principals
    auto user = acl_principal(principal_type::user, "alice");
    auto group = acl_principal(principal_type::group, "developers");
    auto host = acl_host("192.168.1.1");
    const model::topic topic("dev-topic");

    // Create ACL for Group:developers
    acl_entry allow_read(
      group, acl_wildcard_host, acl_operation::read, acl_permission::allow);

    // Create resource and binding
    resource_pattern resource(
      resource_type::topic, topic(), pattern_type::literal);
    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(resource, allow_read);

    // Setup authorizer
    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Test authorization with user who is in the group
    auto result = auth.authorized(
      topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {group});

    // Verify authorization succeeded
    EXPECT_TRUE(bool(result));
    EXPECT_EQ(result.acl, allow_read);
    EXPECT_EQ(result.group, group);
    EXPECT_EQ(result.principal, user);

    // Test authorization without group membership fails
    auto result_no_group = auth.authorized(
      topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});

    EXPECT_FALSE(bool(result_no_group));
}

TEST(AUTHORIZER_TEST, group_principal_acl_deny_precedence) {
    // Create test principals
    auto user = acl_principal(principal_type::user, "bob");
    auto blocked_group = acl_principal(principal_type::group, "blocked");
    auto host = acl_host("192.168.1.1");
    const model::topic topic("sensitive-topic");

    // Create DENY ACL for Group:blocked
    acl_entry deny_write(
      blocked_group,
      acl_wildcard_host,
      acl_operation::write,
      acl_permission::deny);

    // Create ALLOW ACL for User:bob
    acl_entry allow_write(
      user, acl_wildcard_host, acl_operation::write, acl_permission::allow);

    // Create resource and bindings
    resource_pattern resource(
      resource_type::topic, topic(), pattern_type::literal);
    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(resource, deny_write);
    bindings.emplace_back(resource, allow_write);

    // Setup authorizer
    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // Test: User bob is in blocked group - DENY should win
    auto result_with_group = auth.authorized(
      topic,
      acl_operation::write,
      user,
      host,
      security::superuser_required::no,
      {blocked_group});

    // DENY via group should take precedence over user ALLOW
    EXPECT_FALSE(bool(result_with_group));
    EXPECT_EQ(result_with_group.acl, deny_write);
    EXPECT_EQ(result_with_group.group, blocked_group);

    // Test: User bob without blocked group - ALLOW should work
    auto result_without_group = auth.authorized(
      topic,
      acl_operation::write,
      user,
      host,
      security::superuser_required::no,
      {});

    EXPECT_TRUE(bool(result_without_group));
    EXPECT_EQ(result_without_group.acl, allow_write);
    EXPECT_FALSE(result_without_group.group.has_value());
}

// A matching prefix ACL must be found amid prefixed patterns that share the
// queried name's first character but are not prefixes of it, regardless of how
// many such patterns exist: add a matching prefix ACL plus a varying amount of
// that same-first-character noise and assert authorization is unaffected.
class authz_prefix_noise : public ::testing::TestWithParam<size_t> {};

INSTANTIATE_TEST_SUITE_P(
  same_first_char_noise,
  authz_prefix_noise,
  ::testing::Values(size_t{0}, size_t{8}, size_t{100}),
  [](const ::testing::TestParamInfo<size_t>& info) {
      return fmt::format("noise_{}", info.param);
  });

TEST_P(authz_prefix_noise, authz_prefix_match_amid_same_first_char_noise) {
    const size_t noise = GetParam();
    const acl_principal user(principal_type::user, "alice");
    const acl_host host("192.168.0.1");
    const model::topic topic("tz-some-workload-topic");

    const resource_pattern match(
      resource_type::topic, "tz-", pattern_type::prefixed);

    auto auth = make_test_instance();

    chunked_vector<acl_binding> bindings;
    bindings.emplace_back(
      match, acl_entry(user, host, acl_operation::read, acl_permission::allow));

    // prefixed patterns that share the topic's first character ('t') but
    // are never a prefix of it, so they must never match
    for (size_t i = 0; i < noise; ++i) {
        bindings.emplace_back(
          resource_pattern(
            resource_type::topic,
            fmt::format("to-noise-{:06}", i),
            pattern_type::prefixed),
          acl_entry(
            acl_principal(principal_type::user, fmt::format("u{}", i)),
            acl_wildcard_host,
            acl_operation::read,
            acl_permission::allow));
    }
    auth.add_bindings(bindings);

    // granted via the "tz-" prefix regardless of the noise / code path
    auto granted = auth.authorized(
      topic,
      acl_operation::read,
      user,
      host,
      security::superuser_required::no,
      {});
    EXPECT_TRUE(granted.authorized);
    EXPECT_EQ(granted.resource_pattern, match);

    // a topic the "tz-" prefix does not cover is never granted, even though
    // the noise shares its first character
    EXPECT_FALSE(auth
                   .authorized(
                     model::topic("to-uncovered"),
                     acl_operation::read,
                     user,
                     host,
                     security::superuser_required::no,
                     {})
                   .authorized);
}

// The prefix radix index must never drift from `_acls`: across incremental
// adds, a reset, and a removal, find() must discover exactly the prefixed
// patterns the store holds whose name is a prefix of the query -- no more, no
// fewer. Each pattern is granted to a distinct principal so a single find()
// result can be probed per pattern against a brute-force prefix scan.
TEST(AUTHORIZER_TEST, prefix_index_tracks_acls) {
    const acl_host any_host = acl_host::wildcard_host();
    const acl_host query_host("192.168.1.1");

    const std::vector<ss::sstring> names{
      "a", "ab", "abc", "abd", "b", "ba", "t", "tz", "tz-", "tz-events"};

    auto principal_for = [](size_t i) {
        return acl_principal(principal_type::user, fmt::format("p{}", i));
    };
    auto binding_for = [&](size_t i) {
        return acl_binding(
          resource_pattern(
            resource_type::topic, names[i], pattern_type::prefixed),
          acl_entry(
            principal_for(i),
            any_host,
            acl_operation::read,
            acl_permission::allow));
    };

    acl_store store;

    const std::vector<ss::sstring> queries{
      "abc",
      "abd",
      "abcd",
      "abx",
      "b",
      "baz",
      "tz-events-1",
      "tz",
      "t",
      "zzz",
      ""};

    auto verify = [&](const absl::flat_hash_set<size_t>& present) {
        for (const auto& q : queries) {
            const auto matches = store.find(resource_type::topic, q);
            for (size_t i = 0; i < names.size(); ++i) {
                const bool expected = present.contains(i)
                                      && std::string_view(q).starts_with(
                                        std::string_view(names[i]));
                const bool actual = matches.contains(
                  acl_operation::read,
                  principal_for(i),
                  query_host,
                  acl_permission::allow);
                EXPECT_EQ(expected, actual)
                  << "query='" << q << "' prefix='" << names[i] << "'";
            }
        }
    };

    // Incremental add in two batches: the second add must not drop the first
    // batch's index entries.
    chunked_vector<acl_binding> batch;
    for (size_t i = 0; i < 5; ++i) {
        batch.push_back(binding_for(i));
    }
    store.add_bindings(batch);
    verify({0, 1, 2, 3, 4});

    batch.clear();
    for (size_t i = 5; i < names.size(); ++i) {
        batch.push_back(binding_for(i));
    }
    store.add_bindings(batch);
    verify({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    // Reset replaces the entire set: stale names must vanish from the index.
    chunked_vector<acl_binding> reset;
    reset.push_back(binding_for(7)); // "tz"
    reset.push_back(binding_for(9)); // "tz-events"
    store.reset_bindings(reset).get();
    verify({7, 9});

    // Removing an entry empties and prunes its pattern ("tz"); the descendant
    // pattern ("tz-events") must still be discoverable via a descent through
    // where the pruned node was, and the pruned one must no longer match.
    chunked_vector<acl_binding_filter> filters;
    filters.emplace_back(
      resource_pattern_filter(
        resource_type::topic, names[7], pattern_type::prefixed),
      acl_entry_filter::any());
    store.remove_bindings(filters);
    verify({9});
}

namespace {
const acl_host idx_any_host = acl_host::wildcard_host();
const acl_host idx_query_host("192.168.1.1");

acl_binding prefixed_read_allow(std::string_view name, const acl_principal& p) {
    return acl_binding(
      resource_pattern(
        resource_type::topic, ss::sstring{name}, pattern_type::prefixed),
      acl_entry(p, idx_any_host, acl_operation::read, acl_permission::allow));
}

bool idx_allows(
  const acl_store& store, std::string_view topic, const acl_principal& p) {
    return store.find(resource_type::topic, ss::sstring{topic})
      .contains(acl_operation::read, p, idx_query_host, acl_permission::allow);
}
} // namespace

// A second entry added to an already-indexed prefixed pattern takes the
// "pattern already present" path in insert_binding (no index insert). The
// existing index entry must survive and reflect the new entry through its
// stable handle.
TEST(AUTHORIZER_TEST, prefix_index_second_entry_on_existing_pattern) {
    const acl_principal alice(principal_type::user, "alice");
    const acl_principal bob(principal_type::user, "bob");

    acl_store store;
    auto grant = [&](const acl_principal& p) {
        chunked_vector<acl_binding> b;
        b.push_back(prefixed_read_allow("tz-", p));
        store.add_bindings(b);
    };
    grant(alice); // creates + indexes the "tz-" pattern
    grant(bob);   // second entry on the existing pattern

    EXPECT_TRUE(idx_allows(store, "tz-events", alice));
    EXPECT_TRUE(idx_allows(store, "tz-events", bob));
}

// The index stores references into _acls; they must survive the rehashes that
// adding many further patterns triggers (the node-stability the design relies
// on). Index one pattern, add thousands more, then confirm the first still
// resolves.
TEST(AUTHORIZER_TEST, prefix_index_stable_across_acls_growth) {
    const acl_principal alice(principal_type::user, "alice");

    acl_store store;
    chunked_vector<acl_binding> first;
    first.push_back(prefixed_read_allow("early-", alice));
    store.add_bindings(first);

    chunked_vector<acl_binding> bulk;
    for (int i = 0; i < 5000; ++i) {
        bulk.push_back(prefixed_read_allow(
          fmt::format("p-{:06}-", i),
          acl_principal(principal_type::user, fmt::format("u{}", i))));
    }
    store.add_bindings(bulk);

    EXPECT_TRUE(idx_allows(store, "early-topic", alice));
}

// Removing the only entry empties the pattern, which prunes it from both _acls
// and the index, so it must stop matching; re-adding re-creates and re-indexes
// the pattern (it descends through where the pruned node was), and must match
// again -- exercising the prune + re-insert round trip.
TEST(AUTHORIZER_TEST, prefix_index_remove_then_readd) {
    const acl_principal alice(principal_type::user, "alice");

    acl_store store;
    auto grant = [&] {
        chunked_vector<acl_binding> b;
        b.push_back(prefixed_read_allow("tz-", alice));
        store.add_bindings(b);
    };

    grant();
    EXPECT_TRUE(idx_allows(store, "tz-events", alice));

    chunked_vector<acl_binding_filter> filters;
    filters.emplace_back(
      resource_pattern_filter(
        resource_type::topic, ss::sstring{"tz-"}, pattern_type::prefixed),
      acl_entry_filter::any());
    store.remove_bindings(filters);
    EXPECT_FALSE(idx_allows(store, "tz-events", alice));

    grant();
    EXPECT_TRUE(idx_allows(store, "tz-events", alice));
}

namespace {

acl_binding literal_read_allow(std::string_view name, const acl_principal& p) {
    return acl_binding(
      resource_pattern(
        resource_type::topic, ss::sstring{name}, pattern_type::literal),
      acl_entry(p, idx_any_host, acl_operation::read, acl_permission::allow));
}

} // namespace

// reset_bindings() must never be observable in a partial state.
//
// The refill is not a single reactor task: ss::do_for_each defers the remainder
// once the task quota expires, and on the controller snapshot apply path
// authorization checks run on this shard in exactly those gaps. An in-place
// clear-then-refill therefore hands an empty (or half-built) store to those
// checks and denies principals that do hold the ACL -- observed in production
// as transient TOPIC_AUTHORIZATION_FAILED on a correctly-ACLed consumer while a
// controller snapshot was installed, which the client saw as a fatal error.
//
// The grant under test is placed LAST in the replacement set so that a partial
// store cannot happen to contain it: under the old implementation the very
// first interleaved check fails.
TEST_CORO(AUTHORIZER_TEST, reset_bindings_not_observable_as_partial) {
    const acl_principal alice(principal_type::user, "alice");
    constexpr std::string_view granted_topic = "stable-topic";

    acl_store store;
    chunked_vector<acl_binding> initial;
    initial.push_back(literal_read_allow(granted_topic, alice));
    store.add_bindings(initial);
    ASSERT_TRUE_CORO(idx_allows(store, granted_topic, alice));

    // Large enough that the refill is certain to exceed the task quota and
    // yield at least once, with the grant under test rebuilt only at the end.
    constexpr size_t bulk = 50'000;
    chunked_vector<acl_binding> replacement;
    replacement.reserve(bulk + 1);
    for (size_t i = 0; i < bulk; ++i) {
        replacement.push_back(
          literal_read_allow(fmt::format("filler-{:06}", i), alice));
    }
    replacement.push_back(literal_read_allow(granted_topic, alice));

    bool reset_complete = false;
    auto reset = store.reset_bindings(replacement).then([&reset_complete] {
        reset_complete = true;
    });

    // Interleave authorization checks with the rebuild.
    size_t checks = 0;
    size_t denials = 0;
    while (!reset_complete) {
        if (!idx_allows(store, granted_topic, alice)) {
            ++denials;
        }
        ++checks;
        co_await ss::yield();
    }
    co_await std::move(reset);

    // Guards against the test passing trivially: if the rebuild never yielded,
    // nothing interleaved and the race was not exercised at all.
    ASSERT_GT_CORO(checks, 1);
    ASSERT_EQ_CORO(denials, 0);

    // The replacement is fully visible once the future resolves.
    ASSERT_TRUE_CORO(idx_allows(store, granted_topic, alice));
    ASSERT_TRUE_CORO(idx_allows(store, "filler-000000", alice));
}

} // namespace security
