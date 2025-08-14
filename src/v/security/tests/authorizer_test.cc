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
#include "pandaproxy/schema_registry/types.h"
#include "random/generators.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/role.h"
#include "security/role_store.h"
#include "utils/base64.h"

#include <boost/algorithm/string.hpp>
#include <fmt/ostream.h>
#include <gtest/gtest.h>

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
      get_resource_type<pandaproxy::schema_registry::subject>(),
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

    ASSERT_FALSE(
      auth.authorized(kafka::group_id(""), acl_operation::read, user, host));

    acl_entry acl(
      acl_wildcard_user,
      acl_wildcard_host,
      acl_operation::read,
      acl_permission::allow);

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::group, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      kafka::group_id(""), acl_operation::read, user, host);
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

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, allow);
    bindings.emplace_back(resource, deny);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic, acl_operation::read, user, host);

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

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic, acl_operation::read, user, host);
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

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic, acl_operation::read, user1, host);

    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    result = auth.authorized(default_topic, acl_operation::read, user2, host);

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

    result = auth.authorized(default_topic, acl_operation::read, user1, host);

    ASSERT_TRUE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_TRUE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    result = auth.authorized(default_topic, acl_operation::read, user2, host);

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

    result = auth.authorized(default_topic, acl_operation::read, user1, host);

    ASSERT_FALSE(result.authorized);
    ASSERT_EQ(result.acl, acl);
    ASSERT_EQ(result.resource_pattern, resource);
    ASSERT_EQ(result.principal, user1);
    ASSERT_EQ(result.host, host);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_topic());
    ASSERT_FALSE(result.is_superuser);
    ASSERT_FALSE(result.empty_matches);

    result = auth.authorized(default_topic, acl_operation::read, user2, host);

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
      default_topic, acl_operation::read, user, host);

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

    std::vector<acl_binding> bindings;
    resource_pattern wildcard_resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(wildcard_resource, read_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(default_topic, acl_operation::read, user1, host1);

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

    result = auth.authorized(default_topic, acl_operation::write, user1, host1);
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
      default_topic, acl_operation::read, user, host);

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
      default_topic, acl_operation::read, user, host);

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
  std::optional<role_store*> roles = std::nullopt) {
    auto test_allow = [&bind_principal, &roles](
                        acl_operation op, std::set<acl_operation> allowed) {
        acl_principal user(principal_type::user, "alice");
        ASSERT_TRUE(
          user == bind_principal
          || bind_principal.type() == principal_type::role);

        acl_host host("192.168.3.1");

        acl_entry acl(
          bind_principal, acl_wildcard_host, op, acl_permission::allow);

        auto auth = make_test_instance(
          authorizer::allow_empty_matches::no, roles);

        std::vector<acl_binding> bindings;
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
              default_cluster_name, test_op, user, host);
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

    auto test_deny = [&bind_principal, &roles](
                       acl_operation op, std::set<acl_operation> denied) {
        acl_principal user(principal_type::user, "alice");
        ASSERT_TRUE(
          user == bind_principal
          || bind_principal.type() == principal_type::role);

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

        std::vector<acl_binding> bindings;
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
              default_cluster_name, test_op, user, host);
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

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(resource, acl);

    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic, acl_operation::read, user, host);

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

    std::vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    bindings.emplace_back(wildcard_resource, allow_write_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
        filters.emplace_back(wildcard_resource, allow_read_acl);
        auth.remove_bindings(filters);
    }

    absl::flat_hash_set<acl_entry> expected{allow_write_acl};
    ASSERT_EQ(get_acls(auth, wildcard_resource), expected);
}

TEST(AUTHORIZER_TEST, authz_remove_all_acl_wildcard_resource) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
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

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, "foo", pattern_type::prefixed);
    bindings.emplace_back(resource, acl);

    auth.add_bindings(bindings);

    auto result = auth.authorized(
      model::topic("foo-3uk3rlkj"), acl_operation::read, user, host);

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

    std::vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    bindings.emplace_back(prefixed_resource, allow_write_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
        filters.emplace_back(prefixed_resource, allow_read_acl);
        auth.remove_bindings(filters);
    }

    absl::flat_hash_set<acl_entry> expected{allow_write_acl};
    ASSERT_EQ(get_acls(auth, prefixed_resource), expected);
}

TEST(AUTHORIZER_TEST, authz_remove_all_acl_prefixed_resource) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
        filters.emplace_back(prefixed_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    ASSERT_EQ(
      get_acls(auth, acl_binding_filter::any()),
      absl::flat_hash_set<acl_entry>{});
}

TEST(AUTHORIZER_TEST, authz_acls_on_literal_resource) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
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

    std::vector<acl_binding> bindings;
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

    std::vector<acl_binding> bindings;
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
        std::vector<acl_binding> bindings;
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
      model::topic(default_resource.name()), acl_operation::read, user, host);
    ASSERT_FALSE(result.authorized);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_EQ(result.principal, user);
    ASSERT_EQ(result.host, host);
    ASSERT_FALSE(result.is_superuser);
    ASSERT_TRUE(result.empty_matches);
    ASSERT_EQ(result.resource_type, security::resource_type::topic);
    ASSERT_EQ(result.resource_name, default_resource.name());

    std::vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(
      model::topic(default_resource.name()), acl_operation::read, user, host);

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

    std::vector<acl_binding> bindings;
    resource_pattern resource{resource_type::topic, "f", pattern_type::literal};
    bindings.emplace_back(resource, allow_read_acl);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      model::topic("f"), acl_operation::read, user, host);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      model::topic("foo"), acl_operation::read, user, host);
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    bindings.clear();
    resource_pattern resource1(
      resource_type::topic, "_", pattern_type::prefixed);
    bindings.emplace_back(resource1, allow_read_acl);
    auth.add_bindings(bindings);

    result = auth.authorized(
      model::topic("_foo"), acl_operation::read, user, host);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, resource1);

    result = auth.authorized(
      model::topic("_"), acl_operation::read, user, host);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, allow_read_acl);
    ASSERT_EQ(result.resource_pattern, resource1);

    result = auth.authorized(
      model::topic("foo_"), acl_operation::read, user, host);
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());
    ASSERT_TRUE(result.empty_matches);
}

TEST(AUTHORIZER_TEST, authz_get_acls_principal) {
    acl_principal user(principal_type::user, "alice");

    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
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
        std::vector<acl_binding_filter> filters;
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

    auto to_set = [](std::vector<acl_binding> bindings) {
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

    std::vector<acl_binding> bindings;
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
      default_topic, acl_operation::read, user1, host2);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl2);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(default_topic, acl_operation::read, user1, host1);
    ASSERT_FALSE(result);
    ASSERT_EQ(result.acl, acl3);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(default_topic, acl_operation::write, user1, host1);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl4);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(default_topic, acl_operation::write, user1, host2);
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    result = auth.authorized(
      default_topic, acl_operation::describe, user1, host1);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl5);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic, acl_operation::describe, user1, host2);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl5);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(default_topic, acl_operation::alter, user1, host1);
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    result = auth.authorized(default_topic, acl_operation::alter, user1, host2);
    ASSERT_FALSE(result);
    ASSERT_FALSE(result.acl.has_value());
    ASSERT_FALSE(result.resource_pattern.has_value());

    result = auth.authorized(
      default_topic, acl_operation::describe, user2, host1);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl6);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(
      default_topic, acl_operation::describe, user3, host1);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl7);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(default_topic, acl_operation::read, user2, host1);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl6);
    ASSERT_EQ(result.resource_pattern, resource);

    result = auth.authorized(default_topic, acl_operation::write, user3, host1);
    ASSERT_TRUE(bool(result));
    ASSERT_EQ(result.acl, acl7);
    ASSERT_EQ(result.resource_pattern, resource);
}

// a bug had allowed a topic with read permissions (prefix) to authorize a group
// for read permissions when the topic name was a prefix of the group name
TEST(AUTHORIZER_TEST, authz_topic_group_same_name) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;

    resource_pattern resource(
      resource_type::topic, "topic-foo", pattern_type::prefixed);
    bindings.emplace_back(resource, allow_read_acl);

    auth.add_bindings(bindings);

    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    ASSERT_FALSE(auth.authorized(
      kafka::group_id("topic-foo"), acl_operation::read, user, host));
    ASSERT_FALSE(auth.authorized(
      kafka::group_id("topic-foo-xxx"), acl_operation::read, user, host));
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

    std::vector<acl_binding> bindings;

    resource_pattern resource(
      resource_type::topic, topic1(), pattern_type::literal);
    bindings.emplace_back(resource, acl1);
    bindings.emplace_back(resource, acl3);

    role_store roles;
    auto auth = make_test_instance(authorizer::allow_empty_matches::no, &roles);
    auth.add_bindings(bindings);

    // We haven't populated the role store yet
    auto result = auth.authorized(topic1, acl_operation::read, user1, host1);
    EXPECT_FALSE(bool(result));
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.resource_pattern.has_value());
    EXPECT_FALSE(result.role.has_value());

    // Add the role to the store
    EXPECT_TRUE(roles.put(role_name1, the_dietrichsons));

    // check authZ for both role members
    for (const auto& user : {user1, user2}) {
        auto result = auth.authorized(topic1, acl_operation::read, user, host1);
        EXPECT_TRUE(bool(result));
        EXPECT_EQ(result.acl, acl1);
        EXPECT_EQ(result.resource_pattern, resource);
        EXPECT_EQ(result.role, role_name1);
    }

    // confirm that the non-member user3 was not granted read access
    result = auth.authorized(topic1, acl_operation::read, user3, host1);
    EXPECT_FALSE(bool(result));
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_FALSE(result.resource_pattern.has_value());
    EXPECT_FALSE(result.role.has_value());

    // check that the non-member user4 does have write access
    result = auth.authorized(topic1, acl_operation::write, user4, host1);
    EXPECT_TRUE(bool(result));
    EXPECT_EQ(result.acl, acl3);
    EXPECT_EQ(result.resource_pattern, resource);
    EXPECT_FALSE(result.role.has_value());

    // And confirm that role members do NOT have write access
    for (const auto& user : {user1, user2}) {
        auto result = auth.authorized(
          topic1, acl_operation::write, user, host1);
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
    roles.put(role_name1, std::move(r1_members));

    // user3 should now have read permissions
    result = auth.authorized(topic1, acl_operation::read, user3, host1);
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

    std::vector<acl_binding> bindings;
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
        auto result = auth.authorized(default_topic, op, user1, host1);
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
        auto result = auth.authorized(default_topic, op, user1, host1);
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

    std::vector<acl_binding> bindings;
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
        auto result = auth.authorized(default_topic, op, user1, host1);
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
        auto result = auth.authorized(default_topic, op, user1, host1);
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

    std::vector<acl_binding> bindings;
    bindings.emplace_back(
      default_resource,
      acl_entry(
        role, acl_wildcard_host, acl_operation::write, acl_permission::allow));
    auth.add_bindings(bindings);

    ASSERT_EQ(get_acls(auth, wildcard_role), absl::flat_hash_set<acl_entry>{});
    ASSERT_EQ(get_acls(auth, role).size(), 1);

    {
        std::vector<acl_binding_filter> filters;
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
    std::vector<acl_binding> bindings;
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
      default_topic, acl_operation::write, user1, host1);

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

    std::vector<acl_binding> bindings;
    resource_pattern resource(
      resource_type::topic, default_topic(), pattern_type::literal);
    bindings.emplace_back(resource, allow_user);
    auth.add_bindings(bindings);

    auto result = auth.authorized(
      default_topic, acl_operation::read, user1, host1);
    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_user);
    EXPECT_EQ(result.principal, user1);

    result = auth.authorized(default_topic, acl_operation::read, user2, host1);
    EXPECT_FALSE(result.authorized);
    EXPECT_FALSE(result.acl.has_value());
    EXPECT_EQ(result.principal, user2);
    EXPECT_FALSE(result.role.has_value());

    bindings.clear();
    bindings.emplace_back(resource, deny_role);
    auth.add_bindings(bindings);

    result = auth.authorized(default_topic, acl_operation::read, user1, host1);
    EXPECT_TRUE(result.authorized);
    EXPECT_EQ(result.acl, allow_user);
    EXPECT_EQ(result.principal, user1);

    result = auth.authorized(default_topic, acl_operation::read, user2, host1);
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

    std::vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    bindings.emplace_back(wildcard_resource, allow_write_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
        filters.emplace_back(wildcard_resource, allow_read_acl);
        filters.emplace_back(wildcard_resource, allow_write_acl);
        auth.remove_bindings(filters);
    }

    absl::flat_hash_set<acl_entry> expected{};
    ASSERT_EQ(get_acls(auth, wildcard_resource), expected);
}

TEST(AUTHORIZER_TEST, authz_filter_out_non_kafka_resources) {
    namespace ppsr = pandaproxy::schema_registry;
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

    std::vector<acl_binding> bindings;
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
      model::topic("model"), acl_operation::read, user, host);
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
      ppsr::subject("model-value"), acl_operation::read, user, host);
    ASSERT_TRUE(result.is_authorized());

    // Check read implies describe
    result = auth.authorized(
      ppsr::subject("model-key"), acl_operation::describe, user, host);
    ASSERT_TRUE(result.is_authorized());

    // Check read does not imply write
    result = auth.authorized(
      ppsr::subject("model-key"), acl_operation::write, user, host);
    ASSERT_FALSE(result.is_authorized());

    // Check global resource
    result = auth.authorized(
      ppsr::registry_resource(), acl_operation::describe, user, host);
    ASSERT_TRUE(result.is_authorized());

    // Check that describe does not imply read
    result = auth.authorized(
      ppsr::registry_resource(), acl_operation::read, user, host);
    ASSERT_FALSE(result.is_authorized());
}

} // namespace security
