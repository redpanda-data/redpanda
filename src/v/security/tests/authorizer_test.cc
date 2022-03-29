// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "config/mock_property.h"
#include "random/generators.h"
#include "security/authorizer.h"
#include "utils/base64.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

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

static auto get_acls(authorizer& auth, acl_binding_filter filter) {
    absl::flat_hash_set<acl_entry> found;
    auto acls = auth.acls(filter);
    for (auto acl : acls) {
        found.emplace(acl.entry());
    }
    return found;
}
static authorizer make_test_instance(
  authorizer::allow_empty_matches allow = authorizer::allow_empty_matches::no) {
    auto b = []() {
        return config::mock_binding<std::vector<ss::sstring>>(
          std::vector<ss::sstring>{});
    };
    return authorizer(allow, b);
}

BOOST_AUTO_TEST_CASE(resource_type_auto) {
    BOOST_REQUIRE(
      get_resource_type<model::topic>() == security::resource_type::topic);
    BOOST_REQUIRE(
      get_resource_type<kafka::group_id>() == security::resource_type::group);
    BOOST_REQUIRE(
      get_resource_type<security::acl_cluster_name>()
      == security::resource_type::cluster);
    BOOST_REQUIRE(
      get_resource_type<kafka::transactional_id>()
      == security::resource_type::transactional_id);

    BOOST_REQUIRE(
      get_resource_type<model::topic>() == get_resource_type<model::topic>());
    BOOST_REQUIRE(
      get_resource_type<model::topic>()
      != get_resource_type<kafka::group_id>());
}

BOOST_AUTO_TEST_CASE(empty_resource_name) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance();

    BOOST_REQUIRE(
      !auth.authorized(kafka::group_id(""), acl_operation::read, user, host));

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

    BOOST_REQUIRE(
      auth.authorized(kafka::group_id(""), acl_operation::read, user, host));
}

static const model::topic default_topic("topic1");

BOOST_AUTO_TEST_CASE(deny_applies_first) {
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

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(allow_all) {
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

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(super_user_allow) {
    acl_principal user1(principal_type::user, "superuser1");
    acl_principal user2(principal_type::user, "superuser2");
    acl_host host("192.0.4.4");

    config::mock_property<std::vector<ss::sstring>> superuser_config_prop(
      std::vector<ss::sstring>{});
    authorizer auth(
      [&superuser_config_prop]() mutable
      -> config::binding<std::vector<ss::sstring>> {
          return superuser_config_prop.bind();
      });

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

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user1, host));

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user2, host));

    // Adding superusers
    superuser_config_prop.update({"superuser1", "superuser2"});

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user1, host));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user2, host));

    // Revoking a superuser
    superuser_config_prop.update({"superuser2"});

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user1, host));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user2, host));
}

BOOST_AUTO_TEST_CASE(wildcards) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance();

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user, host));

    acl_principal user1(principal_type::user, "alice");
    acl_host host1("192.168.3.1");

    acl_entry read_acl(
      user1, host1, acl_operation::read, acl_permission::allow);

    std::vector<acl_binding> bindings;
    resource_pattern wildcard_resource(
      resource_type::topic, resource_pattern::wildcard, pattern_type::literal);
    bindings.emplace_back(wildcard_resource, read_acl);
    auth.add_bindings(bindings);

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user1, host1));

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

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::write, user1, host1));
}

BOOST_AUTO_TEST_CASE(no_acls_deny) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance();

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(no_acls_allow) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    auto auth = make_test_instance(authorizer::allow_empty_matches::yes);

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(implied_acls) {
    auto test_allow = [](acl_operation op, std::set<acl_operation> allowed) {
        acl_principal user(principal_type::user, "alice");
        acl_host host("192.168.3.1");

        acl_entry acl(user, acl_wildcard_host, op, acl_permission::allow);

        auto auth = make_test_instance();

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
                BOOST_REQUIRE(ok);
            } else {
                BOOST_REQUIRE(!ok);
            }
        }
    };

    auto test_deny = [](acl_operation op, std::set<acl_operation> denied) {
        acl_principal user(principal_type::user, "alice");
        acl_host host("192.168.3.1");

        acl_entry deny(user, acl_wildcard_host, op, acl_permission::deny);

        acl_entry allow(
          user, acl_wildcard_host, acl_operation::all, acl_permission::allow);

        auto auth = make_test_instance();

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
                BOOST_REQUIRE(!ok);
            } else {
                BOOST_REQUIRE(ok);
            }
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

BOOST_AUTO_TEST_CASE(allow_for_all_wildcard_resource) {
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

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(remove_acl_wildcard_resource) {
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
    BOOST_REQUIRE(get_acls(auth, wildcard_resource) == expected);
}

BOOST_AUTO_TEST_CASE(remove_all_acl_wildcard_resource) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
    bindings.emplace_back(wildcard_resource, allow_read_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
        filters.emplace_back(wildcard_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    BOOST_REQUIRE(get_acls(auth, acl_binding_filter::any()).empty());
}

BOOST_AUTO_TEST_CASE(allow_for_all_prefixed_resource) {
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

    BOOST_REQUIRE(auth.authorized(
      model::topic("foo-3uk3rlkj"), acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(remove_acl_prefixed_resource) {
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
    BOOST_REQUIRE(get_acls(auth, prefixed_resource) == expected);
}

BOOST_AUTO_TEST_CASE(remove_all_acl_prefixed_resource) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    auth.add_bindings(bindings);

    {
        std::vector<acl_binding_filter> filters;
        filters.emplace_back(prefixed_resource, acl_entry_filter::any());
        auth.remove_bindings(filters);
    }

    BOOST_REQUIRE(get_acls(auth, acl_binding_filter::any()).empty());
}

BOOST_AUTO_TEST_CASE(acls_on_literal_resource) {
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

    BOOST_REQUIRE(expected == get_acls(auth, default_resource));
    BOOST_REQUIRE(get_acls(auth, wildcard_resource).empty());
    BOOST_REQUIRE(get_acls(auth, prefixed_resource).empty());
}

BOOST_AUTO_TEST_CASE(acls_on_wildcard_resource) {
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

    BOOST_REQUIRE(expected == get_acls(auth, wildcard_resource));
    BOOST_REQUIRE(get_acls(auth, default_resource).empty());
    BOOST_REQUIRE(get_acls(auth, prefixed_resource).empty());
}

BOOST_AUTO_TEST_CASE(acls_on_prefixed_resource) {
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

    BOOST_REQUIRE(expected == get_acls(auth, prefixed_resource));
    BOOST_REQUIRE(get_acls(auth, wildcard_resource).empty());
    BOOST_REQUIRE(get_acls(auth, default_resource).empty());
}

BOOST_AUTO_TEST_CASE(auth_prefix_resource) {
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

    BOOST_REQUIRE(!auth.authorized(
      model::topic(default_resource.name()), acl_operation::read, user, host));

    std::vector<acl_binding> bindings;
    bindings.emplace_back(prefixed_resource, allow_read_acl);
    auth.add_bindings(bindings);

    BOOST_REQUIRE(auth.authorized(
      model::topic(default_resource.name()), acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(single_char) {
    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.3.1");

    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
    bindings.emplace_back(
      resource_pattern(resource_type::topic, "f", pattern_type::literal),
      allow_read_acl);
    auth.add_bindings(bindings);

    BOOST_REQUIRE(
      auth.authorized(model::topic("f"), acl_operation::read, user, host));

    BOOST_REQUIRE(
      !auth.authorized(model::topic("foo"), acl_operation::read, user, host));

    bindings.clear();
    bindings.emplace_back(
      resource_pattern(resource_type::topic, "_", pattern_type::prefixed),
      allow_read_acl);
    auth.add_bindings(bindings);

    BOOST_REQUIRE(
      auth.authorized(model::topic("_foo"), acl_operation::read, user, host));

    BOOST_REQUIRE(
      auth.authorized(model::topic("_"), acl_operation::read, user, host));

    BOOST_REQUIRE(
      !auth.authorized(model::topic("foo_"), acl_operation::read, user, host));
}

BOOST_AUTO_TEST_CASE(get_acls_principal) {
    acl_principal user(principal_type::user, "alice");

    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;
    bindings.emplace_back(
      default_resource,
      acl_entry(
        user, acl_wildcard_host, acl_operation::write, acl_permission::allow));
    auth.add_bindings(bindings);

    BOOST_REQUIRE(
      get_acls(auth, acl_principal(principal_type::user, "*")).empty());
    BOOST_REQUIRE_EQUAL(get_acls(auth, user).size(), 1);

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

    BOOST_REQUIRE_EQUAL(
      get_acls(auth, acl_principal(principal_type::user, "*")).size(), 1);
    BOOST_REQUIRE(get_acls(auth, user).empty());
}

BOOST_AUTO_TEST_CASE(acl_filter) {
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

    BOOST_REQUIRE(
      (absl::flat_hash_set<acl_binding>{acl1, acl2, acl3, acl4})
      == to_set(auth.acls(acl_binding_filter::any())));

    BOOST_REQUIRE(
      (absl::flat_hash_set<acl_binding>{acl1, acl2})
      == to_set(
        auth.acls(acl_binding_filter(resource1, acl_entry_filter::any()))));

    BOOST_REQUIRE(
      (absl::flat_hash_set<acl_binding>{acl4})
      == to_set(auth.acls(
        acl_binding_filter(prefixed_resource, acl_entry_filter::any()))));

    BOOST_REQUIRE(
      (absl::flat_hash_set<acl_binding>{acl3, acl4})
      == to_set(auth.acls(acl_binding_filter(
        resource_pattern_filter(
          std::nullopt,
          resource2.name(),
          resource_pattern_filter::pattern_match{}),
        acl_entry_filter::any()))));
}

BOOST_AUTO_TEST_CASE(topic_acl) {
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

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user1, host2));

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::read, user1, host1));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::write, user1, host1));

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::write, user1, host2));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::describe, user1, host1));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::describe, user1, host2));

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::alter, user1, host1));

    BOOST_REQUIRE(
      !auth.authorized(default_topic, acl_operation::alter, user1, host2));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::describe, user2, host1));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::describe, user3, host1));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::read, user2, host1));

    BOOST_REQUIRE(
      auth.authorized(default_topic, acl_operation::write, user3, host1));
}

// a bug had allowed a topic with read permissions (prefix) to authorize a group
// for read permissions when the topic name was a prefix of the group name
BOOST_AUTO_TEST_CASE(topic_group_same_name) {
    auto auth = make_test_instance();

    std::vector<acl_binding> bindings;

    resource_pattern resource(
      resource_type::topic, "topic-foo", pattern_type::prefixed);
    bindings.emplace_back(resource, allow_read_acl);

    auth.add_bindings(bindings);

    acl_principal user(principal_type::user, "alice");
    acl_host host("192.168.0.1");

    BOOST_REQUIRE(!auth.authorized(
      kafka::group_id("topic-foo"), acl_operation::read, user, host));
    BOOST_REQUIRE(!auth.authorized(
      kafka::group_id("topic-foo-xxx"), acl_operation::read, user, host));
}

} // namespace security
