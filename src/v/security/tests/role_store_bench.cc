/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/mock_property.h"
#include "random/generators.h"
#include "security/authorizer.h"
#include "security/role.h"
#include "security/role_store.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <boost/range/irange.hpp>

#include <vector>

namespace {

using namespace security;

// choose something that will generally circumvent small string optimization
constexpr size_t NAME_LEN = 32;

std::vector<role_member> generate_members(size_t N) {
    std::vector<role_member> mems;
    mems.reserve(N);
    absl::c_for_each(boost::irange(0ul, N), [&mems](auto) {
        mems.emplace_back(
          role_member_type::user,
          random_generators::gen_alphanum_string(NAME_LEN));
    });
    return mems;
}

std::vector<role_name> generate_role_names(size_t N) {
    std::vector<role_name> roles;
    roles.reserve(N);
    absl::c_for_each(boost::irange(0ul, N), [&roles](auto) {
        roles.emplace_back(random_generators::gen_alphanum_string(NAME_LEN));
    });
    return roles;
}
constexpr size_t N_MEMBERS = 1024ul;
constexpr size_t N_ROLES = 512ul;

const std::vector<role_member> members_data = generate_members(N_MEMBERS);
const std::vector<role_member> members_512_data = generate_members(512ul);
const std::vector<role_name> role_names_data = generate_role_names(N_ROLES);

role_store make_store(
  const decltype(role_names_data)& roles = role_names_data,
  const decltype(members_data)& mems = members_data) {
    role_store store;
    for (auto n : roles) {
        role::container_type role_mems;
        for (const auto& m : mems) {
            if (random_generators::get_int(0, 1)) {
                role_mems.insert(m);
            }
        }
        store.put(std::move(n), std::move(role_mems));
    }
    return store;
}

const role_store store_512_r_1Ki_m_data = make_store();
const role_store store_256_r_1Ki_m_data = make_store(
  generate_role_names(256ul), members_data);
const role_store store_128_r_1Ki_m_data = make_store(
  generate_role_names(128ul), members_data);
const role_store store_64_r_1Ki_m_data = make_store(
  generate_role_names(64ul), members_data);
const role_store store_64_r_512_m_data = make_store(
  generate_role_names(64ul), members_512_data);
const role_store store_8_r_1Ki_m_data = make_store(
  generate_role_names(8ul), members_data);

template<bool materialize>
void run_get_member_roles() {
    perf_tests::start_measuring_time();
    const auto& m
      = members_data[random_generators::get_int(members_data.size() - 1)];
    auto rng = store_512_r_1Ki_m_data.roles_for_member(m);
    perf_tests::do_not_optimize(rng);
    if constexpr (materialize) {
        bool is_empty = rng.empty();
        perf_tests::do_not_optimize(is_empty);
    }
    perf_tests::stop_measuring_time();
}

template<bool materialize>
void run_range_queries() {
    perf_tests::start_measuring_time();
    const auto& m
      = members_data[random_generators::get_int(members_data.size() - 1)];
    auto rng = store_512_r_1Ki_m_data.range(
      [&m](const auto& e) { return role_store::has_member(e, m); });
    perf_tests::do_not_optimize(rng);
    if constexpr (materialize) {
        bool is_empty = rng.empty();
        perf_tests::do_not_optimize(is_empty);
    }
    perf_tests::stop_measuring_time();
}

} // namespace

PERF_TEST(role_store_bench, get_member_roles) { run_get_member_roles<true>(); }

PERF_TEST(role_store_bench, get_member_roles_bare_query) {
    run_get_member_roles<false>();
}

PERF_TEST(role_store_bench, user_range_query) { run_range_queries<true>(); }

PERF_TEST(role_store_bench, user_range_query_bare_query) {
    run_range_queries<false>();
}

PERF_TEST(role_store_bench, remove_role) {
    auto store = make_store();
    size_t i = random_generators::get_int(role_names_data.size() - 1);
    perf_tests::start_measuring_time();
    store.remove(role_names_data[i]);
    perf_tests::stop_measuring_time();
}

PERF_TEST(role_store_bench, update_role) {
    auto store = make_store();
    std::vector<std::string_view> member_roles;
    role_member m;
    while (member_roles.empty()) {
        m = members_data[random_generators::get_int(members_data.size() - 1)];
        auto rng = store.roles_for_member(m);
        std::copy(rng.begin(), rng.end(), std::back_inserter(member_roles));
    }

    role_name n{
      member_roles[random_generators::get_int(member_roles.size() - 1)]};
    perf_tests::start_measuring_time();
    auto r = store.get(n).value();
    auto mems = std::move(r).members();
    store.remove(n);
    mems.erase(m);
    store.put(n, std::move(mems));
    perf_tests::stop_measuring_time();
}

PERF_TEST(role_store_bench, put_role) {
    role_store store = make_store();
    role_name name{random_generators::gen_alphanum_string(NAME_LEN)};
    std::vector<role_member> all_members;
    all_members.reserve(N_MEMBERS);
    absl::c_copy(members_data, std::back_inserter(all_members));
    perf_tests::start_measuring_time();
    store.put(std::move(name), std::move(all_members));
    perf_tests::stop_measuring_time();
}

namespace {

authorizer
make_test_authorizer(std::optional<const role_store*> roles = std::nullopt) {
    static role_store _roles;
    auto b = config::mock_binding<std::vector<ss::sstring>>(
      std::vector<ss::sstring>{});

    return {
      authorizer::allow_empty_matches::no,
      std::move(b),
      roles.value_or(&_roles)};
}

void run_authz(
  const role_store& store,
  const std::vector<role_member>& members,
  acl_permission perm = acl_permission::allow,
  size_t n_extra_bindings = 0) {
    auto mem1 = members[random_generators::get_int(members.size() - 1)];
    std::optional<role> role1;
    std::optional<role_name> role1_name;
    while (!role1_name.has_value()) {
        auto rng = store.range(
          [&mem1](const auto& e) { return role_store::has_member(e, mem1); });
        if (rng.empty()) {
            mem1 = members[random_generators::get_int(members.size() - 1)];
            continue;
        }
        role1_name.emplace(rng.front());
        role1.emplace(std::move(store.get(role1_name.value()).value()));
    }

    auto role1_principal = role::to_principal(role1_name.value()());
    acl_principal mem1_principal{
      principal_type::user, ss::sstring(mem1.name())};

    const model::topic topic1("topic1");
    acl_host host1("192.168.1.1");

    auto any_host = acl_host::wildcard_host();

    std::vector<acl_entry> acls = {
      {role1_principal, any_host, acl_operation::read, perm},
      {role1_principal, any_host, acl_operation::write, perm},
      {role1_principal, any_host, acl_operation::describe, perm},
      {role1_principal, any_host, acl_operation::alter, perm}};

    for (auto i : boost::irange(n_extra_bindings)) {
        acls.emplace_back(
          acl_principal{
            principal_type::user,
            fmt::format(
              "{}----{}", i, random_generators::gen_alphanum_string(27))},
          any_host,
          acl_operation::all,
          perm);
    }

    std::vector<acl_binding> bindings;
    for (const auto& acl : acls) {
        resource_pattern resource(
          resource_type::topic, topic1(), pattern_type::prefixed);
        bindings.emplace_back(resource, acl);
    }

    auto auth = make_test_authorizer(&store);
    auth.add_bindings(bindings);

    const auto& m = members[random_generators::get_int(members.size() - 1)];
    acl_principal p{principal_type::user, ss::sstring{m.name()}};

    perf_tests::start_measuring_time();
    auto result = auth.authorized(topic1, acl_operation::read, p, host1);
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

} // namespace

PERF_TEST(role_store_bench, role_authz_512_roles_1Ki_members) {
    run_authz(store_512_r_1Ki_m_data, members_data);
}

PERF_TEST(role_store_bench, role_authz_256_roles_1Ki_members) {
    run_authz(store_256_r_1Ki_m_data, members_data);
}

PERF_TEST(role_store_bench, role_authz_128_roles_1Ki_members) {
    run_authz(store_128_r_1Ki_m_data, members_data);
}

PERF_TEST(role_store_bench, role_authz_64_roles_1Ki_members) {
    run_authz(store_64_r_1Ki_m_data, members_data);
}

PERF_TEST(role_store_bench, role_authz_64_roles_1Ki_members_4_extra_bindings) {
    run_authz(store_64_r_1Ki_m_data, members_data, acl_permission::allow, 4);
}

PERF_TEST(role_store_bench, role_authz_64_roles_1Ki_members_8_extra_bindings) {
    run_authz(store_64_r_1Ki_m_data, members_data, acl_permission::allow, 8);
}

PERF_TEST(role_store_bench, role_authz_64_roles_1Ki_members_16_extra_bindings) {
    run_authz(store_64_r_1Ki_m_data, members_data, acl_permission::allow, 16);
}

PERF_TEST(role_store_bench, role_authz_64_roles_512_members) {
    run_authz(store_64_r_512_m_data, members_512_data);
}

PERF_TEST(role_store_bench, role_authz_64_roles_512_members_deny) {
    run_authz(store_64_r_512_m_data, members_512_data, acl_permission::deny);
}

PERF_TEST(role_store_bench, role_authz_8_roles_1Ki_members) {
    run_authz(store_8_r_1Ki_m_data, members_data);
}

PERF_TEST(role_store_bench, role_authz_empty_store) {
    acl_principal user1{
      principal_type::user, random_generators::gen_alphanum_string(NAME_LEN)};
    const model::topic topic1("tioopic1");
    acl_host host1("192.168.1.1");

    acl_permission perm = acl_permission::allow;

    acl_entry acl1(user1, acl_host::wildcard_host(), acl_operation::read, perm);

    acl_entry acl2(
      user1, acl_host::wildcard_host(), acl_operation::write, perm);

    acl_entry acl3(
      user1, acl_host::wildcard_host(), acl_operation::describe, perm);

    std::vector<acl_binding> bindings;
    for (const auto& acl : {acl1, acl2, acl3}) {
        resource_pattern resource(
          resource_type::topic, topic1(), pattern_type::prefixed);
        bindings.emplace_back(resource, acl);
    }

    auto auth = make_test_authorizer();
    auth.add_bindings(bindings);

    perf_tests::start_measuring_time();
    auto result = auth.authorized(topic1, acl_operation::read, user1, host1);
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}
