/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "random/generators.h"
#include "security/role.h"
#include "security/role_store.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <boost/range/irange.hpp>

#include <vector>

namespace {

using role_name = security::role_name;
using role_member = security::role_member;
using role_member_type = security::role_member_type;
using role = security::role;
using role_store = security::role_store;

namespace {
std::vector<role_member> generate_members(size_t N) {
    std::vector<role_member> mems;
    mems.reserve(N);
    absl::c_for_each(boost::irange(0ul, N), [&mems](auto) {
        mems.emplace_back(
          role_member_type::user, random_generators::gen_alphanum_string(32));
    });
    return mems;
}

std::vector<role_name> generate_role_names(size_t N) {
    std::vector<role_name> roles;
    roles.reserve(N);
    absl::c_for_each(boost::irange(0ul, N), [&roles](auto) {
        roles.emplace_back(random_generators::gen_alphanum_string(32));
    });
    return roles;
}
constexpr size_t N_MEMBERS = 1024ul;
constexpr size_t N_ROLES = 512ul;

const std::vector<role_member> members_data = generate_members(N_MEMBERS);

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
} // namespace

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
    role_name name{random_generators::gen_alphanum_string(32)};
    std::vector<role_member> all_members;
    all_members.reserve(N_MEMBERS);
    absl::c_copy(members_data, std::back_inserter(all_members));
    perf_tests::start_measuring_time();
    store.put(std::move(name), std::move(all_members));
    perf_tests::stop_measuring_time();
}
