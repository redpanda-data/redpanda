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

constexpr size_t N_MEMBERS = 1024ul;
constexpr size_t N_ROLES = 512ul;

static const std::vector<role_member> members_data = [](size_t N) {
    std::vector<role_member> mems;
    mems.reserve(N);
    absl::c_for_each(boost::irange(0ul, N), [&mems](auto) {
        mems.emplace_back(
          role_member_type::user, random_generators::gen_alphanum_string(32));
    });
    return mems;
}(N_MEMBERS);

static const std::vector<role_name> role_names_data = [](size_t N) {
    std::vector<role_name> roles;
    roles.reserve(N);
    absl::c_for_each(boost::irange(0ul, N), [&roles](auto) {
        roles.emplace_back(random_generators::gen_alphanum_string(32));
    });
    return roles;
}(N_ROLES);

role_store make_store() {
    role_store store;
    for (auto n : role_names_data) {
        role::container_type role_mems;
        for (const auto& m : members_data) {
            if (random_generators::get_int(0, 1)) {
                role_mems.insert(m);
            }
        }
        store.put(std::move(n), std::move(role_mems));
    }
    return store;
}

static const role_store store_data = make_store();

template<bool materialize>
void run_get_member_roles() {
    perf_tests::start_measuring_time();
    for (const auto& m : members_data) {
        auto rng = store_data.roles_for_member(m);
        perf_tests::do_not_optimize(rng);
        if constexpr (materialize) {
            bool is_empty = rng.empty();
            perf_tests::do_not_optimize(is_empty);
        }
    }
    perf_tests::stop_measuring_time();
}

template<bool materialize>
void run_range_queries() {
    perf_tests::start_measuring_time();
    for (const auto& m : members_data) {
        auto rng = store_data.range(
          [&m](const auto& e) { return role_store::has_member(e, m); });
        perf_tests::do_not_optimize(rng);
        if constexpr (materialize) {
            bool is_empty = rng.empty();
            perf_tests::do_not_optimize(is_empty);
        }
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
    std::vector<std::string_view> membership;
    role_member m;
    while (membership.empty()) {
        m = members_data[random_generators::get_int(members_data.size() - 1)];
        auto rng = store.roles_for_member(m);
        std::copy(rng.begin(), rng.end(), std::back_inserter(membership));
    }

    role_name n{membership[random_generators::get_int(membership.size() - 1)]};
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
