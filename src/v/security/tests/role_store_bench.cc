/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "random/generators.h"
#include "security/role.h"
#include "security/role_store.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <utility>

using namespace security;

const std::vector<role_member>& get_mems(int n) {
    static std::vector<role_member> mems;
    if (!mems.empty()) {
        return mems;
    }
    for (int i = 0; i < n; ++i) {
        mems.emplace_back(
          role_member_type::user, random_generators::gen_alphanum_string(32));
    }
    return mems;
}

const std::vector<std::pair<role_name, role>>&
get_roles(int n, const std::vector<role_member>& mems) {
    static std::vector<std::pair<role_name, role>> roles;
    if (!roles.empty()) {
        return roles;
    }
    for (int i = 0; i < n; ++i) {
        role::container_type role_mems;
        for (const auto& m : mems) {
            auto ri = random_generators::get_int(0, 1);
            if (ri) {
                role_mems.insert(m);
            }
        }
        roles.emplace_back(
          role_name{random_generators::gen_alphanum_string(32)},
          std::move(role_mems));
    }
    return roles;
}

void run_member_queries(int n, bool iter) {
    security::role_store store;
    const auto& mems = get_mems(n);
    const auto& roles = get_roles(n, mems);
    for (const auto& [n, r] : roles) {
        store.put(n, r);
    }
    perf_tests::start_measuring_time();
    for (const auto& m : mems) {
        auto rng = store.get_member_roles(m);
        if (!rng.has_value() || !iter) {
            continue;
        }
        for (const auto& r : *rng.value()) {
            perf_tests::do_not_optimize(r);
        }
    }
    perf_tests::stop_measuring_time();
}

void run_range_queries(int n, bool iter) {
    security::role_store store;
    const auto& mems = get_mems(n);
    const auto& roles = get_roles(n, mems);
    for (const auto& [n, r] : roles) {
        store.put(n, r);
    }
    perf_tests::start_measuring_time();
    for (const auto& m : mems) {
        auto rng = store.range(
          [&m](const auto& e) { return role_store::has_member(e, m); });
        if (rng.empty() || !iter) {
            continue;
        }
        for (const auto& r : rng) {
            perf_tests::do_not_optimize(r);
        }
    }
    perf_tests::stop_measuring_time();
}

PERF_TEST(role_store_bench, member_get_query) {
    run_member_queries(1000, true);
}

// PERF_TEST(role_store_bench, member_get_query_no_iter) {
//     run_member_queries(1000, false);
// }

PERF_TEST(role_store_bench, user_range_query) { run_range_queries(1000, true); }

// PERF_TEST(role_store_bench, user_range_query_no_iter) {
//     run_range_queries(1000, false);
// }
