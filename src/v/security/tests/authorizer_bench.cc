/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/mock_property.h"
#include "model/fundamental.h"
#include "security/authorizer.h"
#include "security/role_store.h"

#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <memory>
#include <string>
#include <string_view>

// Terminology used in the cases below:
//   - "noise": ACLs that exist but never match the query
//   - "same-first-character noise": noise sharing the query's first character,
//     so it lands inside the scanned range.
//   - "spread noise": noise under many different first characters, so it lands
//     outside the scanned range.
//   - "matching prefix": a stored prefixed pattern that really is a prefix of
//     the query, e.g. "tz-".
// The cases run in three groups: common deployment shapes, then the
// pathological same-first-character shapes at increasing scale, then a
// deny-position sweep isolating where an authorization-ending deny sits within
// the scan.

namespace {

using namespace security;

// The queried topic. Prefixed-ACL candidates are every prefixed pattern
// sharing this name's first character; actual matches are prefixes of it
// ("t", "tz", "tz-", ...).
const model::topic query_topic("tz-some-workload-topic");

constexpr size_t inner_iters = 32;

/// Shape of the ACL store for one benchmark case. "Noise" bindings never
/// match the queried topic; the booleans add specific bindings that do.
struct acl_shape {
    /// same-first-character noise ("topic-NNNNNN"): inside the old scanned
    /// range, never a prefix of the query
    size_t noise_prefixed_same_letter = 0;
    /// spread noise ("<letter>-ns-N"): prefixed patterns under many different
    /// first characters, so outside the scanned range entirely
    size_t noise_prefixed_spread = 0;
    /// literal per-topic grants ("topic-NNNNNN"): found by point lookup, never
    /// scanned
    size_t noise_literal = 0;
    /// extra principals all on the one matching prefix "tz-": grows a single
    /// entry set rather than the number of matching patterns (isolates
    /// entry-set search cost from pattern-scan cost)
    size_t principals_on_matching_prefix = 0;
    /// nested real prefixes of the query ("t", "tz", "tz-", ...), each its own
    /// pattern owned by another principal: several genuine matches per query
    size_t nested_matching_prefixes = 0;
    /// a "tz" grant (matches the query) that sorts before the same-character
    /// noise, so the old scan still walked the whole noise range after finding
    /// it
    bool overlapping_prefix_acl = false;
    /// a "tz" deny the old lazy scan could stop on immediately. Its
    /// adversarial best case (the materialized path scans regardless)
    bool immediate_prefix_deny = false;
    /// "tz-" allow describe for the querying principal (the one real match)
    bool matching_prefix_allow = false;
    /// "*" wildcard allow describe for the querying principal
    bool wildcard_allow = false;
    /// exact-topic literal allow describe for the querying principal
    bool literal_allow = false;
};

const acl_principal alice(principal_type::user, "alice");

std::unique_ptr<authorizer> make_authorizer(const acl_shape& shape) {
    static role_store roles;
    auto superusers = config::mock_binding<std::vector<ss::sstring>>(
      std::vector<ss::sstring>{});
    auto auth = std::make_unique<authorizer>(
      authorizer::allow_empty_matches::no, std::move(superusers), &roles);

    const auto any_host = acl_host::wildcard_host();
    const acl_principal bob(principal_type::user, "bob");

    chunked_vector<acl_binding> bindings;
    auto add = [&](
                 resource_pattern pattern,
                 acl_principal principal,
                 acl_operation op,
                 acl_permission perm) {
        bindings.emplace_back(
          std::move(pattern),
          acl_entry(std::move(principal), any_host, op, perm));
    };

    for (size_t i = 0; i < shape.noise_prefixed_same_letter; ++i) {
        add(
          {resource_type::topic,
           fmt::format("topic-{:06}", i),
           pattern_type::prefixed},
          {principal_type::user, fmt::format("noise-user-{}", i)},
          acl_operation::all,
          acl_permission::allow);
    }

    constexpr std::string_view other_letters = "abcdefghijklmnopqrsuvwxyz";
    for (size_t i = 0; i < shape.noise_prefixed_spread; ++i) {
        add(
          {resource_type::topic,
           fmt::format(
             "{}-ns-{:06}", other_letters[i % other_letters.size()], i),
           pattern_type::prefixed},
          {principal_type::user, fmt::format("noise-user-{}", i)},
          acl_operation::all,
          acl_permission::allow);
    }

    for (size_t i = 0; i < shape.noise_literal; ++i) {
        add(
          {resource_type::topic,
           fmt::format("topic-{:06}", i),
           pattern_type::literal},
          {principal_type::user, fmt::format("noise-user-{}", i)},
          acl_operation::all,
          acl_permission::allow);
    }

    for (size_t i = 0; i < shape.principals_on_matching_prefix; ++i) {
        add(
          {resource_type::topic, "tz-", pattern_type::prefixed},
          {principal_type::user, fmt::format("ns-user-{}", i)},
          acl_operation::all,
          acl_permission::allow);
    }

    for (size_t len = 1; len <= shape.nested_matching_prefixes; ++len) {
        add(
          {resource_type::topic,
           query_topic().substr(0, len),
           pattern_type::prefixed},
          bob,
          acl_operation::write,
          acl_permission::allow);
    }

    if (shape.overlapping_prefix_acl) {
        add(
          {resource_type::topic, "tz", pattern_type::prefixed},
          bob,
          acl_operation::write,
          acl_permission::allow);
    }

    if (shape.immediate_prefix_deny) {
        add(
          {resource_type::topic, "tz", pattern_type::prefixed},
          alice,
          acl_operation::describe,
          acl_permission::deny);
    }

    if (shape.matching_prefix_allow) {
        add(
          {resource_type::topic, "tz-", pattern_type::prefixed},
          alice,
          acl_operation::describe,
          acl_permission::allow);
    }

    if (shape.wildcard_allow) {
        add(
          {resource_type::topic,
           resource_pattern::wildcard,
           pattern_type::literal},
          alice,
          acl_operation::describe,
          acl_permission::allow);
    }

    if (shape.literal_allow) {
        add(
          {resource_type::topic, query_topic(), pattern_type::literal},
          alice,
          acl_operation::describe,
          acl_permission::allow);
    }

    auth->add_bindings(bindings);
    return auth;
}

size_t run_describe_authz(
  authorizer& auth, const chunked_vector<acl_principal>& groups = {}) {
    const acl_host host("192.168.1.1");
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < inner_iters; ++i) {
        auto result = auth.authorized(
          query_topic,
          acl_operation::describe,
          alice,
          host,
          superuser_required::no,
          groups);
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return inner_iters;
}

// Deny-position sweep. `immediate_prefix_deny` places the deny at the front of
// the scanned range: a lazy deny check could stop almost immediately, so it was
// the best case for the old lazy view. To show that advantage is *positional*
// (and that the materialized path is position-independent), place a matching
// deny at controllable points in the scanned range.
//
// The query is a run of identical characters, so its prefixes ("a", "aa", ...)
// sort contiguously. Each noise pattern "<L 'a's>0<i>" sorts just above the
// length-L prefix and is never a prefix of the query, spread across lengths
// [2, len). A deny on the length-d prefix therefore has every noise pattern of
// length >= d scanned before it is reached: d == len -> none precede it
// (front); d == 2 -> all precede it (back).
constexpr size_t deny_query_len = 22;
const model::topic deny_query(std::string(deny_query_len, 'a'));

std::unique_ptr<authorizer>
make_deny_position_authorizer(size_t noise, size_t deny_prefix_len) {
    static role_store roles;
    auto superusers = config::mock_binding<std::vector<ss::sstring>>(
      std::vector<ss::sstring>{});
    auto auth = std::make_unique<authorizer>(
      authorizer::allow_empty_matches::no, std::move(superusers), &roles);

    const auto any_host = acl_host::wildcard_host();
    chunked_vector<acl_binding> bindings;

    for (size_t i = 0; i < noise; ++i) {
        const size_t len = 2 + (i % (deny_query_len - 2));
        bindings.emplace_back(
          resource_pattern{
            resource_type::topic,
            std::string(len, 'a') + "0" + std::to_string(i),
            pattern_type::prefixed},
          acl_entry(
            {principal_type::user, fmt::format("noise-user-{}", i)},
            any_host,
            acl_operation::all,
            acl_permission::allow));
    }

    // the only matching prefix for the query: a deny for the querying principal
    bindings.emplace_back(
      resource_pattern{
        resource_type::topic,
        std::string(deny_prefix_len, 'a'),
        pattern_type::prefixed},
      acl_entry(
        alice, any_host, acl_operation::describe, acl_permission::deny));

    auth->add_bindings(bindings);
    return auth;
}

size_t run_deny_position_authz(authorizer& auth) {
    const acl_host host("192.168.1.1");
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < inner_iters; ++i) {
        auto result = auth.authorized(
          deny_query,
          acl_operation::describe,
          alice,
          host,
          superuser_required::no,
          {});
        perf_tests::do_not_optimize(result);
    }
    perf_tests::stop_measuring_time();
    return inner_iters;
}

} // namespace

/*
 * Expected ACL shapes: the configurations real deployments use.
 */

// the most common deployment: a single wildcard grant
PERF_TEST(authorizer_bench, describe_wildcard_only) {
    auto auth = make_authorizer({.wildcard_allow = true});
    return run_describe_authz(*auth);
}

// per-topic literal grants at scale: point lookups, no range scan involved
PERF_TEST(authorizer_bench, describe_literal_grants_10k) {
    auto auth = make_authorizer(
      {.noise_literal = 10240, .literal_allow = true});
    return run_describe_authz(*auth);
}

// 1k prefixed ACLs spread across distinct first characters, with one matching
// grant among them: almost none fall inside the scanned range. The common
// shape, where prefixed ACLs belong to many independent namespaces
PERF_TEST(authorizer_bench, describe_1k_spread_prefix_acls) {
    auto auth = make_authorizer(
      {.noise_prefixed_spread = 1024, .matching_prefix_allow = true});
    return run_describe_authz(*auth);
}

// deeply nested namespace conventions: 8 distinct prefixes of the query all
// match, so a single lookup returns 8 prefix matches
PERF_TEST(authorizer_bench, describe_nested_prefixes_8) {
    auto auth = make_authorizer(
      {.nested_matching_prefixes = 8, .matching_prefix_allow = true});
    return run_describe_authz(*auth);
}

// per-user grants on a shared namespace prefix: one matching pattern whose
// entry set holds 1k entries (entry-set search cost, not pattern-scan cost)
PERF_TEST(authorizer_bench, describe_shared_prefix_1k_principals) {
    auto auth = make_authorizer(
      {.principals_on_matching_prefix = 1000, .matching_prefix_allow = true});
    return run_describe_authz(*auth);
}

/*
 * Pathological shapes: many prefixed patterns sharing the queried topic's
 * first character, all scanned as candidates on every lookup.
 */

PERF_TEST(authorizer_bench, describe_16_prefix_acls) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 16, .overlapping_prefix_acl = true});
    return run_describe_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_1k_prefix_acls_no_match) {
    auto auth = make_authorizer({.noise_prefixed_same_letter = 1024});
    return run_describe_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_1k_prefix_acls) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 1024, .overlapping_prefix_acl = true});
    return run_describe_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_10k_prefix_acls_no_match) {
    auto auth = make_authorizer({.noise_prefixed_same_letter = 10240});
    return run_describe_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_10k_prefix_acls) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 10240, .overlapping_prefix_acl = true});
    return run_describe_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_10k_prefix_acls_allowed) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 10240,
       .overlapping_prefix_acl = true,
       .matching_prefix_allow = true});
    return run_describe_authz(*auth);
}

// authorization resolves at the first scanned element: the best case for
// lazy evaluation (which could stop there) vs eager materialization (which
// scans the full range regardless)
PERF_TEST(authorizer_bench, describe_10k_immediate_deny) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 10240, .immediate_prefix_deny = true});
    return run_describe_authz(*auth);
}

// group principals multiply the number of deny/allow checks per
// authorization; each check re-scans under lazy evaluation
PERF_TEST(authorizer_bench, describe_10k_prefix_acls_two_groups) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 10240, .overlapping_prefix_acl = true});
    chunked_vector<acl_principal> groups;
    groups.emplace_back(principal_type::group, "g-eng");
    groups.emplace_back(principal_type::group, "g-data");
    return run_describe_authz(*auth, groups);
}

PERF_TEST(authorizer_bench, describe_50k_prefix_acls) {
    auto auth = make_authorizer(
      {.noise_prefixed_same_letter = 51200, .overlapping_prefix_acl = true});
    return run_describe_authz(*auth);
}

// Deny-position sweep over 10k prefixed ACLs. Lazy evaluation's cost tracked
// how far the deny sat from the front of the scan (front == cheap); the
// materialized path pays the same regardless. `describe_10k_deny_front` is the
// equivalent of `describe_10k_immediate_deny`.
PERF_TEST(authorizer_bench, describe_10k_deny_front) {
    auto auth = make_deny_position_authorizer(10240, deny_query_len);
    return run_deny_position_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_10k_deny_middle) {
    auto auth = make_deny_position_authorizer(10240, deny_query_len / 2);
    return run_deny_position_authz(*auth);
}

PERF_TEST(authorizer_bench, describe_10k_deny_back) {
    auto auth = make_deny_position_authorizer(10240, 2);
    return run_deny_position_authz(*auth);
}
