/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/random.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/producer_state.h"
#include "model/metadata.h"
#include "net/unresolved_address.h"
#include "random/generators.h"
#include "security/acl.h"
#include "security/scram_credential.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <bits/stdint-uintn.h>

#include <iterator>
#include <limits>
#include <optional>
#include <vector>

namespace tests {

inline net::unresolved_address random_net_address() {
    return net::unresolved_address(
      random_generators::gen_alphanum_string(
        random_generators::get_int(1, 100)),
      random_generators::get_int(1025, 65535));
}

inline bool random_bool() { return random_generators::get_int(0, 100) > 50; }

template<typename T>
T random_named_string(size_t size = 20) {
    return T{random_generators::gen_alphanum_string(size)};
}

template<typename T>
T random_named_int() {
    return T{random_generators::get_int<typename T::type>(
      0, std::numeric_limits<T>::max())};
}

template<typename Func>
auto random_optional(Func f) {
    using T = decltype(f());
    if (random_bool()) {
        return std::optional<T>(f());
    }
    return std::optional<T>();
}

template<typename Func>
auto random_tristate(Func f) {
    using T = decltype(f());
    if (random_bool()) {
        return tristate<T>{};
    }
    return tristate<T>(random_optional(f));
}

template<typename Fn, typename T = std::invoke_result_t<Fn>>
inline auto random_vector(Fn&& gen, size_t size = 20) -> std::vector<T> {
    std::vector<T> v;
    v.resize(size);
    std::generate_n(v.begin(), size, gen);
    return v;
}

template<
  typename Fn,
  typename... Args,
  typename T = std::invoke_result_t<Fn, Args...>>
inline auto random_frag_vector(Fn&& gen, size_t size = 20, Args&&... args)
  -> fragmented_vector<T> {
    fragmented_vector<T> v;
    while (size-- > 0) {
        v.push_back(gen(std::forward<Args>(args)...));
    }
    return v;
}

template<typename Fn, typename T = std::invoke_result_t<Fn>>
inline auto random_circular_buffer(Fn&& gen, size_t size = 5)
  -> ss::circular_buffer<T> {
    ss::circular_buffer<T> v;
    v.reserve(size);
    std::generate_n(v.begin(), size, gen);
    return v;
}

template<typename Fn, typename T = std::invoke_result_t<Fn>>
inline auto random_chunked_fifo(Fn&& gen, size_t size = 20)
  -> ss::chunked_fifo<T> {
    ss::chunked_fifo<T> v;
    v.reserve(size);
    std::generate_n(std::back_inserter(v), size, gen);
    return v;
}

inline std::vector<std::string> random_strings(size_t size = 20) {
    return random_vector(
      [] { return random_named_string<std::string>(); }, size);
}

inline std::vector<ss::sstring> random_sstrings(size_t size = 20) {
    return random_vector(
      [] { return random_named_string<ss::sstring>(); }, size);
}

inline std::chrono::milliseconds random_duration_ms() {
    constexpr auto max_ns = std::chrono::nanoseconds::max().count();
    auto rand = random_generators::get_int<int64_t>(0, max_ns);
    auto rand_ns = std::chrono::nanoseconds{rand};
    return std::chrono::duration_cast<std::chrono::milliseconds>(rand_ns);
}

template<
  template<typename...>
  typename MapType,
  typename Key,
  typename Value,
  typename Fn>
inline MapType<Key, Value> random_map(Fn&& gen, size_t size = 20) {
    MapType<Key, Value> hm{};
    for (size_t i = 0; i < size; i++) {
        auto [k, v] = gen();
        hm[k] = v;
    }
    return hm;
}

template<typename Key, typename Value, typename Fn>
inline absl::node_hash_map<Key, Value>
random_node_hash_map(Fn&& gen, size_t size = 20) {
    return random_map<absl::node_hash_map, Key, Value, Fn>(
      std::forward<Fn>(gen), size);
}

template<typename Key, typename Value, typename Fn>
inline absl::flat_hash_map<Key, Value>
random_flat_hash_map(Fn&& gen, size_t size = 20) {
    return random_map<absl::flat_hash_map, Key, Value, Fn>(
      std::forward<Fn>(gen), size);
}

template<template<typename...> typename SetType, typename Value, typename Fn>
inline SetType<Value> random_set(Fn&& gen, size_t size = 20) {
    SetType<Value> hs{};
    for (size_t i = 0; i < size; i++) {
        auto v = gen();
        hs.insert(v);
    }
    return hs;
}

template<typename Value, typename Fn>
inline absl::node_hash_set<Value>
random_node_hash_set(Fn&& gen, size_t size = 20) {
    return random_set<absl::node_hash_set, Value>(std::forward<Fn>(gen), size);
}

template<typename Value, typename Fn>
inline absl::btree_set<Value> random_btree_set(Fn&& gen, size_t size = 20) {
    return random_set<absl::btree_set, Value>(std::forward<Fn>(gen), size);
}

inline cluster::producer_ptr random_producer_state() {
    return ss::make_lw_shared<cluster::producer_state>(
      model::producer_identity{
        random_generators::get_int<int64_t>(),
        random_generators::get_int<int16_t>()},
      random_named_int<raft::group_id>(),
      ss::noncopyable_function<void()>{});
}

/*
 * Generate a random duration. Notice that random value is multiplied by 10^6.
 * This is so that roundtrip from ns->ms->ns will work as expected.
 */
template<typename Dur>
inline Dur random_duration() {
    return Dur(
      random_generators::get_int<typename Dur::rep>(-100000, 100000) * 1000000);
}

inline cluster::partition_balancer_status random_balancer_status() {
    return random_generators::random_choice({
      cluster::partition_balancer_status::off,
      cluster::partition_balancer_status::starting,
      cluster::partition_balancer_status::ready,
      cluster::partition_balancer_status::in_progress,
      cluster::partition_balancer_status::stalled,
    });
}

inline cluster::partition_balancer_violations::unavailable_node
random_unavailable_node() {
    return {
      tests::random_named_int<model::node_id>(),
      model::timestamp(random_generators::get_int<int64_t>())};
}

inline cluster::partition_balancer_violations::full_node random_full_node() {
    return {
      tests::random_named_int<model::node_id>(),
      random_generators::get_int<uint32_t>()};
}

inline cluster::partition_balancer_violations
random_partition_balancer_violations() {
    auto random_un_gen = tests::random_vector(
      []() { return random_unavailable_node(); });
    auto random_fn_gen = tests::random_vector(
      []() { return random_full_node(); });
    return {std::move(random_un_gen), std::move(random_fn_gen)};
}

inline security::scram_credential random_credential() {
    return security::scram_credential(
      random_generators::get_bytes(256),
      random_generators::get_bytes(256),
      random_generators::get_bytes(256),
      random_generators::get_int(1, 10));
}

inline security::resource_type random_resource_type() {
    return random_generators::random_choice<security::resource_type>(
      {security::resource_type::cluster,
       security::resource_type::group,
       security::resource_type::topic,
       security::resource_type::transactional_id});
}

inline security::pattern_type random_pattern_type() {
    return random_generators::random_choice<security::pattern_type>(
      {security::pattern_type::literal, security::pattern_type::prefixed});
}

inline security::resource_pattern random_resource_pattern() {
    return {
      random_resource_type(),
      random_generators::gen_alphanum_string(10),
      random_pattern_type()};
}

inline security::acl_principal random_acl_principal() {
    return {
      security::principal_type::user,
      random_generators::gen_alphanum_string(12)};
}

inline security::acl_host create_acl_host() {
    return security::acl_host(ss::net::inet_address("127.0.0.1"));
}

inline security::acl_operation random_acl_operation() {
    return random_generators::random_choice<security::acl_operation>(
      {security::acl_operation::all,
       security::acl_operation::alter,
       security::acl_operation::alter_configs,
       security::acl_operation::describe_configs,
       security::acl_operation::cluster_action,
       security::acl_operation::create,
       security::acl_operation::remove,
       security::acl_operation::read,
       security::acl_operation::idempotent_write,
       security::acl_operation::describe});
}

inline model::record_batch_type random_batch_type() {
    return random_generators::random_choice(
      std::vector<model::record_batch_type>{
        model::record_batch_type::raft_data,
        model::record_batch_type::raft_configuration,
        model::record_batch_type::controller,
        model::record_batch_type::kvstore,
        model::record_batch_type::checkpoint,
        model::record_batch_type::topic_management_cmd,
        model::record_batch_type::ghost_batch,
        model::record_batch_type::id_allocator,
        model::record_batch_type::tx_prepare,
        model::record_batch_type::tx_fence,
        model::record_batch_type::tm_update,
        model::record_batch_type::user_management_cmd,
        model::record_batch_type::acl_management_cmd,
        model::record_batch_type::group_prepare_tx,
        model::record_batch_type::group_commit_tx,
        model::record_batch_type::group_abort_tx,
        model::record_batch_type::node_management_cmd,
        model::record_batch_type::data_policy_management_cmd,
        model::record_batch_type::archival_metadata,
        model::record_batch_type::cluster_config_cmd,
        model::record_batch_type::feature_update,
      });
}

inline security::acl_permission random_acl_permission() {
    return random_generators::random_choice<security::acl_permission>(
      {security::acl_permission::allow, security::acl_permission::deny});
}

inline security::acl_entry random_acl_entry() {
    return {
      random_acl_principal(),
      create_acl_host(),
      random_acl_operation(),
      random_acl_permission()};
}

inline security::acl_binding random_acl_binding() {
    return {random_resource_pattern(), random_acl_entry()};
}

inline security::resource_pattern_filter random_resource_pattern_filter() {
    auto resource = tests::random_optional(
      [] { return random_resource_type(); });

    auto name = tests::random_optional(
      [] { return random_generators::gen_alphanum_string(14); });

    auto pattern = tests::random_optional([] {
        using ret_t = std::variant<
          security::pattern_type,
          security::resource_pattern_filter::pattern_match>;
        if (tests::random_bool()) {
            return ret_t(random_pattern_type());
        } else {
            return ret_t(security::resource_pattern_filter::pattern_match{});
        }
    });

    return {resource, std::move(name), pattern};
}

inline security::acl_entry_filter random_acl_entry_filter() {
    auto principal = tests::random_optional(
      [] { return random_acl_principal(); });

    auto host = tests::random_optional([] { return create_acl_host(); });

    auto operation = tests::random_optional(
      [] { return random_acl_operation(); });

    auto permission = tests::random_optional(
      [] { return random_acl_permission(); });

    return {std::move(principal), host, operation, permission};
}

inline security::acl_binding_filter random_acl_binding_filter() {
    return {random_resource_pattern_filter(), random_acl_entry_filter()};
}

} // namespace tests
