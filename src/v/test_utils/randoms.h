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
#include "cluster/partition_balancer_types.h"
#include "model/metadata.h"
#include "net/unresolved_address.h"
#include "random/generators.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <bits/stdint-uintn.h>

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

inline std::vector<std::string> random_strings(size_t size = 20) {
    return random_vector(
      [] { return random_named_string<std::string>(); }, size);
}

inline std::vector<ss::sstring> random_sstrings(size_t size = 20) {
    return random_vector(
      [] { return random_named_string<ss::sstring>(); }, size);
}

inline std::chrono::milliseconds random_duration_ms() {
    return std::chrono::milliseconds(random_generators::get_int<uint64_t>(
      0, std::chrono::milliseconds::max().count()));
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

} // namespace tests
