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

#include "container/fragmented_vector.h"
#include "random/generators.h"
#include "utils/tristate.h"

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
#include <span>
#include <vector>

namespace tests {

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
auto random_vector(Fn&& gen, size_t size = 20) -> std::vector<T> {
    std::vector<T> v;
    v.resize(size);
    std::generate_n(v.begin(), size, gen);
    return v;
}

template<typename Fn, typename T = std::invoke_result_t<Fn>>
auto random_chunked_vector(Fn&& gen, size_t size = 20) -> chunked_vector<T> {
    chunked_vector<T> v;
    v.reserve(size);
    std::generate_n(std::back_inserter(v), size, gen);
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

template<
  typename Fn,
  typename... Args,
  typename T = std::invoke_result_t<Fn, Args...>>
inline auto random_chunked_vector(Fn&& gen, size_t size = 20, Args&&... args)
  -> chunked_vector<T> {
    chunked_vector<T> v;
    while (size-- > 0) {
        v.push_back(gen(std::forward<Args>(args)...));
    }
    return v;
}

template<typename Fn, typename T = std::invoke_result_t<Fn>>
inline auto
random_circular_buffer(Fn&& gen, size_t size = 5) -> ss::circular_buffer<T> {
    ss::circular_buffer<T> v;
    v.reserve(size);
    std::generate_n(v.begin(), size, gen);
    return v;
}

template<typename Fn, typename T = std::invoke_result_t<Fn>>
inline auto
random_chunked_fifo(Fn&& gen, size_t size = 20) -> ss::chunked_fifo<T> {
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

/*
 * Generate a random duration. Notice that random value is multiplied by 10^6.
 * This is so that roundtrip from ns->ms->ns will work as expected.
 */
template<typename Dur>
inline Dur random_duration() {
    return Dur(
      random_generators::get_int<typename Dur::rep>(-100000, 100000) * 1000000);
}

} // namespace tests
