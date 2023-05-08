#pragma once

#include "reflection/type_traits.h"
#include "serde/envelope.h"
#include "serde/serde_is_enum.h"
#include "tristate.h"
#include "utils/fragmented_vector.h"
#include "utils/uuid.h"

#include <seastar/core/future.hh>
#include <seastar/net/inet_address.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace serde {

namespace detail {

template<class T, template<class, size_t> class C>
struct is_specialization_of_sized : std::false_type {};

template<template<class, size_t> class C, class T, size_t N>
struct is_specialization_of_sized<C<T, N>, C> : std::true_type {};

template<typename T, template<class, size_t> class C>
inline constexpr bool is_specialization_of_sized_v
  = is_specialization_of_sized<T, C>::value;

} // namespace detail

template<typename T>
concept is_absl_btree_map
  = ::detail::is_specialization_of_v<T, absl::btree_map>;

template<typename T>
concept is_absl_flat_hash_map
  = ::detail::is_specialization_of_v<T, absl::flat_hash_map>;

template<typename T>
concept is_absl_btree_set
  = ::detail::is_specialization_of_v<T, absl::btree_set>;

template<typename T>
concept is_absl_flat_hash_set
  = ::detail::is_specialization_of_v<T, absl::flat_hash_set>;

template<typename T>
concept is_fragmented_vector
  = detail::is_specialization_of_sized_v<T, fragmented_vector>;

template<typename T>
concept is_chunked_fifo
  = detail::is_specialization_of_sized_v<T, ss::chunked_fifo>;

template<typename T>
concept is_std_unordered_map
  = ::detail::is_specialization_of_v<T, std::unordered_map>;

template<typename T>
concept is_absl_node_hash_set
  = ::detail::is_specialization_of_v<T, absl::node_hash_set>;

template<typename T>
concept is_absl_node_hash_map
  = ::detail::is_specialization_of_v<T, absl::node_hash_map>;

template<class T>
concept is_chrono_duration
  = ::detail::is_specialization_of_v<T, std::chrono::duration>;

template<typename T>
inline constexpr auto const is_serde_compatible_v
  = is_envelope<T>
    || (std::is_scalar_v<T>  //
        && (!std::is_same_v<float, T> || std::numeric_limits<float>::is_iec559)
        && (!std::is_same_v<double, T> || std::numeric_limits<double>::is_iec559)
        && (!serde_is_enum_v<T> || sizeof(std::decay_t<T>) <= sizeof(serde_enum_serialized_t)))
    || reflection::is_std_vector<T>
    || reflection::is_std_array<T>
    || reflection::is_rp_named_type<T>
    || reflection::is_ss_bool_class<T>
    || reflection::is_std_optional<T>
    || std::is_same_v<T, std::chrono::milliseconds>
    || std::is_same_v<T, iobuf>
    || std::is_same_v<T, ss::sstring>
    || std::is_same_v<T, bytes>
    || std::is_same_v<T, uuid_t>
    || is_absl_btree_set<T>
    || is_absl_btree_map<T>
    || is_absl_flat_hash_set<T>
    || is_absl_flat_hash_map<T>
    || is_absl_node_hash_set<T>
    || is_absl_node_hash_map<T>
    || is_chrono_duration<T>
    || is_std_unordered_map<T>
    || is_fragmented_vector<T> || is_chunked_fifo<T> || reflection::is_tristate<T> || std::is_same_v<T, ss::net::inet_address>;

} // namespace serde
