// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <cinttypes>
#include <type_traits>

namespace serde {

using version_t = uint8_t;

template<version_t V>
struct version {
    static constexpr auto const v = V;
};

template<version_t V>
struct compat_version {
    static constexpr auto const v = V;
};

/**
 * \brief provides versioning (version + compat version)
 *        for serializable aggregate types.
 *
 * \tparam Version         the current type version
 *                         (change for every incompatible update)
 * \tparam CompatVersion   the minimum required version able to parse the type
 */
template<
  typename T,
  typename Version,
  typename CompatVersion = compat_version<Version::v>>
struct envelope {
    bool operator==(envelope const&) const = default;
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
};

namespace detail {

template<typename T, typename = void>
struct has_compat_attribute : std::false_type {};

template<typename T>
struct has_compat_attribute<
  T,
  std::void_t<decltype(std::declval<T>().redpanda_serde_compat_version)>>
  : std::true_type {};

template<typename T, typename = void>
struct has_version_attribute : std::false_type {};

template<typename T>
struct has_version_attribute<
  T,
  std::void_t<decltype(std::declval<T>().redpanda_serde_compat_version)>>
  : std::true_type {};

} // namespace detail

template<typename T>
inline constexpr auto const is_envelope_v = std::conjunction_v<
  detail::has_compat_attribute<T>,
  detail::has_version_attribute<T>>;

} // namespace serde
