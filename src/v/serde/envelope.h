// Copyright 2021 Redpanda Data, Inc.
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
    auto operator<=>(envelope const&) const = default;
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
    static constexpr auto redpanda_inherits_from_envelope = true;
};

/**
 * Checksum envelope uses CRC32c to check data integrity.
 * The idea is that CRC32 has hardware support and is faster than
 * disk and network I/O. So it will not be a bottle neck.
 * This can be changed - for example by a separate template parameter
 * template <..., typename HashAlgo = crc32c>
 */
template<
  typename T,
  typename Version,
  typename CompatVersion = compat_version<Version::v>>
struct checksum_envelope {
    bool operator==(checksum_envelope const&) const = default;
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
    static constexpr auto redpanda_inherits_from_envelope = true;
    static constexpr auto redpanda_serde_build_checksum = true;
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
  std::void_t<decltype(std::declval<T>().redpanda_serde_version)>>
  : std::true_type {};

template<typename T, typename = void>
struct inherits_from_envelope : std::false_type {};

template<typename T>
struct inherits_from_envelope<
  T,
  std::void_t<decltype(std::declval<T>().redpanda_inherits_from_envelope)>>
  : std::true_type {};

template<typename T>
struct compat_version_has_serde_version_type {
    static constexpr auto const value = std::is_same_v<
      std::decay_t<decltype(std::declval<T>().redpanda_serde_compat_version)>,
      version_t>;
};

template<typename T>
struct version_has_serde_version_type {
    static constexpr auto const value = std::is_same_v<
      std::decay_t<decltype(std::declval<T>().redpanda_serde_version)>,
      version_t>;
};

template<typename T, typename = void>
struct has_checksum_attribute : std::false_type {};

template<typename T>
struct has_checksum_attribute<
  T,
  std::void_t<decltype(std::declval<T>().redpanda_serde_build_checksum)>>
  : std::true_type {};

} // namespace detail

template<typename T>
inline constexpr auto const is_envelope_v = std::conjunction_v<
  detail::has_compat_attribute<T>,
  detail::has_version_attribute<T>,
  detail::compat_version_has_serde_version_type<T>,
  detail::version_has_serde_version_type<T>>;

template<typename T>
inline constexpr auto const is_checksum_envelope_v = std::conjunction_v<
  detail::has_compat_attribute<T>,
  detail::has_version_attribute<T>,
  detail::compat_version_has_serde_version_type<T>,
  detail::version_has_serde_version_type<T>,
  detail::has_checksum_attribute<T>>;

template<typename T>
inline constexpr auto const inherits_from_envelope_v
  = detail::inherits_from_envelope<T>::value;

} // namespace serde
