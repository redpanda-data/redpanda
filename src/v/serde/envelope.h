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
#include <concepts>
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
template<typename T, typename Version, typename CompatVersion>
struct envelope {
    bool operator==(envelope const&) const = default;
    auto operator<=>(envelope const&) const = default;
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
    static constexpr auto redpanda_inherits_from_envelope = true;
};

// Overhead of the envelope in bytes: 4 bytes of size, one byte of version,
// one byte of compat version.
static constexpr size_t envelope_header_size = 6;

/**
 * Checksum envelope uses CRC32c to check data integrity.
 * The idea is that CRC32 has hardware support and is faster than
 * disk and network I/O. So it will not be a bottle neck.
 * This can be changed - for example by a separate template parameter
 * template <..., typename HashAlgo = crc32c>
 */
template<typename T, typename Version, typename CompatVersion>
struct checksum_envelope {
    bool operator==(checksum_envelope const&) const = default;
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
    static constexpr auto redpanda_inherits_from_envelope = true;
    static constexpr auto redpanda_serde_build_checksum = true;
};

// Overhead of the envelope in bytes: a checksummed envelope is
// a regular envelope plus 4 bytes of checksum.
static constexpr size_t checksum_envelope_header_size = envelope_header_size
                                                        + 4;

template<typename T, typename Version = const serde::version_t&>
concept is_envelope = requires {
    { T::redpanda_serde_version } -> std::same_as<Version>;
    { T::redpanda_serde_compat_version } -> std::same_as<Version>;
};

template<typename T>
concept is_checksum_envelope
  = is_envelope<T> && T::redpanda_serde_build_checksum;

template<typename T>
concept inherits_from_envelope = T::redpanda_inherits_from_envelope;

} // namespace serde
