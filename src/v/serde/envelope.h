// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <cinttypes>
#include <compare>
#include <concepts>
#include <cstddef>
#include <type_traits>

namespace serde {

using version_t = std::uint8_t;

template<version_t V>
struct version {
    static constexpr const auto v = V;
};

template<version_t V>
struct compat_version {
    static constexpr const auto v = V;
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
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
    static constexpr auto redpanda_inherits_from_envelope = true;

    // The operator definitions below may look strange, but they have the role
    // of ensuring that serde types (which inherit from envelope) do not use
    // the envelope comparison operators.
    //
    // The implementation is templated in order to distinguish the implicit call
    // when the operator is generated for the base class (will have const
    // serde::envelope& on both sides) and accidental calls from sub classes
    // that do not override the operator (will have the derived template type on
    // both sides).

    template<typename O>
    requires(std::is_same_v<
             std::remove_cvref_t<O>,
             envelope<T, Version, CompatVersion>>)
    bool operator==(const O&) const {
        return true;
    }

    template<typename O>
    requires(std::is_same_v<
             std::remove_cvref_t<O>,
             envelope<T, Version, CompatVersion>>)
    std::strong_ordering operator<=>(const O&) const {
        return std::strong_ordering::equal;
    }
};

// Overhead of the envelope in bytes: 4 bytes of size, one byte of version,
// one byte of compat version.
static constexpr std::size_t envelope_header_size = 6;

/**
 * Checksum envelope uses CRC32c to check data integrity.
 * The idea is that CRC32 has hardware support and is faster than
 * disk and network I/O. So it will not be a bottle neck.
 * This can be changed - for example by a separate template parameter
 * template <..., typename HashAlgo = crc32c>
 */
template<typename T, typename Version, typename CompatVersion>
struct checksum_envelope {
    bool operator==(const checksum_envelope&) const = default;
    using value_t = T;
    static constexpr auto redpanda_serde_version = Version::v;
    static constexpr auto redpanda_serde_compat_version = CompatVersion::v;
    static constexpr auto redpanda_inherits_from_envelope = true;
    static constexpr auto redpanda_serde_build_checksum = true;
};

// Overhead of the envelope in bytes: a checksummed envelope is
// a regular envelope plus 4 bytes of checksum.
inline constexpr std::size_t checksum_envelope_header_size
  = envelope_header_size + 4;

template<typename T, typename Version = const serde::version_t&>
concept is_envelope = requires {
    { T::redpanda_serde_version } -> std::same_as<Version>;
    { T::redpanda_serde_compat_version } -> std::same_as<Version>;
};

template<typename T>
concept is_checksum_envelope = is_envelope<T>
                               && T::redpanda_serde_build_checksum;

template<typename T>
concept inherits_from_envelope = T::redpanda_inherits_from_envelope;

} // namespace serde
