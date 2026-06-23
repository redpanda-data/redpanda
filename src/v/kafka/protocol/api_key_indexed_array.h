/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/protocol/types.h"

#include <fmt/format.h>

#include <array>
#include <cstddef>
#include <stdexcept>

namespace kafka {

// Cold helper kept out of line so fmt::format doesn't bloat at() and block it
// from inlining at its hot call sites.
[[noreturn]] inline void throw_api_key_out_of_range(api_key key) {
    throw std::out_of_range(
      fmt::format("api_key_indexed_array::at: {} out of range", key()));
}

/// \brief A dense, array-backed map from kafka::api_key to T spanning two
/// disjoint key regions: the standard Kafka range [0, StandardSize) and the
/// reserved Redpanda range [ReservedBase, ReservedBase + ReservedSize).
///
/// Indexing routes to whichever region a key falls in, so callers treat it as
/// a single array keyed by api_key without paying for the large gap between the
/// two regions. Every member is constexpr, so the same type backs the
/// compile-time dispatch/flex tables and the run-time probe/throughput tables.
template<
  typename T,
  std::size_t StandardSize,
  std::size_t ReservedSize,
  api_key::type ReservedBase = redpanda_api_key_base()>
class api_key_indexed_array {
    // Regions must not overlap, or standard keys in [ReservedBase,
    // StandardSize) would silently alias the reserved region.
    static_assert(
      static_cast<std::size_t>(ReservedBase) >= StandardSize,
      "reserved region overlaps the standard range");

public:
    constexpr api_key_indexed_array() = default;

    /// Fill both regions with \p init (e.g. the flex map's invalid sentinel).
    explicit constexpr api_key_indexed_array(const T& init) {
        _standard.fill(init);
        _reserved.fill(init);
    }

    /// Unchecked access, like std::array::operator[]. Out-of-range keys are
    /// undefined at run time and a compile error under constant evaluation.
    /// Standard keys (the common case) are routed first.
    constexpr T& operator[](api_key key) noexcept {
        if (!is_reserved(key)) [[likely]] {
            return _standard[static_cast<std::size_t>(key())];
        }
        return _reserved[reserved_index(key)];
    }
    constexpr const T& operator[](api_key key) const noexcept {
        if (!is_reserved(key)) [[likely]] {
            return _standard[static_cast<std::size_t>(key())];
        }
        return _reserved[reserved_index(key)];
    }

    /// Checked access with std::array::at semantics: throws std::out_of_range
    /// for any key that is not a valid index in either region. Standard keys
    /// (the common case) are checked and returned first.
    constexpr T& at(api_key key) {
        if (in_standard_range(key)) [[likely]] {
            return _standard[static_cast<std::size_t>(key())];
        }
        if (is_reserved(key)) {
            return _reserved.at(reserved_index(key));
        }
        throw_api_key_out_of_range(key);
    }
    constexpr const T& at(api_key key) const {
        if (in_standard_range(key)) [[likely]] {
            return _standard[static_cast<std::size_t>(key())];
        }
        if (is_reserved(key)) {
            return _reserved.at(reserved_index(key));
        }
        throw_api_key_out_of_range(key);
    }

    /// True iff \p key is a valid index in either region.
    constexpr bool contains(api_key key) const noexcept {
        if (in_standard_range(key)) [[likely]] {
            return true;
        }
        return in_reserved_range(key);
    }

    /// Pointer to the element for \p key, or nullptr if out of range.
    constexpr const T* find(api_key key) const noexcept {
        if (in_standard_range(key)) [[likely]] {
            return &_standard[static_cast<std::size_t>(key())];
        }
        if (in_reserved_range(key)) {
            return &_reserved[reserved_index(key)];
        }
        return nullptr;
    }
    constexpr T* find(api_key key) noexcept {
        if (in_standard_range(key)) [[likely]] {
            return &_standard[static_cast<std::size_t>(key())];
        }
        if (in_reserved_range(key)) {
            return &_reserved[reserved_index(key)];
        }
        return nullptr;
    }

    /// Pointer to the first element (standard region then reserved) for which
    /// pred(api_key, element) is true, or nullptr if none match.
    template<typename Pred>
    // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
    constexpr const T* find_if(Pred&& pred) const noexcept {
        for (std::size_t i = 0; i < StandardSize; ++i) {
            if (pred(api_key(static_cast<api_key::type>(i)), _standard[i])) {
                return &_standard[i];
            }
        }
        for (std::size_t i = 0; i < ReservedSize; ++i) {
            if (
              pred(
                api_key(static_cast<api_key::type>(ReservedBase + i)),
                _reserved[i])) {
                return &_reserved[i];
            }
        }
        return nullptr;
    }

    /// Invoke f(api_key, T&) for every slot, standard region then reserved
    /// region, passing the real api_key (not a 0..N index).
    template<typename F>
    // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
    constexpr void for_each(F&& f) {
        for (std::size_t i = 0; i < StandardSize; ++i) {
            f(api_key(static_cast<api_key::type>(i)), _standard[i]);
        }
        for (std::size_t i = 0; i < ReservedSize; ++i) {
            f(api_key(static_cast<api_key::type>(ReservedBase + i)),
              _reserved[i]);
        }
    }
    template<typename F>
    // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
    constexpr void for_each(F&& f) const {
        for (std::size_t i = 0; i < StandardSize; ++i) {
            f(api_key(static_cast<api_key::type>(i)), _standard[i]);
        }
        for (std::size_t i = 0; i < ReservedSize; ++i) {
            f(api_key(static_cast<api_key::type>(ReservedBase + i)),
              _reserved[i]);
        }
    }

    constexpr bool operator==(const api_key_indexed_array&) const = default;

    static constexpr std::size_t standard_size() { return StandardSize; }
    static constexpr std::size_t reserved_size() { return ReservedSize; }
    static constexpr api_key reserved_base() { return api_key(ReservedBase); }

private:
    static constexpr bool in_standard_range(api_key key) noexcept {
        return key() >= 0 && static_cast<std::size_t>(key()) < StandardSize;
    }
    static constexpr bool in_reserved_range(api_key key) noexcept {
        return is_reserved(key) && reserved_index(key) < ReservedSize;
    }
    static constexpr bool is_reserved(api_key key) noexcept {
        return key() >= ReservedBase;
    }
    static constexpr std::size_t reserved_index(api_key key) noexcept {
        return static_cast<std::size_t>(key() - ReservedBase);
    }

    std::array<T, StandardSize> _standard{};
    std::array<T, ReservedSize> _reserved{};
};

} // namespace kafka
