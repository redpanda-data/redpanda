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

#include "base/likely.h"
#include "base/seastarx.h"

#include <seastar/core/reactor.hh>

#include <cstdint>
#include <type_traits>

namespace internal {

template<typename T, typename NaturallyEmptyable>
class emptyable;

template<typename T>
class emptyable<T, std::true_type> {
public:
    static constexpr bool move_noexcept
      = std::is_nothrow_move_constructible<T>::value;
    static_assert(
      std::is_move_constructible<T>::value, "Types must be move constructible");

    explicit emptyable(T&& v) noexcept(move_noexcept)
      : _v(std::move(v)) {}

    operator bool() const noexcept { return bool(_v); }

    const T& get() const& { return _v; }

    T& get() & { return _v; }

private:
    T _v;
};

template<typename T>
class emptyable<T, std::false_type> {
public:
    static constexpr bool move_noexcept
      = std::is_nothrow_move_constructible<T>::value;
    static_assert(
      std::is_move_constructible<T>::value, "Types must be move constructible");

    explicit emptyable(T&& v) noexcept(move_noexcept)
      : _v(std::move(v))
      , _valid(true) {}

    emptyable(emptyable&& other) noexcept(move_noexcept)
      : _v(std::move(other._v))
      , _valid(std::exchange(other._valid, false)) {}

    emptyable& operator=(emptyable&& other) noexcept(move_noexcept) {
        _v = std::move(other._v);
        _valid = std::exchange(other._valid, false);
        return *this;
    }

    operator bool() const noexcept { return _valid; }

    const T& get() const& { return _v; }

    T& get() & { return _v; }

private:
    T _v;
    bool _valid;
};

template<typename T>
using naturally_emptyable = std::conditional_t<
  (std::is_convertible<T, bool>::value || std::is_constructible<bool, T>::value)
    && !std::is_integral<T>::value,
  std::true_type,
  std::false_type>;

} // namespace internal

/// Marker type that designates a graph of objects that may
/// have originated on a different CPU.
///
/// \c remote<> is a move-only object; it cannot be copied.
///
template<typename T, typename E = ::internal::naturally_emptyable<T>>
class remote final {
public:
    explicit remote(T&& v) noexcept(::internal::emptyable<T, E>::move_noexcept)
      : _v(std::move(v))
      , _origin_cpu(ss::this_shard_id()) {}

    template<typename... Args>
    explicit remote(Args... args) noexcept(
      std::is_nothrow_constructible<T>::value)
      : remote(T(std::forward<Args>(args)...)) {}

    remote(remote&&) = default;
    remote& operator=(remote&&) = default;

    ~remote() noexcept {
        if (unlikely(_v && _origin_cpu != ss::this_shard_id())) {
            std::abort();
        }
    }

    const T& get() const& { return _v.get(); }

    T& get() & { return _v.get(); }

private:
    ::internal::emptyable<T, E> _v;
    ss::shard_id _origin_cpu;
};
