/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <optional>

template<typename T>
class opt final : private std::optional<T> {
public:
    explicit opt(std::optional<T>&& o)
      : std::optional<T>(std::move(o)) {}

    using value_type = T;

    using std::optional<T>::optional;
    using std::optional<T>::operator=;
    using std::optional<T>::value;
    using std::optional<T>::has_value;
    using std::optional<T>::value_or;
    // NOTE: that's not gonna work
    // using std::optional<T>::swap;
    using std::optional<T>::reset;
    using std::optional<T>::emplace;

    // TODO: monadic ops (c++23)

    friend auto operator<=>(const opt<T>&, const opt<T>&) = default;

    // TODO: comparison to std::nullopt_t

    opt& operator=(std::nullopt_t) noexcept {
        reset();
        return *this;
    }
};

template<typename T>
constexpr opt<std::decay_t<T>> make_opt(T&& v) {
    return opt<std::decay_t<T>>(std::forward<T>(v));
}

template<typename T, typename... Args>
constexpr opt<T> make_opt(Args&&... args) {
    return opt<T>{std::in_place, std::forward<Args>(args)...};
}

// TODO(oren): initializer list version
