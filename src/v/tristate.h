/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "serde/serde.h"

#include <fmt/ostream.h>

#include <optional>
#include <variant>
/// The tristate class express a value that can either be present,
/// not set or disabled explicitly.
/// As the name suggest the tristate can have the following three states:
///
/// - Disabled
/// - Not set
/// - Set
///
/// Tristate is helpful for example for configuration parameters saying that
/// feature can be either disabled, or server defaults should be applied
/// or it should be configured with requested value

template<typename T>
class tristate {
public:
    constexpr tristate() noexcept = default;

    tristate(T&& o) noexcept
      : _value{std::move(o)} {}

    tristate(T const& o)
      : _value(o) {}

    tristate& operator=(T const& o) {
        _value = o;
        return *this;
    }

    tristate& operator=(T&& o) noexcept {
        _value = std::move(o);
        return *this;
    }

    constexpr explicit tristate(std::optional<T> t) noexcept
      : _value(std::move(t)) {}

    constexpr bool is_disabled() const {
        return std::holds_alternative<std::monostate>(_value);
    }

    constexpr bool has_value() const {
        return !is_disabled() && get_optional().has_value();
    }

    constexpr const T& operator*() const& { return *get_optional(); }
    constexpr T& operator*() & { return *get_optional(); }
    constexpr T&& operator*() && { return *get_optional(); }

    constexpr T& value() { return get_optional().value(); }
    constexpr const T& value() const { return get_optional().value(); }

    friend constexpr bool
    operator==(const tristate<T>& lhs, const tristate<T>& rhs) {
        return lhs._value == rhs._value;
    }

    friend constexpr bool
    operator!=(const tristate<T>& lhs, const tristate<T>& rhs) {
        return lhs._value != rhs._value;
    }

    friend constexpr bool
    operator<(const tristate<T>& lhs, const tristate<T>& rhs) {
        return lhs._value < rhs._value;
    }

    friend constexpr bool
    operator>(const tristate<T>& lhs, const tristate<T>& rhs) {
        return lhs._value > rhs._value;
    }

    friend constexpr bool
    operator<=(const tristate<T>& lhs, const tristate<T>& rhs) {
        return lhs._value <= rhs._value;
    }

    friend constexpr bool
    operator>=(const tristate<T>& lhs, const tristate<T>& rhs) {
        return lhs._value >= rhs._value;
    }

    friend std::ostream& operator<<(std::ostream& o, tristate<T> t) {
        if (t.is_disabled()) {
            fmt::print(o, "{{disabled}}");
            return o;
        }
        if (t.has_value()) {
            fmt::print(o, "{{{}}}", t.value());
            return o;
        }
        return o << "{}";
    };

private:
    std::optional<T>& get_optional() {
        return std::get<std::optional<T>>(_value);
    }

    const std::optional<T>& get_optional() const {
        return std::get<std::optional<T>>(_value);
    }

    using underlying_t = std::variant<std::monostate, std::optional<T>>;
    underlying_t _value;
};

/// Deserialize tristate using the same format as adl
///
/// Template parameter T is a result value type.
template<class T>
void read_nested(
  iobuf_parser& in, tristate<T>& value, size_t bytes_left_limit) {
    auto state = serde::read_nested<int8_t>(in, bytes_left_limit);
    if (state == -1) {
        value = tristate<T>{};
    } else if (state == 0) {
        value = tristate<T>{std::nullopt};
    }
    value = serde::read_nested<T>(in, bytes_left_limit);
};

/// Serialize tristate using the same format as adl
///
/// Template parameter T is a value type of the tristate.
template<class T>
inline void write(iobuf& out, const tristate<T>& t) {
    if (t.is_disabled()) {
        serde::write(out, static_cast<int8_t>(-1));
    } else if (!t.has_value()) {
        serde::write(out, static_cast<int8_t>(0));
    } else {
        serde::write(out, static_cast<int8_t>(1));
        serde::write(out, t.value());
    }
}
