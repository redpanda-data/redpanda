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

#include "base/seastarx.h"

#include <fmt/ostream.h>

#include <optional>
#include <variant>

using disable_tristate_t = std::monostate;

constexpr static auto disable_tristate = disable_tristate_t{};

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
    using value_type = T;
    constexpr tristate() noexcept
      : tristate(disable_tristate) {}
    constexpr explicit tristate(disable_tristate_t) noexcept
      : _value{disable_tristate} {}

    constexpr explicit tristate(std::optional<T> t) noexcept
      : _value(std::move(t)) {}

    constexpr bool is_disabled() const {
        return std::holds_alternative<disable_tristate_t>(_value);
    }

    /// \brief Checks if the tristate is in the "Set" state. That means
    /// it is not disabled and it holds a value.
    ///
    /// \return true if the tristate is in the "Set" state
    ///         false otherwise
    constexpr bool has_optional_value() const {
        return !is_disabled() && get_optional().has_value();
    }

    /// \brief Checks if the tristate is in the "Disabled or "Set" states.
    /// We often use "Not set" as the default state of a tristate, and
    /// it's useful to check if any explicit changes were made.
    ///
    /// \return true if the tristate is in the "Disabled" or "Set" state
    ///         false otherwise
    constexpr bool is_engaged() const {
        return is_disabled() || get_optional().has_value();
    }

    /// \brief Checks if the tristate is in the not "Disabled" and not "Set"
    /// states. we usually use this state to means that the values should be
    /// retrieved from some other sources, like cluster default values for topic
    /// properties. the logic is !is_engaged, so this is a convenience method
    constexpr bool is_empty() const { return !is_engaged(); }

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
        if (t.has_optional_value()) {
            fmt::print(o, "{{{}}}", t.value());
            return o;
        }
        return o << "{{nullopt}}";
    };

    std::optional<T>& get_optional() {
        return std::get<std::optional<T>>(_value);
    }

    const std::optional<T>& get_optional() const {
        return std::get<std::optional<T>>(_value);
    }

private:
    using underlying_t = std::variant<disable_tristate_t, std::optional<T>>;
    underlying_t _value;
};
