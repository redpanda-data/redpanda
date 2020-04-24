#pragma once

#include "seastarx.h"

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
