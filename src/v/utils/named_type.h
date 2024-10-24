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
#include <fmt/ostream.h>

#include <cstdint>
#include <limits>
#include <ostream>
#include <type_traits>
#include <utility>

namespace detail {

template<typename T, typename Tag, typename IsConstexpr>
class base_named_type;

template<typename T, typename Tag>
class base_named_type<T, Tag, std::true_type> {
public:
    using type = T;
    constexpr base_named_type() = default;
    constexpr explicit base_named_type(const type& v)
      : _value(v) {}
    constexpr explicit base_named_type(type&& v)
      : _value(std::move(v)) {}
    base_named_type(base_named_type&& o) noexcept = default;
    base_named_type& operator=(base_named_type&& o) noexcept = default;
    base_named_type(const base_named_type& o) noexcept = default;
    base_named_type& operator=(const base_named_type& o) noexcept = default;

    friend constexpr bool
    operator==(const base_named_type&, const base_named_type&) noexcept
      = default;
    friend constexpr auto
    operator<=>(const base_named_type&, const base_named_type&) noexcept
      = default;

    constexpr base_named_type& operator++() {
        ++_value;
        return *this;
    }
    constexpr base_named_type operator++(int) {
        auto copy = *this;
        ++_value;
        return copy;
    }
    constexpr base_named_type& operator--() {
        --_value;
        return *this;
    }
    constexpr const base_named_type operator--(int) {
        auto cp = *this;
        --_value;
        return cp;
    }
    constexpr base_named_type operator+(const base_named_type& val) const {
        return base_named_type(_value + val()); // not mutable
    }
    constexpr base_named_type operator+(const type& val) const {
        return base_named_type(_value + val); // not mutable
    }

    constexpr base_named_type operator-(const base_named_type& val) const {
        return base_named_type(_value - val()); // not mutable
    }

    base_named_type& operator+=(const type& val) {
        _value += val;
        return *this;
    }

    // provide overloads for naked type
    friend constexpr bool
    operator==(const base_named_type& lhs, const type& rhs) noexcept {
        return lhs._value == rhs;
    }
    friend constexpr auto
    operator<=>(const base_named_type& lhs, const type& rhs) noexcept {
        return lhs._value <=> rhs;
    }

    // explicit getter
    constexpr type operator()() const { return _value; }
    // implicit conversion operator
    constexpr operator type() const { return _value; }

    static constexpr base_named_type min() {
        return base_named_type(std::numeric_limits<type>::min());
    }

    static constexpr base_named_type max() {
        return base_named_type(std::numeric_limits<type>::max());
    }

    friend std::ostream& operator<<(std::ostream& o, const base_named_type& t) {
        fmt::print(o, "{}", t._value);
        return o;
    };

    friend std::istream& operator>>(std::istream& i, base_named_type& t) {
        return i >> t._value;
    };

protected:
    type _value = std::numeric_limits<T>::min();
};
template<typename T, typename Tag>
class base_named_type<T, Tag, std::false_type> {
public:
    using type = T;
    static constexpr bool move_noexcept
      = std::is_nothrow_move_constructible<T>::value;

    base_named_type() = default;

    template<typename... Args>
    requires std::constructible_from<T, Args...>
    explicit constexpr base_named_type(Args&&... args)
      : _value(std::forward<Args>(args)...) {}

    base_named_type(base_named_type&& o) noexcept(move_noexcept) = default;

    base_named_type& operator=(base_named_type&& o) noexcept(move_noexcept)
      = default;

    base_named_type(const base_named_type& o) = default;

    base_named_type& operator=(const base_named_type& o) = default;

    // provide overloads for naked type
    friend bool
    operator==(const base_named_type& lhs, const type& rhs) noexcept {
        return lhs._value == rhs;
    }

    friend auto operator<=>(
      const base_named_type& lhs,
      const type& rhs) noexcept -> std::strong_ordering {
        // roundabout way to cope with type when it does not provide <=>
        if constexpr (std::three_way_comparable_with<type, type>)
            return lhs._value <=> rhs;
        else {
            if (lhs._value == rhs) {
                return std::strong_ordering::equal;
            } else if (lhs._value < rhs) {
                return std::strong_ordering::less;
            } else {
                return std::strong_ordering::greater;
            }
        }
    }

    // these can be constexpr in c++23
    friend bool
    operator==(const base_named_type& lhs, const base_named_type& rhs) noexcept
      = default;
    friend auto operator<=>(
      const base_named_type& lhs, const base_named_type& rhs) noexcept {
        // delegate to <=> type version
        return lhs <=> rhs._value;
    }

    // explicit getter
    constexpr const type& operator()() const& { return _value; }
    constexpr type operator()() && { return std::move(_value); }
    // implicit conversion operator
    constexpr operator const type&() const& { return _value; }
    constexpr operator type() && { return std::move(_value); }

    friend std::ostream& operator<<(std::ostream& o, const base_named_type& t) {
        fmt::print(o, "{}", t._value);
        return o;
    };

    friend std::istream& operator>>(std::istream& i, base_named_type& t) {
        return i >> t._value;
    };

protected:
    type _value;
};

} // namespace detail

template<typename T, typename Tag>
using named_type = detail::base_named_type<
  T,
  Tag,
  std::conditional_t<std::is_arithmetic_v<T>, std::true_type, std::false_type>>;

namespace std {
template<typename T, typename Tag>
struct hash<named_type<T, Tag>> {
    using type = ::named_type<T, Tag>;
    constexpr size_t operator()(const type& x) const {
        return std::hash<T>()(x);
    }
};
} // namespace std
