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

#include <functional>
#include <ostream>

namespace detail {

struct named_type_tag {};

template<typename T, typename Dervied, typename IsConstexpr>
class base_named_base;

template<typename T, typename Derived>
class base_named_base<T, Derived, std::true_type> : public named_type_tag {
public:
    using base = base_named_base<T, Derived, std::true_type>;
    using type = T;
    constexpr base_named_base() = default;
    constexpr explicit base_named_base(const type& v)
      : _value(v) {}
    constexpr explicit base_named_base(type&& v)
      : _value(std::move(v)) {}
    base_named_base(base_named_base&& o) noexcept = default;
    base_named_base& operator=(base_named_base&& o) noexcept = default;
    base_named_base(const base_named_base& o) noexcept = default;
    base_named_base& operator=(const base_named_base& o) noexcept = default;
    constexpr bool operator==(const base_named_base& other) const {
        return _value == other._value;
    }
    constexpr bool operator!=(const base_named_base& other) const {
        return _value != other._value;
    }
    constexpr bool operator<(const base_named_base& other) const {
        return _value < other._value;
    }
    constexpr bool operator>(const base_named_base& other) const {
        return _value > other._value;
    }
    constexpr bool operator<=(const base_named_base& other) const {
        return _value <= other._value;
    }
    constexpr bool operator>=(const base_named_base& other) const {
        return _value >= other._value;
    }
    constexpr Derived& operator++() {
        ++_value;
        return as_derived();
    }
    constexpr Derived operator++(int) {
        Derived copy = as_derived();
        ++_value;
        return copy;
    }
    constexpr Derived& operator--() {
        --_value;
        return as_derived();
    }
    constexpr const Derived operator--(int) {
        Derived cp = as_derived();
        --_value;
        return cp;
    }
    constexpr Derived operator+(const Derived& val) const {
        return Derived(_value + val()); // not mutable
    }
    constexpr Derived operator+(const type& val) const {
        return Derived(_value + val); // not mutable
    }

    constexpr Derived operator-(const Derived& val) const {
        return Derived(_value - val()); // not mutable
    }

    Derived& operator+=(const type& val) {
        _value += val;
        return as_derived();
    }

    // provide overloads for naked type
    constexpr bool operator==(const type& other) const {
        return _value == other;
    }
    constexpr bool operator!=(const type& other) const {
        return _value != other;
    }
    constexpr bool operator<(const type& other) const { return _value < other; }
    constexpr bool operator>(const type& other) const { return _value > other; }
    constexpr bool operator<=(const type& other) const {
        return _value <= other;
    }
    constexpr bool operator>=(const type& other) const {
        return _value >= other;
    }

    // explicit getter
    constexpr type operator()() const { return _value; }
    // implicit conversion operator
    constexpr operator type() const { return _value; }

    static constexpr Derived min() {
        return Derived(std::numeric_limits<type>::min());
    }

    static constexpr Derived max() {
        return Derived(std::numeric_limits<type>::max());
    }

    friend std::ostream& operator<<(std::ostream& o, const base_named_base& t) {
        return o << "{" << t() << "}";
    };

protected:
    type _value = std::numeric_limits<T>::min();

private:
    Derived& as_derived() { return static_cast<Derived&>(*this); }
};

template<typename T, typename Derived>
class base_named_base<T, Derived, std::false_type> : public named_type_tag {
public:
    using base = base_named_base<T, Derived, std::false_type>;
    using type = T;
    static constexpr bool move_noexcept
      = std::is_nothrow_move_constructible<T>::value;

    base_named_base() = default;
    explicit base_named_base(const type& v)
      : _value(v) {}
    explicit base_named_base(type&& v)
      : _value(std::move(v)) {}

    base_named_base(base_named_base&& o) noexcept(move_noexcept) = default;

    base_named_base& operator=(base_named_base&& o) noexcept(move_noexcept)
      = default;

    base_named_base(const base_named_base& o) = default;

    base_named_base& operator=(const base_named_base& o) = default;

    bool operator==(const base_named_base& other) const {
        return _value == other._value;
    }
    bool operator!=(const base_named_base& other) const {
        return _value != other._value;
    }
    bool operator<(const base_named_base& other) const {
        return _value < other._value;
    }
    bool operator>(const base_named_base& other) const {
        return _value > other._value;
    }
    bool operator<=(const base_named_base& other) const {
        return _value <= other._value;
    }
    bool operator>=(const base_named_base& other) const {
        return _value >= other._value;
    }

    // provide overloads for naked type
    bool operator==(const type& other) const { return _value == other; }
    bool operator!=(const type& other) const { return _value != other; }
    bool operator<(const type& other) const { return _value < other; }
    bool operator>(const type& other) const { return _value > other; }
    bool operator<=(const type& other) const { return _value <= other; }
    bool operator>=(const type& other) const { return _value >= other; }

    // explicit getter
    const type& operator()() const& { return _value; }
    type operator()() && { return std::move(_value); }
    // implicit conversion operator
    operator const type&() const& { return _value; }
    operator type() && { return std::move(_value); }

    friend std::ostream& operator<<(std::ostream& o, const base_named_base& t) {
        return o << "{" << t() << "}";
    };

protected:
    type _value;
};

template<typename T>
concept NamedTypeTrivialSubclass
  = std::derived_from<T, detail::named_type_tag> &&(
    sizeof(T) == sizeof(typename T::type));

} // namespace detail

template<typename T, typename Dervied>
using named_base = detail::base_named_base<
  T,
  Dervied,
  std::conditional_t<std::is_arithmetic_v<T>, std::true_type, std::false_type>>;

namespace std {
template<detail::NamedTypeTrivialSubclass T>
struct hash<T> {
    constexpr size_t operator()(const T& x) const {
        return std::hash<typename T::type>()(x);
    }
};
} // namespace std
