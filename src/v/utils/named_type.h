#pragma once
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <functional> // needed for std::hash
#include <limits>
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
      : _value(v) {
    }
    constexpr explicit base_named_type(type&& v)
      : _value(std::move(v)) {
    }
    base_named_type(base_named_type&& o) noexcept = default;
    base_named_type& operator=(base_named_type&& o) noexcept = default;
    base_named_type(const base_named_type& o) noexcept = default;
    base_named_type& operator=(const base_named_type& o) noexcept = default;
    constexpr bool operator==(const base_named_type& other) const {
        return _value == other._value;
    }
    constexpr bool operator!=(const base_named_type& other) const {
        return _value != other._value;
    }
    constexpr bool operator<(const base_named_type& other) const {
        return _value < other._value;
    }
    constexpr bool operator>(const base_named_type& other) const {
        return _value > other._value;
    }
    constexpr bool operator<=(const base_named_type& other) const {
        return _value <= other._value;
    }
    constexpr bool operator>=(const base_named_type& other) const {
        return _value >= other._value;
    }
    constexpr type& operator++() {
        return ++_value;
    }

    // provide overloads for naked type
    constexpr bool operator==(const type& other) const {
        return _value == other;
    }
    constexpr bool operator!=(const type& other) const {
        return _value != other;
    }
    constexpr bool operator<(const type& other) const {
        return _value < other;
    }
    constexpr bool operator>(const type& other) const {
        return _value > other;
    }
    constexpr bool operator<=(const type& other) const {
        return _value <= other;
    }
    constexpr bool operator>=(const type& other) const {
        return _value >= other;
    }

    // explicit getter
    constexpr type operator()() const {
        return _value;
    }
    // implicit conversion operator
    constexpr operator type() const {
        return _value;
    }

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
    explicit base_named_type(const type& v)
      : _value(v) {
    }
    explicit base_named_type(type&& v)
      : _value(std::move(v)) {
    }

    base_named_type(base_named_type&& o) noexcept(move_noexcept) = default;

    base_named_type& operator=(base_named_type&& o) noexcept(move_noexcept)
      = default;

    base_named_type(const base_named_type& o) = default;

    base_named_type& operator=(const base_named_type& o) = default;

    bool operator==(const base_named_type& other) const {
        return _value == other._value;
    }
    bool operator!=(const base_named_type& other) const {
        return _value != other._value;
    }
    bool operator<(const base_named_type& other) const {
        return _value < other._value;
    }
    bool operator>(const base_named_type& other) const {
        return _value > other._value;
    }
    bool operator<=(const base_named_type& other) const {
        return _value <= other._value;
    }
    bool operator>=(const base_named_type& other) const {
        return _value >= other._value;
    }

    // provide overloads for naked type
    bool operator==(const type& other) const {
        return _value == other;
    }
    bool operator!=(const type& other) const {
        return _value != other;
    }
    bool operator<(const type& other) const {
        return _value < other;
    }
    bool operator>(const type& other) const {
        return _value > other;
    }
    bool operator<=(const type& other) const {
        return _value <= other;
    }
    bool operator>=(const type& other) const {
        return _value >= other;
    }

    // explicit getter
    const type& operator()() const {
        return _value;
    }
    // implicit conversion operator
    operator const type&() const {
        return _value;
    }

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

template<typename T, typename Tag>
inline ostream& operator<<(ostream& o, const ::named_type<T, Tag>& t) {
    return o << "{" << t() << "}";
};

} // namespace std
