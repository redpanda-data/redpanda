#pragma once
#include <seastar/core/sstring.hh>

#include <boost/type_index.hpp>

#include <cstdint>
#include <functional> // needed for std::hash
#include <limits>
#include <type_traits>
#include <utility>

namespace detail {
template<typename T>
struct named_type_is_supported {
    using type = T;
    static constexpr bool value = std::is_arithmetic_v<T>;
};
template<typename T>
struct named_type_constant {
    static constexpr const T min = std::numeric_limits<T>::min();
};
template<typename T>
inline constexpr T named_type_constant_v = named_type_constant<T>::min;
} // namespace detail
template<typename T, typename Tag>
class named_type {
public:
    using type = T;
    static_assert(
      detail::named_type_is_supported<T>::value,
      "T must be a supported min type");

    constexpr named_type() = default;
    constexpr named_type(const type& v)
      : _value(v) {
    }
    constexpr named_type(type&& v)
      : _value(std::move(v)) {
    }
    named_type(named_type&& o) noexcept = default;
    named_type& operator=(named_type&& o) noexcept = default;
    named_type(const named_type& o) noexcept = default;
    named_type& operator=(const named_type& o) noexcept = default;
    constexpr bool operator==(const named_type& other) const {
        return _value == other._value;
    }
    constexpr bool operator!=(const named_type& other) const {
        return _value != other._value;
    }
    constexpr bool operator<(const named_type& other) const {
        return _value < other._value;
    }
    constexpr bool operator>(const named_type& other) const {
        return _value > other._value;
    }
    constexpr bool operator<=(const named_type& other) const {
        return _value <= other._value;
    }
    constexpr bool operator>=(const named_type& other) const {
        return _value >= other._value;
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
    type _value = detail::named_type_constant_v<T>;
};

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
    using type = ::named_type<T, Tag>;
    // caching the name has a big compile time impact
    static const auto name = boost::typeindex::type_id<type>().pretty_name();
    return o << "{" << name << "=" << t() << "}";
};

} // namespace std
