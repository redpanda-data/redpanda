#pragma once

#include <type_traits>
#include <vector>

namespace rpc {
template<typename T>
struct is_std_vector : std::false_type {};
template<typename... Args>
struct is_std_vector<std::vector<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_vector_v = is_std_vector<T>::value;
} // namespace rpc
