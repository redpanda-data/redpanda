#pragma once

// Credits: originally taken from cista.rocks (MIT License)

#include "reflection/arity.h"
#include "reflection/to_tuple.h"

#include <type_traits>
#include <utility>

namespace reflection {

template<typename T, typename Fn>
inline void for_each_field(T& t, Fn&& fn) {
    if constexpr (std::is_pointer_v<T>) {
        if (t != nullptr) {
            for_each_field(*t, std::forward<Fn>(fn));
        }
    } else if constexpr (std::is_scalar_v<T>) {
        fn(t);
    } else {
        std::apply([&](auto&&... args) { ((void)fn(args), ...); }, to_tuple(t));
    }
}

template<typename T, typename Fn>
inline void for_each_field(Fn&& fn) {
    T t{};
    for_each_field<T>(t, std::forward<Fn>(fn));
}

} // namespace reflection
