#pragma once

#include "fundamental.h"

namespace model {

template<typename T>
struct model_limits {};

template<>
struct model_limits<offset> {
    static constexpr offset max() {
        return offset(std::numeric_limits<typename offset::type>::max());
    }
    static constexpr offset min() {
        return offset(std::numeric_limits<typename offset::type>::min());
    }
};
template<>
struct model_limits<term_id> {
    static constexpr term_id max() {
        return term_id(std::numeric_limits<typename term_id::type>::max());
    }
    static constexpr term_id min() {
        return term_id(std::numeric_limits<typename term_id::type>::min());
    }
};

} // namespace model