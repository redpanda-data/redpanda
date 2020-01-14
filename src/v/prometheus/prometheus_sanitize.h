#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <algorithm>

struct prometheus_sanitize {
    static ss::sstring metrics_name(const ss::sstring& n) {
        auto copy = n;
        constexpr char value  = '_';
        std::replace_if(copy.begin(), copy.end(), 
                        [](auto c){ return !std::isalnum(c); }, value);
        return copy;
    }
};
