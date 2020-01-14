#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <algorithm>

struct prometheus_sanitize {
    static ss::sstring metrics_name(ss::sstring n) {
        constexpr char value  = '_';
        std::replace_if(n.begin(), n.end(), 
                        [](auto c){ return !std::isalnum(c); }, value);
        return n;
    }
};
