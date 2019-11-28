#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

struct prometheus_sanitize {
    static sstring metrics_name(sstring n) {
        for (auto i = 0u; i < n.size(); ++i) {
            if (!std::isalnum(n[i])) n[i] = '_';
        }
        return n;
    }
};
