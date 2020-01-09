#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

struct prometheus_sanitize {
    static ss::sstring metrics_name(ss::sstring n) {
        for (char& i : n) {
            if (!std::isalnum(i)) {
                i = '_';
            }
        }
        return n;
    }
};
