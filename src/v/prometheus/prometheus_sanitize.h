#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

struct prometheus_sanitize {
    static sstring metrics_name(sstring n) {
        for (char& i : n) {
            if (!std::isalnum(i)) {
                i = '_';
            }
        }
        return n;
    }
};
