#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

struct custom_aggregate {
    ss::sstring string_value;
    int int_value;
};
