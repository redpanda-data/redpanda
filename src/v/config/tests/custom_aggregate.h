/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

struct custom_aggregate {
    ss::sstring string_value;
    int int_value;

    bool operator==(const custom_aggregate& rhs) const {
        return string_value == rhs.string_value && int_value == rhs.int_value;
    }
};
