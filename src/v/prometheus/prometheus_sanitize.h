/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <algorithm>

struct prometheus_sanitize {
    static ss::sstring metrics_name(const ss::sstring& n) {
        auto copy = n;
        constexpr char value = '_';
        std::replace_if(
          copy.begin(),
          copy.end(),
          [](auto c) { return !std::isalnum(c); },
          value);
        return copy;
    }
};
