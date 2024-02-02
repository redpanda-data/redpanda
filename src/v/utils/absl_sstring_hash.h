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
/**
 * Transpartent hash & eq functors for ss::sstring
 */
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <absl/hash/hash.h>

#include <string_view>
#include <type_traits>

struct sstring_hash {
    using is_transparent = std::true_type;

    size_t operator()(std::string_view v) const {
        return absl::Hash<std::string_view>{}(v);
    }
};

struct sstring_eq {
    using is_transparent = std::true_type;

    bool operator()(std::string_view lhs, std::string_view rhs) const {
        return lhs == rhs;
    }
};

struct sstring_less {
    using is_transparent = std::true_type;

    bool operator()(std::string_view lhs, std::string_view rhs) const {
        return lhs < rhs;
    }
};
