/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "debug_bundle/error.h"
#include "json/document.h"
#include "utils/functional.h"

#include <type_traits>

namespace debug_bundle {

template<typename T>
debug_bundle::result<T> from_json(const json::Value& v) {
    constexpr auto parse_error = []() {
        return error_info{error_code::invalid_parameters, "Failed to parse"};
    };

    if constexpr (std::is_same_v<T, int>) {
        if (v.IsInt()) {
            return v.GetInt();
        }
    } else {
        static_assert(always_false_v<T>, "Not implemented");
    }
    return parse_error();
}

} // namespace debug_bundle
