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
#include "bytes/bytes.h"

#include <boost/algorithm/hex.hpp>

inline bytes convert_from_hex(const std::string& val) {
    std::vector<uint8_t> rv;
    rv.reserve(val.size() / 2);
    boost::algorithm::unhex(val.begin(), val.end(), std::back_inserter(rv));

    return bytes{rv.data(), rv.size()};
}

inline std::string_view bytes_view_to_string_view(bytes_view v) {
    return {// NOLINTNEXTLINE
            reinterpret_cast<const std::string_view::value_type*>(v.data()),
            v.size()};
}

inline std::span<char> bytes_span_to_char_span(bytes_span<> v) {
    // NOLINTNEXTLINE
    return {reinterpret_cast<std::span<char>::value_type*>(v.data()), v.size()};
}
