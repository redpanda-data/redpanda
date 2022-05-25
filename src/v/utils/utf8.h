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

#include <boost/locale/encoding_utf.hpp>

#include <string_view>

// clang-format off
template<typename T>
concept ExceptionThrower = requires(T obj) {
    obj.conversion_error();
};
// clang-format on

namespace {
struct default_thrower {
    [[noreturn]] [[gnu::cold]] void conversion_error() {
        throw std::runtime_error("Cannot decode string as UTF8");
    }
};
} // namespace

template<typename Thrower>
requires ExceptionThrower<Thrower>
inline void validate_utf8(std::string_view s, Thrower&& thrower) {
    try {
        boost::locale::conv::utf_to_utf<char>(
          s.begin(), s.end(), boost::locale::conv::stop);
    } catch (const boost::locale::conv::conversion_error& ex) {
        thrower.conversion_error();
    }
}

inline void validate_utf8(std::string_view s) {
    validate_utf8(s, default_thrower{});
}
