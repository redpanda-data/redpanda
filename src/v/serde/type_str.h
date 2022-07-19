// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <string_view>

namespace serde {

template<typename T>
consteval std::string_view type_str() {
#if defined(__clang__)
    constexpr std::string_view prefix
      = "std::string_view serde::type_str() [T = ";
    constexpr std::string_view suffix = "]";
#elif defined(__GNUC__)
    constexpr std::string_view prefix
      = "consteval std::string_view serde::type_str() [with T = ";
    constexpr std::string_view suffix
      = "; std::string_view = std::basic_string_view<char>]";
#else
#error unsupported compiler
#endif
    auto sig = std::string_view{__PRETTY_FUNCTION__};
    sig.remove_prefix(prefix.size());
    sig.remove_suffix(suffix.size());
    return sig;
}

} // namespace serde
