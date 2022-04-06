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

#if defined(_MSC_VER)
#define SERDE_SIG __FUNCSIG__
#elif defined(__clang__) || defined(__GNUC__)
#define SERDE_SIG __PRETTY_FUNCTION__
#else
#error unsupported compiler
#endif

template<typename T>
constexpr std::string_view type_str() {
#if defined(__clang__)
    constexpr std::string_view prefix
      = "std::string_view serde::type_str() [T = ";
    constexpr std::string_view suffix = "]";
#elif defined(_MSC_VER)
    constexpr std::string_view prefix
      = "class std::basic_string_view<char,struct std::char_traits<char> > "
        "__cdecl serde::type_str<";
    constexpr std::string_view suffix = ">(void)";
#else
    constexpr std::string_view prefix
      = "constexpr std::string_view serde::type_str() [with T = ";
    constexpr std::string_view suffix
      = "; std::string_view = std::basic_string_view<char>]";
#endif

    auto sig = std::string_view{SERDE_SIG};
    sig.remove_prefix(prefix.size());
    sig.remove_suffix(suffix.size());
    return sig;
}

} // namespace serde
