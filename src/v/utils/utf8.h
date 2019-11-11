#pragma once

#include <boost/locale/encoding_utf.hpp>

#include <string_view>

// clang-format off
CONCEPT(
template<typename T>
concept bool ExceptionThrower = requires(T obj) {
    obj.conversion_error();
};
)
// clang-format on

namespace {
struct default_thrower {
    [[noreturn]] [[gnu::cold]] void conversion_error() {
        throw std::runtime_error("Cannot decode string as UTF8");
    }
};
} // namespace

template<typename Thrower>
CONCEPT(requires ExceptionThrower<Thrower>)
static void validate_utf8(std::string_view s, Thrower&& thrower) {
    try {
        boost::locale::conv::utf_to_utf<char>(
          s.begin(), s.end(), boost::locale::conv::stop);
    } catch (const boost::locale::conv::conversion_error& ex) {
        thrower.conversion_error();
    }
}

static void validate_utf8(std::string_view s) {
    validate_utf8(s, default_thrower{});
}
