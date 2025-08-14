/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/format_to.h"

#include <gtest/gtest.h>

struct foo {
    std::string a;
    int32_t b;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{a: {}, b: {}}}", a, b);
    }
};

static_assert(fmt::HasFormatToMethod<foo>);

TEST(Formatter, FormatTo) {
    foo f{.a = "bar", .b = 3};
    EXPECT_EQ("hello: {a: bar, b: 3}", fmt::format("hello: {}", f)) << f;
}

namespace ns {
struct foo {
    std::string a;
    int32_t b;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{a: {}, b: {}}}", a, b);
    }
};

struct bar {
    foo f;
};

std::ostream& operator<<(std::ostream& os, const bar& b) {
    return os << "{f: " << b.f << "}";
}

TEST(Formatter, InsideNamespacedFormatTo) {
    ns::foo f{.a = "bar", .b = 3};
    EXPECT_EQ("hello: {a: bar, b: 3}", fmt::format("hello: {}", f)) << f;
    ns::bar b{.f = f};
    EXPECT_EQ(
      "world: {f: {a: bar, b: 3}}", fmt::format("world: {}", fmt::streamed(b)))
      << b;
}

} // namespace ns

TEST(Formatter, OutsideNamespacedFormatTo) {
    ns::foo f{.a = "bar", .b = 3};
    EXPECT_EQ("hello: {a: bar, b: 3}", fmt::format("hello: {}", f)) << f;
}
