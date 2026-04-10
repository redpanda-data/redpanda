/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "strings/utf8.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>

// ---------------------------------------------------------------------------
// utf8_truncate_min
// ---------------------------------------------------------------------------

TEST(Utf8TruncateMin, FitsWithinLimit) {
    EXPECT_EQ(utf8_truncate_min("hello", 10), "hello");
    EXPECT_EQ(utf8_truncate_min("hello", 5), "hello");
}

TEST(Utf8TruncateMin, EmptyString) {
    EXPECT_EQ(utf8_truncate_min("", 4), "");
    EXPECT_EQ(utf8_truncate_min("", 0), "");
}

TEST(Utf8TruncateMin, ZeroLimit) {
    EXPECT_EQ(utf8_truncate_min("hello", 0), "");
}

TEST(Utf8TruncateMin, AsciiTruncation) {
    // Pure ASCII: truncation always falls on a character boundary.
    EXPECT_EQ(utf8_truncate_min("abcde", 3), "abc");
}

TEST(Utf8TruncateMin, TwoByteBoundary) {
    // "é" = U+00E9 = 0xC3 0xA9 (2 bytes)
    std::string s = "a\xC3\xA9z"; // "aéz" — 4 bytes
    // Limit 2: "a" fits, "é" starts at byte 1 but needs 2 bytes → excluded.
    EXPECT_EQ(utf8_truncate_min(s, 2), "a");
    // Limit 3: "a" (1) + "é" (2) = 3 bytes exactly.
    EXPECT_EQ(utf8_truncate_min(s, 3), "a\xC3\xA9");
    // Limit 4: whole string.
    EXPECT_EQ(utf8_truncate_min(s, 4), s);
}

TEST(Utf8TruncateMin, ThreeByteBoundary) {
    // "€" = U+20AC = 0xE2 0x82 0xAC (3 bytes)
    std::string euro = "\xE2\x82\xAC";
    // Limit 1 or 2: can't fit even the first codepoint.
    EXPECT_EQ(utf8_truncate_min(euro, 1), "");
    EXPECT_EQ(utf8_truncate_min(euro, 2), "");
    // Limit 3: fits exactly.
    EXPECT_EQ(utf8_truncate_min(euro, 3), euro);
    // Multi-codepoint: "a€b" (5 bytes), limit 4 → "a€" (4 bytes).
    std::string s = "a\xE2\x82\xAC"
                    "b";
    EXPECT_EQ(utf8_truncate_min(s, 4), "a\xE2\x82\xAC");
    EXPECT_EQ(utf8_truncate_min(s, 3), "a"); // "€" starts at byte 1, needs 3
}

TEST(Utf8TruncateMin, FourByteBoundary) {
    // U+1F600 (😀) = 0xF0 0x9F 0x98 0x80 (4 bytes)
    std::string emoji = "\xF0\x9F\x98\x80";
    EXPECT_EQ(utf8_truncate_min(emoji, 3), "");
    EXPECT_EQ(utf8_truncate_min(emoji, 4), emoji);
    // "a😀b" (6 bytes), limit 5 → "a😀" (5 bytes).
    std::string s = "a\xF0\x9F\x98\x80"
                    "b";
    EXPECT_EQ(utf8_truncate_min(s, 5), "a\xF0\x9F\x98\x80");
    EXPECT_EQ(utf8_truncate_min(s, 4), "a"); // 😀 starts at 1, needs 4 bytes
}

// ---------------------------------------------------------------------------
// utf8_truncate_max
// ---------------------------------------------------------------------------

TEST(Utf8TruncateMax, EmptyPrefix) {
    // max_bytes=0 → valid prefix is empty → no incrementable codepoint.
    EXPECT_EQ(utf8_truncate_max("hello", 0), std::nullopt);
}

TEST(Utf8TruncateMax, AsciiIncrement) {
    // "abc..." truncated to 2 → prefix "ab", increment 'b' → 'c'.
    auto r = utf8_truncate_max("abcde", 2);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, "ac");
}

TEST(Utf8TruncateMax, LastAsciiIsMaxBeforeSurrogateSkip) {
    // U+D7FF is the last codepoint before the surrogate range.
    // Its UTF-8 encoding is 0xED 0x9F 0xBF.
    // next_cp = U+D800 (surrogate) → skip to U+E000.
    // U+E000 encodes to 0xEE 0x80 0x80.
    std::string s = "a\xED\x9F\xBF"
                    "extra";
    auto r = utf8_truncate_max(s, 4);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, std::string("a\xEE\x80\x80"));
}

TEST(Utf8TruncateMax, TwoByteCodpointIncrement) {
    // "aé" = "a" + U+00E9 (0xC3 0xA9), 3 bytes.
    // Increment U+00E9 → U+00EA (ê) = 0xC3 0xAA.
    std::string s = "a\xC3\xA9"
                    "extra";
    auto r = utf8_truncate_max(s, 3);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, std::string("a\xC3\xAA"));
}

TEST(Utf8TruncateMax, TwoByteCodpointOverflow) {
    // "aÿ" = "a" + U+00FF (0xC3 0xBF), 3 bytes.
    // Incrementing byte-by-byte (wrong) would give 0xC3 0xC0, which is
    // invalid UTF-8. The correct result increments U+00FF → U+0100 (Ā)
    // encoded as 0xC4 0x80.
    std::string s = "a\xC3\xBF"
                    "extra";
    auto r = utf8_truncate_max(s, 3);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, std::string("a\xC4\x80"));
}

TEST(Utf8TruncateMax, ThreeByteCodpointEncodingLengthChange) {
    // U+FFFF = 0xEF 0xBF 0xBF (3 bytes).
    // next_cp = U+10000 = 0xF0 0x90 0x80 0x80 (4 bytes).
    // The encoded next_cp is 1 byte longer, so the result (5 bytes) exceeds
    // max_bytes (4). This is intentional: the Iceberg spec bounds code-point
    // count, not byte length, and the result is still a valid upper bound
    // because the first differing byte (0xF0 > 0xEF) determines the order.
    std::string s = "a\xEF\xBF\xBF"
                    "extra";
    auto r = utf8_truncate_max(s, 4);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, std::string("a\xF0\x90\x80\x80"));
}

TEST(Utf8TruncateMax, FallbackToPreviousCodepoint) {
    // "a" + U+10FFFF (4 bytes), total 5 bytes.
    // U+10FFFF is the max codepoint; can't increment it.
    // Fall back to 'a' → 'b'.
    std::string s = "a\xF4\x8F\xBF\xBF"
                    "extra";
    auto r = utf8_truncate_max(s, 5);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, "b");
}

TEST(Utf8TruncateMax, AllMaxCodepoints) {
    // A string of only U+10FFFF codepoints: no valid upper bound exists.
    std::string s = "\xF4\x8F\xBF\xBF\xF4\x8F\xBF\xBF";
    EXPECT_EQ(utf8_truncate_max(s, 8), std::nullopt);
}

TEST(Utf8TruncateMax, UpperBoundIsStrictlyGreater) {
    // Verify the invariant: result > all strings with the truncated prefix.
    // prefix = "ab", result = "ac". Any string starting with "ab" is < "ac".
    auto r = utf8_truncate_max("abcde", 2);
    ASSERT_TRUE(r.has_value());
    std::string prefix(utf8_truncate_min("abcde", 2));
    EXPECT_GT(*r, prefix + std::string(100, '\xFF'));
}

TEST(Utf8TruncateMax, SingleMaxAscii) {
    // The prefix is a single 0x7F (DEL), the max 1-byte codepoint.
    // next_cp = U+0080, encoded as 0xC2 0x80.
    std::string s = "\x7F"
                    "extra";
    auto r = utf8_truncate_max(s, 1);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, std::string("\xC2\x80"));
}
