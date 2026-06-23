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

#include "base/vassert.h"
#include "strings/utf8.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <string>
#include <string_view>

// Build an iobuf with distinct fragments to exercise state-machine boundary
// handling in is_valid_utf8 and the slow-path rescue.
static iobuf
make_fragmented_iobuf(std::initializer_list<std::string_view> parts) {
    iobuf buf;
    for (auto part : parts) {
        // append_fragments preserves fragment boundaries; a plain append
        // would copy-pack small buffers into a single fragment.
        buf.append_fragments(iobuf::from(part));
    }
    auto n_frags = std::distance(buf.begin(), buf.end());
    vassert(
      n_frags == static_cast<std::ptrdiff_t>(parts.size()),
      "expected {} fragments, got {}",
      parts.size(),
      n_frags);
    return buf;
}

static std::string iobuf_to_string(iobuf buf) {
    std::string s;
    s.reserve(buf.size_bytes());
    for (const auto& frag : buf) {
        s.append(frag.get(), frag.size());
    }
    return s;
}

// ---------------------------------------------------------------------------
// Valid inputs — fast path, zero copy
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, ValidEmpty) {
    auto r = utf8_sanitize(iobuf::from(""));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "");
}

TEST(Utf8Sanitize, ValidAscii) {
    auto r = utf8_sanitize(iobuf::from("hello world"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "hello world");
}

TEST(Utf8Sanitize, Valid2Byte) {
    // U+00E9 LATIN SMALL LETTER E WITH ACUTE: C3 A9
    auto r = utf8_sanitize(iobuf::from("\xC3\xA9"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xC3\xA9");
}

TEST(Utf8Sanitize, ValidFragmentSplitMultiByte) {
    // C3 | A9 — pending=1 carries into second fragment
    auto r = utf8_sanitize(make_fragmented_iobuf({"\xC3", "\xA9"}));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xC3\xA9");
}

// ---------------------------------------------------------------------------
// Invalid inputs — slow path replacement (advance-1-on-error)
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, InvalidBareContinuation) {
    // 0x80 alone — one U+FFFD
    auto r = utf8_sanitize(iobuf::from("\x80"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidTruncated3Byte) {
    // E4 B8 at end of string — two U+FFFD (one per ill-formed byte)
    auto r = utf8_sanitize(iobuf::from("\xE4\xB8"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidTruncated4Byte) {
    // F0 9F 98 at end of string — three U+FFFD (one per ill-formed byte)
    auto r = utf8_sanitize(iobuf::from("\xF0\x9F\x98"));
    EXPECT_EQ(
      iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidTruncated4ByteThenAscii) {
    // F0 9F followed by an ASCII byte — the aborted sequence yields one
    // U+FFFD per consumed byte and the ASCII byte is preserved.
    auto r = utf8_sanitize(
      iobuf::from(
        "\xF0\x9F"
        "A"));
    EXPECT_EQ(
      iobuf_to_string(std::move(r)),
      "\xEF\xBF\xBD\xEF\xBF\xBD"
      "A");
}

TEST(Utf8Sanitize, InvalidTruncated3ByteThenValid2Byte) {
    // E1 80 (aborted 3-byte sequence) followed by C2 80 (valid U+0080) —
    // one U+FFFD per consumed byte of the aborted sequence, then the valid
    // sequence is preserved. Pins advance-1-on-error semantics: the
    // maximal-subpart strategy would emit a single U+FFFD for E1 80.
    auto r = utf8_sanitize(iobuf::from("\xE1\x80\xC2\x80"));
    EXPECT_EQ(
      iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xC2\x80");
}

TEST(Utf8Sanitize, InvalidSurrogate) {
    // U+D800 encoded as UTF-8: ED A0 80 — three U+FFFD.
    // The lead byte ED A0 is rejected as a surrogate; each subsequent
    // continuation byte is then a bare continuation.
    auto r = utf8_sanitize(iobuf::from("\xED\xA0\x80"));
    EXPECT_EQ(
      iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidMixed) {
    // Valid prefix + bare continuation + valid suffix
    auto r = utf8_sanitize(iobuf::from("hello\x80world"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "hello\xEF\xBF\xBDworld");
}

TEST(Utf8Sanitize, InvalidContinuationMidSequence) {
    // C3 expects one continuation, but gets another lead byte — two U+FFFD.
    auto r = utf8_sanitize(iobuf::from("\xC3\xC3"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidOverlong2Byte) {
    // C0 80 — overlong encoding of U+0000, two U+FFFD.
    auto r = utf8_sanitize(iobuf::from("\xC0\x80"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidOverlong3Byte) {
    // E0 9F 80 — overlong (first continuation 0x9F < 0xA0), three U+FFFD.
    auto r = utf8_sanitize(iobuf::from("\xE0\x9F\x80"));
    EXPECT_EQ(
      iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidOutOfRangeLead) {
    // 0xF5 and above are not valid UTF-8 lead bytes (would exceed U+10FFFF).
    auto r = utf8_sanitize(iobuf::from("\xF5"));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD");
}

// ---------------------------------------------------------------------------
// Invalid inputs with fragment boundaries — slow path carry handling
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, InvalidSurrogateFragmentSplit) {
    // ED | A0 80 — surrogate split across fragment boundary; verifies that
    // max_cont=0x9F carries over from the first fragment.
    auto r = utf8_sanitize(make_fragmented_iobuf({"\xED", "\xA0\x80"}));
    EXPECT_EQ(
      iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD");
}

TEST(Utf8Sanitize, InvalidThenFragmentSplitValid) {
    // 80 C3 | A9 — the slow path commits the C3 A9 sequence spanning the
    // fragment boundary.
    auto r = utf8_sanitize(make_fragmented_iobuf({"\x80\xC3", "\xA9"}));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xC3\xA9");
}

TEST(Utf8Sanitize, InvalidThenValid4ByteAcross3Fragments) {
    // 80 F0 | 9F | 98 80 — a 4-byte sequence spanning three fragments is
    // carried and committed by the slow path.
    auto r = utf8_sanitize(
      make_fragmented_iobuf({"\x80\xF0", "\x9F", "\x98\x80"}));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xF0\x9F\x98\x80");
}

TEST(Utf8Sanitize, InvalidTruncatedFragmentSplit) {
    // E4 | B8 — sequence split across fragments and never completed: one
    // U+FFFD per byte.
    auto r = utf8_sanitize(make_fragmented_iobuf({"\xE4", "\xB8"}));
    EXPECT_EQ(iobuf_to_string(std::move(r)), "\xEF\xBF\xBD\xEF\xBF\xBD");
}

// ---------------------------------------------------------------------------
// Large inputs — the slow path does not linearize, so size is unrestricted
// ---------------------------------------------------------------------------

TEST(Utf8Sanitize, LargeValid) {
    std::string big(iobuf::max_linearize_size + 1, 'a');
    auto r = utf8_sanitize(iobuf::from(big));
    EXPECT_EQ(r.size_bytes(), big.size());
}

TEST(Utf8Sanitize, LargeInvalid) {
    // Larger than the linearize limit; each 0x80 → 3-byte U+FFFD.
    std::string big(iobuf::max_linearize_size + 1, '\x80');
    auto r = utf8_sanitize(iobuf::from(big));
    EXPECT_EQ(r.size_bytes(), big.size() * 3);
}
