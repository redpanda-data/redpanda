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

#include "base/units.h"
#include "bytes/iobuf.h"
#include "random/generators.h"
#include "strings/utf8.h"

#include <seastar/testing/perf_tests.hh>

#include <string>
#include <string_view>

namespace {

constexpr size_t input_size = 16_KiB;

// Valid UTF-8 mixing 1-4 byte code points: "redpanda é 中 😀 ".
std::string make_valid_string() {
    constexpr std::string_view pattern
      = "redpanda \xC3\xA9 \xE4\xB8\xAD \xF0\x9F\x98\x80 ";
    std::string s;
    s.reserve(input_size);
    while (s.size() + pattern.size() <= input_size) {
        s.append(pattern);
    }
    s.append(input_size - s.size(), 'a');
    return s;
}

// Deterministic uniformly random bytes; a large share is ill-formed, so the
// rescue path replaces heavily.
std::string make_random_string() {
    std::string s;
    s.reserve(input_size);
    for (size_t i = 0; i < input_size; ++i) {
        s.push_back(static_cast<char>(random_generators::get_int<int>(0, 255)));
    }
    return s;
}

// Valid UTF-8 with 1% of bytes overwritten with 0xFF (never valid in UTF-8):
// the rescue path copies mostly-valid data with sparse replacements.
std::string make_one_percent_invalid_string() {
    std::string s = make_valid_string();
    for (size_t i = 0; i < s.size(); i += 100) {
        s[i] = '\xFF';
    }
    return s;
}

void sanitize_bench(iobuf input) {
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(utf8_sanitize(std::move(input)));
    perf_tests::stop_measuring_time();
}

} // namespace

PERF_TEST(utf8_sanitize, valid_16k) {
    sanitize_bench(iobuf::from(make_valid_string()));
}

// All-ASCII input, the common case for payloads like JSON: every byte is a
// single-byte code point, so this stays on the fast path without ever
// decoding a multi-byte sequence.
PERF_TEST(utf8_sanitize, ascii_16k) {
    sanitize_bench(
      iobuf::from(random_generators::gen_alphanum_string(input_size)));
}

PERF_TEST(utf8_sanitize, random_16k) {
    sanitize_bench(iobuf::from(make_random_string()));
}

PERF_TEST(utf8_sanitize, one_percent_invalid_16k) {
    sanitize_bench(iobuf::from(make_one_percent_invalid_string()));
}
