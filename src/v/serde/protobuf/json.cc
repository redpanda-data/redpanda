/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "serde/protobuf/json.h"

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "base/units.h"
#include "serde/json/writer.h"
#include "serde/protobuf/field_mask.h"
#include "utils/base64.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <algorithm>

namespace serde::pb::json {

using serde::json::token;

peekable_parser::peekable_parser(iobuf&& buf, serde::json::parser_config config)
  : _parser(std::move(buf), config) {}

ss::future<token> peekable_parser::peek() {
    if (_peeked_token.has_value()) {
        co_return _peeked_token.value();
    }
    if (co_await _parser.next()) {
        _peeked_token = _parser.token();
    }
    co_return _parser.token();
}

ss::future<bool> peekable_parser::next() {
    if (_peeked_token) {
        _peeked_token.reset();
        co_return true;
    }
    co_return co_await _parser.next();
}

namespace {

ss::future<> expect_token(peekable_parser* parser, token expected) {
    co_await parser->next();
    if (parser->token() != expected) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected {}, got: {}", expected, parser->token()));
    }
}

} // namespace

ss::future<std::optional<iobuf>> object_key_generator::operator()() {
    if (_begin) {
        _begin = false;
        co_await expect_token(_parser, token::start_object);
    }
    co_await _parser->next();
    if (_parser->token() == token::end_object) {
        co_return std::nullopt;
    } else if (_parser->token() != token::key) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected object key, got: {}", _parser->token()));
    }
    co_return _parser->value_string();
}

ss::future<bool> array_element_generator::operator()() {
    if (_begin) {
        _begin = false;
        co_await expect_token(_parser, token::start_array);
    }
    if (co_await _parser->peek() == token::end_array) {
        co_await _parser->next();
        co_return false;
    }
    co_return true;
}

ss::future<> check_next_eof(peekable_parser* parser) {
    auto has_next = co_await parser->next();
    if (!has_next) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("next token, parser state: {}", parser->token()));
    }
    if (parser->token() != token::eof) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected eof, got: {}", parser->token()));
    }
}

void check_error(peekable_parser* parser) {
    if (parser->token() == token::error) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected no error, got: {}", parser->token()));
    }
}

namespace {
constexpr static size_t max_numeric_string_size = 128;

ss::sstring linearize_iobuf(iobuf b) {
    constexpr static size_t max_allocation_size = 128_KiB;
    if (b.size_bytes() > max_allocation_size) {
        throw std::runtime_error(fmt::format(
          "string too big: {} > {}", b.size_bytes(), max_allocation_size));
    }
    ss::sstring result{ss::sstring::initialized_later{}, b.size_bytes()};
    char* dest = result.data();
    for (const auto& frag : b) {
        dest = std::copy_n(frag.get(), frag.size(), dest);
    }
    return result;
}

template<typename T>
T iobuf_to_number(iobuf b) {
    if (b.size_bytes() > max_numeric_string_size) {
        throw std::runtime_error(fmt::format(
          "number too big: {} > {}", b.size_bytes(), max_numeric_string_size));
    }
    auto str = linearize_iobuf(std::move(b));
    T result{};
    if constexpr (std::is_floating_point_v<T>) {
        bool ok{};
        if constexpr (std::is_same_v<T, float>) {
            ok = absl::SimpleAtof(str, &result);
        } else {
            ok = absl::SimpleAtod(str, &result);
        }
        if (!ok) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("failed to parse number from string: {}", str));
        }
    } else {
        if (!absl::SimpleAtoi<T>(str, &result)) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("failed to parse number from string: {}", str));
        }
    }
    return result;
}

template<typename T>
T read_number(peekable_parser* parser) {
    if (parser->token() == token::value_string) {
        return iobuf_to_number<T>(parser->value_string());
    }
    if (parser->token() == token::value_double) {
        return static_cast<T>(parser->value_double());
    }
    if (parser->token() != token::value_int) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected number, got: {}", parser->token()));
    }
    return static_cast<T>(parser->value_int());
}

} // namespace

template<>
bool transform_map_key(iobuf key_string) {
    if (key_string == "true") {
        return true;
    } else if (key_string == "false") {
        return false;
    } else {
        constexpr size_t hexdump_size = 32;
        throw std::runtime_error(fmt::format(
          "expected boolean, got: {}", key_string.hexdump(hexdump_size)));
    }
}

template<>
int32_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<int32_t>(std::move(key_string));
}

template<>
int64_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<int64_t>(std::move(key_string));
}

template<>
uint32_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<uint32_t>(std::move(key_string));
}

template<>
uint64_t transform_map_key(iobuf key_string) {
    return iobuf_to_number<uint64_t>(std::move(key_string));
}

template<>
float transform_map_key(iobuf key_string) {
    return iobuf_to_number<float>(std::move(key_string));
}

template<>
double transform_map_key(iobuf key_string) {
    return iobuf_to_number<double>(std::move(key_string));
}

template<>
ss::sstring transform_map_key(iobuf key_string) {
    return linearize_iobuf(std::move(key_string));
}

bool read_bool(peekable_parser* parser) {
    switch (parser->token()) {
    case json::token::value_true:
        return true;
    case json::token::value_false:
        return false;
    default:
        throw std::runtime_error(
          fmt::format("expected boolean, got: {}", parser->token()));
    }
}
int32_t read_int32(peekable_parser* parser) {
    return read_number<int32_t>(parser);
}
int64_t read_int64(peekable_parser* parser) {
    return read_number<int64_t>(parser);
}
uint32_t read_uint32(peekable_parser* parser) {
    return read_number<uint32_t>(parser);
}
uint64_t read_uint64(peekable_parser* parser) {
    return read_number<uint64_t>(parser);
}
float read_float(peekable_parser* parser) { return read_number<float>(parser); }
double read_double(peekable_parser* parser) {
    return read_number<double>(parser);
}
ss::sstring read_string(peekable_parser* parser) {
    if (parser->token() != token::value_string) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected string, got: {}", parser->token()));
    }
    return linearize_iobuf(parser->value_string());
}
iobuf read_string_as_bytes(peekable_parser* parser) {
    if (parser->token() != token::value_string) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected string, got: {}", parser->token()));
    }
    return parser->value_string();
}

iobuf read_base64_encoded_bytes(peekable_parser* parser) {
    if (parser->token() != token::value_string) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("expected string, got: {}", parser->token()));
    }
    return base64_to_iobuf(parser->value_string());
}

namespace wellknown {

ss::future<absl::Duration> duration_from_json(peekable_parser* parser) {
    co_await parser->next();
    if (parser->token() == token::value_null) {
        co_return absl::ZeroDuration();
    }
    ss::sstring encoded = read_string(parser);
    if (encoded.empty()) {
        co_return absl::ZeroDuration();
    }
    std::string_view str(encoded);
    size_t int_part_end = 0;
    for (char c : str) {
        if (!absl::ascii_isdigit(c) && c != '-') {
            break;
        }
        ++int_part_end;
    }
    if (int_part_end == 0) {
        throw std::runtime_error(
          fmt::format("invalid duration string: {}", encoded));
    }
    int64_t secs = 0;
    if (!absl::SimpleAtoi(str.substr(0, int_part_end), &secs)) {
        throw std::runtime_error(
          fmt::format("invalid duration string: {}", str));
    }
    constexpr int64_t kMaxSeconds = int64_t{3652500} * 86400;
    if (secs > kMaxSeconds || secs < -kMaxSeconds) {
        throw std::runtime_error(fmt::format("duration out of range: {}", str));
    }
    absl::string_view rest = str.substr(int_part_end);
    int32_t frac_secs = 0;
    size_t frac_digits = 0;
    if (absl::StartsWith(rest, ".")) {
        for (char c : rest.substr(1)) {
            if (!absl::ascii_isdigit(c)) {
                break;
            }
            ++frac_digits;
        }
        auto digits = rest.substr(1, frac_digits);
        if (
          frac_digits == 0 || frac_digits > 9
          || !absl::SimpleAtoi(digits, &frac_secs)) {
            throw std::runtime_error(
              fmt::format("invalid duration string: {}", str));
        }
        rest = rest.substr(frac_digits + 1);
    }
    for (size_t i = 0; i < (9 - frac_digits); ++i) {
        frac_secs *= 10;
    }
    bool is_negative = (secs < 0) || absl::StartsWith(str, "-");
    if (is_negative) {
        frac_secs *= -1;
    }
    if (rest != "s") {
        throw std::runtime_error(
          fmt::format("invalid duration string: {}", str));
    }
    co_return absl::Seconds(secs) + absl::Nanoseconds(frac_secs);
}

iobuf duration_to_json(absl::Duration d) {
    int64_t secs = absl::ToInt64Seconds(d);
    auto nanos = static_cast<int32_t>(
      absl::ToInt64Nanoseconds(d % absl::Seconds(1)));
    if (nanos == 0) {
        return iobuf::from(absl::StrFormat(R"("%ds")", secs));
    }
    size_t digits = 9;
    uint32_t frac_seconds = std::abs(nanos);
    while (frac_seconds % 1000 == 0) {
        frac_seconds /= 1000;
        digits -= 3;
    }
    absl::string_view sign = ((secs < 0) || (nanos < 0)) ? "-" : "";
    return iobuf::from(absl::StrFormat(
      R"("%s%d.%.*ds")", sign, std::abs(secs), digits, frac_seconds));
}

namespace {
void proto_name_to_json_name(std::string_view s, iobuf* b) {
    bool was_underscore = false;
    for (char c : s) {
        if (c != '_') {
            if (was_underscore && absl::ascii_islower(c)) {
                c = absl::ascii_toupper(c);
            }
            b->append(&c, 1);
        }
        was_underscore = c == '_';
    }
}

void json_name_to_proto_name(std::string_view s, std::string* b) {
    for (char c : s) {
        if (absl::ascii_isupper(c)) {
            b->push_back('_');
            c = absl::ascii_tolower(c);
        }
        b->push_back(c);
    }
}
} // namespace

ss::future<field_mask> field_mask_from_json(peekable_parser* parser) {
    co_await parser->next();
    field_mask mask;
    if (parser->token() == token::value_null) {
        co_return mask;
    }
    ss::sstring encoded = read_string(parser);
    if (encoded.empty()) {
        co_return mask;
    }
    std::string proto_name;
    for (std::string_view json_name :
         absl::StrSplit(std::string_view(encoded), ',')) {
        if (json_name.empty()) {
            throw std::runtime_error(
              fmt::format("empty field mask path in: {}", encoded));
        }
        proto_name.clear();
        json_name_to_proto_name(json_name, &proto_name);
        mask.paths.emplace_back(std::string_view(proto_name));
    }
    co_return mask;
}

iobuf field_mask_to_json(const field_mask& mask) {
    iobuf encoded;
    for (const auto& path : mask.paths) {
        if (!encoded.empty()) {
            encoded.append(std::to_array({','}));
        }
        proto_name_to_json_name(path, &encoded);
    }
    serde::json::writer w;
    w.string(encoded);
    return std::move(w).finish();
}

ss::future<absl::Time> timestamp_from_json(peekable_parser* parser) {
    co_await parser->next();
    field_mask mask;
    if (parser->token() == token::value_null) {
        co_return absl::UnixEpoch();
    }
    ss::sstring encoded = read_string(parser);
    if (encoded.empty()) {
        co_return absl::UnixEpoch();
    }
    absl::Time t;
    std::string err;
    constexpr static std::string_view second_resolution_format_spec
      = "%E4Y-%m-%d%ET%H:%M:%S%Ez";
    if (absl::ParseTime(second_resolution_format_spec, encoded, &t, &err)) {
        co_return t;
    }
    constexpr static std::string_view full_subsecond_format_spec
      = "%E4Y-%m-%d%ET%H:%M:%E9S%Ez";
    if (absl::ParseTime(full_subsecond_format_spec, encoded, &t, &err)) {
        co_return t;
    }
    throw std::runtime_error(
      fmt::format("invalid timestamp string: {}: {}", encoded, err));
}

iobuf timestamp_to_json(absl::Time t) {
    serde::json::writer w;
    constexpr static std::string_view format_spec = "%E4Y-%m-%d%ET%H:%M:%E9SZ";
    w.string(absl::FormatTime(format_spec, t, absl::UTCTimeZone()));
    return std::move(w).finish();
}

} // namespace wellknown

} // namespace serde::pb::json
