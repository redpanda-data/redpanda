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

#pragma once

#include "absl/time/time.h"
#include "serde/json/parser.h"
#include "serde/protobuf/field_mask.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace serde::pb::json {

// A serde::json::parser that can be peeked at without consuming the next token.
//
// This is needed for array generators that need to see if they are at the end
// of the array before continuing to parse the next element.
//
// Note that you can't peek and then consume the previous value, this is only
// peeking around the token management.
class peekable_parser {
public:
    explicit peekable_parser(
      iobuf&& buf, serde::json::parser_config config = {});
    ss::future<serde::json::token> peek();
    ss::future<bool> next();
    serde::json::token token() const { return _parser.token(); }
    int64_t value_int() { return _parser.value_int(); }
    double value_double() { return _parser.value_double(); }
    iobuf value_string() { return _parser.value_string(); }
    ss::future<> skip_value() { return _parser.skip_value(); }

private:
    serde::json::parser _parser;
    std::optional<serde::json::token> _peeked_token;
};

// A generator that yields keys from a JSON object.
//
// Use the parser to access the value.
class object_key_generator {
public:
    explicit object_key_generator(peekable_parser* parser)
      : _parser(parser) {}

    ss::future<std::optional<iobuf>> operator()();

private:
    bool _begin = true;
    peekable_parser* _parser;
};

// A generator that yields elements from a JSON array.
//
// Use the parser to access the element.
class array_element_generator {
public:
    explicit array_element_generator(peekable_parser* parser)
      : _parser(parser) {}

    ss::future<bool> operator()();

private:
    bool _begin = true;
    peekable_parser* _parser;
};

// Throw an exception if next is not EOF
ss::future<> check_next_eof(peekable_parser* parser);

// Throw an exception if the parser is in an error state
void check_error(peekable_parser* parser);

// A helper to transform a map key into a structured type.
template<typename T>
T transform_map_key(iobuf key_string);
template<>
bool transform_map_key(iobuf key_string);
template<>
int32_t transform_map_key(iobuf key_string);
template<>
int64_t transform_map_key(iobuf key_string);
template<>
uint32_t transform_map_key(iobuf key_string);
template<>
uint64_t transform_map_key(iobuf key_string);
template<>
float transform_map_key(iobuf key_string);
template<>
double transform_map_key(iobuf key_string);
template<>
ss::sstring transform_map_key(iobuf key_string);

// Helpers to read scalar values from the parser.
bool read_bool(peekable_parser* parser);
int32_t read_int32(peekable_parser* parser);
int64_t read_int64(peekable_parser* parser);
uint32_t read_uint32(peekable_parser* parser);
uint64_t read_uint64(peekable_parser* parser);
float read_float(peekable_parser* parser);
double read_double(peekable_parser* parser);
ss::sstring read_string(peekable_parser* parser);
iobuf read_string_as_bytes(peekable_parser* parser);
iobuf read_base64_encoded_bytes(peekable_parser* parser);

// Well known protos have special representation in the JSON format.
//
// To learn more about each JSON representation, see the comments in the proto
// files.
namespace wellknown {

ss::future<absl::Duration> duration_from_json(peekable_parser* parser);
iobuf duration_to_json(absl::Duration);

ss::future<field_mask> field_mask_from_json(peekable_parser* parser);
iobuf field_mask_to_json(const field_mask&);

ss::future<absl::Time> timestamp_from_json(peekable_parser* parser);
iobuf timestamp_to_json(absl::Time);

} // namespace wellknown

} // namespace serde::pb::json
