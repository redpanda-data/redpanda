/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf_parser.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <climits>

namespace serde::parquet {

// This is the plain encoding that must be supported for types. It is intended
// to be the simplest encoding. Values are encoded back to back.
//
// See:
// https://parquet.apache.org/docs/file-format/data-pages/encodings/#plain-plain--0
template<typename value_type>
class plain_encoder;

template<>
class plain_encoder<boolean_value> {
public:
    void add_value(boolean_value v);
    iobuf get_encoded_buf();
    size_t size_bytes() const;

private:
    iobuf buf;
    uint8_t bits{0};
    uint8_t shift{0};
};

template<typename value_type>
class numeric_plain_encoder {
public:
    void add_value(value_type);
    iobuf get_encoded_buf();
    size_t size_bytes() const;

private:
    iobuf buf;
};

template<>
class plain_encoder<int32_value> : public numeric_plain_encoder<int32_value> {};

template<>
class plain_encoder<int64_value> : public numeric_plain_encoder<int64_value> {};

template<>
class plain_encoder<float32_value>
  : public numeric_plain_encoder<float32_value> {};

template<>
class plain_encoder<float64_value>
  : public numeric_plain_encoder<float64_value> {};

template<>
class plain_encoder<byte_array_value> {
public:
    void add_value(byte_array_value&&);
    iobuf get_encoded_buf();
    size_t size_bytes() const;

private:
    iobuf buf;
};

template<>
class plain_encoder<fixed_byte_array_value> {
public:
    void add_value(fixed_byte_array_value&&);
    iobuf get_encoded_buf();
    size_t size_bytes() const;

private:
    iobuf buf;
};

// Levels (definition and repetition) are encoded using Parquet's hybrid
// run-length encoding/bitpacking schema. Bit packing requires the maximum
// value to be known in advance.
//
// See:
// https://parquet.apache.org/docs/file-format/nestedencoding/
// https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3
//
// If `levels` is empty `max_value` should be `0`.
iobuf encode_levels(
  rep_level max_value, const chunked_vector<rep_level>& levels);
iobuf encode_levels(
  def_level max_value, const chunked_vector<def_level>& levels);

// Stats are encoded using plain encoding, except variable length arrays
// which don't have a length prefix.
iobuf encode_for_stats(boolean_value);
iobuf encode_for_stats(int32_value);
iobuf encode_for_stats(int64_value);
iobuf encode_for_stats(float32_value);
iobuf encode_for_stats(float64_value);
iobuf encode_for_stats(const byte_array_value&);
iobuf encode_for_stats(const fixed_byte_array_value&);

// Decode stats-encoded values (inverse of encode_for_stats).
// For byte arrays, the entire iobuf is the value (no length prefix).
boolean_value decode_stats_boolean(const iobuf& data);
int32_value decode_stats_int32(const iobuf& data);
int64_value decode_stats_int64(const iobuf& data);
float32_value decode_stats_float32(const iobuf& data);
float64_value decode_stats_float64(const iobuf& data);
byte_array_value decode_stats_byte_array(const iobuf& data);

// Decode RLE/bitpack hybrid encoded levels. Handles both RLE runs (from
// our own encoder) and bitpack runs (from external writers like Arrow).
//
// `byte_length` is the number of encoded bytes (from page header V2).
// `max_value` provides the bit width for decoding.
chunked_vector<rep_level> decode_levels(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  rep_level max_value);

chunked_vector<def_level> decode_levels(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  def_level max_value);

// Decode RLE/bitpack hybrid encoded int32 values. Same wire format as
// definition/repetition levels. Used for dictionary indices.
//
// `bit_width` is the number of bits per value (provided by the caller,
// not derived from a max_value).
// `byte_length` is the total encoded byte count.
chunked_vector<int32_t> decode_rle_bp_int32(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  int32_t bit_width);

// Decode PLAIN encoded values.
template<typename value_type>
class plain_decoder;

template<>
class plain_decoder<boolean_value> {
public:
    explicit plain_decoder(iobuf_parser_base& parser);
    boolean_value read_value();

private:
    iobuf_parser_base& _parser;
    uint8_t _bits{0};
    uint8_t _shift{CHAR_BIT}; // start exhausted so first read fetches a byte
};

template<typename value_type>
class numeric_plain_decoder {
public:
    explicit numeric_plain_decoder(iobuf_parser_base& parser);
    value_type read_value();

private:
    iobuf_parser_base& _parser;
};

template<>
class plain_decoder<int32_value> : public numeric_plain_decoder<int32_value> {
    using numeric_plain_decoder::numeric_plain_decoder;
};

template<>
class plain_decoder<int64_value> : public numeric_plain_decoder<int64_value> {
    using numeric_plain_decoder::numeric_plain_decoder;
};

template<>
class plain_decoder<float32_value>
  : public numeric_plain_decoder<float32_value> {
    using numeric_plain_decoder::numeric_plain_decoder;
};

template<>
class plain_decoder<float64_value>
  : public numeric_plain_decoder<float64_value> {
    using numeric_plain_decoder::numeric_plain_decoder;
};

template<>
class plain_decoder<byte_array_value> {
public:
    explicit plain_decoder(iobuf_parser_base& parser);
    byte_array_value read_value();

private:
    iobuf_parser_base& _parser;
};

template<>
class plain_decoder<fixed_byte_array_value> {
public:
    plain_decoder(iobuf_parser_base& parser, int32_t fixed_length);
    fixed_byte_array_value read_value();

private:
    iobuf_parser_base& _parser;
    int32_t _fixed_length;
};

} // namespace serde::parquet
