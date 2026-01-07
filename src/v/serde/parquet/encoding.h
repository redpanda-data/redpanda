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

#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

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
iobuf encode_for_stats(byte_array_value&);
iobuf encode_for_stats(fixed_byte_array_value&);

} // namespace serde::parquet
