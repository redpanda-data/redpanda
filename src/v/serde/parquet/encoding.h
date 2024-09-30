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

#include "serde/parquet/value.h"
#include "src/v/serde/parquet/value.h"

namespace serde::parquet {

iobuf encode_plain(const chunked_vector<boolean_value>& vals);

iobuf encode_plain(const chunked_vector<int32_value>& vals);

iobuf encode_plain(const chunked_vector<int64_value>& vals);

iobuf encode_plain(const chunked_vector<float32_value>& vals);

iobuf encode_plain(const chunked_vector<float64_value>& vals);

iobuf encode_plain(chunked_vector<byte_array_value> vals);

iobuf encode_plain(chunked_vector<fixed_byte_array_value> vals);

} // namespace serde::parquet
