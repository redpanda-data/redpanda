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

#pragma once

#include "serde/parquet/column_array.h"
#include "serde/parquet/metadata.h"

#include <seastar/core/future.hh>

namespace serde::parquet {

/// Decoded column data from a single column chunk.
struct column_chunk_data {
    column_array values;
    chunked_vector<def_level> def_levels;
    chunked_vector<rep_level> rep_levels;
};

/// \brief Decode all pages in a column chunk from serialized bytes.
///
/// Handles Data Page V1, V2, and dictionary pages. Decompresses if needed
/// (async for ZSTD). Supports PLAIN and RLE_DICTIONARY data encodings.
/// The input `data` should contain exactly the bytes for this column chunk
/// (total_compressed_size bytes starting at dictionary_page_offset or
/// data_page_offset).
ss::future<column_chunk_data> decode_column_chunk(
  iobuf data, const column_meta_data& meta, const schema_element& schema_elem);

} // namespace serde::parquet
