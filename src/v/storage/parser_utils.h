/*
 * Copyright 2020 Vectorized, Inc.
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
#include "model/record.h"

namespace storage::internal {

/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(model::record_batch&&);
/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(const model::record_batch&);

/// \brief batch compression
ss::future<model::record_batch>
compress_batch(model::compression, model::record_batch&&);
/// \brief batch compression
ss::future<model::record_batch>
compress_batch(model::compression, const model::record_batch&);

/// \brief resets the size, header crc and payload crc
void reset_size_checksum_metadata(model::record_batch_header&, const iobuf&);

} // namespace storage::internal
