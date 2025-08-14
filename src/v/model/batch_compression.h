// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/compression.h"
#include "model/record.h"

#include <seastar/core/future.hh>

namespace model {

/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(model::record_batch&&);

/// \brief batch decompression
ss::future<model::record_batch> decompress_batch(const model::record_batch&);

/// \brief synchronous batch decompression
model::record_batch decompress_batch_sync(model::record_batch&&);

/// \brief synchronous batch decompression
/// \throw std::runtime_error If provided batch is not compressed
model::record_batch maybe_decompress_batch_sync(const model::record_batch&);

/// \brief batch compression
ss::future<model::record_batch>
  compress_batch(model::compression, model::record_batch);

} // namespace model
