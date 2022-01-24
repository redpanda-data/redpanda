/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/manifest.h"
#include "cloud_storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/iostream.hh>

#include <absl/container/btree_map.h>

namespace cloud_storage {

/// This instance of this class is supposed to be used to
/// translate from redpanda offsets to kafka offsets in the
/// shadow-indexing and recovery contexts.
/// It consumes information stored in the manifest.
class offset_translator final {
public:
    offset_translator(model::offset initial_delta)
      : _initial_delta(initial_delta) {}

    /// Copy source stream into the destination stream
    ///
    /// Patch stream content by removing all non-data batches and adjusting the
    /// record batch offsets/checksums.
    /// The caller is responsible for patching the segement file name and
    /// passing correct base_offset of the original segment.
    ss::future<uint64_t> copy_stream(
      ss::input_stream<char> src,
      ss::output_stream<char> dst,
      retry_chain_node& fib) const;

    /// Get segment name adjusted for all removed offsets
    segment_name get_adjusted_segment_name(
      const manifest::key& s, retry_chain_node& fib) const;

private:
    model::offset _initial_delta;
};

} // namespace cloud_storage
