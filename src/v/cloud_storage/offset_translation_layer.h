/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "model/record_batch_types.h"
#include "storage/offset_translator_state.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/btree_map.h>

namespace cloud_storage {

struct stream_stats {
    model::offset min_offset = model::offset::max();
    model::offset max_offset = model::offset::min();
    uint64_t size_bytes{};
};

/// This instance of this class is supposed to be used to
/// translate from redpanda offsets to kafka offsets in the
/// shadow-indexing and recovery contexts.
/// It consumes information stored in the manifest.
class offset_translator final {
public:
    offset_translator(
      model::offset_delta initial_delta,
      ss::lw_shared_ptr<storage::offset_translator_state> ot_state,
      storage::opt_abort_source_t as = std::nullopt)
      : _initial_delta(initial_delta)
      , _ot_state(ot_state)
      , _as(as) {}

    /// Copy source stream into the destination stream
    ///
    /// Patch stream content by removing all non-data batches and adjusting the
    /// record batch offsets/checksums.
    /// The caller is responsible for patching the segement file name and
    /// passing correct base_offset of the original segment.
    ss::future<stream_stats> copy_stream(
      ss::input_stream<char> src,
      ss::output_stream<char> dst,
      retry_chain_node& fib) const;

private:
    model::offset_delta _initial_delta;
    ss::lw_shared_ptr<storage::offset_translator_state> _ot_state;
    storage::opt_abort_source_t _as;
};

} // namespace cloud_storage
