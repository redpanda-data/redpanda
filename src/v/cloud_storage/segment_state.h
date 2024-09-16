/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "model/fundamental.h"
#include "seastar/core/weak_ptr.hh"
#include "ssx/semaphore.h"
#include "storage/fwd.h"
#include "utils/retry_chain_node.h"

namespace cloud_storage {

class remote_segment;
class remote_partition;
struct materialized_segment_state;
class remote_segment_batch_reader;
class partition_probe;
class ts_read_path_probe;

/// State with materialized segment and cached reader
///
/// The object represent the state in which there is(or was) at
/// least one active reader that consumes data from the
/// remote segment.
struct materialized_segment_state {
    materialized_segment_state(
      const segment_meta& meta,
      const remote_segment_path& path,
      remote_partition& p,
      ssx::semaphore_units u);

    void return_reader(std::unique_ptr<remote_segment_batch_reader> reader);

    /// Borrow reader or make a new one.
    /// In either case return a reader.
    std::unique_ptr<remote_segment_batch_reader> borrow_reader(
      const storage::log_reader_config& cfg,
      retry_chain_logger& ctxlog,
      partition_probe& probe,
      ts_read_path_probe& ts_probe,
      segment_reader_units unit);

    ss::future<> stop();

    void offload(remote_partition* partition);

    const model::ntp& ntp() const;

    /// Base offsetof the segment
    model::offset base_rp_offset() const;

    ss::lw_shared_ptr<remote_segment> segment;
    /// Batch readers that can be used to scan the segment
    std::list<std::unique_ptr<remote_segment_batch_reader>> readers;
    /// Reader access time
    ss::lowres_clock::time_point atime;
    /// List hook for the list of all materalized segments
    intrusive_list_hook _hook;
    /// Set to true if the memory allocation overshot the threshold
    bool overcommit{false};

    /// Removes object from list that it is part of. Used to isolate the object
    /// before stopping it, so that the stop method is only called from one
    /// place.
    void unlink() { _hook.unlink(); }

    /// Record which partition this segment relates to.  This weak_ptr should
    /// never be broken, because our lifetime is shorter than our parent, but
    /// a weak_ptr is preferable to a reference (crash on bug) or a shared_ptr
    /// (prevent parent deallocation on bug).
    ss::weak_ptr<remote_partition> parent;

    /// Units belonging to `materialized_segments`, for managing how many
    /// segments may be concurrently materialized shard-wide
    ssx::semaphore_units _units;
};

} // namespace cloud_storage
