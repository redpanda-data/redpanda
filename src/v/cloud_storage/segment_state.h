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

#include "model/fundamental.h"
#include "seastar/core/weak_ptr.hh"
#include "storage/fwd.h"

class retry_chain_logger;

namespace cloud_storage {

class remote_segment;
class remote_partition;
struct materialized_segment_state;
class remote_segment_batch_reader;
class partition_probe;

/// State that have to be materialized before use
struct offloaded_segment_state {
    explicit offloaded_segment_state(model::offset bo);

    std::unique_ptr<materialized_segment_state>
    materialize(remote_partition& p, kafka::offset offset_key);

    ss::future<> stop();

    offloaded_segment_state offload(remote_partition*);

    model::offset base_rp_offset;

    offloaded_segment_state* operator->() { return this; }

    const offloaded_segment_state* operator->() const { return this; }
};

/// State with materialized segment and cached reader
///
/// The object represent the state in which there is(or was) at
/// least one active reader that consumes data from the
/// remote segment.
struct materialized_segment_state {
    materialized_segment_state(
      model::offset bo, kafka::offset offk, remote_partition& p);

    void return_reader(std::unique_ptr<remote_segment_batch_reader> reader);

    /// Borrow reader or make a new one.
    /// In either case return a reader.
    std::unique_ptr<remote_segment_batch_reader> borrow_reader(
      const storage::log_reader_config& cfg,
      retry_chain_logger& ctxlog,
      partition_probe& probe);

    ss::future<> stop();

    offloaded_segment_state offload(remote_partition* partition);

    const model::ntp& ntp() const;

    /// Base offsetof the segment
    model::offset base_rp_offset;
    /// Key of the segment in _segments collection of the remote_partition
    kafka::offset offset_key;
    ss::lw_shared_ptr<remote_segment> segment;
    /// Batch readers that can be used to scan the segment
    std::list<std::unique_ptr<remote_segment_batch_reader>> readers;
    /// Reader access time
    ss::lowres_clock::time_point atime;
    /// List hook for the list of all materalized segments
    intrusive_list_hook _hook;

    /// Record which partition this segment relates to.  This weak_ptr should
    /// never be broken, because our lifetime is shorter than our parent, but
    /// a weak_ptr is preferable to a reference (crash on bug) or a shared_ptr
    /// (prevent parent deallocation on bug).
    ss::weak_ptr<remote_partition> parent;
};

} // namespace cloud_storage