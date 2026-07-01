/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "cloud_io/admission_control_types.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/prefetch/chunk_reassembler.h"
#include "container/chunked_vector.h"
#include "model/record.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <map>
#include <optional>

namespace cloud_topics::prefetch {

/// Issues concurrent byte-range GETs for one L1 object, buffers out-of-order
/// arrivals in a reorder map, and feeds the contiguous byte prefix to a
/// chunk_reassembler so whole record_batches are decoded in byte order.
///
/// Concurrency: the caller (the scheduler) decides how many dispatch() calls
/// are outstanding; this class does not cap in-flight count.
///
/// Error handling: a GET that fails is recorded as a poisoned range. Decoding
/// stops at the first poisoned position, and take_ready() throws the stored
/// error on the next call that would cross it.
class chunk_downloader {
public:
    /// \param start_position the byte position of the first chunk that will be
    ///        fed to the reassembler (the run start). When set, the decode
    ///        cursor is fixed to this value, so dispatches issued out of order
    ///        — including ones that precede the run-start chunk — cannot move
    ///        the cursor. When unset the cursor is inferred lazily as the
    ///        minimum dispatched position, which is only safe when the
    ///        run-start chunk is dispatched before any drain.
    chunk_downloader(
      l1::io* io,
      l1::object_id oid,
      cloud_io::group_id group,
      std::optional<std::reference_wrapper<ss::abort_source>> abort_source,
      std::optional<size_t> start_position = std::nullopt);

    chunk_downloader(const chunk_downloader&) = delete;
    chunk_downloader(chunk_downloader&&) = delete;
    chunk_downloader& operator=(const chunk_downloader&) = delete;
    chunk_downloader& operator=(chunk_downloader&&) = delete;

    ~chunk_downloader() = default;

    /// Issue a ranged GET for [position, position+size).
    ///
    /// The GET runs concurrently; when it completes the result is placed in
    /// the reorder buffer. Any contiguous prefix starting from the current
    /// decode cursor is fed to the reassembler and the decoded batches made
    /// available via take_ready().
    ss::future<> dispatch(size_t position, size_t size);

    /// Drain all whole batches decoded so far, in byte order.
    /// Throws if the decode cursor has hit a poisoned (errored) chunk.
    [[nodiscard]] chunked_vector<model::record_batch> take_ready();

    /// Bytes currently held as incomplete trailing batch in the reassembler.
    /// Exposed for testing: at a clean run boundary this must be zero.
    size_t reassembler_slack_bytes() const {
        return _reassembler.slack_bytes();
    }

    /// Number of GETs currently in flight (dispatched but not yet landed).
    size_t in_flight() const { return _in_flight; }

    /// Fetch the footer bytes [footer_pos, object_size) and parse them.
    ss::future<l1::footer> fetch_footer(size_t footer_pos, size_t object_size);

    /// Abort all in-flight downloads and wait for them to finish.
    ss::future<> close();

private:
    // Feed any contiguous chunks from _reorder_buf starting at _cursor into
    // the reassembler, advancing _cursor. Appends decoded batches to _ready.
    void _drain_reorder();

    l1::io* _io;
    l1::object_id _oid;
    cloud_io::group_id _group;
    std::optional<std::reference_wrapper<ss::abort_source>> _abort_source_opt;
    ss::abort_source _internal_abort;

    // Position of the next byte we expect to feed to the reassembler. When a
    // start_position is supplied to the constructor it is set here and locked
    // immediately, so out-of-order dispatch (including a chunk that precedes
    // the run start) cannot move it. Without an explicit start_position it is
    // initialized lazily to the minimum position seen across dispatch() calls,
    // which is only safe when the run-start chunk is dispatched before any
    // drain. Once the cursor starts advancing it must not go backwards; the
    // _cursor_locked flag prevents further downward updates.
    std::optional<size_t> _cursor;
    bool _cursor_locked{false};

    // Reorder buffer: position → downloaded iobuf.  Only landed chunks live
    // here; in-flight GETs are not represented.
    std::map<size_t, iobuf> _reorder_buf;

    // If a GET fails, the error is recorded here and all subsequent
    // take_ready() calls that would decode past _poison_pos throw.
    std::optional<size_t> _poison_pos;
    std::optional<std::exception_ptr> _poison_ex;

    chunk_reassembler _reassembler;
    chunked_vector<model::record_batch> _ready;

    size_t _in_flight{0};
    ss::gate _gate;
};

} // namespace cloud_topics::prefetch
