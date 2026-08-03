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

#include "base/seastarx.h"
#include "cluster_link/fwd.h"
#include "cluster_link/schema_registry_sync/source_reader.h"
#include "container/chunked_hash_map.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <cstdint>
#include <memory>

namespace cluster_link::schema_registry_sync {

/// Whether incremental tailing can run against this link's source.
enum class tail_availability : uint8_t {
    /// The reader is positioned and `poll` will report changes.
    available,
    /// This source cannot be tailed (e.g. it does not expose `_schemas` over
    /// the Kafka API); the link falls back to full syncs alone.
    unavailable,
};

/// \brief The refreshes implied by a batch of source Schema Registry changes.
///
/// A tail reader reports *which* targets changed, never the change itself: the
/// caller re-reads each target from the source and runs the same per-subject
/// sync a full sync would. That keeps tail and full sync semantically identical
/// and makes replay harmless, so at-least-once delivery is sufficient.
///
/// Targets are in the source-context namespace, like everything else before the
/// import boundary.
struct tail_batch {
    /// Subjects whose version set may have changed.
    chunked_hash_set<ppsr::context_subject> subjects;
    /// Targets whose mode or compatibility config may have changed: a subject,
    /// a context-level target (empty subject), or the registry-wide global
    /// target.
    chunked_hash_set<ppsr::context_subject> mode_configs;
    /// Contexts whose existence may have changed. A trigger only: which
    /// contexts to delete is decided against the source's whole context list,
    /// never against this set, because a batch names only what changed.
    chunked_hash_set<ppsr::context> contexts;
    /// Set when a per-poll budget cut the batch short and the source already
    /// has more to report. The next poll picks up where this one stopped.
    bool truncated{false};
};

/// \brief Abstraction over a source Schema Registry's change feed, scoped to
/// one link.
///
/// Separate from `source_reader`: that reads the source's current state on
/// demand, while this reports what has changed since the last poll so a tail
/// tick can be near-free when nothing has.
class tail_reader {
public:
    tail_reader() = default;
    tail_reader(const tail_reader&) = delete;
    tail_reader& operator=(const tail_reader&) = delete;
    tail_reader(tail_reader&&) = delete;
    tail_reader& operator=(tail_reader&&) = delete;
    virtual ~tail_reader() = default;

    /// Re-evaluates whether this source can be tailed and, when it can, pins
    /// the position the next `poll` reads from.
    ///
    /// Called at the start of every full sync, before the sync's first source
    /// read. That ordering is what makes the tail lossless: a change recorded
    /// before the pinned position necessarily predates the full sync's reads,
    /// so the full sync observed it, and everything from the pinned position on
    /// is replayed by a later `poll`.
    ///
    /// A source that cannot be tailed is reported as an availability, never as
    /// an error: tailing is an optimization, and a link without it keeps
    /// replicating on its full-sync interval. A reader logs why it could not
    /// arm, so the verdict is for readers that compose others -- one choosing
    /// between change feeds -- rather than for the sync, which does not branch
    /// on it. Only `poll` can fault.
    virtual ss::future<tail_availability> arm(ss::abort_source&) = 0;

    /// Reports the changes recorded since the last poll. Returns an empty batch
    /// -- the common case -- when nothing changed, and likewise on a reader
    /// that did not arm, so callers need no availability check of their own.
    virtual ss::future<source_result<tail_batch>> poll(ss::abort_source&) = 0;

    /// Puts the last polled batch back, so a later `poll` reports it again.
    ///
    /// For a caller that could not apply what it polled: the batch is still on
    /// the source, so replaying it is cheaper than the full sync that would
    /// otherwise have to find those changes. Replay is safe because a batch
    /// names targets rather than carrying them, so applying one twice is a
    /// no-op. The default is a no-op for readers that cannot rewind.
    virtual ss::future<> rewind() { return ss::make_ready_future<>(); }

    /// Releases any resources the reader holds (e.g. a Kafka consumer). Called
    /// once before the reader is destroyed; the default is a no-op for readers
    /// that hold nothing. After stop() no other method may be called.
    virtual ss::future<> stop() { return ss::make_ready_future<>(); }
};

/// \brief Creates one `tail_reader` per link.
class tail_reader_factory {
public:
    tail_reader_factory() = default;
    tail_reader_factory(const tail_reader_factory&) = delete;
    tail_reader_factory& operator=(const tail_reader_factory&) = delete;
    tail_reader_factory(tail_reader_factory&&) = delete;
    tail_reader_factory& operator=(tail_reader_factory&&) = delete;
    virtual ~tail_reader_factory() = default;

    /// \param link the owning link. A reader that tails the source's `_schemas`
    ///        topic takes the link's (shard-local) source Kafka connection from
    ///        it; readers that need no Kafka connection ignore it.
    virtual std::unique_ptr<tail_reader> create(link* link) = 0;
};

} // namespace cluster_link::schema_registry_sync
