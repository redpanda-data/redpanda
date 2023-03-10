/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition.h"
#include "model/fundamental.h"
#include "reflection/async_adl.h"

#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>

namespace coproc {

/// Read state for a single input topic, unique and unshared
struct read_context {
    static constexpr int8_t version{1};
    /// Where to begin reading from, determined by user provided option \ref
    /// topic_ingestion_policy, earliest, latest, or stored
    model::offset absolute_start{};
    /// Highest offset of current in-progress read
    model::offset last_read{};
    /// Highest offset that all materialized topics have observed
    model::offset last_acked{};
    /// Pointer to an input topic
    ss::lw_shared_ptr<cluster::partition> input;
};

/// Write state for topics that have been created stemming from an associated
/// source, the input begint the pointer in \ref read_context
struct write_context {
    static constexpr int8_t version{1};
    /// Where 'key' is the materialized_ntp and offset represets an ack of the
    /// read off of the respective input topic
    using offsets_t = absl::node_hash_map<model::ntp, model::offset>;

    offsets_t offsets;

    absl::btree_set<model::offset> unique_offsets() const {
        absl::btree_set<model::offset> ofs;
        std::transform(
          offsets.cbegin(),
          offsets.cend(),
          std::inserter(ofs, ofs.begin()),
          [](const offsets_t::value_type& p) { return p.second; });
        return ofs;
    }
};

/// Although wrapped in a shared_ptr this is never shared across script fibers
struct source {
    /// Elements from which ingest loop will operate on
    read_context rctx;
    /// Elements from which egest loop will operate on
    write_context wctx;
};

/// Main mapping between source and materialized topics. For each source topic
/// there is a context and a map of materialized topics. Each entry in this map
/// contains a value to denote if its up to date with the latest read or not.
/// This structure can be serialized/deserialized currently used to be persisted
/// to disk as a solution for fault tolerance.
using routes_t = absl::node_hash_map<model::ntp, ss::lw_shared_ptr<source>>;

} // namespace coproc

namespace reflection {

/// adl definitions for inner types of routes_t, these definitions make routes_t
/// serializable & deserializable.
template<>
struct async_adl<coproc::read_context> {
    ss::future<> to(iobuf& out, coproc::read_context&&);
    ss::future<coproc::read_context> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::write_context> {
    ss::future<> to(iobuf& out, coproc::write_context&&);
    ss::future<coproc::write_context> from(iobuf_parser&);
};

template<>
struct async_adl<ss::lw_shared_ptr<coproc::source>> {
    ss::future<> to(iobuf& out, ss::lw_shared_ptr<coproc::source>&&);
    ss::future<ss::lw_shared_ptr<coproc::source>> from(iobuf_parser&);
};

} // namespace reflection
