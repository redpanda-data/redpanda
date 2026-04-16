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

#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <expected>

namespace cloud_topics::l1 {
class io;
} // namespace cloud_topics::l1

namespace cloud_topics::read_replica {

class snapshot_metastore;

// Encapsulates a snapshot of a domain within a metastore, returned by a given
// snapshot provider. Must be destroyed before the snapshot provider that
// provided it is destroyed.
struct snapshot_handle {
    std::unique_ptr<snapshot_metastore> metastore;
    std::optional<lsm::sequence_number> seqno;

    // Meant to be used alongside this snapshot, with the expectation that the
    // extents returned by `metastore` refer to objects accessible by this IO.
    l1::io* io;
};

// Interface for providing access to state in object storage of a given
// metastore domain with some bounded staleness.
class snapshot_provider {
public:
    enum class errc {
        io_error,
        shutting_down,
    };
    using error = detailed_error<errc>;

    virtual ~snapshot_provider() = default;
    virtual ss::future<> stop() = 0;

    // Returns a snapshot for the given domain and bucket that was taken at or
    // after the given refresh time, and had a sequence number greater than or
    // equal to that inputted.
    //
    // If the criteria are already met, this should return immediately with the
    // latest snapshot. Otherwise, triggers a refresh of the underlying
    // database and returns the latest snapshot once complete. As such, callers
    // should be cautious about calling this with a time at or around now() to
    // avoid constantly triggering refreshes, only doing so to ensure
    // consistency across lifecycle events (e.g.  changing leadership,
    // partition movement, etc).
    //
    // TODO: it might be nice if this didn't leak domains as an abstraction,
    // and instead the interface took remote_label and topic_id_partition.
    virtual ss::future<std::expected<snapshot_handle, error>> get_snapshot(
      l1::domain_uuid domain,
      cloud_storage_clients::bucket_name bucket,
      ss::lowres_clock::time_point min_refresh_time,
      std::optional<lsm::sequence_number> min_seqno,
      ss::lowres_clock::duration timeout) = 0;
};

} // namespace cloud_topics::read_replica

template<>
struct fmt::formatter<cloud_topics::read_replica::snapshot_provider::errc>
  : fmt::formatter<std::string_view> {
    using errc = cloud_topics::read_replica::snapshot_provider::errc;

    auto format(errc e, fmt::format_context& ctx) const {
        std::string_view name;
        switch (e) {
        case errc::io_error:
            name = "io_error";
            break;
        case errc::shutting_down:
            name = "shutting_down";
            break;
        }
        return fmt::formatter<std::string_view>::format(name, ctx);
    }
};
