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
#include "cloud_topics/read_replica/snapshot_provider.h"
#include "container/chunked_hash_map.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <expected>

namespace cloud_io {
class remote;
class cache;
} // namespace cloud_io

namespace cloud_topics::read_replica {

class database_refresher;
class snapshot_manager : public snapshot_provider {
public:
    explicit snapshot_manager(
      std::filesystem::path, cloud_io::remote*, cloud_io::cache*);

    ss::future<std::expected<snapshot_handle, error>> get_snapshot(
      l1::domain_uuid,
      cloud_storage_clients::bucket_name,
      ss::lowres_clock::time_point min_refresh_time,
      std::optional<lsm::sequence_number> min_seqno,
      ss::lowres_clock::duration timeout) override;

    ss::future<> stop() override;

    // Test helper: returns the last refresh time for a domain, or nullopt if
    // domain doesn't exist
    std::optional<ss::lowres_clock::time_point>
    last_refresh_time(l1::domain_uuid domain) const;

private:
    ss::gate gate_;
    const std::filesystem::path staging_dir_;
    cloud_io::remote* remote_;
    cloud_io::cache* cache_;

    struct database_entry {
        database_entry() = default;
        ~database_entry();
        database_entry(database_entry&&) noexcept = default;
        database_entry& operator=(database_entry&&) noexcept = default;
        database_entry(const database_entry&) = delete;
        database_entry& operator=(const database_entry&) = delete;

        // A database backing a given metastore domain. The database is
        // periodically refreshed with state from cloud.
        std::unique_ptr<database_refresher> refresher;

        // Timer used to detect when a given database hasn't been accessed in
        // long enough to be cleaned up.
        std::unique_ptr<ss::timer<ss::lowres_clock>> idle_timer;

        // Counter of the number of callers waiting in get_snapshot(). If this
        // remains zero for long enough, the database entry is cleaned up.
        size_t num_waiters{0};
    };

    // Refresh loops maintained per domain.
    chunked_hash_map<l1::domain_uuid, database_entry> databases_;
};

} // namespace cloud_topics::read_replica
