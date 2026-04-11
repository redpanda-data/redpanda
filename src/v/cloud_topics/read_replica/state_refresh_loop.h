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

#include "cloud_storage/remote_label.h"
#include "cloud_topics/read_replica/stm.h"
#include "utils/detailed_error.h"
#include "utils/prefix_logger.h"

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_topics::l1 {
class manifest_io;
} // namespace cloud_topics::l1

namespace cloud_topics::read_replica {

class snapshot_manager;

// Loop that runs for the duration of a given term on the leader. This loop is
// in charge of reading state from object storage about the given partition and
// replicating that state via the state machine.
class state_refresh_loop {
public:
    enum class errc {
        metastore_manifest_error,
        replication_error,
        io_error,
        not_leader,
    };
    using error = detailed_error<errc>;
    state_refresh_loop(
      model::term_id expected_term,
      model::topic_id_partition tidp,
      ss::shared_ptr<stm> stm,
      snapshot_manager* snapshot_mgr,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage::remote_label remote_label,
      cloud_io::remote& remote);

    ~state_refresh_loop();

    void start();
    ss::future<> stop_and_wait();

private:
    bool is_leader() const;
    void log_error(std::string_view prefix, error);

    // Runs a loop that periodically gets a snapshot, determines the partition
    // state with it, and replicates the state so that it can be used to serve
    // the synchronous bits of the Kafka API.
    ss::future<> run_loop();

    // Discover the domain UUID for this partition from the source cluster's
    // metastore manifest.
    ss::future<std::expected<l1::domain_uuid, error>> discover_domain();

    // Read state from cloud database and replicate to STM.
    ss::future<std::expected<void, error>> refresh_state_from_cloud(
      l1::domain_uuid domain, ss::lowres_clock::time_point min_refresh_time);

    ss::gate gate_;
    ss::abort_source as_;
    model::term_id expected_term_;
    model::topic_id_partition tidp_;
    ss::shared_ptr<stm> stm_;
    snapshot_manager* snapshot_mgr_;
    cloud_storage_clients::bucket_name bucket_;
    cloud_storage::remote_label remote_label_;
    cloud_io::remote& remote_;
    std::unique_ptr<l1::manifest_io> metastore_manifest_io_;
    prefix_logger logger_;
};

} // namespace cloud_topics::read_replica

template<>
struct fmt::formatter<cloud_topics::read_replica::state_refresh_loop::errc>
  : fmt::formatter<std::string_view> {
    using errc = cloud_topics::read_replica::state_refresh_loop::errc;

    auto format(errc e, fmt::format_context& ctx) const {
        std::string_view name;
        switch (e) {
        case errc::metastore_manifest_error:
            name = "metastore_manifest_error";
            break;
        case errc::replication_error:
            name = "replication_error";
            break;
        case errc::io_error:
            name = "io_error";
            break;
        case errc::not_leader:
            name = "not_leader";
            break;
        }
        return fmt::formatter<std::string_view>::format(name, ctx);
    }
};
