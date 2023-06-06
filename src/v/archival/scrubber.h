/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/types.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/lifecycle_marker.h"
#include "cluster/fwd.h"
#include "cluster/types.h"

#include <seastar/core/future.hh>

namespace archival {

/**
 * The scrubber is a global sharded service: it runs on all shards, and
 * decides internally which shard will scrub which ranges of objects
 * in object storage.
 */
class scrubber : public housekeeping_job {
public:
    explicit scrubber(
      cloud_storage::remote&,
      cluster::topic_table&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::members_table>&);

    ss::future<run_result>
    run(retry_chain_node& rtc, run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

private:
    enum class purge_status : uint8_t {
        success,

        // Unavailability of backend, timeouts, etc.  We should drop out
        // but the purge can be retried in a future housekeeping iteration.
        retryable_failure,

        // If we cannot possibly finish, for example because we got a 404
        // reading manifest.
        permanent_failure,
    };

    struct purge_result {
        purge_status status;
        size_t ops{0};
    };

    ss::future<purge_result> purge_partition(
      const cluster::nt_lifecycle_marker&,
      const cloud_storage_clients::bucket_name& bucket,
      model::ntp,
      model::initial_revision_id,
      retry_chain_node& rtc);

    struct global_position {
        uint32_t self;
        uint32_t total;
    };

    ss::future<cloud_storage::upload_result> write_remote_lifecycle_marker(
      const cluster::nt_revision&,
      cloud_storage_clients::bucket_name& bucket,
      cloud_storage::lifecycle_status status,
      retry_chain_node& parent_rtc);

    /// Find our index out of all shards in the cluster
    global_position get_global_position();

    ss::abort_source _as;
    ss::gate _gate;

    // A gate holder we keep on behalf of the housekeeping service, when
    // it acquire()s us.
    ss::gate::holder _holder;

    bool _enabled{true};

    cloud_storage::remote& _api;
    cluster::topic_table& _topic_table;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::members_table>& _members_table;
};

} // namespace archival