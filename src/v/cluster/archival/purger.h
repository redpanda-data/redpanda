/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/base_manifest.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/lifecycle_marker.h"
#include "cloud_storage/remote_path_provider.h"
#include "cluster/archival/types.h"
#include "cluster/fwd.h"
#include "cluster/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace archival {

/**
 * The purger is a global sharded service: it runs on all shards, and
 * decides internally which shard will purge which ranges of objects
 * in object storage.
 *
 * The purger's only goal is to remove the uploade objects belonging to deleted
 * topics.
 */
class purger : public housekeeping_job {
public:
    explicit purger(
      cloud_storage::remote&,
      cluster::topic_table&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::members_table>&);

    ss::future<run_result> run(run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

    retry_chain_node* get_root_retry_chain_node() override;

    ss::sstring name() const override;

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
      const cloud_storage::remote_path_provider& path_provider,
      model::ntp,
      model::initial_revision_id,
      retry_chain_node& rtc);

    struct collected_manifests {
        using flat_t
          = std::pair<std::vector<ss::sstring>, std::optional<ss::sstring>>;

        std::optional<ss::sstring> current_serde;
        std::optional<ss::sstring> current_json;
        std::vector<ss::sstring> spillover;

        bool empty() const;
        [[nodiscard]] flat_t flatten();
    };

    ss::future<std::optional<collected_manifests>> collect_manifest_paths(
      const cloud_storage_clients::bucket_name&,
      const cloud_storage::remote_path_provider&,
      model::ntp,
      model::initial_revision_id,
      retry_chain_node&);

    ss::future<purge_result> purge_manifest(
      const cloud_storage_clients::bucket_name&,
      const cloud_storage::remote_path_provider&,
      model::ntp,
      model::initial_revision_id,
      remote_manifest_path,
      cloud_storage::manifest_format,
      retry_chain_node&);

    struct global_position {
        uint32_t self;
        uint32_t total;
    };

    ss::future<cloud_storage::upload_result> write_remote_lifecycle_marker(
      const cluster::nt_revision&,
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage::remote_path_provider& path_provider,
      cloud_storage::lifecycle_status status,
      retry_chain_node& parent_rtc);

    /// Find our index out of all shards in the cluster
    global_position get_global_position();

    ss::abort_source _as;
    retry_chain_node _root_rtc;
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
