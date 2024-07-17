/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cloud_storage/fwd.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/offsets_upload_rpc_types.h"
#include "cluster/cloud_metadata/types.h"
#include "cluster/logger.h"
#include "kafka/server/fwd.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

struct offsets_upload_paths {
    std::vector<ss::sstring> paths;
};
using offsets_upload_result = result<offsets_upload_paths, error_outcome>;

// Encapsulates uploading an offsets snapshot to S3.
class offsets_uploader {
public:
    offsets_uploader(
      cloud_storage_clients::bucket_name bucket,
      ss::sharded<kafka::group_manager>& group_manager,
      ss::sharded<cloud_storage::remote>& remote)
      : _bucket(bucket)
      , _group_manager(group_manager)
      , _remote(remote) {}

    void request_stop() { _as.request_abort(); }

    ss::future<> stop() {
        vlog(clusterlog.debug, "Stopping consumer offsets uploader");
        if (!_as.abort_requested()) {
            _as.request_abort();
        }
        co_await _gate.close();
        vlog(clusterlog.debug, "Stopped consumer offsets uploader");
    }

    ss::future<offsets_upload_reply> upload(offsets_upload_request req);

    ss::future<offsets_upload_result> upload(
      const model::cluster_uuid& uuid,
      const model::ntp&,
      const cluster_metadata_id& meta_id,
      retry_chain_node& retry_node);

private:
    ss::abort_source _as;
    ss::gate _gate;
    const cloud_storage_clients::bucket_name _bucket;
    ss::sharded<kafka::group_manager>& _group_manager;
    ss::sharded<cloud_storage::remote>& _remote;
};

} // namespace cluster::cloud_metadata
