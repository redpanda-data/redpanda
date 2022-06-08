/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage/remote.h"
#include "s3/client.h"

#include <seastar/core/abort_source.hh>

namespace cluster {

class read_replica_manager {
public:
    read_replica_manager(cloud_storage::remote&);

    /**
     * Download remote topic manifest and set remote_properties to cfg.
     *
     * @param cfg custom_assignable_topic_configuration that will be changed
     * @param bucket s3 bucket where the topic manifeset will be downloaded from
     * @param as abourt source that caller can use request abort
     */
    ss::future<errc> set_remote_properties_in_config(
      custom_assignable_topic_configuration& cfg,
      const s3::bucket_name& bucket,
      ss::abort_source& as);

private:
    cloud_storage::remote& _remote;
};

} // namespace cluster
