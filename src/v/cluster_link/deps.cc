/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/deps.h"

#include "cluster_link/errc.h"
#include "cluster_link/utils.h"

namespace cluster_link {

kafka::client::cluster
cluster_factory::create_cluster(const model::metadata& md) {
    return kafka::client::cluster(metadata_to_kafka_config(md));
}

} // namespace cluster_link
