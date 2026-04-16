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

#include "kafka/data/make_exact_offset_replicator.h"

#include "kafka/data/partition_proxy.h"

namespace kafka {

std::unique_ptr<exact_offset_replicator>
make_exact_offset_replicator(const ss::lw_shared_ptr<cluster::partition>& p) {
    return make_partition_proxy(p).make_exact_offset_replicator();
}

} // namespace kafka
