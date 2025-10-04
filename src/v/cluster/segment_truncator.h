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

#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/errc.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <expected>

namespace cluster {
ss::future<std::expected<cloud_storage::segment_meta, cluster::errc>>
truncate_remote_segment(
  cloud_storage::segment_meta,
  kafka::offset last_included_ko,
  const model::ntp&,
  const cloud_storage::partition_manifest&,
  const cloud_storage_clients::bucket_name&,
  const cloud_storage::remote_path_provider&,
  cloud_storage::remote&,
  cloud_storage::cache&,
  retry_chain_node&);
} // namespace cluster
