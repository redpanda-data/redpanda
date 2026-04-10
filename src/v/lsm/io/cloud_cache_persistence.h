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

#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "lsm/io/persistence.h"

namespace lsm::io {

/// Open a data persistence backed by the cloud cache and cloud storage.
ss::future<std::unique_ptr<data_persistence>> open_cloud_cache_data_persistence(
  cloud_io::cache* cache,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix);

/// Open a metadata persistence backed by cloud storage.
ss::future<std::unique_ptr<metadata_persistence>>
open_cloud_metadata_persistence(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix);

} // namespace lsm::io
