/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"

namespace lsm::io {

// Open a data persistence object in the given bucket at the prefix.
ss::future<std::unique_ptr<data_persistence>> open_cloud_data_persistence(
  std::filesystem::path staging_directory,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix,
  ss::sstring staging_prefix);

// Open a metadata persistence object in the given bucket at the prefix.
ss::future<std::unique_ptr<metadata_persistence>>
open_cloud_metadata_persistence(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix);

} // namespace lsm::io
