/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/remote.h"
#include "cloud_storage/cache_service.h"
#include "model/record_batch_reader.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_data {

// TODO: parametrize the clock
model::record_batch_reader make_aggregated_log_reader(
  storage::log_reader_config cfg,
  cloud_storage_clients::bucket_name bucket,
  model::record_batch_reader underlying,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_storage::cloud_storage_cache_api& cache);

} // namespace cloud_data
