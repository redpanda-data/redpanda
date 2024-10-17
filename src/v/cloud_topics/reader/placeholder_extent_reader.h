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

#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/remote.h"
#include "model/record_batch_reader.h"
#include "storage/log_reader.h"

#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics {

/// Create placeholder_extent_reader instance.
///
/// The reader consumes another reader which returns dl_placeholder
/// batches and materializes them on the fly.
/// \param cfg is a log reader config
/// \param bucket is a cloud storage bucket
/// \param underlying is a reader that returns dl_placeholder batches
/// \param api is a cloud_io::remote instance
/// \param cache is a cloud storage cache instance
/// \param rtc is a top level retry chain node
model::record_batch_reader make_placeholder_extent_reader(
  storage::log_reader_config cfg,
  cloud_storage_clients::bucket_name bucket,
  model::record_batch_reader underlying,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_io::basic_cache_service_api<ss::lowres_clock>& cache,
  retry_chain_node& rtc);

} // namespace experimental::cloud_topics
