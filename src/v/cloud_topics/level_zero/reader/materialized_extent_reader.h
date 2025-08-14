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
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "model/record_batch_reader.h"
#include "storage/log_reader.h"

#include <seastar/core/lowres_clock.hh>

namespace experimental::cloud_topics::l0 {

/// Consume the 'underlying' reader that returns placeholders
/// and materialize them by downloading from the cloud storage.
/// The method processes 'underlying' fully. The result is stored
/// in memory so the caller should be careful with this param.
///
/// \param cfg is a log reader config
/// \param bucket is a cloud storage bucket
/// \param query is an array of extent_meta objects
/// \param api is a cloud_io::remote instance
/// \param cache is a cloud storage cache instance
/// \param rtc is a retry chain node to use
/// \param rtc_logger is a logger that should track the progress
ss::future<chunked_vector<model::record_batch>> materialize_placeholders(
  cloud_storage_clients::bucket_name bucket,
  chunked_vector<extent_meta> query,
  cloud_io::remote_api<ss::lowres_clock>& api,
  cloud_io::basic_cache_service_api<ss::lowres_clock>& cache,
  retry_chain_node& rtc,
  retry_chain_logger& logger);

} // namespace experimental::cloud_topics::l0
