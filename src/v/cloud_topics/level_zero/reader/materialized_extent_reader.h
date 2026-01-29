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

#include "base/outcome.h"
#include "cloud_io/remote.h"
#include "cloud_topics/batch_cache/hydrated_cache_api.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l0 {

/// Result of the materialization operation.
/// Even in case of error the probe will track used resources.
struct materialize_result {
    result<chunked_vector<model::record_batch>> batches;
    micro_probe probe;
};

/// Consume the 'underlying' reader that returns placeholders
/// and materialize them by downloading from the cloud storage.
/// The method processes 'underlying' fully. The result is stored
/// in memory so the caller should be careful with this param.
///
/// \param tidp is the topic_id_partition being materialized
/// \param bucket is a cloud storage bucket
/// \param query is an array of extent_meta objects
/// \param api is a cloud_io::remote instance
/// \param cache is a per-partition hydrated cache instance (may be null)
/// \param rtc is a retry chain node to use
/// \param rtc_logger is a logger that should track the progress
ss::future<materialize_result> materialize_placeholders(
  const model::topic_id_partition& tidp,
  cloud_storage_clients::bucket_name bucket,
  chunked_vector<extent_meta> query,
  cloud_io::remote_api<ss::lowres_clock>& api,
  partition_hydrated_cache_api* cache,
  retry_chain_node& rtc,
  retry_chain_logger& logger);

} // namespace cloud_topics::l0
