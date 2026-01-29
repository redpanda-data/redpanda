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
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

struct micro_probe;

// Materialized placeholder extent
//
// Extent represents ctp_placeholder with the data
// that it represents stored in cloud storage cache or
// main memory.
// The extent can be hydrated (the data is moved from the cloud
// storage to disk) or materialized (data is moved to the main
// memory).
struct materialized_extent {
    extent_meta meta;
    iobuf object;
};

/// Fetch data referenced by the placeholder batch and the content of the
/// ctp_placeholder.
/// Return 'true' if the object was downloaded from the cloud storage.
/// Otherwise, if the object was populated from the cache, return 'false'.
ss::future<result<bool>> materialize(
  materialized_extent* extent,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe);

// Get ctp_placeholder and the payload of the object and generate a record
// batch
model::record_batch make_raft_data_batch(materialized_extent extent);

} // namespace cloud_topics::l0
