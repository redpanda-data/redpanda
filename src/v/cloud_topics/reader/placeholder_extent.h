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

#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_utils.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace experimental::cloud_topics {

struct hydrated_L0_object {
    object_id id;
    iobuf payload;
};

// Materialized placeholder extent
//
// Extent represents dl_placeholder with the data
// that it represents stored in cloud storage cache or
// main memory.
// The extent can be hydrated (the data is moved from the cloud
// storage to disk) or materialized (data is moved to the main
// memory).
struct placeholder_extent {
    model::offset base_offset;
    dl_placeholder placeholder;
    ss::lw_shared_ptr<hydrated_L0_object> L0_object;
};

/// Convert record batch to dl_placeholder value and return an empty
/// extent which later has to be hydrated.
placeholder_extent make_placeholder_extent(model::record_batch batch);

/// Fetch data referenced by the placeholder batch and the content of the
/// dl_placeholder.
/// Return 'true' if the object was downloaded from the cloud storage.
/// Otherwise, if the object was populated from the cache, return 'false'.
ss::future<result<bool>> materialize(
  placeholder_extent* extent,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  basic_retry_chain_node<>* rtc);

// Get dl_placeholder and the payload of the object and generate a record
// batch
model::record_batch make_raft_data_batch(placeholder_extent extent);

} // namespace experimental::cloud_topics
