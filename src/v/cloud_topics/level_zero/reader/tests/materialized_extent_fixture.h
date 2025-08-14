/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "container/chunked_vector.h"
#include "mocks.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/core/manual_clock.hh>

#include <chrono>
#include <exception>
#include <limits>
#include <queue>

enum class injected_cache_get_failure {
    none,
    return_error,   // cache get returns nullopt because the file does not
                    // exist
    throw_error,    // actually throws an exception
    throw_shutdown, // throws shutdown exception
};

enum class injected_cache_put_failure {
    none,
    throw_shutdown, // throws 'shutdown' error
    throw_error,    // throws unexpected exception
};

enum class injected_cache_rsv_failure {
    none,
    throw_shutdown, // throws 'shutdown' error
    throw_error,    // throws unexpected exception
};

enum class injected_is_cached_failure {
    none,
    stall_then_ok,   // returns in_progress, next call returns available
    stall_then_fail, // returns in_progress, next call returns
                     // not_available
    noop,            // is_cached is not called
    throw_error,     // throws exception
    throw_shutdown,  // throws shutdown exception
};

enum class injected_cloud_get_failure {
    none,
    return_failure,  // returns 'failed' error code
    return_notfound, // returns 'KeyNotFound' error
    return_timeout,  // returns timeout
    throw_shutdown,  // throws 'shutdown' error
    throw_error,     // throws unexpected exception
};

/// The struct describes the injected failures for one particular placeholder
struct injected_failure {
    // cache get operation
    injected_cache_get_failure cache_get{injected_cache_get_failure::none};
    // cache put operation
    injected_cache_put_failure cache_put{injected_cache_put_failure::none};
    // cache reserve space
    injected_cache_rsv_failure cache_rsv{injected_cache_rsv_failure::none};
    // check cache for status
    injected_is_cached_failure is_cached{injected_is_cached_failure::none};
    // cloud storage get
    injected_cloud_get_failure cloud_get{injected_cloud_get_failure::none};
};

class materialized_extent_fixture : public seastar_test {
public:
    // Generate random batches.
    // This is a source of truth for the test. The goal is to consume
    // these batches from placeholder/cache/cloud indirection.
    ss::future<> add_random_batches(int record_count);

    // Generate the 'partition' collection from the source of truth. If the
    // 'cache' is set to 'true' the data is added to the cloud storage cache.
    // The 'group_by' parameter control how many batches are stored per L0
    // object.
    // 'failures' parameters contains set of injected failures
    void produce_placeholders(
      bool use_cache,
      int group_by,
      std::queue<injected_failure> failures = {},
      int begin = std::numeric_limits<int>::min(),
      int end = std::numeric_limits<int>::max());

    model::offset get_expected_committed_offset();

    /// Create a list of batches that contain placeholders
    chunked_vector<model::record_batch> make_underlying();

    static experimental::cloud_topics::l0::materialized_extent
    make_materialized_extent(model::record_batch batch) {
        experimental::cloud_topics::extent_meta e{
          .base_offset = model::offset_cast(batch.base_offset()),
          .last_offset = model::offset_cast(batch.last_offset()),
        };
        iobuf payload = std::move(batch).release_data();
        iobuf_parser parser(std::move(payload));
        auto record = model::parse_one_record_from_buffer(parser);
        iobuf value = std::move(record).release_value();
        auto placeholder
          = serde::from_iobuf<experimental::cloud_topics::dl_placeholder>(
            std::move(value));
        e.id = placeholder.id;
        e.first_byte_offset = placeholder.offset;
        e.byte_range_size = placeholder.size_bytes;
        return experimental::cloud_topics::l0::materialized_extent{
          .meta = e,
        };
    }

    chunked_vector<model::record_batch> partition;
    chunked_vector<model::record_batch> expected;
    remote_mock remote;
    cache_mock cache;
};
