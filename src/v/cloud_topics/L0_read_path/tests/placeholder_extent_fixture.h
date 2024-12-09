// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "container/fragmented_vector.h"
#include "mocks.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "test_utils/test.h"

#include <seastar/core/circular_buffer.hh>
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

class placeholder_extent_fixture : public seastar_test {
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

    model::record_batch_reader make_log_reader();

    fragmented_vector<model::record_batch> partition;
    fragmented_vector<model::record_batch> expected;
    remote_mock remote;
    cache_mock cache;
};
