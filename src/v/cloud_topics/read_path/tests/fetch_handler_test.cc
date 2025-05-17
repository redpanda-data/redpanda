// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/core/read_pipeline.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/read_path/fetch_request_handler.h"
#include "cloud_topics/read_path/tests/placeholder_extent_fixture.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

#include <chrono>
#include <exception>
#include <queue>
#include <stdexcept>

namespace cloud_topics = experimental::cloud_topics;

ss::logger test_log("L0_fetch_handler_test");

TEST_F_CORO(placeholder_extent_fixture, l0_fetch_handler_test) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    auto ntp = model::controller_ntp;

    auto underlying = make_underlying();

    cloud_topics::core::read_pipeline<> pipeline;

    cloud_topics::fetch_handler l0_fetch_handler(
      pipeline.register_read_pipeline_stage(),
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache);

    vlog(test_log.debug, "Starting L0 fetch handler");

    co_await l0_fetch_handler.start();

    vlog(test_log.debug, "Make reader");
    auto reader = co_await pipeline.make_reader(
      ntp, {.output_size_estimate = 1_MiB, .meta = std::move(underlying)}, 1s);

    ASSERT_TRUE_CORO(reader.has_value());

    ASSERT_TRUE_CORO(reader.value().results == expected);

    vlog(test_log.debug, "Stopping pipeline");
    co_await pipeline.stop();
    co_await l0_fetch_handler.stop();
    vlog(test_log.debug, "Stopped fetch handler");
}
