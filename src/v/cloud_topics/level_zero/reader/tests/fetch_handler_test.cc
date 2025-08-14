/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/level_zero/reader/fetch_request_handler.h"
#include "cloud_topics/level_zero/reader/tests/materialized_extent_fixture.h"
#include "container/chunked_vector.h"
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

static chunked_vector<cloud_topics::extent_meta>
convert_placeholders(const chunked_vector<model::record_batch>& batches) {
    chunked_vector<cloud_topics::extent_meta> res;
    for (const auto& b : batches) {
        auto mext = materialized_extent_fixture::make_materialized_extent(
          b.copy());
        res.push_back(mext.meta);
    }
    return res;
}

TEST_F_CORO(materialized_extent_fixture, l0_fetch_handler_test) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    auto ntp = model::controller_ntp;

    auto underlying = convert_placeholders(make_underlying());

    cloud_topics::l0::read_pipeline<> pipeline;

    cloud_topics::l0::fetch_handler l0_fetch_handler(
      pipeline.register_read_pipeline_stage(),
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &cache);

    vlog(test_log.debug, "Starting L0 fetch handler");

    co_await l0_fetch_handler.start();

    vlog(test_log.debug, "Make reader");
    auto reader = co_await pipeline.make_reader(
      ntp,
      {.output_size_estimate = 1_MiB, .meta = std::move(underlying)},
      ss::lowres_clock::now() + 1s);

    ASSERT_TRUE_CORO(reader.has_value());

    ASSERT_TRUE_CORO(reader.value().results == expected);

    vlog(test_log.debug, "Stopping pipeline");
    co_await pipeline.stop();
    co_await l0_fetch_handler.stop();
    vlog(test_log.debug, "Stopped fetch handler");
}
