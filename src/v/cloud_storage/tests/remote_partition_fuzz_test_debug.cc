/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/tests/util.h"

#include <seastar/core/lowres_clock.hh>

#include <random>

using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

namespace {

ss::future<> scan_until_close(
  remote_partition& partition,
  const storage::log_reader_config& reader_config,
  ss::gate& g) {
    gate_guard guard{g};
    while (!g.is_closed()) {
        try {
            auto translating_reader = co_await partition.make_reader(
              reader_config);
            auto reader = std::move(translating_reader.reader);
            auto headers_read = co_await reader.consume(
              test_consumer(), model::no_timeout);
        } catch (...) {
            test_log.info("Error scanning: {}", std::current_exception());
        }
    }
}

} // anonymous namespace

// Test designed to reproduce a hang seen during shutdown.
FIXTURE_TEST(test_scan_while_shutting_down_dbg, cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [segment_layout, num_data_batches] = generate_segment_layout(
      num_segments, 42, false);
    auto segments = setup_s3_imposter(*this, segment_layout);
    auto base = segments[0].base_offset;

    auto remote_conf = this->get_configuration();

    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    storage::log_reader_config reader_config(
      base, model::offset::max(), ss::default_priority_class());
    static auto bucket = cloud_storage_clients::bucket_name("bucket");

    auto manifest = hydrate_manifest(api.local(), bucket);
    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      api, cache, manifest, bucket, probe);
    auto partition = ss::make_shared<remote_partition>(
      manifest_view, api.local(), this->cache.local(), bucket, probe);
    partition->start().get();
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    ss::gate g;
    ssx::background = scan_until_close(*partition, reader_config, g);

    test_log.info("yielding control - 1");
    auto close_fut = ss::maybe_yield()
                       .then([] {
                           test_log.info("yielding control - 2");
                           return ss::maybe_yield();
                       })
                       .then([] {
                           test_log.info("yielding control - 3");
                           return ss::maybe_yield();
                       })
                       .then([] {
                           test_log.info("sleeping for 10ms");
                           return ss::sleep(std::chrono::milliseconds(10));
                       })
                       .then([this, &g]() mutable {
                           test_log.info("shutting down pool");
                           pool.local().shutdown_connections();
                           test_log.info("pool shut down");
                           return g.close();
                       });
    ss::with_timeout(model::timeout_clock::now() + 60s, std::move(close_fut))
      .get();
}
