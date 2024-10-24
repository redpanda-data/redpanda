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
#include "cloud_storage/download_exception.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/tests/util.h"
#include "model/record_batch_types.h"

#include <seastar/core/lowres_clock.hh>

#include <fmt/chrono.h>

#include <random>

using namespace cloud_storage;

static const remote_path_provider path_provider(std::nullopt, std::nullopt);

inline ss::logger test_log("test"); // NOLINT

static std::vector<model::record_batch_header>
scan_remote_partition_incrementally_with_reuploads(
  cloud_storage_fixture& fixt,
  model::offset base,
  model::offset max,
  std::vector<in_memory_segment> segments,
  size_t maybe_max_segments = 0,
  size_t maybe_max_readers = 0) {
    ss::lowres_clock::update();
    auto conf = fixt.get_configuration();
    if (maybe_max_segments) {
        config::shard_local_cfg()
          .cloud_storage_max_materialized_segments_per_shard.set_value(
            maybe_max_segments);
    }
    if (maybe_max_readers) {
        config::shard_local_cfg()
          .cloud_storage_max_segment_readers_per_shard.set_value(
            maybe_max_readers);
    }
    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    auto manifest = hydrate_manifest(fixt.api.local(), fixt.bucket_name);
    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      fixt.api, fixt.cache, manifest, fixt.bucket_name, path_provider);
    auto partition = ss::make_shared<remote_partition>(
      manifest_view,
      fixt.api.local(),
      fixt.cache.local(),
      fixt.bucket_name,
      probe);
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    partition->start().get();

    std::vector<model::record_batch_header> headers;

    storage::log_reader_config reader_config(
      base, max, ss::default_priority_class());

    // starting max_bytes
    constexpr size_t max_bytes_limit = 4_KiB;
    reader_config.max_bytes = max_bytes_limit;

    auto next = base;
    auto next_insync_offset = model::next_offset(manifest.get_insync_offset());
    auto drop_reupload_flag = [&segments] {
        for (auto& s : segments) {
            s.do_not_reupload = true;
        }
    };
    auto maybe_reupload_range = [&fixt,
                                 &manifest,
                                 &next_insync_offset,
                                 &segments](model::offset begin) {
        // if this is true, start from prev segment, not the one which is
        // the closest to 'begin'
        auto shift_one_back = random_generators::get_int(0, 4) == 0;
        auto ix = 0;
        for (auto& s : segments) {
            if (s.base_offset > begin) {
                break;
            }
            if (!shift_one_back) {
                ix++;
            } else {
                shift_one_back = false;
            }
        }
        // choose how many segments to merge together
        auto n = random_generators::get_int(0, 4);
        vlog(
          test_log.debug,
          "reuploading {} segments starting from offset {}, insync_offset: {}, "
          "num segments: {}",
          n,
          segments[ix].base_offset,
          next_insync_offset,
          segments.size());
        auto merge_segments = [&segments, &manifest](int begin, int end) {
            auto meta_ptr = manifest.get(segments[begin].base_offset);
            if (meta_ptr->is_compacted) {
                vlog(
                  test_log.debug,
                  "segment {}-{} is already compacted, skipping",
                  meta_ptr->base_offset,
                  meta_ptr->committed_offset);
                return;
            }
            BOOST_REQUIRE(end - begin > 1);
            end = std::clamp(end, end, static_cast<int>(segments.size()));
            in_memory_segment& first = segments[begin];
            const in_memory_segment& last = segments[end - 1];
            vlog(
              test_log.debug,
              "merging segments {}-{} and {}-{}",
              first.base_offset,
              first.max_offset,
              last.base_offset,
              last.max_offset);
            for (int i = 1 + begin; i < end; i++) {
                auto& s = segments[i];
                first.base_offset = std::min(first.base_offset, s.base_offset);
                first.max_offset = std::max(first.max_offset, s.max_offset);
                first.do_not_reupload = false;
                first.num_config_batches += s.num_config_batches;
                first.num_config_records += s.num_config_records;
                std::copy(
                  std::make_move_iterator(s.records.begin()),
                  std::make_move_iterator(s.records.end()),
                  std::back_inserter(first.records));
                first.bytes.append(s.bytes.data(), s.bytes.size());
                std::copy(
                  s.headers.begin(),
                  s.headers.end(),
                  std::back_inserter(first.headers));
                std::copy(
                  s.file_offsets.begin(),
                  s.file_offsets.end(),
                  std::back_inserter(first.file_offsets));
            }
            segments.erase(
              segments.begin() + 1 + begin, segments.begin() + end);
        };
        if (n > 1) {
            merge_segments(ix, ix + n);
            reupload_compacted_segments(fixt, manifest, segments);
            manifest.advance_insync_offset(next_insync_offset);
            next_insync_offset = model::next_offset(next_insync_offset);
        } else if (n == 1) {
            segments[ix].do_not_reupload = false;
            reupload_compacted_segments(fixt, manifest, segments);
            manifest.advance_insync_offset(next_insync_offset);
            next_insync_offset = model::next_offset(next_insync_offset);
        }
        vlog(
          test_log.debug,
          "completed reuploading {} segments, num segments: {}",
          n,
          segments.size());
    };

    int num_fetches = 0;
    while (next < max) {
        reader_config.start_offset = next;
        reader_config.max_bytes = random_generators::get_int(
          max_bytes_limit - 1);
        drop_reupload_flag();
        maybe_reupload_range(next);
        vlog(test_log.info, "reader_config {}", reader_config);
        auto reader = partition->make_reader(reader_config).get().reader;
        auto headers_read
          = reader.consume(test_consumer(), model::no_timeout).get();
        if (headers_read.empty()) {
            break;
        }
        for (const auto& header : headers_read) {
            vlog(test_log.info, "header {}", header);
        }
        next = headers_read.back().last_offset() + model::offset(1);
        std::copy(
          headers_read.begin(),
          headers_read.end(),
          std::back_inserter(headers));
        num_fetches++;
    }

    BOOST_REQUIRE(num_fetches > 0);
    vlog(test_log.info, "{} fetch operations performed", num_fetches);
    return headers;
}
/// This test scans the entire range of offsets
FIXTURE_TEST(
  test_remote_partition_scan_translate_full_random, cloud_storage_fixture) {
    vlog(
      test_log.info,
      "Seed used for read workload: {}",
      random_generators::internal::seed);

    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);
    auto headers_read = scan_remote_partition(
      *this,
      base,
      max,
      random_generators::get_int(5, 20),
      random_generators::get_int(5, 20));
    model::offset expected_offset{0};
    for (const auto& header : headers_read) {
        BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
        expected_offset = header.last_offset() + model::offset(1);
    }
    BOOST_REQUIRE_EQUAL(headers_read.size(), num_data_batches);
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random, cloud_storage_fixture) {
    vlog(
      test_log.info,
      "Seed used for read workload: {}",
      random_generators::internal::seed);

    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);
    auto headers_read = scan_remote_partition_incrementally(
      *this,
      base,
      max,
      0,
      random_generators::get_int(5, 20),
      random_generators::get_int(5, 20));
    model::offset expected_offset{0};
    for (const auto& header : headers_read) {
        BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
        expected_offset = header.last_offset() + model::offset(1);
    }
    BOOST_REQUIRE_EQUAL(headers_read.size(), num_data_batches);
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_overlaps,
  cloud_storage_fixture) {
    vlog(
      test_log.info,
      "Seed used for read workload: {}",
      random_generators::internal::seed);

    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::overlapping_segments);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    try {
        scan_remote_partition_incrementally(*this, base, max);
    } catch (const download_exception& ex) {
        vlog(test_log.warn, "timeout connecting to s3 impostor: {}", ex.what());
    }
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_duplicates,
  cloud_storage_fixture) {
    constexpr int num_segments = 500;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(
      *this, batch_types, manifest_inconsistency::duplicate_offset_ranges);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    try {
        scan_remote_partition_incrementally(*this, base, max);
    } catch (const download_exception& ex) {
        vlog(test_log.warn, "timeout connecting to s3 impostor: {}", ex.what());
    }
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_tx_fence,
  cloud_storage_fixture) {
    vlog(
      test_log.info,
      "Seed used for read workload: {}",
      random_generators::internal::seed);

    constexpr int num_segments = 1000;
    const auto [segment_layout, num_data_batches] = generate_segment_layout(
      num_segments, 42, false);
    auto segments = setup_s3_imposter(*this, segment_layout);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    try {
        auto headers_read = scan_remote_partition_incrementally(
          *this, base, max, 0, 20, 20);
        model::offset expected_offset{0};
        size_t ix_header = 0;
        for (const auto& ix_seg : segment_layout) {
            for (const auto& batch : ix_seg) {
                if (batch.type == model::record_batch_type::tx_fence) {
                    expected_offset++;
                } else if (batch.type == model::record_batch_type::raft_data) {
                    auto header = headers_read[ix_header];
                    BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
                    expected_offset = header.last_offset() + model::offset(1);
                    ix_header++;
                } else {
                    // raft_configuratoin or archival_metadata
                    // no need to update expected_offset or ix_header
                }
            }
        }
    } catch (const download_exception& ex) {
        vlog(test_log.warn, "timeout connecting to s3 impostor: {}", ex.what());
    }
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_tx_fence_random_lso,
  cloud_storage_fixture) {
    vlog(
      test_log.info,
      "Seed used for read workload: {}",
      random_generators::internal::seed);

    constexpr int num_segments = 1000;
    const auto [segment_layout, num_data_batches] = generate_segment_layout(
      num_segments, 42, false);
    auto segments = setup_s3_imposter(*this, segment_layout);
    auto base = segments[0].base_offset;
    auto max = segments.back().max_offset;
    vlog(test_log.debug, "offset range: {}-{}", base, max);

    try {
        auto headers_read
          = scan_remote_partition_incrementally_with_closest_lso(
            *this, base, max, 5, 5);
        vlog(test_log.debug, "{} record batches consumed", headers_read.size());
        model::offset expected_offset{0};
        size_t ix_header = 0;
        for (const auto& ix_seg : segment_layout) {
            for (const auto& batch : ix_seg) {
                if (batch.type == model::record_batch_type::tx_fence) {
                    expected_offset++;
                } else if (batch.type == model::record_batch_type::raft_data) {
                    auto header = headers_read[ix_header];
                    BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
                    expected_offset = header.last_offset() + model::offset(1);
                    ix_header++;
                } else {
                    // raft_configuratoin or archival_metadata
                    // no need to update expected_offset or ix_header
                }
            }
        }
    } catch (const download_exception& ex) {
        vlog(test_log.warn, "timeout connecting to s3 impostor: {}", ex.what());
    }
}

FIXTURE_TEST(
  test_remote_partition_scan_incrementally_random_with_reuploads,
  cloud_storage_fixture) {
    vlog(
      test_log.info,
      "Seed used for read workload: {}",
      random_generators::internal::seed);

    constexpr int num_segments = 1000;
    const auto [batch_types, num_data_batches] = generate_segment_layout(
      num_segments, 42);
    auto segments = setup_s3_imposter(*this, batch_types);
    auto base = segments[0].base_offset;
    auto max = segments[num_segments - 1].max_offset;
    vlog(test_log.debug, "full offset range: {}-{}", base, max);

    try {
        auto headers_read = scan_remote_partition_incrementally_with_reuploads(
          *this,
          base,
          max,
          std::move(segments),
          random_generators::get_int(5, 20),
          random_generators::get_int(5, 20));
        model::offset expected_offset{0};
        for (const auto& header : headers_read) {
            BOOST_REQUIRE_EQUAL(expected_offset, header.base_offset);
            expected_offset = header.last_offset() + model::offset(1);
        }
        BOOST_REQUIRE_EQUAL(headers_read.size(), num_data_batches);
    } catch (const download_exception& ex) {
        vlog(test_log.warn, "timeout connecting to s3 impostor: {}", ex.what());
    }
}

namespace {

ss::future<> scan_until_close(
  ss::shared_ptr<remote_partition> partition,
  storage::log_reader_config reader_config,
  ss::gate& g) {
    test_log.info("starting scan_until_close");
    auto _ = ss::defer([] { test_log.info("exiting scan_until_close"); });
    auto guard = g.hold();
    auto counter = size_t{0};
    while (!g.is_closed()) {
        try {
            test_log.info("running scan loop nr {}", counter);
            auto translating_reader = co_await partition->make_reader(
              reader_config);
            auto reader = std::move(translating_reader.reader);
            auto headers_read = co_await reader.consume(
              test_consumer(), model::no_timeout);
            test_log.info("done scan loop {}", counter);
            ++counter;
        } catch (...) {
            test_log.warn("Error scanning: {}", std::current_exception());
        }
    }
}

} // anonymous namespace

// Test designed to reproduce a hang seen during shutdown.
FIXTURE_TEST(test_scan_while_shutting_down, cloud_storage_fixture) {
    constexpr int num_segments = 1000;
    const auto [segment_layout, num_data_batches] = generate_segment_layout(
      num_segments, 42, false);
    auto segments = setup_s3_imposter(*this, segment_layout);
    auto base = segments[0].base_offset;

    auto remote_conf = this->get_configuration();

    auto m = ss::make_lw_shared<cloud_storage::partition_manifest>(
      manifest_ntp, manifest_revision);

    auto manifest = hydrate_manifest(api.local(), bucket_name);
    partition_probe probe(manifest.get_ntp());
    auto manifest_view = ss::make_shared<async_manifest_view>(
      api, cache, manifest, bucket_name, path_provider);
    auto partition = ss::make_shared<remote_partition>(
      manifest_view, api.local(), this->cache.local(), bucket_name, probe);
    partition->start().get();
    auto partition_stop = ss::defer([&partition] { partition->stop().get(); });

    test_log.info("starting scan op");
    ss::gate g;
    auto scan_future = scan_until_close(
      partition,
      storage::log_reader_config(
        base, model::offset::max(), ss::default_priority_class()),
      g);
    auto close_fut
      = ss::maybe_yield()
          .then([] { return ss::maybe_yield(); })
          .then([] { return ss::maybe_yield(); })
          .then([] { return ss::sleep(10ms); })
          .then([this, &g]() mutable {
              auto begin_shutdown = std::chrono::steady_clock::now();
              test_log.info("shutting down connections");
              pool.local().shutdown_connections();
              test_log.info("closing gate");
              return g.close().then([begin_shutdown] {
                  auto end_shutdown = std::chrono::steady_clock::now();
                  test_log.info("gate closed");
                  return end_shutdown - begin_shutdown;
              });
          });
    // NOTE: see issues/11271
    BOOST_TEST_CONTEXT("scan_unit_close should terminate in a finite amount of "
                       "time at shutdown") {
        test_log.info("waiting on close future with timeout");
        BOOST_CHECK_LE(close_fut.get(), 60s);
        test_log.info(
          "waiting on scan_future, this should be immediately available");
        BOOST_CHECK(scan_future.available());
        scan_future.get();
    }
}
