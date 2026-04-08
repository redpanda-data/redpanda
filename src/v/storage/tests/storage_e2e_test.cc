// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "base/vassert.h"
#include "bytes/bytes.h"
#include "config/mock_property.h"
#include "container/chunked_circular_buffer.h"
#include "finjector/stress_fiber.h"
#include "model/fundamental.h"
#include "model/limits.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/future-util.h"
#include "storage/batch_cache.h"
#include "storage/disk_log_impl.h"
#include "storage/log_housekeeping_meta.h"
#include "storage/log_manager.h"
#include "storage/log_reader.h"
#include "storage/ntp_config.h"
#include "storage/record_batch_builder.h"
#include "storage/segment_utils.h"
#include "storage/tests/batch_generators.h"
#include "storage/tests/common.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/log_gap_analysis.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/random_bytes.h"
#include "test_utils/randoms.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test_macros.h"
#include "utils/directory_walker.h"
#include "utils/tristate.h"

#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <fmt/chrono.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <limits>
#include <numeric>
#include <optional>
#include <vector>

static ss::logger e2e_test_log("storage_e2e_test");

void validate_offsets(
  model::offset base,
  const std::vector<model::record_batch_header>& write_headers,
  const chunked_circular_buffer<model::record_batch>& read_batches) {
    ASSERT_EQ(write_headers.size(), read_batches.size());
    auto it = read_batches.begin();
    model::offset next_base = base;
    for (const auto& h : write_headers) {
        ASSERT_EQ(it->base_offset(), next_base);
        // last offset delta is inclusive (record with this offset belongs to
        // previous batch)
        next_base += (h.last_offset_delta + model::offset(1));
        it++;
    }
}

void compact_and_prefix_truncate(
  storage::log& log, storage::housekeeping_config cfg) {
    ss::abort_source as;
    auto eviction_future = log.monitor_eviction(as);

    log.housekeeping(cfg).get();

    if (eviction_future.available()) {
        auto evict_until = eviction_future.get();
        log
          .truncate_prefix(
            storage::truncate_prefix_config{model::next_offset(evict_until)})
          .get();
    } else {
        as.request_abort();
        eviction_future.ignore_ready_future();
    }
}

TEST_F(storage_test_fixture, test_assigning_offsets_in_single_segment_log) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(log, 10);
    log->flush().get();
    auto batches = read_and_validate_all_batches(log);

    ASSERT_EQ(headers.size(), batches.size());
    auto lstats = log->offsets();
    auto last_term_start_offset = log->find_last_term_start_offset();
    ASSERT_EQ(lstats.dirty_offset, batches.back().last_offset());
    ASSERT_EQ(last_term_start_offset, batches.front().base_offset());
    ASSERT_EQ(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

TEST_F(storage_test_fixture, append_twice_to_same_segment) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(log, 10);
    log->flush().get();
    auto headers_2 = append_random_batches(log, 10);
    log->flush().get();
    std::move(
      std::begin(headers_2), std::end(headers_2), std::back_inserter(headers));
    auto batches = read_and_validate_all_batches(log);

    ASSERT_EQ(headers.size(), batches.size());
    auto lstats = log->offsets();
    auto last_term_start_offset = log->find_last_term_start_offset();
    ASSERT_EQ(last_term_start_offset, batches.front().base_offset());
    ASSERT_EQ(lstats.dirty_offset, batches.back().last_offset());
    ASSERT_EQ(lstats.committed_offset, batches.back().last_offset());
};

TEST_F(storage_test_fixture, test_assigning_offsets_in_multiple_segment) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(1_KiB);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(log, 10);
    log->flush().get();
    auto batches = read_and_validate_all_batches(log);

    ASSERT_EQ(headers.size(), batches.size());
    auto lstats = log->offsets();
    auto last_term_start_offset = log->find_last_term_start_offset();
    ASSERT_EQ(last_term_start_offset, batches.front().base_offset());
    ASSERT_EQ(lstats.dirty_offset, batches.back().last_offset());
    ASSERT_EQ(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

TEST_F(storage_test_fixture, test_single_record_per_segment) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(
      log,
      10,
      model::term_id(1),
      std::nullopt,
      [](std::optional<model::timestamp> ts = std::nullopt) {
          chunked_circular_buffer<model::record_batch> batches;
          batches.push_back(
            model::test::make_random_batch(
              model::offset(0),
              1,
              true,
              model::record_batch_type::raft_data,
              std::nullopt,
              ts));
          return batches;
      });
    log->flush().get();
    auto batches = read_and_validate_all_batches(log);
    SUCCEED() << fmt::format("Flushed log: {}", *log);
    ASSERT_EQ(headers.size(), batches.size());
    auto lstats = log->offsets();
    auto last_term_start_offset = log->find_last_term_start_offset();
    ASSERT_EQ(last_term_start_offset, batches.front().base_offset());
    ASSERT_EQ(lstats.dirty_offset, batches.back().last_offset());
    ASSERT_EQ(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

TEST_F(storage_test_fixture, test_segment_rolling) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10 * 1024);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(
      log,
      10,
      model::term_id(1),
      std::nullopt,
      [](std::optional<model::timestamp> ts = std::nullopt)
        -> chunked_circular_buffer<model::record_batch> {
          chunked_circular_buffer<model::record_batch> batches;
          batches.push_back(
            model::test::make_random_batch(
              model::offset(0),
              1,
              true,
              model::record_batch_type::raft_data,
              std::nullopt,
              ts));
          return batches;
      },
      storage::log_append_config::fsync::no,
      false);
    log->flush().get();
    auto batches = read_and_validate_all_batches(log);
    SUCCEED() << fmt::format("Flushed log: {}", *log);
    ASSERT_EQ(headers.size(), batches.size());
    auto lstats = log->offsets();
    auto last_term_start_offset = log->find_last_term_start_offset();
    ASSERT_EQ(last_term_start_offset, batches.front().base_offset());
    ASSERT_EQ(lstats.dirty_offset, batches.back().last_offset());
    ASSERT_EQ(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
    /// Do the second append round
    auto new_headers = append_random_batches(
      log,
      10,
      model::term_id(1),
      std::nullopt,
      [](std::optional<model::timestamp> ts = std::nullopt) {
          chunked_circular_buffer<model::record_batch> batches;
          batches.push_back(
            model::test::make_random_batch(
              model::offset(0),
              1,
              true,
              model::record_batch_type::raft_data,
              std::nullopt,
              ts));
          return batches;
      },
      storage::log_append_config::fsync::no,
      false);
    auto new_lstats = log->offsets();
    ASSERT_GE(new_lstats.committed_offset, lstats.committed_offset);
    auto new_batches = read_and_validate_all_batches(
      log, lstats.committed_offset);
    ASSERT_EQ(last_term_start_offset, batches.front().base_offset());
    ASSERT_EQ(new_lstats.committed_offset, new_batches.back().last_offset());
};

TEST_F(storage_test_fixture, test_reading_range_from_a_log) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(log, 10);
    log->flush().get();
    auto batches = read_and_validate_all_batches(log);

    // range from base of beging to last of end
    auto range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].last_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
    // Range is inclusive base offset points to batch[7] so it have to be
    // included
    range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].base_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);

    range = read_range_to_vector(
      log, batches[3].last_offset(), batches[7].base_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);

    // Range that starts and ends in the middle of the same batch.
    range = read_range_to_vector(
      log,
      batches[3].base_offset() + model::offset(batches[3].record_count() / 3),
      batches[3].base_offset()
        + model::offset(batches[3].record_count() / 3 * 2LL));
    ASSERT_EQ(range.size(), 1);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);

    // Range that starts and ends in the middle of batches.
    range = read_range_to_vector(
      log,
      batches[3].base_offset() + model::offset(batches[3].record_count() / 2),
      batches[7].base_offset() + model::offset(batches[7].record_count() / 2));
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
};

TEST_F(storage_test_fixture, test_reading_range_from_a_log_with_write_caching) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(
      log,
      10,
      model::term_id(0),
      std::nullopt,
      {},
      storage::log_append_config::fsync::no,
      false);

    // Reclaim everything from cache.
    storage::testing_details::log_manager_accessor::batch_cache(mgr).clear();

    auto batches = read_and_validate_all_batches(log);
    ASSERT_EQ(batches.size(), headers.size());

    // range from base of beging to last of end
    auto range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].last_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
    // Range is inclusive base offset points to batch[7] so it have to be
    // included
    range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].base_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);

    range = read_range_to_vector(
      log, batches[3].last_offset(), batches[7].base_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
    // range from base of beging to the middle of end
    range = read_range_to_vector(
      log,
      batches[3].base_offset(),
      batches[7].base_offset() + model::offset(batches[7].record_count() / 2));
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
};

TEST_F(storage_test_fixture, test_truncation_with_write_caching) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    auto headers = append_random_batches(
      log,
      10,
      model::term_id(0),
      std::nullopt,
      {},
      storage::log_append_config::fsync::no,
      false);

    const auto truncate_batch_ix = headers.size() - 2;
    const auto truncate_offset = [&]() {
        model::offset o{0};
        for (size_t i = 0; i < truncate_batch_ix; i++) {
            o += headers[i].record_count;
        }
        return o;
    }();

    log->truncate(storage::truncate_config(truncate_offset)).get();

    // Reclaim everything from cache.
    storage::testing_details::log_manager_accessor::batch_cache(mgr).clear();

    auto batches = read_and_validate_all_batches(log);
    ASSERT_EQ(batches.size(), truncate_batch_ix);

    // range from base of beging to last of end
    auto range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].last_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
    // Range is inclusive base offset points to batch[7] so it have to be
    // included
    range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].base_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);

    range = read_range_to_vector(
      log, batches[3].last_offset(), batches[7].base_offset());
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
    // range from base of beging to the middle of end
    range = read_range_to_vector(
      log,
      batches[3].base_offset(),
      batches[7].base_offset() + model::offset(batches[7].record_count() / 2));
    ASSERT_EQ(range.size(), 5);
    ASSERT_EQ(range.front().header().crc, batches[3].header().crc);
    ASSERT_EQ(range.back().header().crc, batches[7].header().crc);
};

TEST_F(storage_test_fixture, test_rolling_term) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    std::vector<model::record_batch_header> headers;
    model::offset current_offset = model::offset{0};
    for (auto i = 0; i < 5; i++) {
        auto term_start_offset = current_offset;
        auto part = append_random_batches(log, 1, model::term_id(i));
        for (auto h : part) {
            current_offset += h.last_offset_delta + 1;
        }
        log->flush().get();
        ASSERT_EQ(
          model::term_id(i),
          log->get_term(current_offset - model::offset(1)).value());
        auto last_term_start_offset = log->find_last_term_start_offset();
        ASSERT_EQ(last_term_start_offset, term_start_offset);
        std::move(part.begin(), part.end(), std::back_inserter(headers));
    }

    auto read_batches = read_and_validate_all_batches(log);
    auto lstats = log->offsets();
    ASSERT_EQ(lstats.dirty_offset, read_batches.back().last_offset());
    ASSERT_EQ(lstats.committed_offset, read_batches.back().last_offset());
};

TEST_F(storage_test_fixture, test_append_batches_from_multiple_terms) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Testing type: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, {ntp, mgr.config().base_dir});
    std::vector<model::record_batch_header> headers;
    chunked_circular_buffer<model::record_batch> batches;
    std::vector<size_t> term_batches_counts;
    for (auto i = 0; i < 5; i++) {
        auto term_batches
          = model::test::make_random_batches(model::offset(0), 10).get();
        for (auto& b : term_batches) {
            b.set_term(model::term_id(i));
        }
        term_batches_counts.push_back(term_batches.size());
        std::move(
          std::begin(term_batches),
          std::end(term_batches),
          std::back_inserter(batches));
    }
    storage::log_append_config append_cfg{
      storage::log_append_config::fsync::yes, model::no_timeout};
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    std::move(reader)
      .for_each_ref(log->make_appender(append_cfg), append_cfg.timeout)
      .get();
    log->flush().get();

    auto read_batches = read_and_validate_all_batches(log);
    auto lstats = log->offsets();
    ASSERT_EQ(lstats.dirty_offset, read_batches.back().last_offset());
    ASSERT_EQ(lstats.committed_offset, read_batches.back().last_offset());
    size_t next = 0;
    int expected_term = 0;
    for (auto c : term_batches_counts) {
        for (size_t i = next; i < next + c; ++i) {
            ASSERT_EQ(read_batches[i].term(), model::term_id(expected_term));
        }
        expected_term++;
        next = next + c;
    }
}
struct custom_ts_batch_generator {
    explicit custom_ts_batch_generator(model::timestamp start_ts)
      : _start_ts(start_ts) {}

    chunked_circular_buffer<model::record_batch> operator()(
      [[maybe_unused]] std::optional<model::timestamp> ts = std::nullopt) {
        // The input timestamp is unused, this class does its own timestamping
        auto batches = model::test::make_random_batches(
                         model::offset(0), random_generators::get_int(1, 10))
                         .get();

        for (auto& b : batches) {
            b.header().first_timestamp = _start_ts;
            _start_ts = model::timestamp(_start_ts() + b.record_count());
            b.header().max_timestamp = _start_ts;
            _start_ts = model::timestamp(_start_ts() + 1);
        }
        return batches;
    }
    model::timestamp _start_ts;
};

void append_custom_timestamp_batches(
  ss::shared_ptr<storage::log> log,
  int batch_count,
  model::term_id term,
  model::timestamp base_ts) {
    auto current_ts = base_ts;
    for (int i = 0; i < batch_count; ++i) {
        iobuf key = iobuf::from("key");
        iobuf value = iobuf::from("v");

        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(key.copy(), value.copy());

        auto batch = std::move(builder).build();

        batch.set_term(term);
        batch.header().first_timestamp = current_ts;
        batch.header().max_timestamp = current_ts;
        auto reader = model::make_memory_record_batch_reader(
          {std::move(batch)});
        storage::log_append_config cfg{
          .should_fsync = storage::log_append_config::fsync::no,
          .timeout = model::no_timeout,
        };

        std::move(reader)
          .for_each_ref(log->make_appender(cfg), cfg.timeout)
          .get();
        current_ts = model::timestamp(current_ts() + 1);
    }
}

TEST_F(
  storage_test_fixture, test_timestamp_updates_when_max_timestamp_is_not_set) {
    auto append_batch_with_no_max_ts = [](
                                         ss::shared_ptr<storage::log> log,
                                         model::term_id term,
                                         model::timestamp base_ts) {
        auto current_ts = base_ts;

        iobuf key = iobuf::from("key");
        iobuf value = iobuf::from("v");

        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(key.copy(), value.copy());

        auto batch = std::move(builder).build();

        batch.set_term(term);
        batch.header().first_timestamp = current_ts;
        // EXPLICITLY SET TO MISSING
        batch.header().max_timestamp = model::timestamp::missing();
        auto reader = model::make_memory_record_batch_reader(
          {std::move(batch)});
        storage::log_append_config cfg{
          .should_fsync = storage::log_append_config::fsync::no,
          .timeout = model::no_timeout,
        };

        std::move(reader)
          .for_each_ref(log->make_appender(cfg), cfg.timeout)
          .get();
        current_ts = model::timestamp(current_ts() + 1);
    };

    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = manage_log(mgr, std::move(ntp_cfg));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(100));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(110));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(120));
    log->force_roll().get();

    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(200));
    // reordered timestamps
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(230));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(220));
    auto& segments = log->segments();
    // first segment
    ASSERT_EQ(segments[0]->index().base_timestamp(), model::timestamp(100));
    ASSERT_EQ(segments[0]->index().max_timestamp(), model::timestamp(120));
    // second segment
    ASSERT_EQ(segments[1]->index().base_timestamp(), model::timestamp(200));
    ASSERT_EQ(segments[1]->index().max_timestamp(), model::timestamp(230));
};

TEST_F(storage_test_fixture, test_time_based_eviction) {
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    ASSERT_TRUE(feature_table.local().is_active(
      features::feature::broker_time_based_retention));
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto disk_log = manage_log(mgr, std::move(ntp_cfg));

    // keep track of broker timestamp, to predict expected evictions
    auto broker_t0 = model::timestamp_clock::now();
    constexpr static auto broker_ts_sep = 5s;

    // 1. segment timestamps from 100 to 110
    // broker timestamp: broker_t0
    append_custom_timestamp_batches(
      disk_log, 10, model::term_id(0), model::timestamp(100));
    disk_log->force_roll().get();

    // 2. segment timestamps from 200 to 230
    // b ts: broker_t0 + broker_ts_sep
    ss::sleep(broker_ts_sep).get();
    append_custom_timestamp_batches(
      disk_log, 30, model::term_id(0), model::timestamp(200));
    disk_log->force_roll().get();

    // 3. segment timestamps from 231 to 250
    // b_ts: broker_t0 + broker_ts_sep + broker_ts_sep
    ss::sleep(broker_ts_sep).get();
    append_custom_timestamp_batches(
      disk_log, 20, model::term_id(0), model::timestamp(231));

    /**
     *  Log contains 3 segments with following timestamps:
     * t0         t0+sep    t0+2*sep
     * [100..110][200..230][231..261]
     */

    storage::housekeeping_config ccfg_no_compact(
      model::to_timestamp(broker_t0 - (2 * broker_ts_sep)),
      std::nullopt,
      model::offset::min(), // should prevent compaction
      model::offset::min(),
      model::offset::min(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    auto before = disk_log->offsets();
    disk_log->housekeeping(ccfg_no_compact).get();
    auto after = disk_log->offsets();
    ASSERT_EQ(after.start_offset, before.start_offset);

    auto make_compaction_cfg =
      [&as](model::timestamp_clock::time_point timestamp) {
          SUCCEED() << fmt::format("compacting up to {}", timestamp);
          return storage::housekeeping_config(
            model::to_timestamp(timestamp),
            std::nullopt,
            model::offset::max(),
            model::offset::max(),
            model::offset::max(),
            std::nullopt,
            std::nullopt,
            0ms,
            as);
      };

    // gc with timestamp -1s, all segments should be evicted
    compact_and_prefix_truncate(*disk_log, make_compaction_cfg(broker_t0 - 2s));
    ASSERT_EQ(disk_log->segments().size(), 1);
    ASSERT_EQ(
      disk_log->segments().front()->offsets().get_base_offset(),
      model::offset(40));
    ASSERT_EQ(
      disk_log->segments().back()->offsets().get_dirty_offset(),
      model::offset(59));
};

TEST_F(storage_test_fixture, test_size_based_eviction) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = manage_log(mgr, std::move(ntp_cfg));
    auto headers = append_random_batches(log, 10);
    log->flush().get();
    auto all_batches = read_and_validate_all_batches(log);
    size_t first_size = std::accumulate(
      all_batches.begin(),
      all_batches.end(),
      size_t(0),
      [](size_t acc, model::record_batch& b) { return acc + b.size_bytes(); });

    auto lstats = log->offsets();
    SUCCEED() << fmt::format("Offsets to be evicted {}", lstats);
    headers = append_random_batches(log, 10);
    auto new_batches = read_and_validate_all_batches(log);
    size_t total_size = std::accumulate(
      new_batches.begin(),
      new_batches.end(),
      size_t(0),
      [](size_t acc, model::record_batch& b) { return acc + b.size_bytes(); });

    // Set the max number of bytes to the total size of the log.
    // This will prevent compaction.
    storage::housekeeping_config ccfg_no_compact(
      model::timestamp::min(),
      total_size + first_size,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    compact_and_prefix_truncate(*log, ccfg_no_compact);

    auto new_lstats = log->offsets();
    ASSERT_EQ(new_lstats.start_offset, lstats.start_offset);

    // max log size
    const auto max_size = (total_size - first_size) + 1;

    /*
     * logic for determining the cut point. same as in disk log impl. we use
     * this here to figure out which segment we expect to get removed based on
     * the target size, then use that to check the expected new log offsets.
     */
    model::offset last_offset;
    size_t reclaimed_size = 0;
    for (auto& seg : log->segments()) {
        reclaimed_size += seg->size_bytes();
        if (log->size_bytes() - reclaimed_size < max_size) {
            break;
        }
        last_offset = seg->offsets().get_dirty_offset();
    }

    storage::housekeeping_config ccfg(
      model::timestamp::min(),
      max_size,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    compact_and_prefix_truncate(*log, ccfg);

    new_lstats = log->offsets();
    SUCCEED() << fmt::format("Final offsets {}", new_lstats);
    ASSERT_EQ(new_lstats.start_offset, last_offset + model::offset(1));

    {
        auto batches = read_and_validate_all_batches(log);
        size_t size = std::accumulate(
          batches.begin(),
          batches.end(),
          size_t(0),
          [](size_t acc, model::record_batch& b) {
              return acc + b.size_bytes();
          });
        /*
         * the max size is a soft target. in practice we can't reclaim space on
         * a per-byte granularity, and the real policy is to not violate max
         * size target, but to get as close as possible.
         */
        ASSERT_GE(size, max_size);

        /*
         * but, we do expect to have removed some data
         */
        ASSERT_LT(size, total_size);
    }
};

TEST_F(storage_test_fixture, test_eviction_notification) {
    ss::promise<model::offset> last_evicted_offset;
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = manage_log(mgr, std::move(ntp_cfg));
    (void)log->monitor_eviction(as).then(
      [&last_evicted_offset](model::offset o) mutable {
          last_evicted_offset.set_value(o);
      });

    append_random_batches(
      log,
      10,
      model::term_id(0),
      std::nullopt,
      custom_ts_batch_generator(model::timestamp::now()));
    log->flush().get();
    ss::sleep(1s).get(); // ensure time separation for broker timestamp
    model::timestamp gc_ts
      = model::timestamp::now(); // this ts is after the above batch
    auto lstats_before = log->offsets();
    SUCCEED() << fmt::format("Offsets to be evicted {}", lstats_before);
    ss::sleep(1s).get(); // ensure time separation between gc_ts and next batch
    append_random_batches(
      log,
      10,
      model::term_id(0),
      std::nullopt,
      custom_ts_batch_generator(model::timestamp(gc_ts() + 10)));
    storage::housekeeping_config ccfg(
      gc_ts,
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    log->housekeeping(ccfg).get();

    std::ignore = last_evicted_offset.get_future().get();
    log->housekeeping(ccfg).get();
    auto lstats_after = log->offsets();

    ASSERT_EQ(lstats_before.start_offset, lstats_after.start_offset);
};

/**
 * Appends batch_count batches which have exactly batch_sz bytes when
 * serialized, with each batch having 1 record.
 */
ss::future<storage::append_result> append_exactly(
  ss::shared_ptr<storage::log> log,
  size_t batch_count,
  size_t batch_sz,
  std::optional<bytes> key = std::nullopt,
  model::record_batch_type batch_type = model::record_batch_type::raft_data) {
    vassert(
      batch_sz > model::packed_record_batch_header_size,
      "Batch size must be greater than {}, requested {}",
      model::packed_record_batch_header_size,
      batch_sz);
    storage::log_append_config append_cfg{
      storage::log_append_config::fsync::no, model::no_timeout};

    chunked_circular_buffer<model::record_batch> batches;
    auto val_sz = batch_sz - model::packed_record_batch_header_size;
    iobuf key_buf{};

    if (key) {
        key_buf = bytes_to_iobuf(*key);
    }

    auto real_batch_size = sizeof(model::record_attributes::type) // attributes
                           + vint::vint_size(0)   // timestamp delta
                           + vint::vint_size(0)   // offset_delta
                           + key_buf.size_bytes() // key size
                           + vint::vint_size(0)   // headers size
                           + 2;

    if (key) {
        real_batch_size += vint::vint_size(
          static_cast<int32_t>(key_buf.size_bytes()));
    } else {
        real_batch_size += vint::vint_size(-1);
    }

    real_batch_size += vint::vint_size(val_sz - real_batch_size);

    val_sz -= real_batch_size;

    for (size_t i = 0; i < batch_count; ++i) {
        storage::record_batch_builder builder(batch_type, model::offset{});
        iobuf value = bytes_to_iobuf(tests::random_bytes(val_sz));
        builder.add_raw_kv(key_buf.copy(), std::move(value));

        batches.push_back(std::move(builder).build());
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    return std::move(rdr).for_each_ref(
      log->make_appender(append_cfg), model::no_timeout);
}

TEST_F(storage_test_fixture, write_concurrently_with_gc) {
    auto cfg = default_log_config(test_dir);
    // make sure segments are small
    cfg.max_segment_size = config::mock_binding<size_t>(1000);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    model::offset last_append_offset{};
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    append_exactly(log, 10, 100).get();
    ASSERT_EQ(log->offsets().dirty_offset, model::offset(9));

    std::vector<ss::future<>> futures;

    ss::semaphore _sem{1};
    int appends = 100;
    int batches_per_append = 5;
    auto compact = [&log, &as]() mutable {
        storage::housekeeping_config ccfg(
          model::timestamp::min(),
          1000,
          model::offset::max(),
          model::offset::max(),
          model::offset::max(),
          std::nullopt,
          std::nullopt,
          0ms,
          as);
        return log->housekeeping(ccfg);
    };

    auto append =
      [&log, &_sem, batches_per_append, &last_append_offset]() mutable {
          return ss::with_semaphore(
            _sem, 1, [&log, batches_per_append, &last_append_offset]() mutable {
                return ss::sleep(10ms).then(
                  [&log, batches_per_append, &last_append_offset] {
                      return append_exactly(log, batches_per_append, 100)
                        .then([&last_append_offset](
                                storage::append_result result) mutable {
                            ASSERT_GT(result.last_offset, last_append_offset);
                            last_append_offset = result.last_offset;
                        });
                  });
            });
      };

    auto loop = ss::do_until([&as] { return as.abort_requested(); }, compact);

    for (int i = 0; i < appends; ++i) {
        futures.push_back(append());
    }

    ss::when_all(futures.begin(), futures.end()).get();

    as.request_abort();
    loop.get();
    auto lstats_after = log->offsets();
    ASSERT_EQ(
      lstats_after.dirty_offset,
      model::offset(9 + appends * batches_per_append));
};

/**
 * This test executes operations which may be executed by Raft layer without
 * synchronization. i.e. appends, reads, flushes and prefix truncations. The
 * test validates if the offsets are correctly assigned i.e. if any batch did
 * not get the same offset assigned twice.
 */
TEST_F(storage_test_fixture, append_concurrent_with_prefix_truncate) {
    auto cfg = default_log_config(test_dir);
    // start stress fiber to make the test less likely to trigger race
    // conditions
    stress_fiber_manager stress_mgr;
    stress_config stress_cfg;
    stress_cfg.min_spins_per_scheduling_point = random_generators::get_int(
      1, 100);
    stress_cfg.max_spins_per_scheduling_point = random_generators::get_int(
      1000, 10000);
    stress_cfg.num_fibers = random_generators::get_int(100, 200);
    stress_mgr.start(stress_cfg);

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    model::offset last_append_base_offset{};
    model::offset last_append_end_offset{};
    auto ntp = model::controller_ntp;

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = manage_log(mgr, std::move(ntp_cfg));

    bool stop = false;
    size_t cnt = 0;
    std::vector<model::record_batch_type> types{
      model::record_batch_type::raft_data,
      model::record_batch_type::raft_configuration};
#ifndef NDEBUG
    static constexpr size_t stop_after = 10;
#else
    static constexpr size_t stop_after = 200;
#endif
    auto append = [&] {
        return append_exactly(
                 log,
                 1,
                 random_generators::get_int(75, 237),
                 std::nullopt,
                 random_generators::random_choice(types))
          .then([&](storage::append_result result) {
              SUCCEED() << fmt::format("append result: {}", result);
              vassert(
                result.base_offset > last_append_base_offset,
                "Invalid append result base offset: {}. The same base offset "
                "was already assigned.",
                last_append_base_offset,
                result.base_offset);
              vassert(
                result.last_offset > last_append_end_offset,
                "Invalid append result last offset: {}. The same last offset "
                "was already assigned.",
                last_append_end_offset,
                result.last_offset);
              last_append_base_offset = result.base_offset;
              last_append_end_offset = result.last_offset;
              cnt++;
              if (cnt >= stop_after) {
                  stop = true;
              }
              return ss::sleep(
                std::chrono::milliseconds(random_generators::get_int(10, 100)));
          });
    };

    auto flush = [&] { return log->flush().discard_result(); };

    auto read = [&] {
        auto lstats = log->offsets();
        return log
          ->make_reader(
            storage::local_log_reader_config(
              lstats.start_offset, model::offset::max()))
          .then([](auto reader) {
              return ss::sleep(
                       std::chrono::milliseconds(
                         random_generators::get_int(15, 30)))
                .then([r = std::move(reader)]() mutable {
                    return model::consume_reader_to_memory(
                      std::move(r), model::no_timeout);
                })
                .discard_result();
          });
    };

    auto prefix_truncate = [&] {
        auto offset = model::next_offset(log->offsets().dirty_offset);

        return log->truncate_prefix(storage::truncate_prefix_config(offset))
          .then([offset] {
              SUCCEED() << fmt::format("prefix truncate at: {}", offset);
              return ss::sleep(
                std::chrono::milliseconds(random_generators::get_int(5, 20)));
          });
    };
    /**
     * Execute all operations concurrently
     */
    auto f_1 = ss::do_until([&] { return stop; }, [&] { return append(); });
    auto f_2 = ss::do_until(
      [&] { return stop; }, [&] { return prefix_truncate(); });
    auto f_3 = ss::do_until([&] { return stop; }, [&] { return read(); });
    auto f_4 = ss::do_until([&] { return stop; }, [&] { return flush(); });

    ss::when_all(std::move(f_1), std::move(f_2), std::move(f_3), std::move(f_4))
      .get();
    stress_mgr.stop().get();
};

TEST_F(storage_test_fixture, empty_segment_recovery) {
    auto cfg = default_log_config(test_dir);
    auto ntp = model::ntp("default", "test", 0);
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    /**
     * 1) add segment
     * 2) append some batches (no flush)
     * 3) truncate
     * 4) add empty segment
     */

    storage::disk_log_builder builder(cfg);
    model::record_batch_type bt = model::record_batch_type::raft_data;
    using should_flush_t = storage::disk_log_builder::should_flush_after;
    storage::log_append_config appender_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .timeout = model::no_timeout};
    builder | storage::start(ntp) | storage::add_segment(0)
      | storage::add_random_batch(
        0,
        1,
        storage::maybe_compress_batches::yes,
        bt,
        appender_cfg,
        should_flush_t::no)
      | storage::add_random_batch(
        1,
        5,
        storage::maybe_compress_batches::yes,
        bt,
        appender_cfg,
        should_flush_t::no)
      | storage::add_random_batch(
        6,
        140,
        storage::maybe_compress_batches::yes,
        bt,
        appender_cfg,
        should_flush_t::no);

    builder.get_log()
      ->truncate(storage::truncate_config(model::offset(6)))
      .get();

    builder | storage::add_segment(6);
    builder.stop().get();

    /**
     * Log state after setup
     *
     * 1) after append
     *
     *          {segment #1: [0,0][1,5][6,145]}
     *
     * 2) after truncate
     *
     *          {segment #1: [0,0][1,5]}
     *
     * 3) after adding new segment
     *
     *          {segment #1: [0,0][1,5]}{segment #2: }
     */

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    auto offsets_after_recovery = log->offsets();

    // Append single batch
    storage::log_appender appender = log->make_appender(
      storage::log_append_config{
        .should_fsync = storage::log_append_config::fsync::no,
        .timeout = model::no_timeout});
    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(0), 1, false));

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    rdr.for_each_ref(std::move(appender), model::no_timeout).get();

    // we truncate at {6} so we expect dirty offset equal {5}
    ASSERT_EQ(offsets_after_recovery.dirty_offset, model::offset(5));

    // after append we expect offset to be equal to {6} as one record was
    // appended
    ASSERT_EQ(log->offsets().dirty_offset, model::offset(6));
}

TEST_F(storage_test_fixture, test_compaction_preserve_state) {
    auto cfg = default_log_config(test_dir);
    auto ntp = model::ntp("default", "test", 0);
    // compacted topic
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    ss::abort_source as;

    storage::disk_log_builder builder(cfg);
    model::record_batch_type bt = model::record_batch_type::raft_configuration;
    using should_flush_t = storage::disk_log_builder::should_flush_after;
    storage::log_append_config appender_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .timeout = model::no_timeout};

    // single segment
    builder | storage::start(ntp) | storage::add_segment(0)
      | storage::add_random_batch(
        0,
        1,
        storage::maybe_compress_batches::no,
        bt,
        appender_cfg,
        should_flush_t::no)
      | storage::add_random_batch(
        1,
        1,
        storage::maybe_compress_batches::no,
        bt,
        appender_cfg,
        should_flush_t::no);

    builder.stop().get();
    SUCCEED() << fmt::format("Before recovery: {}", builder.get_log());
    // recover
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));

    storage::housekeeping_config compaction_cfg(
      model::timestamp::min(),
      1,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    auto log = manage_log(mgr, std::move(ntp_cfg));
    auto offsets_after_recovery = log->offsets();
    SUCCEED() << fmt::format("After recovery: {}", *log);
    // trigger compaction
    log->housekeeping(compaction_cfg).get();
    auto offsets_after_compact = log->offsets();
    SUCCEED() << fmt::format(
      "After compaction, offsets: {}, {}", offsets_after_compact, *log);

    // Append single batch
    storage::log_appender appender = log->make_appender(
      storage::log_append_config{
        .should_fsync = storage::log_append_config::fsync::no,
        .timeout = model::no_timeout});

    chunked_circular_buffer<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(0), 1, false));

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    std::move(rdr).for_each_ref(std::move(appender), model::no_timeout).get();

    // before append offsets should be equal to {1}, as we stopped there
    ASSERT_EQ(offsets_after_recovery.dirty_offset, model::offset(1));
    ASSERT_EQ(offsets_after_compact.dirty_offset, model::offset(1));

    // after append we expect offset to be equal to {2}
    ASSERT_EQ(log->offsets().dirty_offset, model::offset(2));
}

ss::future<> append_single_record_batch_coro(
  ss::shared_ptr<storage::log> log,
  int cnt,
  model::term_id term,
  size_t val_size = 0,
  bool rand_key = false) {
    for (int i = 0; i < cnt; ++i) {
        ss::sstring key_str;
        if (rand_key) {
            key_str = ssx::sformat(
              "key_{}", random_generators::get_int<uint64_t>());
        } else {
            key_str = "key";
        }
        iobuf key = iobuf::from(key_str.c_str());
        bytes val_bytes;
        if (val_size > 0) {
            val_bytes = tests::random_bytes(val_size);
        } else {
            ss::sstring v = ssx::sformat("v-{}", i);
            val_bytes = bytes::from_string(v.c_str());
        }
        iobuf value = bytes_to_iobuf(val_bytes);
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        builder.add_raw_kv(std::move(key), std::move(value));
        auto batch = std::move(builder).build();
        batch.set_term(term);
        auto reader = model::make_memory_record_batch_reader(
          {std::move(batch)});
        storage::log_append_config cfg{
          .should_fsync = storage::log_append_config::fsync::no,
          .timeout = model::no_timeout,
        };

        co_await std::move(reader).for_each_ref(
          log->make_appender(cfg), cfg.timeout);
    }
}

void append_single_record_batch(
  ss::shared_ptr<storage::log> log,
  int cnt,
  model::term_id term,
  size_t val_size = 0,
  bool rand_key = false) {
    append_single_record_batch_coro(log, cnt, term, val_size, rand_key).get();
}

/**
 * Test scenario:
 *   1) append few single record batches in term 1
 *   2) truncate in the middle of segment
 *   3) append some more batches to the same segment
 *   4) roll term by appending to new segment
 *   5) restart log manager
 *
 * NOTE:
 *  flushing after each operation doesn't influence the test
 *
 * Expected outcome:
 *   Segment offsets should be correctly recovered.
 */
TEST_F(storage_test_fixture, truncate_and_roll_segment) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;

    {
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

        SUCCEED() << fmt::format("config: {}", mgr.config());
        auto ntp = model::ntp("default", "test", 0);
        auto log = manage_log(
          mgr, storage::ntp_config(ntp, mgr.config().base_dir));
        // 1) append few single record batches in term 1
        append_single_record_batch(log, 14, model::term_id(1));
        log->flush().get();
        check_dirty_and_closed_segment_bytes(log);
        // 2) truncate in the middle of segment
        model::offset truncate_at(7);
        SUCCEED() << fmt::format("Truncating at offset:{}", truncate_at);
        log->truncate(storage::truncate_config(truncate_at)).get();
        // 3) append some more batches to the same segment
        append_single_record_batch(log, 10, model::term_id(1));
        log->flush().get();
        check_dirty_and_closed_segment_bytes(log);
        //  4) roll term by appending to new segment
        append_single_record_batch(log, 1, model::term_id(8));
        log->flush().get();
        check_dirty_and_closed_segment_bytes(log);
    }
    // 5) restart log manager
    {
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

        auto ntp = model::ntp("default", "test", 0);
        auto log = manage_log(
          mgr, storage::ntp_config(ntp, mgr.config().base_dir));
        auto read = read_and_validate_all_batches(log);

        for (model::offset o(0); o < log->offsets().committed_offset; ++o) {
            ASSERT_TRUE(log->get_term(o).has_value());
        }
    }
}

TEST_F(storage_test_fixture, compacted_log_truncation) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    ss::abort_source as;
    {
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

        SUCCEED() << fmt::format("config: {}", mgr.config());
        auto ntp = model::ntp("default", "test", 0);
        auto log = manage_log(
          mgr,
          storage::ntp_config(
            ntp,
            mgr.config().base_dir,
            std::make_unique<storage::ntp_config::default_overrides>(
              overrides)));
        // append some batches to first segment (all batches have the same key)
        append_single_record_batch(log, 14, model::term_id(1));

        storage::housekeeping_config c_cfg(
          model::timestamp::min(),
          std::nullopt,
          model::offset::max(),
          model::offset::max(),
          model::offset::max(),
          std::nullopt,
          std::nullopt,
          0ms,
          as);
        log->flush().get();
        check_dirty_and_closed_segment_bytes(log);
        model::offset truncate_at(7);
        SUCCEED() << fmt::format("Truncating at offset:{}", truncate_at);
        log->truncate(storage::truncate_config(truncate_at)).get();
        // roll segment
        append_single_record_batch(log, 10, model::term_id(2));
        log->flush().get();
        check_dirty_and_closed_segment_bytes(log);

        // roll segment
        append_single_record_batch(log, 1, model::term_id(8));
        check_dirty_and_closed_segment_bytes(log);
        // compact log
        log->housekeeping(c_cfg).get();
        check_dirty_and_closed_segment_bytes(log);
    }

    // force recovery
    {
        SUCCEED() << "recovering";
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
        auto ntp = model::ntp("default", "test", 0);
        auto log = manage_log(
          mgr, storage::ntp_config(ntp, mgr.config().base_dir));

        auto read = read_and_validate_all_batches(log);
        auto lstats = log->offsets();
        for (model::offset o = lstats.start_offset; o < lstats.committed_offset;
             ++o) {
            ASSERT_TRUE(log->get_term(o).has_value());
        }
    }
}

TEST_F(storage_test_fixture, check_segment_roll_after_compacted_log_truncate) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    ss::abort_source as;

    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));
    // append some batches to first segment (all batches have the same key)
    append_single_record_batch(log, 14, model::term_id(1));

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    log->flush().get();
    check_dirty_and_closed_segment_bytes(log);
    model::offset truncate_at(7);
    SUCCEED() << fmt::format("Truncating at offset:{}", truncate_at);
    ASSERT_EQ(log->segment_count(), 1);
    log->truncate(storage::truncate_config(truncate_at)).get();
    append_single_record_batch(log, 10, model::term_id(1));
    log->flush().get();
    check_dirty_and_closed_segment_bytes(log);

    // segment should be rolled after truncation
    ASSERT_EQ(log->segment_count(), 2);
    log->housekeeping(c_cfg).get();
    check_dirty_and_closed_segment_bytes(log);

    auto read = read_and_validate_all_batches(log);
    ASSERT_EQ(read.begin()->base_offset(), model::offset(6));
    ASSERT_EQ(read.begin()->last_offset(), model::offset(6));
    ASSERT_EQ(read[read.size() - 1].base_offset(), model::offset(16));
    ASSERT_EQ(read[read.size() - 1].last_offset(), model::offset(16));
}

TEST_F(storage_test_fixture, check_max_segment_size) {
    auto cfg = default_log_config(test_dir);

    auto mock = config::mock_property<size_t>(20_GiB);
    // defaults
    cfg.max_segment_size = mock.bind();
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    using overrides_t = storage::ntp_config::default_overrides;
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = manage_log(mgr, std::move(ntp_cfg));

    ASSERT_EQ(log->segments().size(), 0);

    // Write 100 * 1_KiB batches, should yield only 1 segment
    auto result = append_exactly(log, 100, 1_KiB).get(); // 100*1_KiB
    int size1 = log->segments().size();
    ASSERT_EQ(size1, 1);

    // Update cluster level configuration and force a roll.
    mock.update(20_KiB);
    log->force_roll().get();

    // 30 * 1_KiB should yield 2 new segments.
    result = append_exactly(log, 30, 1_KiB).get();
    int size2 = log->segments().size();
    ASSERT_EQ(size2 - size1, 2);

    // override segment size with ntp_config
    overrides_t ov;
    ov.segment_size = 40_KiB;
    log->set_overrides(ov);
    log->force_roll().get();

    // 60 * 1_KiB batches should yield 2 segments.
    result = append_exactly(log, 60, 1_KiB).get(); // 60*1_KiB
    int size3 = log->segments().size();
    ASSERT_EQ(size3 - size2, 2);
}

TEST_F(storage_test_fixture, check_max_segment_size_limits) {
    auto cfg = default_log_config(test_dir);

    // Apply limits to the effective segment size: we may configure
    // something different per-topic, but at runtime the effective
    // segment size will be clamped to this range.
    config::shard_local_cfg().log_segment_size_min.set_value(
      std::make_optional(50_KiB));
    config::shard_local_cfg().log_segment_size_max.set_value(
      std::make_optional(200_KiB));

    std::exception_ptr ex;
    try {
        // Initially 100KiB configured segment size, it is within the range
        auto mock = config::mock_property<size_t>(100_KiB);

        cfg.max_segment_size = mock.bind();

        ss::abort_source as;
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
        auto ntp = model::ntp("default", "test", 0);
        storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
        auto log = manage_log(mgr, std::move(ntp_cfg));

        ASSERT_EQ(log->segments().size(), 0);

        // Write 100 * 1_KiB batches, should yield 1 full segment
        auto result = append_exactly(log, 50, 1_KiB).get(); // 100*1_KiB
        ASSERT_EQ(log->segments().size(), 1);
        result = append_exactly(log, 100, 1_KiB).get(); // 100*1_KiB
        ASSERT_EQ(log->segments().size(), 2);

        // A too-low segment size: should be clamped to the lower bound
        mock.update(1_KiB);
        log->force_roll().get();
        ASSERT_EQ(log->segments().size(), 3);

        // Exceeding the apparent segment size doesn't roll, because it was
        // clamped
        result = append_exactly(log, 5, 1_KiB).get();
        ASSERT_EQ(log->segments().size(), 3);
        // Exceeding the lower bound segment size does cause a roll
        result = append_exactly(log, 55, 1_KiB).get();
        ASSERT_EQ(log->segments().size(), 4);

        // A too-high segment size: should be clamped to the upper bound
        mock.update(2000_KiB);
        log->force_roll().get();
        ASSERT_EQ(log->segments().size(), 5);
        // Exceeding the upper bound causes a roll, even if we didn't reach
        // the user-configured segment size
        result = append_exactly(log, 201, 1_KiB).get();
        ASSERT_EQ(log->segments().size(), 6);
    } catch (...) {
        ex = std::current_exception();
    }

    config::shard_local_cfg().log_segment_size_min.reset();
    config::shard_local_cfg().log_segment_size_max.reset();

    if (ex) {
        throw ex;
    }
}

TEST_F(storage_test_fixture, partition_size_while_cleanup) {
    auto cfg = default_log_config(test_dir);
    // make sure segments are small
    cfg.max_segment_size = config::mock_binding<size_t>(10_KiB);
    cfg.compacted_segment_size = config::mock_binding<size_t>(10_KiB);
    // we want force reading most recent compacted batches, disable the cache
    cfg.cache = storage::with_cache::no;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    // enable both deletion and compaction
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion
                                 | model::cleanup_policy_bitflags::compaction;

    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    auto sz_initial = log->get_probe().partition_size();
    SUCCEED() << fmt::format("sz_initial={}", sz_initial);
    ASSERT_EQ(sz_initial, 0);

    // Add 100 batches with one event each, all events having the same key
    static constexpr size_t batch_size = 1_KiB; // Size visible to Kafka API
    static constexpr size_t input_batch_count = 100;

    append_exactly(
      log, input_batch_count, batch_size, bytes::from_string("key"))
      .get(); // 100*1_KiB

    // Test becomes non-deterministic if we allow flush in background: flush
    // explicitly instead.
    log->flush().get();

    // Read back and validate content of log pre-compaction.
    ASSERT_EQ(
      log->get_probe().partition_size(), input_batch_count * batch_size);
    ASSERT_EQ(read_and_validate_all_batches(log).size(), input_batch_count);
    auto lstats_before = log->offsets();
    ASSERT_EQ(
      lstats_before.committed_offset, model::offset{input_batch_count - 1});
    ASSERT_EQ(lstats_before.start_offset, model::offset{0});

    storage::housekeeping_config ccfg(
      model::timestamp::min(),
      50_KiB,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    // Compact 10 times, with a configuration calling for 60kiB max log size.
    // This results in prefix truncating at offset 50.
    for (int i = 0; i < 10; ++i) {
        compact_and_prefix_truncate(*log, ccfg);
    }
    log->get_probe().partition_size();

    as.request_abort();
    auto lstats_after = log->offsets();
    ASSERT_EQ(lstats_after.committed_offset, lstats_before.committed_offset);
    ASSERT_EQ(lstats_after.start_offset, model::offset{50});

    auto batches = read_and_validate_all_batches(log);
    auto total_batch_size = std::accumulate(
      batches.begin(),
      batches.end(),
      0,
      [](size_t sum, const model::record_batch& b) {
          return sum + b.size_bytes();
      });

    auto& segments = log->segments();
    // One historic segment (all historic batches compacted down to 1), plus
    // one active segment.
    ASSERT_EQ(segments.size(), 2);

    // The log-scope reported size must be equal to the sum of the batch sizes
    auto expected_size = total_batch_size;
    auto actual_size = log->get_probe().partition_size();
    SUCCEED() << fmt::format(
      "expected_size={}, actual_size={}", expected_size, actual_size);
    ASSERT_EQ(actual_size, expected_size);
};

TEST_F(storage_test_fixture, check_segment_size_jitter) {
    auto cfg = default_log_config(test_dir);

    // Switch on jitter: it is off by default in default_log_config because
    // for most tests randomness is undesirable.
    cfg.segment_size_jitter = storage::jitter_percents{5};

    // defaults
    cfg.max_segment_size = config::mock_binding<size_t>(100_KiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    std::vector<log_holder> logs;
    for (int i = 0; i < 5; ++i) {
        auto ntp = model::ntp("default", ssx::sformat("test-{}", i), 0);
        storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
        auto log = manage_log(mgr, std::move(ntp_cfg));
        append_exactly(log, 2000, 100).get();
        logs.push_back(std::move(log));
    }
    std::vector<size_t> sizes;
    for (auto& l : logs) {
        auto& segs = l->segments();
        sizes.push_back((*segs.begin())->size_bytes());
    }
    ASSERT_EQ(
      std::all_of(
        sizes.begin(),
        sizes.end(),
        [size = sizes[0]](auto other) { return size == other; }),
      false);
}

TEST_F(storage_test_fixture, adjacent_segment_compaction) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // build some segments
    append_single_record_batch(log, 20, model::term_id(1));
    log->force_roll().get();
    append_single_record_batch(log, 30, model::term_id(1));
    log->force_roll().get();
    append_single_record_batch(log, 40, model::term_id(1));
    log->force_roll().get();
    append_single_record_batch(log, 50, model::term_id(1));
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 4);

    auto all_have_broker_timestamp = [&] {
        auto segments = std::vector<ss::lw_shared_ptr<storage::segment>>(
          log->segments().begin(), log->segments().end());
        ASSERT_TRUE(
          std::ranges::all_of(
            segments,
            [](auto bt) { return bt.has_value(); },
            [](auto& seg) { return seg->index().broker_timestamp(); }));
    };

    all_have_broker_timestamp();

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    // There are 4 segments, and the last is the active segments.

    // Check if it honors max_compactible offset by setting it to the base
    // offset of first segment. Nothing should be compacted.
    const auto first_segment_offsets = log->segments().front()->offsets();
    c_cfg.compact.max_removable_local_log_offset
      = first_segment_offsets.get_base_offset();
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 4);

    // Now compact without restricting removable offset.
    c_cfg.compact.max_removable_local_log_offset = model::offset::max();

    // The first three will merge.
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 2);

    // No change since we can't combine with appender segment.
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 2);
    all_have_broker_timestamp();
}

TEST_F(storage_test_fixture, adjacent_segment_compaction_terms) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // build some segments
    append_single_record_batch(log, 20, model::term_id(1));
    append_single_record_batch(log, 30, model::term_id(2));
    log->force_roll().get();
    append_single_record_batch(log, 30, model::term_id(2));
    append_single_record_batch(log, 40, model::term_id(3));
    append_single_record_batch(log, 50, model::term_id(4));
    append_single_record_batch(log, 50, model::term_id(5));
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 6);

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    // compact all the individual segments
    // the two segments with term 2 can be combined
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 5);

    // no more pairs with the same term
    log->housekeeping(c_cfg).get();
    log->housekeeping(c_cfg).get();
    log->housekeeping(c_cfg).get();
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 5);

    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(log->segments()[i]->offsets().get_term()(), i + 1);
    }
}

TEST_F(storage_test_fixture, max_adjacent_segment_compaction) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(6_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // add a segment with random keys until a certain size
    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    add_segment(2_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(2_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(5_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 6);

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    // self compaction steps
    // the first two segments are combined 2+2=4 < 6 MB
    // the fourth, fifth and sixth can be combined 5 + 16KB + 16KB < 6 MB
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 3);

    // that's all that can be done. the next seg is an appender
    log->housekeeping(c_cfg).get();
    ASSERT_EQ(log->segment_count(), 3);
}

TEST_F(storage_test_fixture, adjacent_segment_compaction_range_u32_bounds) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("kafka", "tapioca", 0);
    auto log = manage_log(mgr, storage::ntp_config(ntp, mgr.config().base_dir));
    auto* disk_log = dynamic_cast<storage::disk_log_impl*>(log.get());

    append_single_record_batch(log, 1, model::term_id(0));
    disk_log->force_roll().get();
    append_single_record_batch(log, 1, model::term_id(0));
    disk_log->force_roll().get();
    log->flush().get();
    ASSERT_EQ(disk_log->segment_count(), 3);

    auto& segs = disk_log->segments();

    // Need to mark the segments as self compacted so that they are eligible for
    // adjacent segment compaction.
    for (auto& seg : segs) {
        if (!seg->has_appender()) {
            seg->index().maybe_set_self_compact_timestamp(
              model::timestamp::now());
        }
    }

    // Last non-active segment
    auto& back = segs[1];

    int64_t u32_max = static_cast<int64_t>(
      std::numeric_limits<uint32_t>::max());

    // const_cast is ridiculous
    auto& back_offset_tracker = const_cast<storage::segment::offset_tracker&>(
      back->offsets());

    // Set the last non-active segment's dirty offset to one past the uint32_t
    // max- this should make the segment non-eligible for adjacent compaction
    // with the first segment with a base offset of 0.
    int64_t one_past_u32_max = u32_max + 1;
    back_offset_tracker.set_offset(
      storage::segment::offset_tracker::dirty_offset_t{one_past_u32_max});

    ss::abort_source as;
    compaction::compaction_config cfg(
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      as);
    auto ranges = disk_log->find_adjacent_compaction_ranges(cfg);
    ASSERT_TRUE(!ranges.has_value());

    // Set the last non-active segment's dirty offset to the uint32_t
    // max- this should make the segment eligible for adjacent compaction
    // with the first segment, since the offset range no longer exceeds the
    // uint32_t max.
    back_offset_tracker.set_offset(
      storage::segment::offset_tracker::dirty_offset_t{u32_max});
    ranges = disk_log->find_adjacent_compaction_ranges(cfg);
    ASSERT_TRUE(ranges.has_value());
    ASSERT_EQ(ranges->size(), 1);
    auto& range = ranges->front();

    auto range_segments = std::vector<ss::lw_shared_ptr<storage::segment>>(
      range.first, range.second);

    // Compare filenames for equality
    ASSERT_EQ(range_segments[0]->filename(), segs[0]->filename());
    ASSERT_EQ(range_segments[1]->filename(), segs[1]->filename());
};

TEST_F(storage_test_fixture, many_segment_locking) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    append_single_record_batch(log, 20, model::term_id(1));
    log->force_roll().get();
    append_single_record_batch(log, 30, model::term_id(2));
    log->force_roll().get();
    append_single_record_batch(log, 40, model::term_id(3));
    log->force_roll().get();
    append_single_record_batch(log, 50, model::term_id(4));
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 4);

    chunked_vector<ss::lw_shared_ptr<storage::segment>> segments;
    std::copy(
      log->segments().begin(),
      log->segments().end(),
      std::back_inserter(segments));
    segments.pop_back(); // discard the active segment
    ASSERT_EQ(segments.size(), 3);

    {
        auto locks = storage::internal::write_lock_segments(
                       segments, std::chrono::seconds(1), 1)
                       .get();
        ASSERT_TRUE(locks.size() == segments.size());
    }

    {
        auto lock = segments[2]->write_lock().get();
        ASSERT_THROW(
          storage::internal::write_lock_segments(
            segments, std::chrono::seconds(1), 1)
            .get(),
          ss::semaphore_timed_out);
    }

    {
        auto locks = storage::internal::write_lock_segments(
                       segments, std::chrono::seconds(1), 1)
                       .get();
        ASSERT_TRUE(locks.size() == segments.size());
    }
}

TEST_F(storage_test_fixture, reader_reusability_test_parser_header) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));
    // first small batch
    append_exactly(log, 1, 128).get();
    // then large batches
    append_exactly(log, 1, 128_KiB).get();
    append_exactly(log, 1, 128_KiB).get();
    log->flush().get();

    storage::local_log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      4096,
      std::nullopt,
      std::nullopt,
      std::nullopt);

    /**
     * Turn on strict max bytes to leave header in parser
     */
    reader_cfg.strict_max_bytes = true;

    model::offset next_to_read;
    {
        auto reader = log->make_reader(reader_cfg).get();

        auto rec = model::consume_reader_to_memory(
                     std::move(reader), model::no_timeout)
                     .get();
        next_to_read = rec.back().last_offset() + model::offset(1);
        ASSERT_EQ(rec.size(), 1);
    }
    {
        reader_cfg.start_offset = next_to_read;
        reader_cfg.max_bytes = 150_KiB;
        auto reader = log->make_reader(reader_cfg).get();

        auto rec = model::consume_reader_to_memory(
                     std::move(reader), model::no_timeout)
                     .get();

        ASSERT_EQ(rec.size(), 1);
    }
}

TEST_F(storage_test_fixture, compaction_backlog_calculation) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    overrides.min_cleanable_dirty_ratio = tristate<double>{0.0};

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // add a segment with random keys until a certain size
    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    add_segment(2_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(2_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(5_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 5);

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    auto& segments = log->segments();
    {
        auto backlog_size = log->compaction_backlog();
        size_t expected_backlog = 0;
        for (auto& s : segments) {
            if (!s->has_appender()) {
                expected_backlog += s->size_bytes();
            }
        }
        ASSERT_EQ(backlog_size, expected_backlog);
    }

    // Perform compaction.
    log->housekeeping(c_cfg).get();

    ASSERT_EQ(log->segment_count(), 2);
    {
        auto backlog_size = log->compaction_backlog();
        // Log should be fully clean.
        ASSERT_EQ(backlog_size, 0);
    }

    // Addition of a new segment results in all of the bytes of the log being
    // added to backlog due to min.cleanable.dirty.ratio=0.0
    add_segment(16_KiB, model::term_id(1));
    log->force_roll().get();
    ASSERT_EQ(log->segment_count(), 3);
    {
        auto backlog_size = log->compaction_backlog();
        size_t expected_backlog = 0;
        for (auto& s : segments) {
            if (!s->has_appender()) {
                expected_backlog += s->size_bytes();
            }
        }
        ASSERT_EQ(backlog_size, expected_backlog);
    }
}

TEST_F(storage_test_fixture, not_compacted_log_backlog) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // add a segment with random keys until a certain size
    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    add_segment(2_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(2_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(5_MiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->force_roll().get();
    add_segment(16_KiB, model::term_id(1));
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 5);

    ASSERT_EQ(log->compaction_backlog(), 0);
}

ss::future<model::record_batch_reader::data_t> copy_reader_to_memory(
  model::record_batch_reader& reader,
  model::timeout_clock::time_point timeout) {
    using data_t = model::record_batch_reader::data_t;
    class memory_batch_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
            _result.push_back(b.copy());
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        data_t end_of_stream() { return std::move(_result); }

    private:
        data_t _result;
    };
    return reader.for_each_ref(memory_batch_consumer{}, timeout);
}

TEST_F(storage_test_fixture, disposing_in_use_reader) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));
    for (auto i = 1; i < 100; ++i) {
        append_single_record_batch(log, 1, model::term_id(1), 128, true);
    }
    log->flush().get();
    // read only up to 4096 bytes, this way a reader will still be in a cache
    storage::local_log_reader_config reader_cfg(
      model::offset(0),
      model::offset::max(),
      4096,
      std::nullopt,
      std::nullopt,
      std::nullopt);

    auto truncate_f = ss::now();
    {
        auto reader = log->make_reader(reader_cfg).get();

        auto rec = copy_reader_to_memory(reader, model::no_timeout).get();

        ASSERT_EQ(rec.back().last_offset(), model::offset(17));
        truncate_f = log->truncate(storage::truncate_config(model::offset(5)));
        // yield to allow truncate fiber to reach waiting for a lock
        ss::sleep(200ms).get();
    }
    // yield again to allow truncate fiber to finish
    tests::cooperative_spin_wait_with_timeout(200ms, [&truncate_f]() {
        return truncate_f.available();
    }).get();

    // we should be able to finish truncate immediately since reader was
    // destroyed
    ASSERT_TRUE(truncate_f.available());
    truncate_f.get();
}

model::record_batch make_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (auto i = 0; i < 2; ++i) {
        builder.add_raw_kv(
          bytes_to_iobuf(tests::random_bytes(128)),
          bytes_to_iobuf(tests::random_bytes(10_KiB)));
    }
    return std::move(builder).build();
}

TEST_F(storage_test_fixture, committed_offset_updates) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(500_MiB);
    cfg.file_config = std::nullopt;
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    auto append = [&] {
        // Append single batch
        storage::log_appender appender = log->make_appender(
          storage::log_append_config{
            .should_fsync = storage::log_append_config::fsync::no,
            .timeout = model::no_timeout});

        chunked_circular_buffer<model::record_batch> batches;
        batches.push_back(
          model::test::make_random_batch(model::offset(0), 1, false));

        auto rdr = model::make_memory_record_batch_reader(std::move(batches));
        return std::move(rdr).for_each_ref(
          std::move(appender), model::no_timeout);
    };

    ssx::mutex write_mutex{"e2e_test::write_mutex"};
    /**
     * Sequence of events is as follow:
     *
     * 1. acquire mutex
     * 2. append batches to log
     * 3. dispatch log flush
     * 4. release mutex
     * 5. when flush finishes validate if committed offset >= append result
     *
     */
    auto append_with_lock = [&] {
        return write_mutex.get_units().then([&](ssx::semaphore_units u) {
            return append().then(
              [&, u = std::move(u)](storage::append_result res) mutable {
                  auto f = log->flush();
                  u.return_all();
                  return f.then([&, dirty = res.last_offset] {
                      auto lstats = log->offsets();
                      ASSERT_GE(lstats.committed_offset, dirty);
                  });
              });
        });
    };

    std::vector<ss::future<>> futures;
    futures.reserve(100);

    for (auto i = 0; i < 100; ++i) {
        futures.push_back(append_with_lock());
    }

    bool run_monitor = true;
    auto prev_stable_offset = model::offset{};
    auto prev_committed_offset = model::offset{};

    auto monitor = ss::now().then([&] {
        return ss::do_until(
          [&] { return !run_monitor; },
          [&]() mutable -> auto {
              if (log->segment_count() == 0) {
                  return ss::now();
              }
              auto stable
                = log->segments().back()->offsets().get_stable_offset();
              EXPECT_LE(prev_stable_offset, stable);
              prev_stable_offset = stable;

              auto committed
                = log->segments().back()->offsets().get_committed_offset();
              EXPECT_LE(prev_committed_offset, committed);
              prev_committed_offset = committed;

              return ss::now();
          });
    });

    ss::when_all_succeed(futures.begin(), futures.end()).get();
    run_monitor = false;
    monitor.get();
}

TEST_F(storage_test_fixture, changing_cleanup_policy_back_and_forth) {
    // issue: https://github.com/redpanda-data/redpanda/issues/2214
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::no;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // add a segment, some of the record keys in batches are random and some of
    // them are the same to generate offset gaps after compaction
    auto add_segment = [&log](size_t size) {
        do {
            // 10 records per batch
            for (int i = 0; i < 10; ++i) {
                ss::sstring key_str;
                bool random_key = random_generators::get_int(0, 100) < 50;
                if (random_key) {
                    key_str = ssx::sformat(
                      "key_{}", random_generators::get_int<uint64_t>());
                } else {
                    key_str = "key";
                }
                iobuf key = iobuf::from(key_str.c_str());
                bytes val_bytes = tests::random_bytes(1024);

                iobuf value = bytes_to_iobuf(val_bytes);
                storage::record_batch_builder builder(
                  model::record_batch_type::raft_data, model::offset(0));
                builder.add_raw_kv(std::move(key), std::move(value));
                auto batch = std::move(builder).build();
                batch.set_term(model::term_id(0));
                auto reader = model::make_memory_record_batch_reader(
                  {std::move(batch)});
                storage::log_append_config cfg{
                  .should_fsync = storage::log_append_config::fsync::no,
                  .timeout = model::no_timeout,
                };

                std::move(reader)
                  .for_each_ref(log->make_appender(cfg), cfg.timeout)
                  .get();
            }
        } while (log->segments().back()->size_bytes() < size);
    };
    // add 2 log segments
    add_segment(1_MiB);
    log->force_roll().get();
    add_segment(1_MiB);
    log->force_roll().get();
    add_segment(1_MiB);
    log->flush().get();

    ASSERT_EQ(log->segment_count(), 3);

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    // self compaction steps
    log->housekeeping(c_cfg).get();
    log->housekeeping(c_cfg).get();

    // read all batches
    auto first_read = read_and_validate_all_batches(log);

    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    // update cleanup policy to deletion
    log->set_overrides(overrides);
    log->notify_compaction_update();

    // read all batches again
    auto second_read = read_and_validate_all_batches(log);

    ASSERT_EQ(first_read.size(), second_read.size());
}

ss::future<chunked_circular_buffer<model::record_batch>>
copy_to_mem(model::record_batch_reader& reader) {
    using data_t = chunked_circular_buffer<model::record_batch>;
    class memory_batch_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            _result.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        data_t end_of_stream() { return std::move(_result); }

    private:
        data_t _result;
    };

    return reader.consume(memory_batch_consumer{}, model::no_timeout);
}

TEST_F(storage_test_fixture, reader_prevents_log_shutdown) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(
                   storage::ntp_config(
                     ntp,
                     mgr.config().base_dir,
                     std::make_unique<storage::ntp_config::default_overrides>(
                       overrides)))
                 .get();
    log->stm_hookset()->start();
    // append some batches
    append_exactly(log, 5, 128).get();

    // drain whole log with reader so it is not reusable anymore.
    storage::local_log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      std::numeric_limits<int64_t>::max(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    log->stm_hookset()->stop();

    auto f = ss::now();
    {
        auto reader = log->make_reader(reader_cfg).get();
        auto batches = copy_to_mem(reader).get();

        f = mgr.shutdown(ntp);
    }
    f.get();
}

TEST_F(storage_test_fixture, test_querying_term_last_offset) {
    auto cfg = default_log_config(test_dir);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));
    // append some baches in term 0
    append_random_batches(log, 10, model::term_id(0));
    auto lstats_term_0 = log->offsets();
    // append some batches in term 1
    append_random_batches(log, 10, model::term_id(1));
    // force segment roll
    log->force_roll().get();
    // append more batches in the same term
    append_random_batches(log, 10, model::term_id(1));
    auto lstats_term_1 = log->offsets();
    // append some batche sin term 2
    append_random_batches(log, 10, model::term_id(2));

    ASSERT_EQ(
      lstats_term_0.dirty_offset,
      log->get_term_last_offset(model::term_id(0)).value());
    ASSERT_EQ(
      lstats_term_1.dirty_offset,
      log->get_term_last_offset(model::term_id(1)).value());
    ASSERT_EQ(
      log->offsets().dirty_offset,
      log->get_term_last_offset(model::term_id(2)).value());

    ASSERT_TRUE(!log->get_term_last_offset(model::term_id(3)).has_value());
    // prefix truncate log at end offset fo term 0

    log
      ->truncate_prefix(
        storage::truncate_prefix_config(
          lstats_term_0.dirty_offset + model::offset(1)))
      .get();

    ASSERT_TRUE(!log->get_term_last_offset(model::term_id(0)).has_value());
}

void write_batch(
  ss::shared_ptr<storage::log> log,
  ss::sstring key,
  int value,
  model::record_batch_type batch_type,
  bool is_control) {
    storage::record_batch_builder builder(batch_type, model::offset(0));
    if (is_control) {
        builder.set_control_type();
    }

    builder.add_raw_kv(serde::to_iobuf(std::move(key)), serde::to_iobuf(value));

    auto batch = std::move(builder).build();
    batch.set_term(model::term_id(0));
    auto reader = model::make_memory_record_batch_reader({std::move(batch)});
    storage::log_append_config cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .timeout = model::no_timeout,
    };

    std::move(reader).for_each_ref(log->make_appender(cfg), cfg.timeout).get();
}

absl::
  flat_hash_map<std::tuple<model::record_batch_type, bool, ss::sstring>, int>
  compact_in_memory(ss::shared_ptr<storage::log> log) {
    auto rdr = log
                 ->make_reader(
                   storage::local_log_reader_config(
                     model::offset(0), model::offset::max()))
                 .get();

    absl::flat_hash_map<
      std::tuple<model::record_batch_type, bool, ss::sstring>,
      int>
      ret;
    auto batches = model::consume_reader_to_memory(
                     std::move(rdr), model::no_timeout)
                     .get();

    for (auto& b : batches) {
        b.for_each_record(
          [&ret, bt = b.header().type, ctrl = b.header().attrs.is_control()](
            model::record r) {
              auto k = std::make_tuple(
                bt, ctrl, serde::from_iobuf<ss::sstring>(r.key().copy()));
              ret.insert_or_assign(k, serde::from_iobuf<int>(r.value().copy()));
          });
    }

    return ret;
}

TEST_F(storage_test_fixture, test_compacting_batches_of_different_types) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::no;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    // the same key but three different batch types
    write_batch(log, "key_1", 1, model::record_batch_type::raft_data, false);
    write_batch(log, "key_1", 1, model::record_batch_type::raft_data, true);
    write_batch(log, "key_1", 10, model::record_batch_type::tm_update, false);
    write_batch(
      log, "key_1", 100, model::record_batch_type::archival_metadata, false);

    write_batch(log, "key_1", 2, model::record_batch_type::raft_data, false);
    write_batch(log, "key_1", 3, model::record_batch_type::raft_data, false);
    write_batch(log, "key_1", 4, model::record_batch_type::raft_data, false);
    write_batch(log, "key_1", 4, model::record_batch_type::raft_data, true);

    write_batch(log, "key_1", 20, model::record_batch_type::tm_update, false);
    write_batch(log, "key_1", 30, model::record_batch_type::tm_update, false);
    write_batch(log, "key_1", 40, model::record_batch_type::tm_update, false);

    write_batch(log, "key_1", 200, model::record_batch_type::tm_update, false);
    write_batch(log, "key_1", 300, model::record_batch_type::tm_update, false);
    write_batch(log, "key_1", 400, model::record_batch_type::tm_update, false);

    log->force_roll().get();

    log->flush().get();

    ASSERT_EQ(log->segment_count(), 2);

    storage::housekeeping_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    auto before_compaction = compact_in_memory(log);

    ASSERT_EQ(before_compaction.size(), 4);
    // compact
    log->housekeeping(c_cfg).get();
    auto after_compaction = compact_in_memory(log);

    ASSERT_TRUE(before_compaction == after_compaction);
}

TEST_F(storage_test_fixture, read_write_truncate) {
    /**
     * Test validating concurrent reads, writes and truncations
     */
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());

    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(mgr, storage::ntp_config(ntp, mgr.config().base_dir));

    int cnt = 0;
    int max = 500;
    ssx::mutex log_mutex{"e2e_test::log_mutex"};
    auto produce = ss::do_until(
      [&] { return cnt > max; },
      [&log, &cnt, &log_mutex] {
          chunked_circular_buffer<model::record_batch> batches;
          for (int i = 0; i < 20; ++i) {
              storage::record_batch_builder builder(
                model::record_batch_type::raft_data, model::offset(0));

              builder.add_raw_kv(
                reflection::to_iobuf("key"), reflection::to_iobuf("value"));
              batches.push_back(std::move(builder).build());
          }
          auto reader = model::make_memory_record_batch_reader(
            std::move(batches));

          storage::log_append_config cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .timeout = model::no_timeout,
          };
          SUCCEED() << "append";
          return log_mutex
            .with([reader = std::move(reader), cfg, &log]() mutable {
                SUCCEED() << "append_lock";
                return std::move(reader).for_each_ref(
                  log->make_appender(cfg), cfg.timeout);
            })
            .then([](storage::append_result res) {
                SUCCEED() << fmt::format("append_result: {}", res.last_offset);
            })
            .then([&log] { return log->flush(); })
            .finally([&cnt] { cnt++; });
      });

    auto read = ss::do_until(
      [&] { return cnt > max; },
      [&log, &cnt] {
          auto offset = log->offsets();
          if (offset.dirty_offset == model::offset{}) {
              return ss::now();
          }
          storage::local_log_reader_config cfg(
            std::max(model::offset(0), offset.dirty_offset - model::offset(10)),
            cnt % 2 == 0 ? offset.dirty_offset - model::offset(2)
                         : offset.dirty_offset);
          auto start = ss::steady_clock_type::now();
          return log->make_reader(cfg)
            .then([start](model::record_batch_reader rdr) {
                // assert that creating a reader took less than 5 seconds
                EXPECT_LT((ss::steady_clock_type::now() - start) / 1ms, 5000);
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](chunked_circular_buffer<model::record_batch> batches) {
                if (batches.empty()) {
                    SUCCEED() << "read empty range";
                    return;
                }
                SUCCEED() << fmt::format(
                  "read range: {}, {}",
                  batches.front().base_offset(),
                  batches.back().last_offset());
            });
      });

    auto truncate = ss::do_until(
      [&] { return cnt > max; },
      [&log, &log_mutex] {
          auto offset = log->offsets();
          if (offset.dirty_offset == model::offset{}) {
              return ss::now();
          }
          return log_mutex.with([&log] {
              auto offset = log->offsets();
              SUCCEED() << fmt::format("truncate offsets: {}", offset);
              auto start = ss::steady_clock_type::now();
              auto orig_cnt = log->get_log_truncation_counter();
              return log
                ->truncate(storage::truncate_config(offset.dirty_offset))
                .finally([start, orig_cnt, &log] {
                    // assert that truncation took less than 5 seconds
                    ASSERT_LT(
                      (ss::steady_clock_type::now() - start) / 1ms, 5000);
                    auto new_cnt = log->get_log_truncation_counter();
                    ASSERT_GT(new_cnt, orig_cnt);
                    SUCCEED() << "truncate_done";
                });
          });
      });

    produce.get();
    read.get();
    truncate.get();
}

TEST_F(storage_test_fixture, write_truncate_compact) {
    /**
     * Test validating concurrent reads, writes and truncations
     */
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(1_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    int cnt = 0;
    int max = 50;
    bool done = false;
    ssx::mutex log_mutex{"e2e_test::log_mutex"};
    auto produce
      = ss::do_until(
          [&] { return cnt > max || done; },
          [&log, &cnt, &log_mutex] {
              chunked_circular_buffer<model::record_batch> batches;
              for (int i = 0; i < 20; ++i) {
                  storage::record_batch_builder builder(
                    model::record_batch_type::raft_data, model::offset(0));

                  builder.add_raw_kv(
                    reflection::to_iobuf(
                      ssx::sformat("key-{}", random_generators::get_int(100))),
                    reflection::to_iobuf("value"));
                  batches.push_back(std::move(builder).build());
              }
              auto reader = model::make_memory_record_batch_reader(
                std::move(batches));

              storage::log_append_config cfg{
                .should_fsync = storage::log_append_config::fsync::no,
                .timeout = model::no_timeout,
              };
              return log_mutex
                .with([reader = std::move(reader), cfg, &log]() mutable {
                    return std::move(reader).for_each_ref(
                      log->make_appender(cfg), cfg.timeout);
                })
                .then([](storage::append_result res) {
                    SUCCEED()
                      << fmt::format("append_result: {}", res.last_offset);
                })
                .then([&log] { return log->flush(); })
                .finally([&cnt] { cnt++; });
          })
          .finally([&] { done = true; });

    auto truncate
      = ss::do_until(
          [&] { return done; },
          [&log, &log_mutex] {
              auto offset = log->offsets();
              if (offset.dirty_offset == model::offset{}) {
                  return ss::now();
              }
              return log_mutex.with([&log] {
                  auto offset = log->offsets();
                  auto truncate_at = model::offset{
                    random_generators::get_int<int64_t>(
                      offset.dirty_offset() / 2, offset.dirty_offset)};
                  SUCCEED() << fmt::format(
                    "truncate offsets: {}, truncate at: {}",
                    offset,
                    truncate_at);

                  auto orig_cnt = log->get_log_truncation_counter();
                  return log->truncate(storage::truncate_config(truncate_at))
                    .then_wrapped([&log, o = truncate_at, orig_cnt](
                                    ss::future<> f) {
                        vassert(
                          !f.failed(),
                          "truncation failed with {}",
                          f.get_exception());
                        ASSERT_LE(
                          log->offsets().dirty_offset, model::prev_offset(o));
                        if (
                          log->offsets().dirty_offset < model::prev_offset(o)) {
                            auto new_cnt = log->get_log_truncation_counter();
                            ASSERT_GT(new_cnt, orig_cnt);
                        }
                    });
              });
          })
          .finally([&] { done = true; });

    auto compact = ss::do_until(
                     [&] { return done; },
                     [&log, &as] {
                         return log
                           ->housekeeping(
                             storage::housekeeping_config(
                               model::timestamp::min(),
                               std::nullopt,
                               model::offset::max(),
                               model::offset::max(),
                               model::offset::max(),
                               std::nullopt,
                               std::nullopt,
                               0ms,
                               as))
                           .handle_exception_type(
                             [](const storage::segment_closed_exception&) {

                             })
                           .handle_exception([](std::exception_ptr e) {
                               SUCCEED()
                                 << fmt::format("compaction exception - {}", e);
                           });
                     })
                     .finally([&] { done = true; });

    compact.get();
    SUCCEED() << "compact_done";
    produce.get();
    SUCCEED() << "produce_done";
    truncate.get();
    SUCCEED() << "truncate_done";

    // Ensure we've cleaned up all our staging segments such that a removal of
    // the log results in nothing leftover.
    auto dir_path = log->config().work_directory();
    try {
        log = {};
        mgr.remove(ntp).get();
    } catch (...) {
        directory_walker walker;
        walker
          .walk(
            dir_path,
            [](const ss::directory_entry& de) {
                SUCCEED() << fmt::format("Leftover file: {}", de.name);
                return ss::make_ready_future<>();
            })
          .get();
    }
    ASSERT_EQ(false, ss::file_exists(dir_path).get());
};

// This test acts as a regression test to ensure that non raft data batches
// that contribute to offset translator gaps are never compacted. Compacting
// them away results in incorrect Kafka offsets upon offset translator state
// rebuild (eg: partition movement).
//
// Note to the developer: If you are here because this test failed on
// your patch, it usually means that you made a non raft data batch
// compactible, which is not allowed for reason described above.
// (unless the test itself is buggy, ofcourse :))
TEST_F(storage_test_fixture, compaction_non_raft_batches_regression_test) {
    class logging_consumer {
    public:
        ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
            vlog(
              e2e_test_log.trace,
              "batch type: {}, base offset: {}, records: {}",
              b.header().type,
              b.base_offset(),
              b.record_count());
            _total_batches += 1;
            _total_records += b.header().record_count;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        void end_of_stream() {
            vlog(
              e2e_test_log.trace,
              "Total batches: {}, records: {}",
              _total_batches,
              _total_records);
        }

    private:
        size_t _total_batches = 0;
        size_t _total_records = 0;
    };

    auto print_batch_info = [](ss::shared_ptr<storage::log> log) {
        storage::local_log_reader_config reader_cfg(
          model::offset(0), model::offset::max());
        auto reader = log->make_reader(reader_cfg).get();
        std::move(reader)
          .for_each_ref(logging_consumer{}, model::no_timeout)
          .discard_result()
          .get();
    };

    auto cfg = default_log_config(test_dir);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)),
      raft::group_id{0},
      model::offset_translator_batch_types());
    log->start(std::nullopt, as).get();

    storage::log_append_config appender_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .timeout = model::no_timeout,
    };

    auto append_random_batch = [&](model::record_batch_type bt) {
        storage::record_batch_builder builder(bt, model::offset(0));
        auto num_records = random_generators::get_int(1, 5);
        for (int i = 0; i < num_records; i++) {
            builder.add_raw_kv(
              reflection::to_iobuf(ssx::sformat("key-{}", i)),
              bytes_to_iobuf(tests::random_bytes(5)));
        }
        auto reader = model::make_memory_record_batch_reader(
          std::move(builder).build());
        std::move(reader)
          .for_each_ref(log->make_appender(appender_cfg), model::no_timeout)
          .discard_result()
          .then([&log] { return log->flush(); })
          .get();
    };

    // populate some data, load duplicates of each batch type and
    // sprinkle some raft batches around.
    using enum_int_t = std::underlying_type_t<model::record_batch_type>;
    auto max = static_cast<enum_int_t>(model::record_batch_type::MAX);
    for (enum_int_t type = 2; type <= max; type++) {
        auto bt = static_cast<model::record_batch_type>(type);
        append_random_batch(bt);
        if (tests::random_bool()) {
            // some interleaved data batches
            append_random_batch(model::record_batch_type::raft_data);
        }
        append_random_batch(bt);
    }

    print_batch_info(log);

    // Save the translation map for comparison after compaction.
    chunked_hash_map<model::offset, model::offset> log_to_kafka;
    chunked_hash_map<model::offset, model::offset> kafka_to_log;
    model::offset begin{0};
    for (model::offset o = begin; o < log->offsets().dirty_offset;
         o = model::next_offset(o)) {
        log_to_kafka[o] = log->from_log_offset(o);
        kafka_to_log[o] = log->to_log_offset(o);
    }

    // compact the log
    log->force_roll().get();
    storage::housekeeping_config compaction_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    log->housekeeping(compaction_cfg).get();

    print_batch_info(log); // debugging

    // reset offset translation state to the compacted state of the log.
    auto& disk_log = reinterpret_cast<storage::disk_log_impl&>(*log);
    disk_log.offset_translator().remove_persistent_state().get();
    log = {};
    mgr.shutdown(ntp).get();
    log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)),
      raft::group_id{0},
      model::offset_translator_batch_types());
    log->start(std::nullopt, as).get();

    // validate the translation by comparing it with state before
    // compaction.
    for (model::offset o = begin; o < log->offsets().dirty_offset;
         o = model::next_offset(o)) {
        vlog(e2e_test_log.trace, "checking offset: {}", o);
        ASSERT_EQ(log->from_log_offset(o), log_to_kafka[o]);
        ASSERT_EQ(log->to_log_offset(o), kafka_to_log[o]);
    }
    log = {};
    mgr.shutdown(ntp).get();
}

TEST_F(storage_test_fixture, compaction_truncation_corner_cases) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    auto ntp = model::ntp("default", "test", 0);

    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    auto large_batch = [](int key) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(
          reflection::to_iobuf(ssx::sformat("key-{}", key)),
          bytes_to_iobuf(tests::random_bytes(33_KiB)));
        return std::move(builder).build();
    };

    auto write_and_compact =
      [&](chunked_circular_buffer<model::record_batch> batches) {
          auto reader = model::make_memory_record_batch_reader(
            std::move(batches));

          storage::log_append_config appender_cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .timeout = model::no_timeout,
          };

          std::move(reader)
            .for_each_ref(log->make_appender(appender_cfg), model::no_timeout)
            .discard_result()
            .then([&log] { return log->flush(); })
            .get();

          log
            ->housekeeping(
              storage::housekeeping_config(
                model::timestamp::min(),
                std::nullopt,
                model::offset::max(),
                model::offset::max(),
                model::offset::max(),
                std::nullopt,
                std::nullopt,
                0ms,
                as))
            .get();
      };
    {
        /**
         * Truncate with dirty offset being preceeded by a gap
         *
         * segment: [[batch (base_offset: 0)][gap][batch (base_offset: 10)]]
         *
         */
        chunked_circular_buffer<model::record_batch> batches;

        // first batch
        batches.push_back(large_batch(1));
        // 10 batches with the same key
        for (int i = 0; i < 10; ++i) {
            batches.push_back(large_batch(2));
        }
        // roll segment with new term append
        auto b = large_batch(1);
        b.set_term(model::term_id(10));
        batches.push_back(std::move(b));

        write_and_compact(std::move(batches));

        model::offset truncate_offset(10);

        log->truncate(storage::truncate_config(truncate_offset)).get();
        SUCCEED() << fmt::format(
          "truncated at: {}, offsets: {}", truncate_offset, log->offsets());
        ASSERT_EQ(log->offsets().dirty_offset, model::offset(0));
    }

    {
        /**
         * Truncate with the first offset available in the log
         * segment: [batch: (base_offset: 11)]
         *
         */
        chunked_circular_buffer<model::record_batch> batches;

        // first batch
        batches.push_back(large_batch(1));
        // 10 batches with the same key
        for (int i = 0; i < 10; ++i) {
            batches.push_back(large_batch(2));
        }
        // roll segment with new term append
        for (auto i = 1; i < 10; ++i) {
            auto b = large_batch(i + 20);
            b.set_term(model::term_id(10));
            batches.push_back(std::move(b));
        }

        write_and_compact(std::move(batches));

        model::offset truncate_offset(11);
        log->truncate_prefix(storage::truncate_prefix_config(truncate_offset))
          .get();

        log->truncate(storage::truncate_config(truncate_offset)).get();
        SUCCEED() << fmt::format(
          "truncated at: {}, offsets: {}", truncate_offset, log->offsets());
        // empty log have a dirty offset equal to (start_offset - 1)
        ASSERT_EQ(log->offsets().dirty_offset, model::offset(10));
    }
}

static storage::log_gap_analysis analyze(storage::log& log) {
    // TODO factor out common constant
    storage::local_log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      10_MiB,
      std::nullopt,
      std::nullopt,
      std::nullopt);
    return storage::make_log_gap_analysis(
      log.make_reader(reader_cfg).get(), model::offset(0));
}

TEST_F(storage_test_fixture, test_max_compact_offset) {
    // Test setup.
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    // (1) append some random data, with limited number of distinct keys, so
    // compaction can make progress.
    auto headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20, model::term_id{0}, model::timestamp::now());

    // (2) remember log offset, roll log, and produce more messages
    log->flush().get();
    auto first_stats = log->offsets();
    SUCCEED() << fmt::format("Offsets to be compacted {}", first_stats);
    log->force_roll().get();
    headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20, model::term_id{0}, model::timestamp::now());

    ss::sleep(1s).get(); // ensure time separation for max.compaction.lag.ms

    // (3) roll log and trigger compaction, analyzing offset gaps before and
    // after, to observe compaction behavior.
    log->flush().get();
    auto second_stats = log->offsets();
    auto pre_compact_gaps = analyze(*log);
    log->force_roll().get();
    auto max_compact_offset = first_stats.committed_offset;
    storage::housekeeping_config ccfg(
      model::timestamp::max(), // no time-based deletion
      std::nullopt,
      max_compact_offset,
      max_compact_offset,
      max_compact_offset,
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    log->housekeeping(ccfg).get();
    auto final_stats = log->offsets();
    auto post_compact_gaps = analyze(*log);

    // (4) check correctness.
    SUCCEED() << fmt::format("pre-compact gaps {}", pre_compact_gaps);
    SUCCEED() << fmt::format("post-compact gaps {}", post_compact_gaps);

    // Compaction doesn't change offset values, it creates holes in offset
    // space.
    ASSERT_TRUE(final_stats.committed_offset == second_stats.committed_offset);

    // No gaps before compacting, and >0 gaps after.
    ASSERT_EQ(pre_compact_gaps.num_gaps, 0);
    ASSERT_GT(post_compact_gaps.num_gaps, 0);
    // Verify no compaction happened past the max_compactable_offset we
    // specified.
    ASSERT_LE(post_compact_gaps.first_gap_start, max_compact_offset);
    ASSERT_LE(post_compact_gaps.last_gap_end, max_compact_offset);
    ASSERT_LE(
      log->max_eligible_for_compacted_reupload_offset(model::offset{0}),
      max_compact_offset);
};

TEST_F(storage_test_fixture, test_self_compaction_while_reader_is_open) {
    // Test setup.
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    // (1) append some random data, with limited number of distinct keys, so
    // compaction can make progress.
    auto headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20);

    // (2) remember log offset, roll log, and produce more messages
    log->flush().get();

    log->force_roll().get();
    headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20);

    // (3) roll log and trigger compaction, analyzing offset gaps before and
    // after, to observe compaction behavior.
    log->flush().get();

    log->force_roll().get();
    storage::housekeeping_config ccfg(
      model::timestamp::max(), // no time-based deletion
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    auto& segment = *(log->segments().begin());
    auto stream = segment->offset_data_stream(model::offset(0)).get();
    log->housekeeping(std::move(ccfg)).get();
    stream.close().get();
};

TEST_F(storage_test_fixture, test_simple_compaction_rebuild_index) {
    // Test setup.
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    // Append some linear kv ints
    int num_appends = 5;
    append_random_batches<linear_int_kv_batch_generator>(
      log, num_appends, model::term_id(0), model::timestamp::min());
    log->flush().get();
    log->force_roll().get();
    ASSERT_EQ(log->segment_count(), 2);

    // Remove compacted indexes to trigger a full index rebuild.
    auto index_path = log->segments()[0]->path().to_compacted_index();
    ASSERT_TRUE(std::filesystem::remove(index_path));

    auto batches = read_and_validate_all_batches(log);
    ASSERT_EQ(
      batches.size(),
      num_appends * linear_int_kv_batch_generator::batches_per_call);
    ASSERT_TRUE(std::all_of(batches.begin(), batches.end(), [](auto& b) {
        return b.record_count()
               == linear_int_kv_batch_generator::records_per_batch;
    }));

    storage::housekeeping_config ccfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      as);

    log->housekeeping(ccfg).get();

    batches = read_and_validate_all_batches(log);
    ASSERT_EQ(
      batches.size(),
      num_appends * linear_int_kv_batch_generator::batches_per_call);
    linear_int_kv_batch_generator::validate_post_compaction(std::move(batches));
};

struct compact_test_args {
    model::offset max_compact_offs;
    long num_compactable_msg;
    long msg_per_segment;
    long segments;
    long expected_compacted_segments;
    long num_expected_gaps;
    bool keys_use_segment_id;
};

static void
do_compact_test(const compact_test_args args, storage_test_fixture& f) {
    // Test setup.
    auto cfg = f.default_log_config(f.test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = f.make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    tlog.info("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = f.manage_log(mgr, std::move(ntp_cfg));

    auto append_batch = [](
                          ss::shared_ptr<storage::log> log,
                          model::term_id term,
                          std::optional<int> segment_id = std::nullopt) {
        const auto key_str = ssx::sformat("key{}", segment_id.value_or(-1));
        iobuf key = iobuf::from(key_str.data());
        iobuf value = tests::random_iobuf(100);

        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(key.copy(), value.copy());

        auto batch = std::move(builder).build();

        batch.set_term(term);
        batch.header().first_timestamp = model::timestamp::now();
        auto reader = model::make_memory_record_batch_reader(
          {std::move(batch)});
        storage::log_append_config cfg{
          .should_fsync = storage::log_append_config::fsync::no,
          .timeout = model::no_timeout,
        };

        std::move(reader)
          .for_each_ref(log->make_appender(cfg), cfg.timeout)
          .get();
    };

    for (int s = 0; s < args.segments; s++) {
        std::optional<int> key = args.keys_use_segment_id
                                   ? s
                                   : std::optional<int>{};
        for (int i = 0; i < args.msg_per_segment; i++) {
            append_batch(log, model::term_id(0), key);
        }
        log->force_roll().get();
    }
    append_batch(log, model::term_id(0)); // write single message for final
                                          // segment after last roll

    log->flush().get();
    auto pre_gaps = analyze(*log);
    auto pre_stats = log->offsets();
    ASSERT_EQ(pre_stats.committed_offset, args.segments * args.msg_per_segment);
    ASSERT_EQ(pre_gaps.num_gaps, 0);
    tlog.info("pre-compact stats: {}, analysis: {}", pre_stats, pre_gaps);

    storage::housekeeping_config ccfg(
      model::timestamp::max(), // no time-based deletion
      std::nullopt,
      model::offset(args.max_compact_offs),
      model::offset(args.max_compact_offs),
      model::offset(args.max_compact_offs),
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    log->housekeeping(ccfg).get();
    auto final_stats = log->offsets();
    auto final_gaps = analyze(*log);
    tlog.info("post-compact stats: {}, analysis: {}", final_stats, final_gaps);
    ASSERT_EQ(
      final_stats.committed_offset, args.segments * args.msg_per_segment);

    // If we used keys with segment IDs for records in each segment, we
    // should have one huge gap at the beginning of each compacted segment.
    // If we used the same key for each record, we should only expect one gap
    // after compaction runs across the entire window of segments.
    ASSERT_EQ(final_gaps.num_gaps, args.num_expected_gaps);
    ASSERT_EQ(final_gaps.first_gap_start, model::offset(0));

    // If adjacent segment compaction worked in order from oldest to newest, we
    // could use this assert.
    // We can compact the whole first segment, ending at num_compactible_msg -
    // 1, but compaction leaves at least one message per key in the segment,
    // thus the - 2 here.
    //   ASSERT_EQ(
    //     final_gaps.last_gap_end, model::offset(args.num_compactable_msg -
    //     2));
    //  Instead, we use weaker assert for now:

    ASSERT_LE(final_gaps.last_gap_end, args.max_compact_offs);
    ASSERT_LE(
      log->max_eligible_for_compacted_reupload_offset(model::offset{0}),
      args.max_compact_offs);
}

TEST_F(storage_test_fixture, test_max_compact_offset_mid_segment) {
    // Create a log with three segments. We will set max compactible offset to
    // be in the middle of the second segment. This should cause only the first
    // segment to be compacted, as we do not support partial compaction of a
    // segment.
    do_compact_test(
      {.max_compact_offs = model::offset(150),
       .num_compactable_msg = 100,
       .msg_per_segment = 100,
       .segments = 3,
       .expected_compacted_segments = 1,
       .num_expected_gaps = 1,
       .keys_use_segment_id = false},
      *this);
}

TEST_F(storage_test_fixture, test_max_compact_offset_unset) {
    // Same as above, but leave max_compact_offset unset.
    do_compact_test(
      {.max_compact_offs = model::offset::max(),
       // XXX AJF: I expected this to be 300, because I force segment roll
       // after writing the third segment.
       .num_compactable_msg = 200,
       .msg_per_segment = 100,
       .segments = 3,
       .expected_compacted_segments = 3,
       .num_expected_gaps = 1,
       .keys_use_segment_id = false},
      *this);
}

TEST_F(storage_test_fixture, test_max_compact_offset_unset_use_segment_ids) {
    // Use segment IDs for keys, thereby preventing compaction from reducing
    // down to just one record in the last segment (each segment will have 1,
    // unique record)
    do_compact_test(
      {.max_compact_offs = model::offset::max(),
       .num_compactable_msg = 200,
       .msg_per_segment = 100,
       .segments = 3,
       .expected_compacted_segments = 3,
       .num_expected_gaps = 3,
       .keys_use_segment_id = true},
      *this);
}

TEST_F(storage_test_fixture, test_bytes_eviction_overrides) {
    size_t batch_size = 128;
    size_t segment_size = 512_KiB;
    auto batches_per_segment = size_t(segment_size / batch_size);
    auto batch_cnt = batches_per_segment * 10 + 1;
    SUCCEED() << fmt::format(
      "using batch of size {}, with {} batches per segment, total batches: {}",
      batch_size,
      batches_per_segment,
      batch_cnt);

    struct test_case {
        std::optional<size_t> default_local_bytes;
        std::optional<size_t> default_cloud_bytes;
        tristate<size_t> topic_local_bytes;
        tristate<size_t> topic_cloud_bytes;
        bool cloud_storage;
        size_t expected_bytes_left;
    };
    std::vector<test_case> test_cases;
    auto retain_segments = [&](int n) { return segment_size * n + batch_size; };
    /**
     * Retention disabled
     */
    test_cases.push_back(
      test_case{
        std::nullopt,                   // default local
        std::nullopt,                   // default cloud
        tristate<size_t>(std::nullopt), // topic local
        tristate<size_t>(std::nullopt), // topic cloud
        false,
        batch_size * batch_cnt,
      });

    test_cases.push_back(
      test_case{
        std::nullopt,                   // default local
        std::nullopt,                   // default cloud
        tristate<size_t>(std::nullopt), // topic local
        tristate<size_t>(std::nullopt), // topic cloud
        true,
        batch_size * batch_cnt,
      });
    /**
     * Local retention takes precedence over cloud retention
     */
    // defaults
    test_cases.push_back(
      test_case{
        retain_segments(4),             // default local
        retain_segments(6),             // default cloud
        tristate<size_t>(std::nullopt), // topic local
        tristate<size_t>(std::nullopt), // topic cloud
        true,
        segment_size * 4 + batch_size,
      });

    // per topic configuration
    test_cases.push_back(
      test_case{
        retain_segments(4),                     // default local
        retain_segments(6),                     // default cloud
        tristate<size_t>(segment_size * 2 + 1), // topic local
        tristate<size_t>(segment_size * 3 + 1), // topic cloud
        true,
        segment_size * 2 + batch_size,
      });
    // /**
    //  * Local retention is capped by cloud retention
    //  */
    // defaults, local retention is larger than remote one, it should be capped
    test_cases.push_back(
      test_case{
        retain_segments(5),             // default local
        retain_segments(3),             // default cloud
        tristate<size_t>(std::nullopt), // topic local
        tristate<size_t>(std::nullopt), // topic cloud
        true,
        segment_size * 3 + batch_size,
      });

    // defaults, local retention is disabled, it should be replaced by cloud one
    test_cases.push_back(
      test_case{
        std::nullopt,                   // default local
        retain_segments(3),             // default cloud
        tristate<size_t>(std::nullopt), // topic local
        tristate<size_t>(std::nullopt), // topic cloud
        true,
        segment_size * 3 + batch_size,
      });

    // topic configuration, local retention is larger than remote one, it
    // should be capped
    test_cases.push_back(
      test_case{
        retain_segments(6),                   // default local
        retain_segments(8),                   // default cloud
        tristate<size_t>(retain_segments(5)), // topic local
        tristate<size_t>(retain_segments(2)), // topic cloud
        true,
        segment_size * 2 + batch_size,
      });
    //  topic configuration, local retention is disabled, it should be
    // replaced by cloud one
    test_cases.push_back(
      test_case{
        retain_segments(6),                   // default local
        retain_segments(8),                   // default cloud
        tristate<size_t>{},                   // topic local
        tristate<size_t>(retain_segments(2)), // topic cloud
        true,
        segment_size * 2 + batch_size,
      });

    // cloud storage disabled, use whatever is there in cloud settings
    test_cases.push_back(
      test_case{
        retain_segments(6),                   // default local
        retain_segments(8),                   // default cloud
        tristate<size_t>(retain_segments(2)), // topic local
        tristate<size_t>(retain_segments(5)), // topic cloud
        false,
        segment_size * 5 + batch_size,
      });

    test_cases.push_back(
      test_case{
        retain_segments(2),             // default local
        retain_segments(6),             // default cloud
        tristate<size_t>(std::nullopt), // topic local
        tristate<size_t>(std::nullopt), // topic cloud
        false,
        segment_size * 6 + batch_size,
      });

    size_t i = 0;
    for (auto& tc : test_cases) {
        SUCCEED() << fmt::format("Running case {}", i++);
        auto cfg = default_log_config(test_dir);
        // enable cloud storage
        config::shard_local_cfg().cloud_storage_enabled.set_value(
          tc.cloud_storage);

        cfg.max_segment_size = config::mock_binding<size_t>(
          size_t(segment_size));
        cfg.retention_bytes = config::mock_binding<std::optional<size_t>>(
          std::optional<size_t>(tc.default_cloud_bytes));

        config::shard_local_cfg().retention_bytes.set_value(
          tc.default_cloud_bytes);
        config::shard_local_cfg()
          .retention_local_target_bytes_default.set_value(
            tc.default_local_bytes);

        cfg.segment_size_jitter = storage::jitter_percents(0);
        ss::abort_source as;
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
        auto ntp = model::ntp(model::kafka_namespace, "test", 0);
        storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
        storage::ntp_config::default_overrides overrides;

        bool have_overrides = false;
        if (tc.cloud_storage) {
            have_overrides = true;
            overrides.shadow_indexing_mode = model::shadow_indexing_mode::full;
            overrides.storage_mode = model::redpanda_storage_mode::tiered;
        }

        if (
          tc.topic_cloud_bytes.has_optional_value()
          || tc.topic_cloud_bytes.is_disabled()) {
            have_overrides = true;
            overrides.retention_bytes = tc.topic_cloud_bytes;
        }

        if (
          tc.topic_cloud_bytes.has_optional_value()
          || tc.topic_cloud_bytes.is_disabled()) {
            have_overrides = true;
            overrides.retention_local_target_bytes = tc.topic_cloud_bytes;
        }

        if (have_overrides) {
            ntp_cfg.set_overrides(overrides);
        }

        auto log = manage_log(mgr, std::move(ntp_cfg));
        auto deferred_rm = ss::defer([&mgr, ntp, &log]() mutable {
            log = {};
            mgr.remove(ntp).get();
        });
        ASSERT_EQ(log->segment_count(), 0);

        for (size_t i = 0; i < batch_cnt; ++i) {
            append_exactly(log, 1, batch_size).get();
        }
        ASSERT_EQ(log->segment_count(), 11);

        compact_and_prefix_truncate(
          *log,
          storage::housekeeping_config(
            model::timestamp::min(),
            cfg.retention_bytes(),
            model::offset::max(),
            model::offset::max(),
            model::offset::max(),
            std::nullopt,
            std::nullopt,
            0ms,
            as));

        // retention won't violate the target
        ASSERT_GE(log->size_bytes(), tc.expected_bytes_left);
        ASSERT_GT(log->size_bytes(), tc.expected_bytes_left - segment_size);
    }
    config::shard_local_cfg().cloud_storage_enabled.reset();
    config::shard_local_cfg().retention_bytes.reset();
    config::shard_local_cfg().retention_local_target_bytes_default.reset();
}

TEST_F(storage_test_fixture, issue_8091) {
    /**
     * Test validating concurrent reads, writes and truncations
     */
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());

    auto ntp = model::ntp(model::kafka_namespace, "test", 0);
    auto log = manage_log(mgr, storage::ntp_config(ntp, mgr.config().base_dir));

    int cnt = 0;
    int max = 50; // NB: Reduced for GTest due to OOM; hopefully temporary.
    ssx::mutex log_mutex{"e2e_test::log_mutex"};
    model::offset last_truncate;

    auto produce = ss::do_until(
      [&] { return cnt > max; },
      [&log, &cnt, &log_mutex] {
          chunked_circular_buffer<model::record_batch> batches;
          auto bt = random_generators::random_choice(
            std::vector<model::record_batch_type>{
              model::record_batch_type::raft_data,
              model::record_batch_type::raft_configuration});

          // single batch
          storage::record_batch_builder builder(bt, model::offset(0));
          if (bt == model::record_batch_type::raft_data) {
              builder.add_raw_kv(
                reflection::to_iobuf("key"),
                bytes_to_iobuf(tests::random_bytes(16 * 1024)));
          } else {
              builder.add_raw_kv(
                std::nullopt, bytes_to_iobuf(tests::random_bytes(128)));
          }
          batches.push_back(std::move(builder).build());

          auto reader = model::make_memory_record_batch_reader(
            std::move(batches));

          storage::log_append_config cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .timeout = model::no_timeout,
          };
          SUCCEED() << "append";
          return log_mutex
            .with([reader = std::move(reader), cfg, &log]() mutable {
                SUCCEED() << "append_lock";
                return std::move(reader)
                  .for_each_ref(log->make_appender(cfg), cfg.timeout)
                  .then([](storage::append_result res) {
                      SUCCEED()
                        << fmt::format("append_result: {}", res.last_offset);
                  })
                  .then([&log] { return log->flush(); });
            })
            .finally([&cnt] { cnt++; });
      });

    auto read = ss::do_until(
      [&] { return cnt > max; },
      [&log, &last_truncate] {
          if (last_truncate == model::offset{}) {
              return ss::now();
          }
          auto offset = log->offsets();
          storage::local_log_reader_config cfg(
            last_truncate - model::offset(1), offset.dirty_offset);
          cfg.type_filter = model::record_batch_type::raft_data;

          auto start = ss::steady_clock_type::now();
          return log->make_reader(cfg)
            .then([start](model::record_batch_reader rdr) {
                // assert that creating a reader took less than 5 seconds
                EXPECT_LT((ss::steady_clock_type::now() - start) / 1ms, 5000);
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](chunked_circular_buffer<model::record_batch> batches) {
                if (batches.empty()) {
                    SUCCEED() << "read empty range";
                    return;
                }
                SUCCEED() << fmt::format(
                  "read range: {}, {}",
                  batches.front().base_offset(),
                  batches.back().last_offset());
            });
      });

    auto truncate = ss::do_until(
      [&] { return cnt > max; },
      [&log, &log_mutex, &last_truncate] {
          auto offset = log->offsets();
          if (offset.dirty_offset == model::offset{}) {
              return ss::now();
          }
          return log_mutex
            .with([&log, &last_truncate] {
                auto offset = log->offsets();
                SUCCEED() << fmt::format("truncate offsets: {}", offset);
                auto start = ss::steady_clock_type::now();
                last_truncate = offset.dirty_offset;
                return log
                  ->truncate(storage::truncate_config(offset.dirty_offset))
                  .finally([start] {
                      // assert that truncation took less than 5 seconds
                      ASSERT_LT(
                        (ss::steady_clock_type::now() - start) / 1ms, 5000);
                      SUCCEED() << "truncate_done";
                  });
            })
            .then([] { return ss::sleep(10ms); });
      });

    produce.get();
    read.get();
    truncate.get();
    // at the end of this test there must be no batch parse errors
    ASSERT_EQ(log->get_probe().get_batch_parse_errors(), 0);
}

struct batch_summary {
    model::offset base;
    model::offset last;
    size_t batch_size;
    model::timestamp base_ts;
    model::timestamp max_ts;
};

struct batch_summary_accumulator {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        size_t sz = summaries.empty() ? 0 : acc_size.back();
        batch_summary summary{
          .base = b.base_offset(),
          .last = b.last_offset(),
          .batch_size = b.data().size_bytes()
                        + model::packed_record_batch_header_size,
          .base_ts = b.header().first_timestamp,
          .max_ts = b.header().max_timestamp,
        };
        summaries.push_back(summary);
        acc_size.push_back(sz + summary.batch_size);
        prev_size.push_back(sz);
        co_return ss::stop_iteration::no;
    }
    auto end_of_stream() { return std::move(*this); }

    std::vector<batch_summary> summaries;
    std::vector<size_t> acc_size;
    std::vector<size_t> prev_size;
};

struct batch_size_accumulator {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        auto batch_size = b.data().size_bytes()
                          + model::packed_record_batch_header_size;
        *size_bytes += batch_size;
        co_return ss::stop_iteration::no;
    }
    bool end_of_stream() const { return false; }
    size_t* size_bytes;
};

using private_flags = model::record_batch_reader::private_flags;

namespace model {
struct record_batch_reader_accessor {
    static storage::log_reader* get_impl(model::record_batch_reader& r) {
        auto impl = r._impl.get();
        // Google test doesn't handle assertions here well.
        EXPECT_TRUE(impl != nullptr);
        auto impl_log_reader = dynamic_cast<storage::log_reader*>(impl);
        EXPECT_TRUE(impl_log_reader) << "impl was not a log reader";
        return impl_log_reader;
    }

    static private_flags get_flags(model::record_batch_reader& r) {
        auto flags = r._impl->get_flags();
        EXPECT_TRUE(flags.has_value()) << "private flags unset";
        return *flags;
    };
};
} // namespace model

TEST_F(storage_test_fixture, reader_reusability_max_bytes) {
    constexpr size_t total_log_bytes = 1_MiB;

    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(2_MiB);
    storage::ntp_config::default_overrides overrides;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("config: {}", mgr.config());

    int log_num = 0;

    auto test_case = [&](
                       size_t bytes_per_batch,
                       size_t reader_max_bytes,
                       bool second_read_reusable = true) {
        {
            SCOPED_TRACE(
              fmt::format(
                "bytes_per_batch={}, reader_max_bytes={}",
                bytes_per_batch,
                reader_max_bytes));
            auto ntp = model::ntp(
              "default", fmt::format("test-{}", log_num++), 0);
            auto log = manage_log(
              mgr,
              storage::ntp_config(
                ntp,
                mgr.config().base_dir,
                std::make_unique<storage::ntp_config::default_overrides>(
                  overrides)));

            append_exactly(
              log, total_log_bytes / bytes_per_batch, bytes_per_batch)
              .get();
            log->flush().get();

            storage::local_log_reader_config reader_cfg(
              model::offset(0),
              model::model_limits<model::offset>::max(),
              reader_max_bytes,
              std::nullopt,
              std::nullopt,
              std::nullopt);

            reader_cfg.skip_batch_cache = true;

            auto read_one = [&](
                              std::string_view label,
                              bool expected_reusable,
                              bool expected_cached) {
                auto reader = log->make_reader(reader_cfg).get();
                auto finalize = ss::defer([&] {
                    model::consume_reader_to_memory(
                      std::move(reader), model::no_timeout)
                      .get();
                });

                auto summary
                  = reader
                      .consume(batch_summary_accumulator{}, model::no_timeout)
                      .get();

                auto flags = model::record_batch_reader_accessor::get_flags(
                  reader);

                {
                    SCOPED_TRACE(fmt::format("label={}", label));
                    EXPECT_EQ(flags.is_reusable, expected_reusable);
                    EXPECT_EQ(flags.was_cached, expected_cached);
                }

                return summary;
            };

            // Do the first read, we don't expect the reader to come from cache
            // as this is the first read from this ntp.
            auto summary = read_one("first", true, false);

            reader_cfg.start_offset = summary.summaries.back().last
                                      + model::offset(1);

            read_one("second", second_read_reusable, true);
        }
    };

    // 128_KiB is special because it is the default storage_read_buffer_size
    for (auto offset0 : {-1, 0, 1}) {
        for (auto offset1 : {-1, 0, 1}) {
            test_case(128_KiB + offset0, 128_KiB + offset1);
        }
    }

    // any size that can fit 3 times into the total_log_size should have the
    // reader should be reusable since we hit the bytes limit on the reader
    // config rather than exhausting the reader
    auto sizes = std::vector<size_t>{1000, 10000, 200000};
    // test all combinations of the above sizes for both bytes per batch and
    // reader max bytes
    for (auto bytes_per_batch : sizes) {
        for (auto reader_max_bytes : sizes) {
            test_case(bytes_per_batch, reader_max_bytes);
        }
    }

    // if we can only fit 2 batches in the log, the reader will be exhausted
    // after the second reader, so should be !is_resusable
    test_case(400000, 300000, false);
}

TEST_F(
  storage_test_fixture, test_offset_range_size_after_mid_segment_truncation) {
    size_t num_segments = 2;
    model::offset first_segment_last_offset;
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("redpanda", "test-topic", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);

    auto log = manage_log(mgr, std::move(ntp_cfg));
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          custom_ts_batch_generator(model::timestamp::now()));
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    // Prefix truncate such that offset 1 is the new log start.
    log->truncate_prefix(storage::truncate_prefix_config(model::offset(1)))
      .get();

    // Run size queries on ranges that don't exist in the log, but whose range
    // is still included in a segment.

    EXPECT_TRUE(
      log->offset_range_size(model::offset(0), model::offset(1)).get()
      == std::nullopt);

    EXPECT_TRUE(
      log
        ->offset_range_size(
          model::offset(0),
          storage::log::offset_range_size_requirements_t{
            .target_size = 1,
            .min_size = 0,
          })
        .get()
      == std::nullopt);
}

TEST_F(storage_test_fixture, test_offset_range_size) {
#ifdef NDEBUG
    size_t num_test_cases = 5000;
    size_t num_segments = 300;
#else
    size_t num_test_cases = 500;
    size_t num_segments = 30;
#endif
    // The test generates 300 segments with random data and the record batch map
    // for it. It generates parameters for the method randomly, invokes the
    // method and validates the result using the batch map. It also checks some
    // corner cases at the end of the test (out of range access, etc).
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("redpanda", "test-topic", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          custom_ts_batch_generator(model::timestamp::now()));
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    storage::local_log_reader_config reader_cfg(
      model::offset(0), model::offset::max());
    auto reader = log->make_reader(reader_cfg).get();

    auto acc = std::move(reader)
                 .consume(batch_summary_accumulator{}, model::no_timeout)
                 .get();

    for (size_t i = 0; i < num_test_cases; i++) {
        auto& summaries = acc.summaries;
        auto ix_base = model::test::get_int((size_t)0, summaries.size() - 1);
        auto ix_last = model::test::get_int(ix_base, summaries.size() - 1);
        auto base = summaries[ix_base].base;
        auto last = summaries[ix_last].last;
        auto base_ts = summaries[ix_base].base_ts;
        auto max_ts = summaries[ix_last].max_ts;

        auto expected_size = acc.acc_size[ix_last] - acc.prev_size[ix_base];
        auto result = log->offset_range_size(base, last).get();

        ASSERT_TRUE(result.has_value());

        vlog(
          e2e_test_log.debug,
          "base: {}, last: {}, base_ts: {}, max_ts: {}, expected size: {}, "
          "actual size: {}",
          base,
          last,
          base_ts,
          max_ts,
          expected_size,
          result->on_disk_size);

        ASSERT_EQ(expected_size, result->on_disk_size);
        ASSERT_EQ(last, result->last_offset);
        ASSERT_EQ(base_ts, result->first_timestamp);
        ASSERT_EQ(max_ts, result->last_timestamp);

        // Validate using the segment reader
        size_t consumed_size = 0;
        storage::local_log_reader_config reader_cfg(base, result->last_offset);
        reader_cfg.skip_readers_cache = true;
        reader_cfg.skip_batch_cache = true;
        auto log_rdr = log->make_reader(std::move(reader_cfg)).get();
        batch_size_accumulator size_acc{
          .size_bytes = &consumed_size,
        };
        std::move(log_rdr).consume(size_acc, model::no_timeout).get();
        ASSERT_EQ(expected_size, result->on_disk_size);
    }

    auto new_start_offset = model::next_offset(first_segment_last_offset);
    log->truncate_prefix(storage::truncate_prefix_config(new_start_offset))
      .get();

    auto lstat = log->offsets();
    ASSERT_EQ(lstat.start_offset, new_start_offset);

    // Check that out of range access triggers exception.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::offset(0), model::next_offset(new_start_offset))
        .get()
      == std::nullopt);

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::next_offset(new_start_offset),
          model::next_offset(lstat.committed_offset))
        .get()
      == std::nullopt);
};

TEST_F(storage_test_fixture, test_offset_range_size2) {
#ifdef NDEBUG
    size_t num_test_cases = 5000;
    size_t num_segments = 300;
#else
    size_t num_test_cases = 500;
    size_t num_segments = 30;
#endif
    // This test generates 300 segments and creates a record batch map.
    // Then it runs size-based offset_range_size method overload with
    // randomly generated parameters 5000 times. The record batch map
    // is used to find expected offset range size.
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("redpanda", "test-topic", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          custom_ts_batch_generator(model::timestamp::now()));
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    storage::local_log_reader_config reader_cfg(
      model::offset(0), model::offset::max());
    auto reader = log->make_reader(reader_cfg).get();
    auto acc = std::move(reader)
                 .consume(batch_summary_accumulator{}, model::no_timeout)
                 .get();
    auto& summaries = acc.summaries;

    for (size_t i = 0; i < num_test_cases; i++) {
        // - pick 'base' randomly
        // - pick target upload size randomly
        // - do the query
        // - use the offset field of the result to compute
        //   the expected upload size
        // - compare it to on_disk_size field of the result
        auto base_ix = model::test::get_int((size_t)0, summaries.size() - 1);
        auto base = summaries[base_ix].base;
        auto base_ts = summaries[base_ix].base_ts;
        auto max_size = acc.acc_size.back() - acc.prev_size[base_ix];
        auto min_size = storage::segment_index::default_data_buffer_step;
        auto target_size = model::test::get_int(min_size, max_size);
        auto result = log
                        ->offset_range_size(
                          base,
                          storage::log::offset_range_size_requirements_t{
                            .target_size = target_size,
                            .min_size = 0,
                          })
                        .get();

        ASSERT_TRUE(result.has_value());
        auto last_offset = result->last_offset;
        size_t result_ix = 0;
        for (auto s : summaries) {
            if (s.last == last_offset) {
                break;
            }
            result_ix++;
        }
        auto expected_size = acc.acc_size[result_ix] - acc.prev_size[base_ix];
        ASSERT_EQ(expected_size, result->on_disk_size);
        ASSERT_EQ(base_ts, result->first_timestamp);
        ASSERT_EQ(summaries[result_ix].max_ts, result->last_timestamp);

        // Validate using the segment reader
        size_t consumed_size = 0;
        storage::local_log_reader_config reader_cfg(base, result->last_offset);
        reader_cfg.skip_readers_cache = true;
        reader_cfg.skip_batch_cache = true;
        auto log_rdr = log->make_reader(std::move(reader_cfg)).get();
        batch_size_accumulator size_acc{
          .size_bytes = &consumed_size,
        };
        std::move(log_rdr).consume(size_acc, model::no_timeout).get();
        ASSERT_EQ(expected_size, result->on_disk_size);
    }

    auto new_start_offset = model::next_offset(first_segment_last_offset);
    log->truncate_prefix(storage::truncate_prefix_config(new_start_offset))
      .get();

    auto lstat = log->offsets();
    ASSERT_EQ(lstat.start_offset, new_start_offset);

    // Check that out of range access triggers exception.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::offset(0),
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = 1,
          })
        .get()
      == std::nullopt);

    // Query committed offset of the last batch, expect
    // no result.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          summaries.back().last,
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = 0,
          })
        .get()
      == std::nullopt);

    // Query offset out of range to trigger the exception.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::next_offset(summaries.back().last),
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = 0,
          })
        .get()
      == std::nullopt);

    // Check that the last batch can be measured independently
    auto res = log
                 ->offset_range_size(
                   summaries.back().base,
                   storage::log::offset_range_size_requirements_t{
                     .target_size = 0x10000,
                     .min_size = 0,
                   })
                 .get();

    // Only one batch is returned
    ASSERT_EQ(res->last_offset, lstat.committed_offset);
    ASSERT_EQ(res->on_disk_size, acc.acc_size.back() - acc.prev_size.back());

    // Check that we can measure the size of the log tail. This is needed for
    // timed uploads.
    size_t tail_length = 5;

    for (size_t i = 0; i < std::min(tail_length, summaries.size()); i++) {
        auto ix_batch = summaries.size() - 1 - i;
        res = log
                ->offset_range_size(
                  summaries.at(ix_batch).base,
                  storage::log::offset_range_size_requirements_t{
                    .target_size = 0x10000,
                    .min_size = 0,
                  })
                .get();

        ASSERT_EQ(res->last_offset, lstat.committed_offset);
        ASSERT_EQ(
          res->on_disk_size, acc.acc_size.back() - acc.prev_size.at(ix_batch));
    }

    // Check that the min_size is respected
    ASSERT_TRUE(
      log
        ->offset_range_size(
          summaries.back().base,
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = summaries.back().batch_size + 1,
          })
        .get()
      == std::nullopt);
};

TEST_F(storage_test_fixture, test_offset_range_size_compacted) {
#ifdef NDEBUG
    size_t num_test_cases = 5000;
    size_t num_segments = 300;
#else
    size_t num_test_cases = 500;
    size_t num_segments = 30;
#endif
    // This test generates 300 segments and creates a record batch map.
    // Then it runs compaction and creates a second record batch map. Then it
    // uses offset_range_size method to fetch segments of various sizes. The
    // parameters for the method are picked up randomly from the pre-compaction
    // map. The expected result (size of the offset range after compaction) is
    // calculated using post-compaction batch map. Test checks various corner
    // cases at the end.
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("kafka", "test-topic", 0);

    storage::ntp_config::default_overrides overrides{
      .cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction,
    };
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(i),
          model::timestamp::now(),
          key_limited_random_batch_generator());
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    // Build the maps before and after compaction (nc_ vs c_) to reflect the
    // changes

    // Read non-compacted version
    storage::local_log_reader_config nc_reader_cfg(
      model::offset(0), model::offset::max());
    auto nc_reader = log->make_reader(nc_reader_cfg).get();

    auto nc_summary = std::move(nc_reader)
                        .consume(batch_summary_accumulator{}, model::no_timeout)
                        .get();

    auto& nc_summaries = nc_summary.summaries;
    auto& nc_acc_size = nc_summary.acc_size;

    // Compact topic
    vlog(e2e_test_log.info, "Starting compaction");
    storage::housekeeping_config h_cfg(
      model::timestamp::min(),
      std::nullopt,
      log->offsets().committed_offset,
      log->offsets().committed_offset,
      log->offsets().committed_offset,
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    log->housekeeping(h_cfg).get();

    // Read compacted version
    storage::local_log_reader_config c_reader_cfg(
      model::offset(0), model::offset::max());
    auto c_reader = log->make_reader(c_reader_cfg).get();
    auto c_acc = std::move(c_reader)
                   .consume(batch_summary_accumulator{}, model::no_timeout)
                   .get();

    auto& c_summaries = c_acc.summaries;
    auto& c_acc_size = c_acc.acc_size;
    auto& c_prev_size = c_acc.prev_size;

    auto num_compacted = nc_summaries.size() - c_summaries.size();
    vlog(
      e2e_test_log.info,
      "Number of compacted batches: {} (before {}, after {})",
      num_compacted,
      nc_summaries.size(),
      c_summaries.size());
    vlog(
      e2e_test_log.info,
      "Size before compaction {}, size after compaction {}",
      nc_acc_size.back(),
      c_acc_size.back());
    ASSERT_TRUE(num_compacted > 0);

    for (size_t i = 0; i < num_test_cases; i++) {
        auto ix_base = model::test::get_int((size_t)0, nc_summaries.size() - 1);
        auto ix_last = model::test::get_int(ix_base, nc_summaries.size() - 1);
        auto base = nc_summaries[ix_base].base;
        auto last = nc_summaries[ix_last].last;

        // To find expected size we need to first search c_summaries
        auto c_it_base = std::lower_bound(
          c_summaries.begin(),
          c_summaries.end(),
          batch_summary{.base = base},
          [](const batch_summary& lhs, const batch_summary& rhs) {
              return lhs.base < rhs.base;
          });
        auto c_ix_base = std::distance(c_summaries.begin(), c_it_base);
        auto c_it_last = std::upper_bound(
          c_summaries.begin(),
          c_summaries.end(),
          batch_summary{.last = last},
          [](const batch_summary& lhs, const batch_summary& rhs) {
              return lhs.last < rhs.last;
          });
        bool fully_compacted_range = false;
        if (c_it_last != c_summaries.begin()) {
            c_it_last = std::prev(c_it_last);
        } else {
            // fully compacted
            fully_compacted_range = true;
        }
        auto c_ix_last = std::distance(c_summaries.begin(), c_it_last);
        auto expected_size = fully_compacted_range
                               ? 0
                               : c_acc_size[c_ix_last] - c_prev_size[c_ix_base];
        vlog(
          e2e_test_log.debug,
          "NON-COMPACTED offset range: {}-{} (indexes: {}-{}), "
          "COMPACTED offset range: {}-{} (indexes: {}-{})",
          base,
          last,
          ix_base,
          ix_last,
          c_summaries[c_ix_base].base,
          c_summaries[c_ix_last].last,
          c_ix_base,
          c_ix_last);

        auto result = log->offset_range_size(base, last).get();

        ASSERT_TRUE(result.has_value());

        vlog(
          e2e_test_log.debug,
          "base: {}, last: {}, expected size: {}, actual size: {}",
          base,
          last,
          expected_size,
          result->on_disk_size);

        ASSERT_EQ(expected_size, result->on_disk_size);
        ASSERT_EQ(last, result->last_offset);

        size_t consumed_size = 0;
        storage::local_log_reader_config c_reader_cfg(
          base, result->last_offset);
        c_reader_cfg.skip_readers_cache = true;
        c_reader_cfg.skip_batch_cache = true;
        auto c_log_rdr = log->make_reader(std::move(c_reader_cfg)).get();
        batch_size_accumulator c_size_acc{
          .size_bytes = &consumed_size,
        };
        std::move(c_log_rdr).consume(c_size_acc, model::no_timeout).get();
        ASSERT_EQ(expected_size, result->on_disk_size);
    }

    auto new_start_offset = model::next_offset(first_segment_last_offset);
    log->truncate_prefix(storage::truncate_prefix_config(new_start_offset))
      .get();

    auto lstat = log->offsets();
    ASSERT_EQ(lstat.start_offset, new_start_offset);

    // Check that out of range access triggers exception.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::offset(0), model::next_offset(new_start_offset))
        .get()
      == std::nullopt);

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::next_offset(new_start_offset),
          model::next_offset(lstat.committed_offset))
        .get()
      == std::nullopt);
};

TEST_F(storage_test_fixture, test_offset_range_size2_compacted) {
#ifdef NDEBUG
    size_t num_test_cases = 1000;
    size_t num_segments = 300;
#else
    size_t num_test_cases = 100;
    size_t num_segments = 30;
#endif
    // This test generates 300 segments and creates a record batch map.
    // Then it runs compaction and creates a second record batch map. Then it
    // uses offset_range_size method to fetch segments of various sizes. We need
    // to maps to be able to start on every offset, not only offsets that
    // survived compaction. The pre-compaction map is used to pick starting
    // point. The post-compaction map is used to calculate the expected size.
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("kafka", "test-topic", 0);

    storage::ntp_config::default_overrides overrides{
      .cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction,
    };
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          model::timestamp::now(),
          key_limited_random_batch_generator());
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    // Build the maps before and after compaction (nc_ vs c_) to reflect the
    // changes

    // Read non-compacted version
    storage::local_log_reader_config nc_reader_cfg(
      model::offset(0), model::offset::max());
    auto nc_reader = log->make_reader(nc_reader_cfg).get();

    auto nc_summary = std::move(nc_reader)
                        .consume(batch_summary_accumulator{}, model::no_timeout)
                        .get();

    auto& nc_summaries = nc_summary.summaries;
    auto& nc_acc_size = nc_summary.acc_size;

    // Compact topic
    vlog(e2e_test_log.info, "Starting compaction");
    storage::housekeeping_config h_cfg(
      model::timestamp::min(),
      std::nullopt,
      log->offsets().committed_offset,
      log->offsets().committed_offset,
      log->offsets().committed_offset,
      std::nullopt,
      std::nullopt,
      0ms,
      as);
    log->housekeeping(h_cfg).get();

    // Read compacted version
    storage::local_log_reader_config c_reader_cfg(
      model::offset(0), model::offset::max());
    auto c_reader = log->make_reader(c_reader_cfg).get();

    auto c_acc = std::move(c_reader)
                   .consume(batch_summary_accumulator{}, model::no_timeout)
                   .get();

    auto& c_summaries = c_acc.summaries;
    auto& c_acc_size = c_acc.acc_size;
    auto& c_prev_size = c_acc.prev_size;

    auto num_compacted = nc_summaries.size() - c_summaries.size();
    vlog(
      e2e_test_log.info,
      "Number of compacted batches: {} (before {}, after {})",
      num_compacted,
      nc_summaries.size(),
      c_summaries.size());
    vlog(
      e2e_test_log.info,
      "Size before compaction {}, size after compaction {}",
      nc_acc_size.back(),
      c_acc_size.back());
    ASSERT_TRUE(num_compacted > 0);

    for (const auto& s : c_summaries) {
        vlog(
          e2e_test_log.debug,
          "compacted segment {}-{} size: {}",
          s.base,
          s.last,
          s.batch_size);
    }

    for (size_t i = 0; i < num_test_cases; i++) {
        // - pick 'base' randomly (use non-compacted list to make sure
        //   that some requests are starting inside the gaps)
        // - translate to compacted index
        // - pick target upload size randomly
        // - do the query
        // - query the log and compare the result
        auto base_ix = model::test::get_int((size_t)0, nc_summaries.size() - 1);
        auto base = nc_summaries[base_ix].base;
        // convert to compacted
        auto c_it_base = std::lower_bound(
          c_summaries.begin(),
          c_summaries.end(),
          batch_summary{.base = base},
          [](const batch_summary& lhs, const batch_summary& rhs) {
              return lhs.base < rhs.base;
          });
        auto c_ix_base = std::distance(c_summaries.begin(), c_it_base);
        // we should use size after compaction in the query
        auto max_size = c_acc_size.back() - c_prev_size[c_ix_base];
        auto min_size = 1UL;
        auto target_size = model::test::get_int(min_size, max_size);

        vlog(
          e2e_test_log.debug,
          "NON-COMPACTED start offset: {}, COMPACTED start offset: {}, target "
          "size: {}",
          base,
          c_summaries[c_ix_base].base,
          c_ix_base,
          target_size);

        auto result = log
                        ->offset_range_size(
                          base,
                          storage::log::offset_range_size_requirements_t{
                            .target_size = target_size,
                            .min_size = 0,
                          })
                        .get();
        ASSERT_TRUE(result.has_value());
        auto last_offset = result->last_offset;

        size_t expected_size = 0;

        storage::local_log_reader_config c_reader_cfg(base, last_offset);
        c_reader_cfg.skip_readers_cache = true;
        c_reader_cfg.skip_batch_cache = true;
        auto c_log_rdr = log->make_reader(std::move(c_reader_cfg)).get();
        batch_size_accumulator c_size_acc{
          .size_bytes = &expected_size,
        };
        std::move(c_log_rdr).consume(c_size_acc, model::no_timeout).get();
        ASSERT_EQ(expected_size, result->on_disk_size);
        ASSERT_TRUE(result->on_disk_size >= target_size);
    }

    SUCCEED() << fmt::format("Prefix truncating");
    auto new_start_offset = model::next_offset(first_segment_last_offset);
    log->truncate_prefix(storage::truncate_prefix_config(new_start_offset))
      .get();

    auto lstat = log->offsets();
    ASSERT_EQ(lstat.start_offset, new_start_offset);

    // Check that out of range access triggers exception.

    SUCCEED() << fmt::format("Checking for null on out-of-range");
    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::offset(0),
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = 1,
          })
        .get()
      == std::nullopt);

    // Query committed offset of the last batch, expect
    // no result.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          nc_summaries.back().last,
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = 0,
          })
        .get()
      == std::nullopt);

    // Query offset out of range to trigger the exception.

    ASSERT_TRUE(
      log
        ->offset_range_size(
          model::next_offset(c_summaries.back().last),
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = 0,
          })
        .get()
      == std::nullopt);

    SUCCEED() << "Checking the last batch";
    // Check that the last batch can be measured independently
    auto res = log
                 ->offset_range_size(
                   c_summaries.back().base,
                   storage::log::offset_range_size_requirements_t{
                     .target_size = 0x10000,
                     .min_size = 0,
                   })
                 .get();

    // Only one batch is returned
    ASSERT_EQ(res->last_offset, lstat.committed_offset);
    ASSERT_EQ(res->on_disk_size, c_acc_size.back() - c_prev_size.back());

    // Check that we can measure the size of the log tail. This is needed for
    // timed uploads.
    size_t tail_length = 5;

    for (size_t i = 0; i < std::min(tail_length, c_summaries.size()); i++) {
        SUCCEED() << fmt::format("Checking i = {}", i);
        auto ix_batch = c_summaries.size() - 1 - i;
        res = log
                ->offset_range_size(
                  c_summaries.at(ix_batch).base,
                  storage::log::offset_range_size_requirements_t{
                    .target_size = 0x10000,
                    .min_size = 0,
                  })
                .get();

        ASSERT_EQ(res->last_offset, lstat.committed_offset);
        ASSERT_EQ(
          res->on_disk_size, c_acc_size.back() - c_prev_size.at(ix_batch));
    }

    // Check that the min_size is respected
    SUCCEED() << "Checking the back segment";
    ASSERT_TRUE(
      log
        ->offset_range_size(
          c_summaries.back().base,
          storage::log::offset_range_size_requirements_t{
            .target_size = 0x10000,
            .min_size = c_summaries.back().batch_size + 1,
          })
        .get()
      == std::nullopt);
};

namespace storage {
class segment_index_observer {
public:
    explicit segment_index_observer(segment_index& index, size_t segment_size)
      : _index{index}
      , _size(segment_size) {}

    size_t max_step_size() const {
        chunked_vector<size_t> diffs;
        chunked_vector<uint64_t> pos
          = _index._state.index.copy_position_index();
        diffs.reserve(pos.size() + 1);
        std::adjacent_difference(
          pos.begin(), pos.end(), std::back_inserter(diffs));

        // Difference from last position index to end of file might be the
        // largest step.
        auto last = _size - pos.back();
        diffs.push_back(last);
        return std::ranges::max(diffs);
    }

    static std::optional<size_t> disk_usage_size(segment_index& index) {
        return index._disk_usage_size;
    }

private:
    segment_index& _index;
    size_t _size;
};
} // namespace storage

TEST_F(storage_test_fixture, test_offset_range_size_incremental) {
#ifdef NDEBUG
    size_t num_segments = 300;
#else
    size_t num_segments = 30;
#endif
    // This test attempts to consume the log incrementally using overload
    // of the offset_range_size that gets offset+size and returns size+last
    // offset; The idea is to get the size of the section and then to consume it
    // using the reader and compare. Then repeat starting from the last offset
    // + 1. The total size of the log should be consumed.
    // The test is repeated with different target sizes starting from very
    // small. The small step is not expected to always return small log regions
    // because it's limited by the sampling step of the index.
    struct size_target {
        size_t target;
        size_t min_size;
        size_t max_size;
    };
    std::vector<size_target> size_classes = {
      {100, 0, 100},
      {2000, 256, 2000},
      {10_KiB, 1_KiB, 10_KiB},
      {50_KiB, 1_KiB, 50_KiB},
      {500_KiB, 1_KiB, 500_KiB},
      {1_MiB, 1_KiB, 1_MiB},
      {10_MiB, 1_KiB, 10_MiB},
      {100_MiB, 1_KiB, 100_MiB},
    };
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("redpanda", "test-topic", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          custom_ts_batch_generator(model::timestamp::now()));
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }
    size_t max_step_size = 0;
    for (auto& s : log->segments()) {
        if (s->size_bytes() == 0) {
            continue;
        }
        vlog(
          e2e_test_log.info,
          "Index {} has {} elements",
          s->index().path(),
          s->index().size());
        ASSERT_TRUE(s->index().size() > 0);
        max_step_size = std::max(
          max_step_size,
          storage::segment_index_observer{s->index(), s->size_bytes()}
            .max_step_size());
    }

    ASSERT_TRUE(max_step_size > 0);
    vlog(
      e2e_test_log.info,
      "Max index step size among all segments: {}",
      max_step_size);

    for (auto& sc : size_classes) {
        sc.max_size += max_step_size + sc.target;
    }

    storage::local_log_reader_config reader_cfg(
      model::offset(0), model::offset::max());
    auto reader = log->make_reader(reader_cfg).get();

    auto acc = std::move(reader)
                 .consume(batch_summary_accumulator{}, model::no_timeout)
                 .get();

    // Total log size in bytes
    auto full_log_size = acc.acc_size.back();

    ASSERT_EQ(log->size_bytes(), full_log_size);

    for (auto [target_size, min_size, max_size] : size_classes) {
        model::offset last_offset;
        bool done = false;
        while (!done) {
            auto base = model::next_offset(last_offset);
            auto res = log
                         ->offset_range_size(
                           base,
                           storage::log::offset_range_size_requirements_t{
                             .target_size = target_size,
                             .min_size = min_size,
                           })
                         .get();
            ASSERT_TRUE(res.has_value());
            last_offset = res->last_offset;
            done = last_offset == log->offsets().committed_offset;
            vlog(
              e2e_test_log.info,
              "Requested {}({}min, {}max) bytes, got {} bytes for offset {}",
              target_size,
              min_size,
              max_size,
              res->on_disk_size,
              res->last_offset);
            ASSERT_TRUE(res->on_disk_size > min_size);
            ASSERT_TRUE(res->on_disk_size < max_size);

            // scan the range using the storage reader and compare

            size_t measured_size = 0;
            batch_size_accumulator acc{};
            acc.size_bytes = &measured_size;

            storage::local_log_reader_config reader_cfg(base, res->last_offset);
            reader_cfg.skip_readers_cache = true;
            reader_cfg.skip_batch_cache = true;
            auto reader = log->make_reader(reader_cfg).get();
            std::move(reader).consume(acc, model::no_timeout).get();
            vlog(
              e2e_test_log.info,
              "Expected size: {}, actual size: {}",
              measured_size,
              res->on_disk_size);
            ASSERT_EQ(measured_size, res->on_disk_size);
        }
    }
};

TEST_F(storage_test_fixture, dirty_ratio) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp,
        mgr.config().base_dir,
        std::make_unique<storage::ntp_config::default_overrides>(overrides)));

    auto* disk_log = static_cast<storage::disk_log_impl*>(log.get());

    // add a segment with random keys until a certain size
    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    static constexpr double tolerance = 1.0e-6;
    uint64_t closed_segments_size_bytes = 0;
    uint64_t dirty_segments_size_bytes = 0;
    auto assert_on_new_segment = [&disk_log,
                                  &closed_segments_size_bytes,
                                  &dirty_segments_size_bytes](size_t index) {
        auto new_segment_size_bytes = disk_log->segments()[index]->size_bytes();
        closed_segments_size_bytes += new_segment_size_bytes;
        dirty_segments_size_bytes += new_segment_size_bytes;
        auto expected_dirty_ratio
          = static_cast<double>(dirty_segments_size_bytes)
            / static_cast<double>(closed_segments_size_bytes);
        ASSERT_EQ(disk_log->dirty_segment_bytes(), dirty_segments_size_bytes);
        ASSERT_EQ(disk_log->closed_segment_bytes(), closed_segments_size_bytes);
        ASSERT_NEAR(disk_log->dirty_ratio(), expected_dirty_ratio, tolerance);
    };

    auto compact_and_assert = [&disk_log,
                               &closed_segments_size_bytes,
                               &dirty_segments_size_bytes,
                               &as]() {
        // Perform sliding window compaction, which will fully cleanly compact
        // the log.
        static const compaction::compaction_config compact_cfg(
          model::offset::max(),
          model::offset::max(),
          model::offset::max(),
          std::nullopt,
          std::nullopt,
          as);
        disk_log->sliding_window_compact(compact_cfg).get();

        dirty_segments_size_bytes = 0;

        ASSERT_EQ(disk_log->dirty_segment_bytes(), dirty_segments_size_bytes);
        ASSERT_EQ(disk_log->closed_segment_bytes(), closed_segments_size_bytes);
        ASSERT_NEAR(disk_log->dirty_ratio(), 0.0, tolerance);
    };

    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll().get();
    ASSERT_EQ(disk_log->segment_count(), 2);
    assert_on_new_segment(0);
    compact_and_assert();

    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll().get();
    ASSERT_EQ(disk_log->segment_count(), 3);
    assert_on_new_segment(1);
    compact_and_assert();

    add_segment(5_MiB, model::term_id(1));
    disk_log->force_roll().get();
    ASSERT_EQ(disk_log->segment_count(), 4);
    assert_on_new_segment(2);
    compact_and_assert();

    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll().get();
    ASSERT_EQ(disk_log->segment_count(), 5);
    assert_on_new_segment(3);
    compact_and_assert();

    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll().get();
    ASSERT_EQ(disk_log->segment_count(), 6);
    assert_on_new_segment(4);
    compact_and_assert();

    // Add more segments, don't perform compaction to allow dirty_segment_bytes
    // to remain non-zero.
    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll().get();
    assert_on_new_segment(5);

    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll().get();
    assert_on_new_segment(6);

    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll().get();
    assert_on_new_segment(7);

    std::vector<model::offset> base_offsets;
    base_offsets.reserve(disk_log->segment_count());
    for (const auto& s : disk_log->segments()) {
        base_offsets.push_back(s->offsets().get_base_offset());
    }
    // Last base offset is not needed for truncation
    base_offsets.pop_back();
    std::reverse(base_offsets.begin(), base_offsets.end());

    auto prev_dirty_segment_bytes = disk_log->dirty_segment_bytes();
    auto prev_closed_segment_bytes = disk_log->dirty_segment_bytes();
    auto prev_dirty_ratio = disk_log->dirty_ratio();

    // Gradually truncate the log and see that dirty/closed segment bytes
    // have decreased.
    for (const auto& base_offset : base_offsets) {
        disk_log->truncate(storage::truncate_config(base_offset)).get();

        auto new_dirty_segment_bytes = disk_log->dirty_segment_bytes();
        auto new_closed_segment_bytes = disk_log->dirty_segment_bytes();
        auto new_dirty_ratio = disk_log->dirty_ratio();
        ASSERT_LE(new_dirty_segment_bytes, prev_dirty_segment_bytes);
        ASSERT_LE(new_closed_segment_bytes, prev_closed_segment_bytes);
        ASSERT_LE(new_dirty_ratio, prev_dirty_ratio);
        prev_dirty_segment_bytes = new_dirty_segment_bytes;
        prev_closed_segment_bytes = new_closed_segment_bytes;
        prev_dirty_ratio = new_dirty_ratio;
    }

    ASSERT_EQ(disk_log->dirty_segment_bytes(), 0);
    ASSERT_EQ(disk_log->closed_segment_bytes(), 0);
    ASSERT_NEAR(disk_log->dirty_ratio(), 0.0, tolerance);
}

TEST_F(storage_test_fixture, dirty_and_closed_bytes_bookkeeping) {
    auto log_cfg = default_log_config(test_dir);
    using namespace storage;

    ss::abort_source abs;
    auto ntp = model::ntp("test_ns", "test_tpc", 0);

    ntp_config::default_overrides overrides;
    overrides.retention_bytes = tristate<size_t>{1};
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction
        | model::cleanup_policy_bitflags::deletion;

    std::unique_ptr<log_manager> mgr = std::make_unique<log_manager>(
      log_cfg, kvstore, resources, feature_table);
    auto deferred = ss::defer([&mgr]() mutable { mgr->stop().get(); });
    auto log = mgr
                 ->manage(ntp_config(
                   ntp,
                   mgr->config().base_dir,
                   std::make_unique<ntp_config::default_overrides>(overrides)))
                 .get();
    log->stm_hookset()->start();
    auto* disk_log = static_cast<disk_log_impl*>(log.get());

    housekeeping_config cfg{
      model::timestamp::max(),
      1,
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      0ms,
      abs};

    // add a segment with random keys until a certain size
    auto add_segment = [&](size_t size, model::term_id term) {
        do {
            static const auto to_add = 1_KiB;
            append_single_record_batch(log, 1, term, to_add, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    using func_t = std::function<void()>;

    // Adds a new (random sized) segment to the log.
    auto add_segment_func = [&]() {
        auto size = random_generators::get_int(4_KiB, 10_MiB);
        add_segment(size, model::term_id(1));
    };

    // Force rolls the log.
    auto force_roll_func = [&]() { disk_log->force_roll().get(); };

    // Restarts the log manager- this has the added benefit of forcing recovery
    // from the existing segment set.
    auto restart_log_manager_func = [&]() {
        log->stm_hookset()->stop();
        mgr->stop().get();
        mgr = std::make_unique<log_manager>(
          log_cfg, kvstore, resources, feature_table);
        log = mgr
                ->manage(ntp_config(
                  ntp,
                  mgr->config().base_dir,
                  std::make_unique<ntp_config::default_overrides>(overrides)))
                .get();
        disk_log = static_cast<disk_log_impl*>(log.get());
        log->stm_hookset()->start();
    };

    auto adjacent_merge_func = [&]() {
        disk_log
          ->adjacent_merge_compact(disk_log->segments().copy(), cfg.compact)
          .get();
    };

    auto sliding_window_func = [&]() {
        disk_log->sliding_window_compact(cfg.compact).get();
    };

    // Prefix truncate at a random point in the log.
    auto prefix_truncate_func = [&]() {
        auto dirty_offset = disk_log->offsets().dirty_offset;
        auto offset = random_generators::get_int(1L, dirty_offset());
        disk_log->truncate_prefix(truncate_prefix_config(model::offset(offset)))
          .get();
    };

    // Truncate to a random point in the log.
    auto truncate_func = [&]() {
        auto dirty_offset = disk_log->offsets().dirty_offset;
        auto offset = random_generators::get_int(1L, dirty_offset());
        disk_log->truncate(storage::truncate_config(model::offset{offset}))
          .get();
    };

    std::vector<func_t> funcs = {
      add_segment_func,
      force_roll_func,
      restart_log_manager_func,
      adjacent_merge_func,
      sliding_window_func,
      prefix_truncate_func,
      truncate_func};

#ifdef NDEBUG
    static constexpr int num_operations = 50;
#else
    static constexpr int num_operations = 10;
#endif

    for (int i = 0; i < num_operations; ++i) {
        // Invoke one of the functions that influences the dirty and closed
        // bytes of the log.
        random_generators::random_choice(funcs)();
        check_dirty_and_closed_segment_bytes(log);
        // We should drive segment production to ensure there is always
        // something for the functions to do.
        add_segment_func();

        // Recheck post segment addition.
        check_dirty_and_closed_segment_bytes(log);
    }

    // Sliding window compact down the rest of the log
    bool did_compact = true;
    while (did_compact) {
        did_compact = disk_log->sliding_window_compact(cfg.compact).get();
        check_dirty_and_closed_segment_bytes(log);
    }
    log->stm_hookset()->stop();
}

TEST_F(storage_test_fixture, negative_dirty_and_closed_bytes_triggers_reset) {
    using namespace storage;
    disk_log_builder b;
    b | start();
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    const auto num_segs = 5;
    const auto start_offset = 0;
    const auto records_per_seg = 150;
    for (int i = 0; i < num_segs; ++i) {
        auto offset = start_offset + i * records_per_seg;
        b | add_segment(offset)
          | add_random_batch(
            offset, records_per_seg, maybe_compress_batches::yes);
        disk_log.force_roll().get();
    }

    auto saved_dirty_segment_bytes = disk_log.dirty_segment_bytes();
    auto saved_closed_segment_bytes = disk_log.closed_segment_bytes();
    ASSERT_EQ(disk_log.size_bytes(), saved_dirty_segment_bytes);
    ASSERT_EQ(saved_dirty_segment_bytes, saved_closed_segment_bytes);

    // Add a large negative number to dirty segment bytes.
    ssize_t large_negative_number
      = -5 * static_cast<ssize_t>(disk_log.size_bytes());
    b.add_dirty_segment_bytes(large_negative_number);

    ASSERT_LT(disk_log.dirty_segment_bytes(), 0);
    // Calling disk_log.dirty_ratio() will trigger
    // disk_log_impl::reset_dirty_and_closed_bytes(), a safety hatch that should
    // reset the dirty segment bytes to the proper value.
    auto dirty_ratio = disk_log.dirty_ratio();

    static const double epsilon = 1.0e-6;
    ASSERT_NEAR(dirty_ratio, 1.0, epsilon);

    // Dirty segment bytes should be equal to its value before the bogus
    // negative amount was added.
    ASSERT_EQ(saved_dirty_segment_bytes, disk_log.dirty_segment_bytes());

    // We can repeat the same process with the closed segment bytes.
    b.add_closed_segment_bytes(large_negative_number);

    ASSERT_LT(disk_log.closed_segment_bytes(), 0);

    // Trigger the safety hatch again.
    dirty_ratio = disk_log.dirty_ratio();

    ASSERT_NEAR(dirty_ratio, 1.0, epsilon);

    ASSERT_EQ(saved_closed_segment_bytes, disk_log.closed_segment_bytes());
}

TEST_F(storage_test_fixture, compaction_scheduling) {
    using log_manager_accessor = storage::testing_details::log_manager_accessor;
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    std::vector<log_holder> logs;

    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    ov.min_cleanable_dirty_ratio = tristate<double>{0.2};

    for (const auto& topic : {"tapioca", "cassava", "kudzu"}) {
        auto ntp = model::ntp("kafka", topic, 0);
        auto log = manage_log(
          mgr,
          storage::ntp_config(
            ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)));
        logs.push_back(std::move(log));
    }

    auto& meta_list = log_manager_accessor::logs_list(mgr);

    using bflags = storage::log_housekeeping_meta::bitflags;

    static constexpr auto is_set = [](bflags var, auto flag) {
        return (var & flag) == flag;
    };

    // Floating point comparison tolerance
    static constexpr auto tol = 1.0e-6;

    auto append_and_force_roll = [this](auto& log, int num_batches = 10) {
        auto headers = append_random_batches<linear_int_kv_batch_generator>(
          log, num_batches, model::term_id{0}, model::timestamp::now());
        log->force_roll().get();
    };

    // Attempt a housekeeping scan with no partitions to compact
    log_manager_accessor::housekeeping_scan(mgr).get();

    for (const auto& meta : meta_list) {
        ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
        ASSERT_TRUE(!is_set(meta.flags, bflags::compacted));
    }

    // Append batches and force roll with first log- it should be the only one
    // compacted
    append_and_force_roll(logs[0], 30);
    ASSERT_NEAR(logs[0]->dirty_ratio(), 1.0, tol);

    log_manager_accessor::housekeeping_scan(mgr).get();

    for (const auto& meta : meta_list) {
        bool expect_compacted = meta.handle->config().ntp()
                                == logs[0]->config().ntp();
        ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
        ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
        ASSERT_EQ(expect_compacted, is_set(meta.flags, bflags::compacted));
        auto batches = read_and_validate_all_batches(logs[0]);
        linear_int_kv_batch_generator::validate_post_compaction(
          std::move(batches));
    }

    // Append fewer batches and force roll with second log- it should be the
    // only one compacted
    append_and_force_roll(logs[1], 20);
    ASSERT_NEAR(logs[1]->dirty_ratio(), 1.0, tol);

    log_manager_accessor::housekeeping_scan(mgr).get();

    for (const auto& meta : meta_list) {
        bool expect_compacted = meta.handle->config().ntp()
                                == logs[1]->config().ntp();
        ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
        ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
        ASSERT_EQ(expect_compacted, is_set(meta.flags, bflags::compacted));
        auto batches = read_and_validate_all_batches(logs[1]);
        linear_int_kv_batch_generator::validate_post_compaction(
          std::move(batches));
    }

    // Append batches and force roll all logs- all of them will be compacted
    for (auto& log : logs) {
        append_and_force_roll(log, 10);
    }

    ASSERT_GE(logs[0]->dirty_ratio(), 1.0 / 3.0);
    ASSERT_GE(logs[1]->dirty_ratio(), 1.0 / 2.0);
    ASSERT_NEAR(logs[2]->dirty_ratio(), 1.0, tol);

    log_manager_accessor::housekeeping_scan(mgr).get();

    // Logs in the meta list will be ordered w/r/t their dirty ratio
    // (descending) post compaction
    auto log_it = logs.rbegin();
    for (const auto& meta : meta_list) {
        ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
        ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
        ASSERT_TRUE(is_set(meta.flags, bflags::should_compact));
        ASSERT_TRUE(is_set(meta.flags, bflags::compacted));
        ASSERT_EQ(meta.handle->config().ntp(), (*log_it)->config().ntp());
        ++log_it;
    }

    for (auto& log : logs) {
        auto batches = read_and_validate_all_batches(log);
    }
}

TEST_F(storage_test_fixture, max_compaction_lag) {
    using log_manager_accessor = storage::testing_details::log_manager_accessor;
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    std::vector<ss::shared_ptr<storage::log>> logs;

    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    ov.min_cleanable_dirty_ratio = tristate<double>{1.0};
    ov.max_compaction_lag_ms = 1000ms;

    auto ntp = model::ntp("kafka", "tapioca", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)));

    using bflags = storage::log_housekeeping_meta::bitflags;
    static constexpr auto is_set = [](bflags var, auto flag) {
        return (var & flag) == flag;
    };

    auto append_and_force_roll = [this, &log](int num_batches = 10) {
        auto headers = append_random_batches<linear_int_kv_batch_generator>(
          log, num_batches);
        log->force_roll().get();
    };

    // Append and close one segment, then compact.
    // The one closed segment is compacted because the log is 100% dirty.
    auto start = std::chrono::steady_clock::now();
    append_and_force_roll();
    ASSERT_EQ(log->dirty_ratio(), 1.0);
    log_manager_accessor::housekeeping_scan(mgr).get();

    auto& meta = log_manager_accessor::logs_list(mgr).front();
    ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compacted));
    read_and_validate_all_batches(log);

    // Append more batches and compact.
    // Now the log is half-dirty, which isn't dirty enough to compact.
    append_and_force_roll();
    ASSERT_LT(log->dirty_ratio(), 1.0);
    log_manager_accessor::housekeeping_scan(mgr).get();

    ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
    // This test should run fast enough that the log is not compacted,
    // but on the rare occasion it doesn't, let's not fail.
    ASSERT_TRUE(
      !is_set(meta.flags, bflags::compacted)
      || (std::chrono::steady_clock::now() - start >= ov.max_compaction_lag_ms.value()));
    read_and_validate_all_batches(log);

    // Wait for the max lag to pass, then compact.
    // Compaction should happen despite the low dirty ratio.
    ss::sleep(ov.max_compaction_lag_ms.value() + 50ms).get();
    log_manager_accessor::housekeeping_scan(mgr).get();

    ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compacted));
    read_and_validate_all_batches(log);
}

TEST_F(storage_test_fixture, min_compaction_lag) {
    using log_manager_accessor = storage::testing_details::log_manager_accessor;
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    // Surely, no test could run this slow...
    ov.min_compaction_lag_ms = 100000s;

    auto ntp = model::ntp("kafka", "tapioca", 0);
    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)));

    using bflags = storage::log_housekeeping_meta::bitflags;
    static constexpr auto is_set = [](bflags var, auto flag) {
        return (var & flag) == flag;
    };

    auto append_and_force_roll = [this, &log]() {
        auto headers = append_random_batches<linear_int_kv_batch_generator>(
          log, 10);
        log->force_roll().get();
    };

    // Append and close one segment, then compact.
    // Because of min lag, no segments should be compacted.
    append_and_force_roll();
    log_manager_accessor::housekeeping_scan(mgr).get();

    auto& meta = log_manager_accessor::logs_list(mgr).front();
    ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
    // The log was compacted, but no segments were eligible due to min lag.
    ASSERT_TRUE(is_set(meta.flags, bflags::compacted));
    for (const auto& batch : read_and_validate_all_batches(log)) {
        // The batches should not have been compacted.
        ASSERT_EQ(
          batch.record_count(),
          linear_int_kv_batch_generator::records_per_batch);
    }

    // Roll to reset compaction checks. Appending more batches breaks
    // the batch generator's expectation after compacting.
    log->force_roll().get();
    overrides_t ov2;
    ov2.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    ov2.min_compaction_lag_ms = 0s; // The default.
    log->set_overrides(ov2);

    log_manager_accessor::housekeeping_scan(mgr).get();
    ASSERT_TRUE(is_set(meta.flags, bflags::lifetime_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compaction_checked));
    ASSERT_TRUE(is_set(meta.flags, bflags::compacted));
    auto batches = read_and_validate_all_batches(log);
    linear_int_kv_batch_generator::validate_post_compaction(std::move(batches));
}

// Ensures that the log_housekeeping_meta level locking/concurrency in
// the log_manager is race free during ntp removal, addition, and housekeeping.
// By allowing regular housekeeping to run on a quick interval and also
// triggering urgent garbage collection, we create contention over a log_meta's
// housekeeping lock, all while ntps are being concurrently removed.
TEST_F(storage_test_fixture, log_manager_concurrent_housekeeping_and_removal) {
#ifdef NDEBUG
    int num_rounds = 100;
#else
    int num_rounds = 50;
#endif

    ss::abort_source as;
    static constexpr auto abort_func_sleep = 100ms;
    static constexpr auto manage_func_sleep = 25ms;
    static constexpr auto remove_func_sleep = 50ms;
    static constexpr auto trigger_gc_func_sleep = 50ms;
    static constexpr auto log_compaction_interval_ms = 50ms;

    auto log_cfg = default_log_config(test_dir);
    log_cfg.compaction_interval
      = config::mock_binding<std::chrono::milliseconds>(
        log_compaction_interval_ms);
    storage::log_manager mgr = make_log_manager(log_cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    mgr.start().get();
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction
                                 | model::cleanup_policy_bitflags::deletion;

    ov.min_cleanable_dirty_ratio = tristate<double>{};

    auto abort_fut = ss::do_until(
                       [&] { return num_rounds == 0; },
                       [&]() {
                           --num_rounds;
                           return ss::sleep(abort_func_sleep);
                       })
                       .then([&]() {
                           as.request_abort();
                           return ss::now();
                       });

    auto manage_ntp_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          auto ntp = model::random_ntp();
          ntp.ns = model::kafka_namespace;
          return mgr
            .manage(
              storage::ntp_config(
                ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)))
            .then([]([[maybe_unused]] auto log) {
                return ss::sleep(manage_func_sleep);
            });
      });

    auto remove_ntp_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          auto managed_ntps = mgr.get_all_ntps();
          if (managed_ntps.empty()) {
              return ss::now();
          }

          auto ntp_to_remove = random_generators::random_choice(
            std::vector<model::ntp>(managed_ntps.begin(), managed_ntps.end()));

          mgr.get(ntp_to_remove)->stm_hookset()->start();
          mgr.get(ntp_to_remove)->stm_hookset()->stop();
          return mgr.remove(ntp_to_remove).then([]() {
              return ss::sleep(remove_func_sleep);
          });
      });

    auto trigger_gc_fut = ss::do_until(
      [&] { return as.abort_requested(); },
      [&]() {
          mgr.trigger_gc();
          return ss::sleep(trigger_gc_func_sleep);
      });

    ss::when_all(
      std::move(abort_fut),
      std::move(manage_ntp_fut),
      std::move(trigger_gc_fut),
      std::move(remove_ntp_fut))
      .get();

    for (const auto& ntp : mgr.get_all_ntps()) {
        auto stmm = mgr.get(ntp)->stm_hookset();
        stmm->start();
        stmm->stop();
    }
}

TEST_F(storage_test_fixture, disk_usage_with_log_throwing_exception) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp_a = model::ntp("kafka", "a", 0);
    auto log_a = manage_log(
      mgr, storage::ntp_config(ntp_a, mgr.config().base_dir));
    auto ntp_b = model::ntp("kafka", "b", 0);
    auto log_b = manage_log(
      mgr, storage::ntp_config(ntp_b, mgr.config().base_dir));

    // Closing the housekeeping gate will cause log->disk_usage() to throw an
    // exception.
    auto* disk_log = static_cast<storage::disk_log_impl*>(log_b.get());
    disk_log->gate().close().get();

    // Check that the disk usage is still collected, even if one of the logs
    // throws an exception for some reason.
    ASSERT_NO_THROW(mgr.disk_usage().get());

    // Reassign the gate so there is no double close().
    disk_log->gate() = ss::gate{};
};

TEST_F(storage_test_fixture, prefix_truncate_offset_range_size) {
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("kafka", "test-topic", 0);

    storage::ntp_config::default_overrides overrides{
      .cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion,
    };
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));

    auto log = manage_log(mgr, std::move(ntp_cfg));

    size_t num_segments = 1;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          key_limited_random_batch_generator());
        log->force_roll().get();
    }

    auto dirty_offset = log->offsets().dirty_offset;
    auto new_start_offset = model::offset{
      random_generators::get_int(dirty_offset())};

    SUCCEED() << fmt::format(
      "Prefix truncating at offset {}", new_start_offset);

    log->truncate_prefix(storage::truncate_prefix_config(new_start_offset))
      .get();

    auto lstat = log->offsets();
    ASSERT_EQ(lstat.start_offset, new_start_offset);

    for (int64_t o = model::next_offset(new_start_offset)(); o < dirty_offset();
         ++o) {
        // Expect that no errors are thrown by querying the offset range size
        // for any offset.
        ASSERT_NO_THROW(log
                          ->offset_range_size(
                            model::offset{o},
                            storage::log::offset_range_size_requirements_t{
                              .target_size = 0x10000,
                              .min_size = 1,
                            })
                          .get());
    }
}

TEST_F(storage_test_fixture, log_compaction_enable_sliding_window) {
    // Simulate enabling sliding window after starting Redpanda with it disabled
    // by setting the compaction_reserved_memory in the memory group to 0.
    auto& mem_groups = memory_groups();
    testing::system_memory_groups_accessor::compaction_reserved_memory(
      mem_groups) = 0;
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("kafka", "a", 0);
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)));

    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    // Add a few segments.
    auto num_segments = 5;
    for (int i = 0; i < num_segments; ++i) {
        add_segment(2_MiB, model::term_id(0));
        log->force_roll().get();
    }

    // config::shard_local_cfg().log_compaction_use_sliding_window is still
    // `true` at this point. Assert this call doesn't crash when a map with
    // capacity 0 is used.
    storage::testing_details::log_manager_accessor::housekeeping_scan(mgr)
      .get();
}

struct segment_fields {
    ssize_t size;
    model::term_id term;
    bool mark_as_stable;
    std::optional<model::offset> base_offset_override{std::nullopt};
    std::optional<model::offset> dirty_offset_override{std::nullopt};
};

struct sliding_ranges_test_case {
    ss::sstring desc;
    std::vector<segment_fields> segment_fields;
    std::optional<model::offset> new_start_offset{std::nullopt};
    std::optional<uint32_t> max_segment_count{std::nullopt};
    std::optional<uint32_t> max_range_count{std::nullopt};
    // These ranges have the same inclusivity as the iterator
    // constructor for std::vector, i.e [first, last).
    std::vector<std::pair<size_t, size_t>> expected_ranges;
};

TEST_F(storage_test_fixture, find_sliding_ranges) {
    scoped_config test_local_cfg;
    auto log_cfg = default_log_config(test_dir);
    log_cfg.max_compacted_segment_size = config::mock_binding<size_t>(1_MiB);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(log_cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    // add a segment with random keys until a certain size
    auto add_segment = [](auto& log, size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 1, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    static constexpr int64_t u32_max = std::numeric_limits<uint32_t>::max();
    // Don't forget that the active segment will occupy the last index
    // beyond the segment fields detailed below.
    // clang-format off
    std::vector<sliding_ranges_test_case> test_cases = {
      sliding_ranges_test_case{
	  .desc="Mix of ranges due to raft terms and max size",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},     // 0 A
	    {100_KiB, model::term_id{0}, true},   // 1 A
	    {500_KiB, model::term_id{0}, true},   // 2 A
	    {1024_KiB, model::term_id{0}, true},  // 3  -
	    {1_KiB, model::term_id{1}, true},     // 4   B
	    {2_KiB, model::term_id{1}, true},     // 5   B
	    {3_KiB, model::term_id{1}, true},     // 6   B
	    {100_KiB, model::term_id{1}, true},   // 7   B
	    {50_KiB, model::term_id{2}, true},    // 8    -
	    {50_KiB, model::term_id{3}, true},    // 9    -
	    {50_KiB, model::term_id{4}, true},    // 10   -
	    {50_KiB, model::term_id{5}, true},    // 11   -
	    {1024_KiB, model::term_id{5}, true}}, // 12   -
	                                          // 13   - (Active)
	  .expected_ranges = {
	    {0, 3}, {4, 8}}},
      sliding_ranges_test_case{
	  .desc="Some unstable segments creating gap in ranges",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},    // 0 -
	    {100_KiB, model::term_id{0}, false}, // 1 -
	    {500_KiB, model::term_id{0}, false}, // 2 -
	    {1024_KiB, model::term_id{0}, true}, // 3 -
	    {1_KiB, model::term_id{1}, true},    // 4  A
	    {2_KiB, model::term_id{1}, true},    // 5  A
	    {3_KiB, model::term_id{1}, true},    // 6  A
	    {100_KiB, model::term_id{1}, true}}, // 7  A
	                                         // 8   - (Active)
	  .expected_ranges = {{4, 8}}},
      sliding_ranges_test_case{
	  .desc="One unstable segment",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, false}}, // 0 -
	                                        // 1 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="One stable segment",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true}}, // 0 -
	                                       // 1 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="All stable segments of the same term with total size less than max compacted segment size",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},    // 0 A
	    {100_KiB, model::term_id{0}, true},  // 1 A
	    {100_KiB, model::term_id{0}, true}}, // 2 A
	                                         // 3  - (Active)
	  .expected_ranges = {{0, 3}}},
      sliding_ranges_test_case{
	  .desc="Alternating stable and unstable segments",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},     // 0 -
	    {100_KiB, model::term_id{0}, false},  // 1 -
	    {100_KiB, model::term_id{0}, true},   // 2 -
	    {100_KiB, model::term_id{0}, false},  // 3 -
	    {100_KiB, model::term_id{0}, true},   // 4 -
	    {100_KiB, model::term_id{0}, false}}, // 5 -
	                                          // 6 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="All unstable segments",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, false},    // 0 -
	    {100_KiB, model::term_id{0}, false},  // 1 -
	    {100_KiB, model::term_id{0}, false},  // 2 -
	    {100_KiB, model::term_id{0}, false},  // 3 -
	    {100_KiB, model::term_id{0}, false},  // 4 -
	    {100_KiB, model::term_id{0}, false}}, // 5 -
	                                          // 6 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="All unique segment terms",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},    // 0 -
	    {100_KiB, model::term_id{1}, true},  // 1 -
	    {100_KiB, model::term_id{2}, true},  // 2 -
	    {100_KiB, model::term_id{3}, true},  // 3 -
	    {100_KiB, model::term_id{4}, true},  // 4 -
	    {100_KiB, model::term_id{5}, true}}, // 5 -
	                                         // 6 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="All stable segments with boundaries set by max compacted segment size",
	  .segment_fields={
	    {500_KiB, model::term_id{0}, true},        // 0 A
	    {500_KiB, model::term_id{0}, true},        // 1 A
	    {100_KiB, model::term_id{0}, true},        // 2  -
	    {1_MiB - 10_KiB, model::term_id{0}, true}, // 3   B
	    {5_KiB, model::term_id{0}, true},          // 4   B
	    {100_KiB, model::term_id{0}, true}},       // 5    -
	                                               // 6    - (Active)
	  .expected_ranges = {{0, 2}, {3, 5}}},
      sliding_ranges_test_case{
	  .desc="Just one valid segment at the end",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, false},   // 0 -
	    {100_KiB, model::term_id{0}, false}, // 1 -
	    {100_KiB, model::term_id{0}, false}, // 2 -
	    {100_KiB, model::term_id{0}, false}, // 3 -
	    {100_KiB, model::term_id{0}, false}, // 4 -
	    {100_KiB, model::term_id{0}, true}}, // 5 -
	                                         // 6 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="uint32_t max boundary allowing segment merging",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},                                          // 0 A
	    {100_KiB, model::term_id{0}, true, std::nullopt, model::offset{u32_max}}}, // 1 A
	                                                                               // 2 - (Active)
	  .expected_ranges = {{0, 2}}},
      sliding_ranges_test_case{
	  .desc="one past uint32_t max boundary preventing segment merging",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},                                            // 0 -
	    {100_KiB, model::term_id{0}, true, std::nullopt, model::offset{u32_max+1}}}, // 1 -
	                                                                                 // 2 - (Active)
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="Mergable segments below new_start_offset should be ignored",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true},     // 0 -
	    {100_KiB, model::term_id{0}, true},   // 1 -
	    {500_KiB, model::term_id{0}, true}},  // 2 -
	                                          // 3 - (Active)
	  .new_start_offset=model::offset::max(),
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="Some mergable segment ranges above and below new_start_offset.",
	  .segment_fields={
	    {5_KiB, model::term_id{0}, true, model::offset{0}},      // 0 -
	    {100_KiB, model::term_id{0}, true, model::offset{1}},    // 1 -
	    {500_KiB, model::term_id{0}, true, model::offset{2}},    // 2 -
	    {5_KiB, model::term_id{0}, true, model::offset{101}},    // 3  A
	    {100_KiB, model::term_id{0}, true, model::offset{102}},  // 4  A
	    {500_KiB, model::term_id{0}, true, model::offset{103}}}, // 5  A
	                                                             // 6   - (Active)
	  .new_start_offset=model::offset{100},
	  .expected_ranges = {{3, 6}}},
      sliding_ranges_test_case{
	  .desc="All mergeable segments, but number of max segments is limited to 0 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 -
	    {1_KiB, model::term_id{0}, true},  // 1 -
	    {1_KiB, model::term_id{0}, true},  // 2 -
	    {1_KiB, model::term_id{0}, true},  // 3 -
	    {1_KiB, model::term_id{0}, true},  // 4 -
	    {1_KiB, model::term_id{0}, true}}, // 5 -
	                                       // 6 - (Active)
	  .max_segment_count = 0,
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="All mergeable segments, but number of max segments is limited to 1 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 -
	    {1_KiB, model::term_id{0}, true},  // 1 -
	    {1_KiB, model::term_id{0}, true},  // 2 -
	    {1_KiB, model::term_id{0}, true},  // 3 -
	    {1_KiB, model::term_id{0}, true},  // 4 -
	    {1_KiB, model::term_id{0}, true}}, // 5 -
	                                       // 6 - (Active)
	  .max_segment_count = 1,
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="All mergeable segments, but number of max segments is limited to 2 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 A
	    {1_KiB, model::term_id{0}, true},  // 1 A
	    {1_KiB, model::term_id{0}, true},  // 2  B
	    {1_KiB, model::term_id{0}, true},  // 3  B
	    {1_KiB, model::term_id{0}, true},  // 4   C
	    {1_KiB, model::term_id{0}, true}}, // 5   C
	                                       // 6    - (Active)
	  .max_segment_count = 2,
	  .expected_ranges = {{0, 2}, {2, 4}, {4, 6}}},
      sliding_ranges_test_case{
	  .desc="Mergeable pairs of segments with ascending raft terms, but number of max ranges is limited to 0 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 -
	    {1_KiB, model::term_id{0}, true},  // 1 -
	    {1_KiB, model::term_id{1}, true},  // 2 -
	    {1_KiB, model::term_id{1}, true},  // 3 -
	    {1_KiB, model::term_id{2}, true},  // 4 -
	    {1_KiB, model::term_id{2}, true}}, // 5 -
	                                       // 6 - (Active)
	  .max_range_count = 0,
	  .expected_ranges = {}},
      sliding_ranges_test_case{
	  .desc="Mergeable pairs of segments with ascending raft terms, but number of max ranges is limited to 1 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 A
	    {1_KiB, model::term_id{0}, true},  // 1 A
	    {1_KiB, model::term_id{1}, true},  // 2  -
	    {1_KiB, model::term_id{1}, true},  // 3  -
	    {1_KiB, model::term_id{2}, true},  // 4  -
	    {1_KiB, model::term_id{2}, true}}, // 5  -
	                                       // 6  - (Active)
	  .max_range_count = 1,
	  .expected_ranges = {{0, 2}}},
      sliding_ranges_test_case{
	  .desc="Mergeable pairs of segments with ascending raft terms, but number of max ranges is limited to 2 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 A
	    {1_KiB, model::term_id{0}, true},  // 1 A
	    {1_KiB, model::term_id{1}, true},  // 2  B
	    {1_KiB, model::term_id{1}, true},  // 3  B
	    {1_KiB, model::term_id{2}, true},  // 4  -
	    {1_KiB, model::term_id{2}, true}}, // 5  -
	                                       // 6  - (Active)
	  .max_range_count = 2,
	  .expected_ranges = {{0, 2}, {2, 4}}},
      sliding_ranges_test_case{
	  .desc="Mergeable pairs of segments with ascending raft terms, but number of max ranges is limited to 3 via cluster config.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 A
	    {1_KiB, model::term_id{0}, true},  // 1 A
	    {1_KiB, model::term_id{1}, true},  // 2  B
	    {1_KiB, model::term_id{1}, true},  // 3  B
	    {1_KiB, model::term_id{2}, true},  // 4   C
	    {1_KiB, model::term_id{2}, true}}, // 5   C
	                                       // 6    - (Active)
	  .max_range_count = 3,
	  .expected_ranges = {{0, 2}, {2, 4}, {4, 6}}},
      sliding_ranges_test_case{
	  .desc="segment_count=2, range_count = 1 means only one pair of segments merged at a time.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 A
	    {1_KiB, model::term_id{0}, true},  // 1 A
	    {1_KiB, model::term_id{0}, true},  // 2  -
	    {1_KiB, model::term_id{1}, true},  // 3  -
	    {1_KiB, model::term_id{1}, true},  // 4  -
	    {1_KiB, model::term_id{1}, true}}, // 5  -
	                                       // 6   - (Active)
	  .max_segment_count = 2,
	  .max_range_count = 1,
	  .expected_ranges = {{0, 2}}},
      sliding_ranges_test_case{
	  .desc="Mixture of max range and segment count restricting range space.",
	  .segment_fields={
	    {1_KiB, model::term_id{0}, true},  // 0 A
	    {1_KiB, model::term_id{0}, true},  // 1 A
	    {1_KiB, model::term_id{0}, true},  // 2 A
	    {1_KiB, model::term_id{0}, true},  // 3  B
	    {1_KiB, model::term_id{0}, true},  // 4  B
	    {2_MiB, model::term_id{1}, true},  // 5   -
	    {1_KiB, model::term_id{1}, true},  // 6   -
	    {1_KiB, model::term_id{2}, true},  // 7    C
	    {1_KiB, model::term_id{2}, true},  // 8    C
	    {1_KiB, model::term_id{2}, true},  // 9    C
	    {1_KiB, model::term_id{2}, true},  // 10    -
	    {1_KiB, model::term_id{3}, true},  // 11    -
	    {1_KiB, model::term_id{3}, true}}, // 12    -
	                                       // 13     - (Active)
	  .max_segment_count = 3,
	  .max_range_count = 3,
	  .expected_ranges = {{0, 3}, {3, 5}, {7, 10}}},
    };
    // clang-format on

    for (int test_case_index = 0; const auto& test_case : test_cases) {
        vlog(e2e_test_log.info, "Running test case: {}", test_case.desc);
        const auto& segment_fields = test_case.segment_fields;
        const auto& expected_ranges = test_case.expected_ranges;
        test_local_cfg.get("log_compaction_merge_max_segments_per_range")
          .set_value(test_case.max_segment_count);
        test_local_cfg.get("log_compaction_merge_max_ranges")
          .set_value(test_case.max_range_count);
        auto ntp = model::ntp(
          "default", fmt::format("test-{}", test_case_index++), 0);
        auto log = manage_log(
          mgr,
          storage::ntp_config(
            ntp,
            mgr.config().base_dir,
            std::make_unique<storage::ntp_config::default_overrides>(
              overrides)));

        auto* disk_log = static_cast<storage::disk_log_impl*>(log.get());

        for (const auto& segment_field : segment_fields) {
            add_segment(log, segment_field.size, segment_field.term);
            disk_log->force_roll().get();
        }

        compaction::compaction_config cfg(
          model::offset::max(),
          model::offset::max(),
          model::offset::max(),
          std::nullopt,
          std::nullopt,
          as);

        for (size_t i = 0; i < segment_fields.size(); ++i) {
            auto& seg = disk_log->segments()[i];
            if (segment_fields[i].mark_as_stable) {
                // We need to self compact segments before they are
                // considered in the adjacent compaction ranges
                seg->index().maybe_set_self_compact_timestamp(
                  model::timestamp::now());
            }

            auto& ot = const_cast<storage::segment::offset_tracker&>(
              seg->offsets());
            if (segment_fields[i].dirty_offset_override.has_value()) {
                // Override the dirty offset of the segment, if specified
                ot.set_offset(
                  storage::segment::offset_tracker::dirty_offset_t{
                    *segment_fields[i].dirty_offset_override});
            }
            if (segment_fields[i].base_offset_override.has_value()) {
                // Override the base offset of the segment, if specified
                storage::testing_details::offset_tracker_accessor::base_offset(
                  ot) = *segment_fields[i].base_offset_override;
            }
        }

        std::unordered_map<ss::sstring, size_t> segment_filename_index_map;
        for (size_t i = 0; const auto& segment : disk_log->segments()) {
            segment_filename_index_map[segment->filename()] = i++;
        }

        auto adjacent_ranges = disk_log->find_adjacent_compaction_ranges(
          cfg, test_case.new_start_offset);
        if (!expected_ranges.empty()) {
            ASSERT_TRUE(adjacent_ranges.has_value());
            ASSERT_EQ(adjacent_ranges->size(), expected_ranges.size());
            for (size_t expected_ranges_index = 0;
                 const auto& seg_it : *adjacent_ranges) {
                auto first_index = segment_filename_index_map.at(
                  (*seg_it.first)->filename());
                ASSERT_EQ(
                  first_index, expected_ranges[expected_ranges_index].first);
                auto second_index = segment_filename_index_map.at(
                  (*seg_it.second)->filename());
                ASSERT_EQ(
                  second_index, expected_ranges[expected_ranges_index].second);
                ++expected_ranges_index;
            }
        } else {
            ASSERT_TRUE(!adjacent_ranges.has_value());
        }
    }
}

TEST_F(storage_test_fixture, segment_cached_disk_usage_set_after_compaction) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("kafka", "a", 0);
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)));

    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    auto add_segment_func = [&](this auto) -> ss::future<> {
        auto size = random_generators::get_int(4_KiB, 10_MiB);
        add_segment(size, model::term_id(0));
        co_await log->force_roll();
    };

    add_segment_func().get();
    add_segment_func().get();

    ss::abort_source as;
    compaction::compaction_config cfg(
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      as);
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);

    auto check_cached_sizes = [](auto& seg) {
        auto disk
          = storage::testing_details::segment_accessor::data_disk_usage_size(
            *seg);
        auto cidx
          = storage::testing_details::segment_accessor::compaction_index_size(
            *seg);
        auto sidx = storage::segment_index_observer::disk_usage_size(
          seg->index());
        ASSERT_TRUE(disk.has_value());
        ASSERT_TRUE(cidx.has_value());
        ASSERT_TRUE(sidx.has_value());

        ASSERT_EQ(disk.value(), ss::file_size(seg->path().string()).get());
        ASSERT_EQ(
          cidx.value(),
          ss::file_size(seg->path().to_compacted_index().string()).get());
        ASSERT_EQ(
          sidx.value(), ss::file_size(seg->path().to_index().string()).get());
    };

    auto& segs = log->segments();

    // Test self-compaction
    {
        auto& s = segs[0];
        // Freshly rolled segment should have cached sizes set.
        check_cached_sizes(s);
        disk_log.segment_self_compact(cfg, s).get();
        check_cached_sizes(s);
    }

    // Test adjacent compaction
    {
        disk_log.adjacent_merge_compact(segs.copy(), cfg).get();
        for (auto& s : segs) {
            if (s->has_self_compact_timestamp()) {
                check_cached_sizes(s);
            }
        }
    }

    // Test sliding window compaction
    {
        disk_log.sliding_window_compact(cfg).get();
        for (auto& s : segs) {
            if (s->has_self_compact_timestamp()) {
                ASSERT_TRUE(s->has_clean_compact_timestamp());
                check_cached_sizes(s);
            }
        }
    }
}

TEST_F(storage_test_fixture, delete_retention_ms_with_ts) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    auto delete_retention_ms = 10ms;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    ov.delete_retention_ms = tristate<std::chrono::milliseconds>(
      delete_retention_ms);
    ov.shadow_indexing_mode = model::shadow_indexing_mode::full;
    ov.storage_mode = model::redpanda_storage_mode::tiered;
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));

    auto log = manage_log(mgr, std::move(ntp_cfg));

    // delete_retention_ms() should be nullopt
    ASSERT_EQ(log->config().delete_retention_ms(), std::nullopt);
};

TEST_F(storage_test_fixture, delete_retention_ms_without_ts) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    auto delete_retention_ms = 10ms;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    ov.delete_retention_ms = tristate<std::chrono::milliseconds>(
      delete_retention_ms);
    ov.shadow_indexing_mode = model::shadow_indexing_mode::disabled;
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = manage_log(mgr, std::move(ntp_cfg));

    // delete_retention_ms() should have value
    ASSERT_EQ(
      log->config().delete_retention_ms(),
      std::make_optional(delete_retention_ms));
};

TEST_F(storage_test_fixture, test_get_file_offset_lock_precheck) {
    // - Generate a few segments
    // - Acquire read locks from each (simulating internals of
    //   offset_range_size)
    // - Queue up a write lock behind one of them, in the background
    // - Then call get_file_offset on that segment
    // - get_file_offset  should throw ss::semaphore_timed_out

    constexpr size_t num_segments = 5;
    ss::gate gate{};

    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("redpanda", "test-topic", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          custom_ts_batch_generator(model::timestamp::now()));
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    auto& segments = log->segments();

    {
        std::vector<ss::future<ss::rwlock::holder>> f_locks;
        f_locks.reserve(segments.size());
        for (auto& s : segments) {
            f_locks.emplace_back(s->read_lock());
        }

        auto seg = *std::next(segments.begin(), num_segments / 2);

        ssx::spawn_with_gate(gate, [seg] {
            return seg->write_lock().then([](auto) { return ss::now(); });
        });

        EXPECT_THROW(
          dynamic_cast<storage::disk_log_impl*>(log.get())
            ->get_file_offset(
              seg,
              std::nullopt,
              seg->offsets().get_committed_offset(),
              storage::boundary_type::exclusive)
            .get(),
          ss::timed_out_error);
    }
}

TEST_F(storage_test_fixture, test_offset_range_size_lock_timeout) {
    // similar to above, offset_range_size should time out in this scenario if
    // we provide a sensible deadline

    constexpr size_t num_segments = 5;
    ss::gate gate{};

    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("redpanda", "test-topic", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(0),
          std::nullopt,
          custom_ts_batch_generator(model::timestamp::now()));
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    auto& segments = log->segments();

    {
        std::vector<ss::future<ss::rwlock::holder>> f_locks;
        f_locks.reserve(segments.size());
        for (auto& s : segments) {
            f_locks.emplace_back(s->read_lock());
        }

        auto seg = *std::next(segments.begin(), num_segments / 2);

        ssx::spawn_with_gate(gate, [seg] {
            return seg->write_lock().then([](auto) { return ss::now(); });
        });

        EXPECT_THROW(
          log
            ->offset_range_size(
              seg->offsets().get_base_offset(),
              seg->offsets().get_committed_offset(),
              ss::semaphore::clock::now() + 1s)
            .get(),
          ss::timed_out_error);
    }
}

TEST_F(storage_test_fixture, test_max_eligible_for_compacted_reupload_offset) {
    constexpr size_t num_segments = 2;
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    SUCCEED() << fmt::format("Configuration: {}", mgr.config());
    auto ntp = model::ntp("kafka", "test-topic", 0);

    storage::ntp_config::default_overrides overrides{
      .cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction,
    };
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));

    auto log = manage_log(mgr, std::move(ntp_cfg));

    model::offset first_segment_last_offset;
    for (size_t i = 0; i < num_segments; i++) {
        append_random_batches(
          log,
          10,
          model::term_id(i),
          model::timestamp::now(),
          key_limited_random_batch_generator());
        if (first_segment_last_offset == model::offset{}) {
            first_segment_last_offset = log->offsets().dirty_offset;
        }
        log->force_roll().get();
    }

    ss::sleep(1s).get(); // ensure time separation for max.compaction.lag.ms

    auto& segments = log->segments();
    auto first = *std::next(segments.begin(), 0);
    auto second = *std::next(segments.begin(), 1);

    auto mco = [&log](model::offset from) {
        return log->max_eligible_for_compacted_reupload_offset(from);
    };

    EXPECT_FALSE(mco(first->offsets().get_base_offset()).has_value());
    EXPECT_FALSE(mco(second->offsets().get_base_offset()).has_value());
    EXPECT_FALSE(mco(first->offsets().get_committed_offset()).has_value());
    EXPECT_FALSE(mco(model::offset::max()).has_value());
    EXPECT_FALSE(mco(model::offset::min()).has_value());

    {
        // Compact the first segment only
        vlog(e2e_test_log.info, "Starting compaction");
        storage::housekeeping_config h_cfg(
          model::timestamp::min(),
          std::nullopt,
          first->offsets().get_committed_offset(),
          first->offsets().get_committed_offset(),
          first->offsets().get_committed_offset(),
          std::nullopt,
          std::nullopt,
          0ms,
          as);
        log->housekeeping(h_cfg).get();

        EXPECT_TRUE(mco(first->offsets().get_base_offset()).has_value());
        EXPECT_EQ(
          mco(first->offsets().get_base_offset()),
          first->offsets().get_committed_offset());
        EXPECT_FALSE(mco(second->offsets().get_base_offset()).has_value());
        EXPECT_TRUE(mco(first->offsets().get_committed_offset()).has_value());
        EXPECT_EQ(
          mco(first->offsets().get_committed_offset()),
          first->offsets().get_committed_offset());
    }

    {
        // Now the rest of the topic
        vlog(e2e_test_log.info, "Starting compaction");
        storage::housekeeping_config h_cfg(
          model::timestamp::min(),
          std::nullopt,
          log->offsets().committed_offset,
          log->offsets().committed_offset,
          log->offsets().committed_offset,
          std::nullopt,
          std::nullopt,
          0ms,
          as);
        log->housekeeping(h_cfg).get();

        EXPECT_TRUE(mco(first->offsets().get_base_offset()).has_value());
        EXPECT_EQ(
          mco(first->offsets().get_base_offset()),
          second->offsets().get_committed_offset());
        EXPECT_TRUE(mco(second->offsets().get_base_offset()).has_value());
        EXPECT_EQ(
          mco(second->offsets().get_base_offset()),
          second->offsets().get_committed_offset());

        EXPECT_TRUE(mco(first->offsets().get_committed_offset()).has_value());
        EXPECT_EQ(
          mco(first->offsets().get_committed_offset()),
          second->offsets().get_committed_offset());
    }
}

TEST_F(storage_test_fixture, test_eligible_for_compacted_reupload) {
    using namespace storage;
    disk_log_builder b;
    b | start();
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    const auto num_segs = 2;
    const auto start_offset = 0;
    const auto records_per_seg = 150;
    for (int i = 0; i < num_segs; ++i) {
        auto offset = start_offset + i * records_per_seg;
        b | add_segment(offset)
          | add_random_batch(
            offset, records_per_seg, maybe_compress_batches::yes);
        disk_log.force_roll().get();
    }

    auto& seg1 = b.get_segment(0);
    auto& seg2 = b.get_segment(1);

    for (auto s : std::array{&seg1, &seg2}) {
        s->mark_as_compacted_segment();
        ASSERT_FALSE(s->has_self_compact_timestamp());
        ASSERT_FALSE(s->has_clean_compact_timestamp());
    }

    auto efcr = [&disk_log](model::offset f, model::offset l) {
        return disk_log.eligible_for_compacted_reupload(f, l);
    };

    // segments are initially not eligible for upload (no timestamps or marks)
    ASSERT_FALSE(efcr(
      seg1.offsets().get_base_offset(), seg2.offsets().get_committed_offset()));

    auto set_sliding_window = [](bool v) -> scoped_config {
        scoped_config cfg;
        cfg.get("log_compaction_use_sliding_window").set_value(v);
        return cfg;
    };

    {
        auto loc_cfg = set_sliding_window(false);

        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg1.offsets().get_committed_offset()));

        seg1.index().maybe_set_self_compact_timestamp(model::timestamp::now());

        ASSERT_TRUE(efcr(
          seg1.offsets().get_base_offset(),
          seg1.offsets().get_committed_offset()));

        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));

        seg2.index().maybe_set_self_compact_timestamp(model::timestamp::now());

        ASSERT_TRUE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));
    }

    {
        auto loc_cfg = set_sliding_window(true);
        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));
    }

    auto set_collectable = [](bool v) {
        scoped_config cfg;
        model::cleanup_policy_bitflags policy;
        if (v) {
            std::istringstream{"compact,delete"} >> policy;
        } else {
            std::istringstream{"compact"} >> policy;
        }
        cfg.get("log_cleanup_policy").set_value(policy);
        return cfg;
    };

    auto sliding_window_cfg = set_sliding_window(true);

    {
        auto loc_cfg = set_collectable(true);

        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg1.offsets().get_committed_offset()));

        seg1.mark_as_finished_windowed_compaction();

        ASSERT_TRUE(efcr(
          seg1.offsets().get_base_offset(),
          seg1.offsets().get_committed_offset()));

        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));

        seg2.mark_as_finished_windowed_compaction();

        ASSERT_TRUE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));
    }

    {
        auto loc_cfg = set_collectable(false);
        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));

        seg1.index().maybe_set_clean_compact_timestamp(model::timestamp::now());
        ASSERT_TRUE(efcr(
          seg1.offsets().get_base_offset(),
          seg1.offsets().get_committed_offset()));

        ASSERT_FALSE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));

        seg2.index().maybe_set_clean_compact_timestamp(model::timestamp::now());

        ASSERT_TRUE(efcr(
          seg1.offsets().get_base_offset(),
          seg2.offsets().get_committed_offset()));
    }

    ASSERT_TRUE(efcr(
      seg1.offsets().get_base_offset(), seg2.offsets().get_committed_offset()));

    ASSERT_FALSE(efcr(
      seg1.offsets().get_base_offset(),
      model::next_offset(seg2.offsets().get_committed_offset())));

    seg1.unmark_as_compacted_segment();

    ASSERT_FALSE(efcr(
      seg1.offsets().get_base_offset(), seg2.offsets().get_committed_offset()));
}

TEST_F(storage_test_fixture, adjacent_merge_compaction_advances_generation_id) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("kafka", "a", 0);
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    auto log = manage_log(
      mgr,
      storage::ntp_config(
        ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov)));

    auto add_segment = [&log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (log->segments().back()->size_bytes() < size);
    };

    auto add_segment_func = [&]() {
        auto size = random_generators::get_int(4_KiB, 10_MiB);
        add_segment(size, model::term_id(0));
        log->force_roll().get();
    };

    add_segment_func();
    add_segment_func();

    ss::abort_source as;
    compaction::compaction_config cfg(
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      as);
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);

    auto& segs = log->segments();
    ASSERT_EQ(segs.size(), 3);

    // Self compact the front segment which will be the target segment for the
    // adjacent merge so that we already account for the generation_id
    // advancement from that operation.
    disk_log.segment_self_compact(cfg, segs.front()).get();

    auto gen_id_before = segs.front()->get_generation_id();

    {
        disk_log.adjacent_merge_compact(segs.copy(), cfg).get();
    }
    ASSERT_EQ(segs.size(), 2);

    auto gen_id_after = segs.front()->get_generation_id();

    ASSERT_EQ(gen_id_after(), gen_id_before() + 1);
}

TEST_F(storage_test_fixture, truncate_prefix_append_and_close) {
    using namespace storage;
    disk_log_builder b;
    b | start();
    auto log = b.get_log();
    auto& disk_log = b.get_disk_log_impl();
    const auto num_segs = 5;
    const auto start_offset = 0;
    const auto records_per_seg = 150;
    for (int i = 0; i < num_segs; ++i) {
        auto offset = start_offset + i * records_per_seg;
        b | add_segment(offset)
          | add_random_batch(
            offset, records_per_seg, maybe_compress_batches::yes);
        disk_log.force_roll().get();
    }

    ss::abort_source as;
    ssx::mutex log_mutex{"e2e_test::log_mutex"};
    auto random_sleep = [](int min, int max) {
        return ss::sleep(
          std::chrono::milliseconds(random_generators::get_int(min, max)));
    };
    static constexpr auto append_sleep_min = 5;
    static constexpr auto append_sleep_max = 20;
    auto append_fut = ss::do_until(
      [&]() { return as.abort_requested(); },
      [&]() {
          auto maybe_u = log_mutex.try_get_units();
          if (!maybe_u.has_value()) {
              return ss::now();
          }
          auto u = std::move(maybe_u).value();
          return append_single_record_batch_coro(log, 10, model::term_id{0})
            .then([&, u = std::move(u)]() mutable {
                u.return_all();
                return random_sleep(append_sleep_min, append_sleep_max);
            });
      });

    static constexpr auto close_sleep_min = 150;
    static constexpr auto close_sleep_max = 200;
    auto close_fut = random_sleep(close_sleep_min, close_sleep_max).then([&]() {
        return log_mutex.get_units().then([&](ssx::semaphore_units u) {
            (void)u;
            log_mutex.broken();
            // Calling `builder.stop()` will stop the log_manager and therefore
            // close the log.
            return b.stop();
        });
    });
    static constexpr auto prefix_truncate_sleep_min = 100;
    static constexpr auto prefix_truncate_sleep_max = 149;
    // Expect 0 segments after prefix truncation in the happy path.
    size_t expected_segment_count = 0;
    auto prefix_truncate_fut
      = random_sleep(prefix_truncate_sleep_min, prefix_truncate_sleep_max)
          .then([&]() {
              return log_mutex.get_units().then([&](ssx::semaphore_units u) {
                  as.request_abort();
                  u.return_all();
                  return log
                    ->truncate_prefix(truncate_prefix_config(
                      model::next_offset(log->offsets().dirty_offset)))
                    .handle_exception([&](const std::exception_ptr&) {
                        // We may have raced with a close() operation when
                        // attempting to obtain locks within
                        // `truncate_prefix()`. In this case, we should expect
                        // that no segments were removed from the log.
                        expected_segment_count = log->segment_count();
                        return ss::now();
                    });
              });
          });

    ss::when_all(
      std::move(append_fut),
      std::move(close_fut),
      std::move(prefix_truncate_fut))
      .get();

    ASSERT_EQ(log->segment_count(), expected_segment_count);
}
