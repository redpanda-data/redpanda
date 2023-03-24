// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "config/mock_property.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/record_batch_builder.h"
#include "storage/segment_utils.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/log_gap_analysis.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/tmp_dir.h"
#include "units.h"
#include "utils/directory_walker.h"
#include "utils/to_string.h"
#include "vassert.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <iterator>
#include <numeric>
#include <optional>
#include <vector>

storage::disk_log_impl* get_disk_log(storage::log log) {
    return dynamic_cast<storage::disk_log_impl*>(log.get_impl());
}

void validate_offsets(
  model::offset base,
  const std::vector<model::record_batch_header>& write_headers,
  const ss::circular_buffer<model::record_batch>& read_batches) {
    BOOST_REQUIRE_EQUAL(write_headers.size(), read_batches.size());
    auto it = read_batches.begin();
    model::offset next_base = base;
    for (auto const& h : write_headers) {
        BOOST_REQUIRE_EQUAL(it->base_offset(), next_base);
        // last offset delta is inclusive (record with this offset belongs to
        // previous batch)
        next_base += (h.last_offset_delta + model::offset(1));
        it++;
    }
}

void compact_and_prefix_truncate(
  storage::disk_log_impl& log, storage::compaction_config cfg) {
    ss::abort_source as;
    auto eviction_future = log.monitor_eviction(as);

    log.compact(cfg).get();

    if (eviction_future.available()) {
        auto evict_until = eviction_future.get();
        log
          .truncate_prefix(storage::truncate_prefix_config{
            model::next_offset(evict_until), ss::default_priority_class()})
          .get();
    } else {
        as.request_abort();
        eviction_future.ignore_ready_future();
    }
}

FIXTURE_TEST(
  test_assinging_offsets_in_single_segment_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(
      lstats.last_term_start_offset, batches.front().base_offset());
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

FIXTURE_TEST(append_twice_to_same_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto headers_2 = append_random_batches(log, 10);
    log.flush().get0();
    std::move(
      std::begin(headers_2), std::end(headers_2), std::back_inserter(headers));
    auto batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.last_term_start_offset, batches.front().base_offset());
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, batches.back().last_offset());
};

FIXTURE_TEST(test_assigning_offsets_in_multiple_segment, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(1_KiB);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.last_term_start_offset, batches.front().base_offset());
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

FIXTURE_TEST(test_single_record_per_segment, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(
      log,
      10,
      model::term_id(1),
      [](std::optional<model::timestamp> ts = std::nullopt) {
          ss::circular_buffer<model::record_batch> batches;
          batches.push_back(model::test::make_random_batch(
            model::offset(0),
            1,
            true,
            model::record_batch_type::raft_data,
            std::nullopt,
            ts));
          return batches;
      });
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);
    info("Flushed log: {}", log);
    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.last_term_start_offset, batches.front().base_offset());
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

FIXTURE_TEST(test_segment_rolling, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10 * 1024);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(
      log,
      10,
      model::term_id(1),
      [](std::optional<model::timestamp> ts = std::nullopt)
        -> ss::circular_buffer<model::record_batch> {
          ss::circular_buffer<model::record_batch> batches;
          batches.push_back(model::test::make_random_batch(
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
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);
    info("Flushed log: {}", log);
    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.last_term_start_offset, batches.front().base_offset());
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
    /// Do the second append round
    auto new_headers = append_random_batches(
      log,
      10,
      model::term_id(1),
      [](std::optional<model::timestamp> ts = std::nullopt) {
          ss::circular_buffer<model::record_batch> batches;
          batches.push_back(model::test::make_random_batch(
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
    auto new_lstats = log.offsets();
    BOOST_REQUIRE_GE(new_lstats.committed_offset, lstats.committed_offset);
    auto new_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(
      lstats.last_term_start_offset, batches.front().base_offset());
    BOOST_REQUIRE_EQUAL(
      new_lstats.committed_offset, new_batches.back().last_offset());
};

FIXTURE_TEST(test_reading_range_from_a_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Configuration: {}", mgr.config());
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);

    // range from base of beging to last of end
    auto range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].last_offset());
    BOOST_REQUIRE_EQUAL(range.size(), 5);
    BOOST_REQUIRE_EQUAL(range.front().header().crc, batches[3].header().crc);
    BOOST_REQUIRE_EQUAL(range.back().header().crc, batches[7].header().crc);
    // Range is inclusive base offset points to batch[7] so it have to be
    // included
    range = read_range_to_vector(
      log, batches[3].base_offset(), batches[7].base_offset());
    BOOST_REQUIRE_EQUAL(range.size(), 5);
    BOOST_REQUIRE_EQUAL(range.front().header().crc, batches[3].header().crc);
    BOOST_REQUIRE_EQUAL(range.back().header().crc, batches[7].header().crc);

    range = read_range_to_vector(
      log, batches[3].last_offset(), batches[7].base_offset());
    BOOST_REQUIRE_EQUAL(range.size(), 5);
    BOOST_REQUIRE_EQUAL(range.front().header().crc, batches[3].header().crc);
    BOOST_REQUIRE_EQUAL(range.back().header().crc, batches[7].header().crc);
    // range from base of beging to the middle of end
    range = read_range_to_vector(
      log,
      batches[3].base_offset(),
      batches[7].base_offset() + model::offset(batches[7].record_count() / 2));
    BOOST_REQUIRE_EQUAL(range.size(), 5);
    BOOST_REQUIRE_EQUAL(range.front().header().crc, batches[3].header().crc);
    BOOST_REQUIRE_EQUAL(range.back().header().crc, batches[7].header().crc);
};

FIXTURE_TEST(test_rolling_term, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    std::vector<model::record_batch_header> headers;
    model::offset current_offset = model::offset{0};
    for (auto i = 0; i < 5; i++) {
        auto term_start_offset = current_offset;
        auto part = append_random_batches(log, 1, model::term_id(i));
        for (auto h : part) {
            current_offset += h.last_offset_delta + 1;
        }
        log.flush().get();
        BOOST_REQUIRE_EQUAL(
          model::term_id(i),
          log.get_term(current_offset - model::offset(1)).value());
        auto lstats = log.offsets();
        BOOST_REQUIRE_EQUAL(lstats.last_term_start_offset, term_start_offset);
        std::move(part.begin(), part.end(), std::back_inserter(headers));
    }

    auto read_batches = read_and_validate_all_batches(log);
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, read_batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(
      lstats.committed_offset, read_batches.back().last_offset());
};

FIXTURE_TEST(test_append_batches_from_multiple_terms, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Testing type: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    std::vector<model::record_batch_header> headers;
    ss::circular_buffer<model::record_batch> batches;
    std::vector<size_t> term_batches_counts;
    for (auto i = 0; i < 5; i++) {
        auto term_batches = model::test::make_random_batches(
          model::offset(0), 10);
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
      storage::log_append_config::fsync::yes,
      ss::default_priority_class(),
      model::no_timeout};
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    std::move(reader)
      .for_each_ref(log.make_appender(append_cfg), append_cfg.timeout)
      .get0();
    log.flush().get();

    auto read_batches = read_and_validate_all_batches(log);
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, read_batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(
      lstats.committed_offset, read_batches.back().last_offset());
    size_t next = 0;
    int expected_term = 0;
    for (auto c : term_batches_counts) {
        for (size_t i = next; i < next + c; ++i) {
            BOOST_REQUIRE_EQUAL(
              read_batches[i].term(), model::term_id(expected_term));
        }
        expected_term++;
        next = next + c;
    }
}
struct custom_ts_batch_generator {
    explicit custom_ts_batch_generator(model::timestamp start_ts)
      : _start_ts(start_ts) {}

    ss::circular_buffer<model::record_batch> operator()(
      [[maybe_unused]] std::optional<model::timestamp> ts = std::nullopt) {
        // The input timestamp is unused, this class does its own timestamping
        auto batches = model::test::make_random_batches(
          model::offset(0), random_generators::get_int(1, 10));

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
  storage::log log,
  int batch_count,
  model::term_id term,
  model::timestamp base_ts) {
    auto current_ts = base_ts;
    for (int i = 0; i < batch_count; ++i) {
        iobuf key = bytes_to_iobuf(bytes("key"));
        iobuf value = bytes_to_iobuf(bytes("v"));

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
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout,
        };

        std::move(reader)
          .for_each_ref(log.make_appender(cfg), cfg.timeout)
          .get();
        current_ts = model::timestamp(current_ts() + 1);
    }
}

FIXTURE_TEST(
  test_timestamp_updates_when_max_timestamp_is_not_set, storage_test_fixture) {
    auto append_batch_with_no_max_ts =
      [](storage::log log, model::term_id term, model::timestamp base_ts) {
          auto current_ts = base_ts;

          iobuf key = bytes_to_iobuf(bytes("key"));
          iobuf value = bytes_to_iobuf(bytes("v"));

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
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout,
          };

          std::move(reader)
            .for_each_ref(log.make_appender(cfg), cfg.timeout)
            .get();
          current_ts = model::timestamp(current_ts() + 1);
      };

    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(100));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(110));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(120));
    disk_log->force_roll(ss::default_priority_class()).get0();

    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(200));
    // reordered timestamps
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(230));
    append_batch_with_no_max_ts(log, model::term_id(0), model::timestamp(220));
    auto& segments = disk_log->segments();
    // first segment
    BOOST_REQUIRE_EQUAL(
      segments[0]->index().base_timestamp(), model::timestamp(100));
    BOOST_REQUIRE_EQUAL(
      segments[0]->index().max_timestamp(), model::timestamp(120));
    // second segment
    BOOST_REQUIRE_EQUAL(
      segments[1]->index().base_timestamp(), model::timestamp(200));
    BOOST_REQUIRE_EQUAL(
      segments[1]->index().max_timestamp(), model::timestamp(230));
};

FIXTURE_TEST(test_time_based_eviction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    // 1. segment timestamps from 100 to 110
    append_custom_timestamp_batches(
      log, 10, model::term_id(0), model::timestamp(100));
    disk_log->force_roll(ss::default_priority_class()).get0();

    // 2. segment timestamps from 200 to 230
    append_custom_timestamp_batches(
      log, 30, model::term_id(0), model::timestamp(200));

    disk_log->force_roll(ss::default_priority_class()).get0();
    // 3. segment timestamps from 231 to 250
    append_custom_timestamp_batches(
      log, 20, model::term_id(0), model::timestamp(231));

    /**
     *  Log contains 3 segments with following timestamps:
     *
     * [100..110][200..230][231..261]
     */

    storage::compaction_config ccfg_no_compact(
      model::timestamp(200),
      std::nullopt,
      model::offset::min(), // should prevent compaction
      ss::default_priority_class(),
      as);
    auto before = log.offsets();
    log.compact(ccfg_no_compact).get0();
    auto after = log.offsets();
    BOOST_REQUIRE_EQUAL(after.start_offset, before.start_offset);

    auto make_compaction_cfg = [&as](int timestamp) {
        return storage::compaction_config(
          model::timestamp(timestamp),
          std::nullopt,
          model::offset::max(),
          ss::default_priority_class(),
          as);
    };

    // gc with timestamp 50, no segments should be evicted
    compact_and_prefix_truncate(*disk_log, make_compaction_cfg(50));
    BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 3);
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().front()->offsets().base_offset, model::offset(0));
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().back()->offsets().dirty_offset, model::offset(59));

    // gc with timestamp 102, no segments should be evicted
    compact_and_prefix_truncate(*disk_log, make_compaction_cfg(102));
    BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 3);
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().front()->offsets().base_offset, model::offset(0));
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().back()->offsets().dirty_offset, model::offset(59));
    // gc with timestamp 201, should evict first segment
    compact_and_prefix_truncate(*disk_log, make_compaction_cfg(201));
    BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 2);
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().front()->offsets().base_offset, model::offset(10));
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().back()->offsets().dirty_offset, model::offset(59));
    // gc with timestamp 240, should evict first segment
    compact_and_prefix_truncate(*disk_log, make_compaction_cfg(240));
    BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 1);
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().front()->offsets().base_offset, model::offset(40));
    BOOST_REQUIRE_EQUAL(
      disk_log->segments().back()->offsets().dirty_offset, model::offset(59));
};

FIXTURE_TEST(test_size_based_eviction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto all_batches = read_and_validate_all_batches(log);
    size_t first_size = std::accumulate(
      all_batches.begin(),
      all_batches.end(),
      size_t(0),
      [](size_t acc, model::record_batch& b) { return acc + b.size_bytes(); });

    auto lstats = log.offsets();
    info("Offsets to be evicted {}", lstats);
    headers = append_random_batches(log, 10);
    auto new_batches = read_and_validate_all_batches(log);
    size_t total_size = std::accumulate(
      new_batches.begin(),
      new_batches.end(),
      size_t(0),
      [](size_t acc, model::record_batch& b) { return acc + b.size_bytes(); });

    // Set the max number of bytes to the total size of the log.
    // This will prevent compaction.
    storage::compaction_config ccfg_no_compact(
      model::timestamp::min(),
      total_size + first_size,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    compact_and_prefix_truncate(*disk_log, ccfg_no_compact);

    auto new_lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(new_lstats.start_offset, lstats.start_offset);

    storage::compaction_config ccfg(
      model::timestamp::min(),
      (total_size - first_size) + 1,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    compact_and_prefix_truncate(*disk_log, ccfg);

    new_lstats = log.offsets();
    info("Final offsets {}", new_lstats);
    BOOST_REQUIRE_EQUAL(
      new_lstats.start_offset, lstats.dirty_offset + model::offset(1));
};

FIXTURE_TEST(test_eviction_notification, storage_test_fixture) {
    ss::promise<model::offset> last_evicted_offset;
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    (void)log.monitor_eviction(as).then(
      [&last_evicted_offset](model::offset o) mutable {
          last_evicted_offset.set_value(o);
      });

    auto headers = append_random_batches(
      log,
      10,
      model::term_id(0),
      custom_ts_batch_generator(model::timestamp::now()));
    log.flush().get0();
    model::timestamp gc_ts = headers.back().max_timestamp;
    auto lstats_before = log.offsets();
    info("Offsets to be evicted {}", lstats_before);
    headers = append_random_batches(
      log,
      10,
      model::term_id(0),
      custom_ts_batch_generator(model::timestamp(gc_ts() + 10)));
    storage::compaction_config ccfg(
      gc_ts,
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    log.compact(ccfg).get0();

    auto offset = last_evicted_offset.get_future().get0();
    log.compact(ccfg).get0();
    auto lstats_after = log.offsets();

    BOOST_REQUIRE_EQUAL(lstats_before.start_offset, lstats_after.start_offset);
    // wait for compaction
    log.compact(ccfg).get0();
    log
      .truncate_prefix(storage::truncate_prefix_config{
        model::next_offset(offset), ss::default_priority_class()})
      .get();
    auto compacted_lstats = log.offsets();
    info("Compacted offsets {}", compacted_lstats);
    // check if compaction happend
    BOOST_REQUIRE_EQUAL(
      compacted_lstats.start_offset,
      lstats_before.dirty_offset + model::offset(1));
};
ss::future<storage::append_result> append_exactly(
  storage::log log,
  size_t batch_count,
  size_t batch_sz,
  std::optional<bytes> key = std::nullopt) {
    vassert(
      batch_sz > model::packed_record_batch_header_size,
      "Batch size must be greater than {}, requested {}",
      model::packed_record_batch_header_size,
      batch_sz);
    storage::log_append_config append_cfg{
      storage::log_append_config::fsync::no,
      ss::default_priority_class(),
      model::no_timeout};

    ss::circular_buffer<model::record_batch> batches;
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

    for (int i = 0; i < batch_count; ++i) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset{});
        iobuf value = bytes_to_iobuf(random_generators::get_bytes(val_sz));
        builder.add_raw_kv(key_buf.copy(), std::move(value));

        batches.push_back(std::move(builder).build());
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    return std::move(rdr).for_each_ref(
      log.make_appender(append_cfg), model::no_timeout);
}

FIXTURE_TEST(write_concurrently_with_gc, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    // make sure segments are small
    cfg.max_segment_size = config::mock_binding<size_t>(1000);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    model::offset last_append_offset{};
    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();

    append_exactly(log, 10, 100).get0();
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset(9));

    std::vector<ss::future<>> futures;

    ss::semaphore _sem{1};
    int appends = 100;
    int batches_per_append = 5;
    auto compact = [log, &as]() mutable {
        storage::compaction_config ccfg(
          model::timestamp::min(),
          1000,
          model::offset::max(),
          ss::default_priority_class(),
          as);
        return log.compact(ccfg);
    };

    auto append =
      [log, &_sem, batches_per_append, &last_append_offset]() mutable {
          return ss::with_semaphore(
            _sem, 1, [log, batches_per_append, &last_append_offset]() mutable {
                return ss::sleep(10ms).then(
                  [log, batches_per_append, &last_append_offset] {
                      return append_exactly(log, batches_per_append, 100)
                        .then([&last_append_offset,
                               log](storage::append_result result) mutable {
                            BOOST_REQUIRE_GT(
                              result.last_offset, last_append_offset);
                            last_append_offset = result.last_offset;
                        });
                  });
            });
      };

    auto loop = ss::do_until([&as] { return as.abort_requested(); }, compact);

    for (int i = 0; i < appends; ++i) {
        futures.push_back(append());
    }

    ss::when_all(futures.begin(), futures.end()).get0();

    as.request_abort();
    loop.get0();
    auto lstats_after = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats_after.dirty_offset,
      model::offset(9 + appends * batches_per_append));
};

FIXTURE_TEST(empty_segment_recovery, storage_test_fixture) {
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
      .io_priority = ss::default_priority_class(),
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
      .truncate(storage::truncate_config(
        model::offset(6), ss::default_priority_class()))
      .get0();

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
    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });

    auto offsets_after_recovery = log.offsets();

    // Append single batch
    storage::log_appender appender = log.make_appender(
      storage::log_append_config{
        .should_fsync = storage::log_append_config::fsync::no,
        .io_priority = ss::default_priority_class(),
        .timeout = model::no_timeout});
    ss::circular_buffer<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(0), 1, false));

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    rdr.for_each_ref(std::move(appender), model::no_timeout).get0();

    // we truncate at {6} so we expect dirty offset equal {5}
    BOOST_REQUIRE_EQUAL(offsets_after_recovery.dirty_offset, model::offset(5));

    // after append we expect offset to be equal to {6} as one record was
    // appended
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset(6));
}

FIXTURE_TEST(test_compation_preserve_state, storage_test_fixture) {
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
      .io_priority = ss::default_priority_class(),
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
    info("Before recovery: {}", builder.get_log());
    // recover
    storage::log_manager mgr = make_log_manager(cfg);
    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));

    storage::compaction_config compaction_cfg(
      model::timestamp::min(),
      1,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto offsets_after_recovery = log.offsets();
    info("After recovery: {}", log);
    // trigger compaction
    log.compact(compaction_cfg).get0();
    auto offsets_after_compact = log.offsets();
    info("After compaction, offsets: {}, {}", offsets_after_compact, log);

    // Append single batch
    storage::log_appender appender = log.make_appender(
      storage::log_append_config{
        .should_fsync = storage::log_append_config::fsync::no,
        .io_priority = ss::default_priority_class(),
        .timeout = model::no_timeout});

    ss::circular_buffer<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(0), 1, false));

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    std::move(rdr).for_each_ref(std::move(appender), model::no_timeout).get0();

    // before append offsets should be equal to {1}, as we stopped there
    BOOST_REQUIRE_EQUAL(offsets_after_recovery.dirty_offset, model::offset(1));
    BOOST_REQUIRE_EQUAL(offsets_after_compact.dirty_offset, model::offset(1));

    // after append we expect offset to be equal to {2}
    BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset(2));
}

void append_single_record_batch(
  storage::log log,
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
        iobuf key = bytes_to_iobuf(bytes(key_str.c_str()));
        bytes val_bytes;
        if (val_size > 0) {
            val_bytes = random_generators::get_bytes(val_size);
        } else {
            ss::sstring v = ssx::sformat("v-{}", i);
            val_bytes = bytes(v.c_str());
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
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout,
        };

        std::move(reader)
          .for_each_ref(log.make_appender(cfg), cfg.timeout)
          .get0();
    }
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
FIXTURE_TEST(truncate_and_roll_segment, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;

    {
        storage::log_manager mgr = make_log_manager(cfg);

        info("config: {}", mgr.config());
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = model::ntp("default", "test", 0);
        auto log
          = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
        // 1) append few single record batches in term 1
        append_single_record_batch(log, 14, model::term_id(1));
        log.flush().get0();
        // 2) truncate in the middle of segment
        model::offset truncate_at(7);
        info("Truncating at offset:{}", truncate_at);
        log
          .truncate(
            storage::truncate_config(truncate_at, ss::default_priority_class()))
          .get0();
        // 3) append some more batches to the same segment
        append_single_record_batch(log, 10, model::term_id(1));
        log.flush().get0();
        //  4) roll term by appending to new segment
        append_single_record_batch(log, 1, model::term_id(8));
        log.flush().get0();
    }
    // 5) restart log manager
    {
        storage::log_manager mgr = make_log_manager(cfg);

        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = model::ntp("default", "test", 0);
        auto log
          = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
        auto read = read_and_validate_all_batches(log);

        for (model::offset o(0); o < log.offsets().committed_offset; ++o) {
            BOOST_REQUIRE(log.get_term(o).has_value());
        }
    }
}

FIXTURE_TEST(compacted_log_truncation, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    ss::abort_source as;
    {
        storage::log_manager mgr = make_log_manager(cfg);

        info("config: {}", mgr.config());
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = model::ntp("default", "test", 0);
        auto log = mgr
                     .manage(storage::ntp_config(
                       ntp,
                       mgr.config().base_dir,
                       std::make_unique<storage::ntp_config::default_overrides>(
                         overrides)))
                     .get0();
        // append some batches to first segment (all batches have the same key)
        append_single_record_batch(log, 14, model::term_id(1));

        storage::compaction_config c_cfg(
          model::timestamp::min(),
          std::nullopt,
          model::offset::max(),
          ss::default_priority_class(),
          as);
        log.flush().get0();
        model::offset truncate_at(7);
        info("Truncating at offset:{}", truncate_at);
        log
          .truncate(
            storage::truncate_config(truncate_at, ss::default_priority_class()))
          .get0();
        // roll segment
        append_single_record_batch(log, 10, model::term_id(2));
        log.flush().get0();

        // roll segment
        append_single_record_batch(log, 1, model::term_id(8));
        // compact log
        log.compact(c_cfg).get0();
    }

    // force recovery
    {
        BOOST_TEST_MESSAGE("recovering");
        storage::log_manager mgr = make_log_manager(cfg);
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = model::ntp("default", "test", 0);
        auto log
          = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();

        auto read = read_and_validate_all_batches(log);
        auto lstats = log.offsets();
        for (model::offset o = lstats.start_offset; o < lstats.committed_offset;
             ++o) {
            BOOST_REQUIRE(log.get_term(o).has_value());
        }
    }
}

FIXTURE_TEST(
  check_segment_roll_after_compacted_log_truncate, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    ss::abort_source as;

    storage::log_manager mgr = make_log_manager(cfg);

    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();
    // append some batches to first segment (all batches have the same key)
    append_single_record_batch(log, 14, model::term_id(1));

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    log.flush().get0();
    model::offset truncate_at(7);
    info("Truncating at offset:{}", truncate_at);
    BOOST_REQUIRE_EQUAL(log.segment_count(), 1);
    log
      .truncate(
        storage::truncate_config(truncate_at, ss::default_priority_class()))
      .get0();
    append_single_record_batch(log, 10, model::term_id(1));
    log.flush().get0();

    // segment should be rolled after truncation
    BOOST_REQUIRE_EQUAL(log.segment_count(), 2);
    log.compact(c_cfg).get0();

    auto read = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read.begin()->base_offset(), model::offset(6));
    BOOST_REQUIRE_EQUAL(read.begin()->last_offset(), model::offset(6));
    BOOST_REQUIRE_EQUAL(read[read.size() - 1].base_offset(), model::offset(16));
    BOOST_REQUIRE_EQUAL(read[read.size() - 1].last_offset(), model::offset(16));
}

FIXTURE_TEST(check_max_segment_size, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);

    auto mock = config::mock_property<size_t>(20_GiB);
    // defaults
    cfg.max_segment_size = mock.bind();
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    using overrides_t = storage::ntp_config::default_overrides;
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 0);

    // Write 100 * 1_KiB batches, should yield only 1 segment
    auto result = append_exactly(log, 100, 1_KiB).get0(); // 100*1_KiB
    int size1 = disk_log->segments().size();
    BOOST_REQUIRE_EQUAL(size1, 1);

    // Update cluster level configuration and force a roll.
    mock.update(20_KiB);
    disk_log->force_roll(ss::default_priority_class()).get();

    // 30 * 1_KiB should yield 2 new segments.
    result = append_exactly(log, 30, 1_KiB).get0();
    int size2 = disk_log->segments().size();
    BOOST_REQUIRE_EQUAL(size2 - size1, 2);

    // override segment size with ntp_config
    overrides_t ov;
    ov.segment_size = 40_KiB;
    disk_log->update_configuration(ov).get();
    disk_log->force_roll(ss::default_priority_class()).get();

    // 60 * 1_KiB batches should yield 2 segments.
    result = append_exactly(log, 60, 1_KiB).get0(); // 60*1_KiB
    int size3 = disk_log->segments().size();
    BOOST_REQUIRE_EQUAL(size3 - size2, 2);
}

FIXTURE_TEST(check_max_segment_size_limits, storage_test_fixture) {
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
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = model::ntp("default", "test", 0);
        storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
        auto log = mgr.manage(std::move(ntp_cfg)).get0();
        auto disk_log = get_disk_log(log);

        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 0);

        // Write 100 * 1_KiB batches, should yield 1 full segment
        auto result = append_exactly(log, 50, 1_KiB).get0(); // 100*1_KiB
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 1);
        result = append_exactly(log, 100, 1_KiB).get0(); // 100*1_KiB
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 2);

        // A too-low segment size: should be clamped to the lower bound
        mock.update(1_KiB);
        disk_log->force_roll(ss::default_priority_class()).get();
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 3);

        // Exceeding the apparent segment size doesn't roll, because it was
        // clamped
        result = append_exactly(log, 5, 1_KiB).get0();
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 3);
        // Exceeding the lower bound segment size does cause a roll
        result = append_exactly(log, 55, 1_KiB).get0();
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 4);

        // A too-high segment size: should be clamped to the upper bound
        mock.update(2000_KiB);
        disk_log->force_roll(ss::default_priority_class()).get();
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 5);
        // Exceeding the upper bound causes a roll, even if we didn't reach
        // the user-configured segment size
        result = append_exactly(log, 201, 1_KiB).get0();
        BOOST_REQUIRE_EQUAL(disk_log->segments().size(), 6);
    } catch (...) {
        ex = std::current_exception();
    }

    config::shard_local_cfg().log_segment_size_min.reset();
    config::shard_local_cfg().log_segment_size_max.reset();

    if (ex) {
        throw ex;
    }
}

FIXTURE_TEST(partition_size_while_cleanup, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    // make sure segments are small
    cfg.max_segment_size = config::mock_binding<size_t>(10_KiB);
    cfg.compacted_segment_size = config::mock_binding<size_t>(10_KiB);
    // we want force reading most recent compacted batches, disable the cache
    cfg.cache = storage::with_cache::no;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    using overrides_t = storage::ntp_config::default_overrides;
    overrides_t ov;
    // enable both deletion and compaction
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion
                                 | model::cleanup_policy_bitflags::compaction;

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(
      ntp, mgr.config().base_dir, std::make_unique<overrides_t>(ov));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();

    auto sz_initial = get_disk_log(log)->get_probe().partition_size();
    info("sz_initial={}", sz_initial);
    BOOST_REQUIRE_EQUAL(sz_initial, 0);

    // Add 100 batches with one event each, all events having the same key
    static constexpr size_t batch_size = 1_KiB; // Size visible to Kafka API
    static constexpr size_t input_batch_count = 100;

    append_exactly(log, input_batch_count, batch_size, "key")
      .get0(); // 100*1_KiB

    // Test becomes non-deterministic if we allow flush in background: flush
    // explicitly instead.
    log.flush().get();

    // Read back and validate content of log pre-compaction.
    BOOST_REQUIRE_EQUAL(
      get_disk_log(log)->get_probe().partition_size(),
      input_batch_count * batch_size);
    BOOST_REQUIRE_EQUAL(
      read_and_validate_all_batches(log).size(), input_batch_count);
    auto lstats_before = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats_before.committed_offset, model::offset{input_batch_count - 1});
    BOOST_REQUIRE_EQUAL(lstats_before.start_offset, model::offset{0});

    storage::compaction_config ccfg(
      model::timestamp::min(),
      50_KiB,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    // Compact 10 times, with a configuration calling for 60kiB max log size.
    // This results in prefix truncating at offset 50.
    for (int i = 0; i < 10; ++i) {
        compact_and_prefix_truncate(*get_disk_log(log), ccfg);
    }
    get_disk_log(log)->get_probe().partition_size();

    as.request_abort();
    auto lstats_after = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats_after.committed_offset, lstats_before.committed_offset);
    BOOST_REQUIRE_EQUAL(lstats_after.start_offset, model::offset{50});

    auto batches = read_and_validate_all_batches(log);
    auto total_batch_size = std::accumulate(
      batches.begin(),
      batches.end(),
      0,
      [](size_t sum, const model::record_batch& b) {
          return sum + b.size_bytes();
      });

    auto& segments = get_disk_log(log)->segments();
    // One historic segment (all historic batches compacted down to 1), plus
    // one active segment.
    BOOST_REQUIRE_EQUAL(segments.size(), 2);

    // The log-scope reported size must be equal to the sum of the batch sizes
    auto expected_size = total_batch_size;
    auto actual_size = get_disk_log(log)->get_probe().partition_size();
    info("expected_size={}, actual_size={}", expected_size, actual_size);
    BOOST_REQUIRE_EQUAL(actual_size, expected_size);
};

FIXTURE_TEST(check_segment_size_jitter, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);

    // Switch on jitter: it is off by default in default_log_config because
    // for most tests randomness is undesirable.
    cfg.segment_size_jitter = storage::jitter_percents{5};

    // defaults
    cfg.max_segment_size = config::mock_binding<size_t>(100_KiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    std::vector<storage::log> logs;
    for (int i = 0; i < 5; ++i) {
        auto ntp = model::ntp("default", ssx::sformat("test-{}", i), 0);
        storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
        auto log = mgr.manage(std::move(ntp_cfg)).get0();
        append_exactly(log, 2000, 100).get0();
        logs.push_back(log);
    }
    std::vector<size_t> sizes;
    for (auto& l : logs) {
        auto& segs = get_disk_log(l)->segments();
        sizes.push_back((*segs.begin())->size_bytes());
    }
    BOOST_REQUIRE_EQUAL(
      std::all_of(
        sizes.begin(),
        sizes.end(),
        [size = sizes[0]](auto other) { return size == other; }),
      false);
}

FIXTURE_TEST(adjacent_segment_compaction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    // build some segments
    auto disk_log = get_disk_log(log);
    append_single_record_batch(log, 20, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 30, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 40, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 50, model::term_id(1));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 4);

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    // Self compactions complete.
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 4);

    // Check if it honors max_compactible offset by resetting it to the base
    // offset of first segment. Nothing should be compacted.
    const auto first_segment_offsets = disk_log->segments().front()->offsets();
    c_cfg.max_collectible_offset = first_segment_offsets.base_offset;
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 4);

    // reset
    c_cfg.max_collectible_offset = model::offset::max();

    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 3);

    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 2);

    // no change since we can't combine with appender segment
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 2);
}

FIXTURE_TEST(adjacent_segment_compaction_terms, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    // build some segments
    auto disk_log = get_disk_log(log);
    append_single_record_batch(log, 20, model::term_id(1));
    append_single_record_batch(log, 30, model::term_id(2));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 30, model::term_id(2));
    append_single_record_batch(log, 40, model::term_id(3));
    append_single_record_batch(log, 50, model::term_id(4));
    append_single_record_batch(log, 50, model::term_id(5));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 6);

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 6);

    // the two segments with term 2 can be combined
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 5);

    // no more pairs with the same term
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 5);

    for (int i = 0; i < 5; i++) {
        BOOST_REQUIRE_EQUAL(disk_log->segments()[i]->offsets().term(), i + 1);
    }
}

FIXTURE_TEST(max_adjacent_segment_compaction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(6_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto disk_log = get_disk_log(log);

    // add a segment with random keys until a certain size
    auto add_segment = [&log, disk_log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (disk_log->segments().back()->size_bytes() < size);
    };

    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(5_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 6);

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    // self compaction steps
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 6);

    // the first two segments are combined 2+2=4 < 6 MB
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 5);

    // the new first and second are too big 4+5 > 6 MB but the second and third
    // can be combined 5 + 15KB < 6 MB
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 4);

    // then the next 16 KB can be folded in
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 3);

    // that's all that can be done. the next seg is an appender
    log.compact(c_cfg).get0();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 3);
}

FIXTURE_TEST(many_segment_locking, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto disk_log = get_disk_log(log);
    append_single_record_batch(log, 20, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 30, model::term_id(2));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 40, model::term_id(3));
    disk_log->force_roll(ss::default_priority_class()).get();
    append_single_record_batch(log, 50, model::term_id(4));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 4);

    std::vector<ss::lw_shared_ptr<storage::segment>> segments;
    std::copy(
      disk_log->segments().begin(),
      disk_log->segments().end(),
      std::back_inserter(segments));
    segments.pop_back(); // discard the active segment
    BOOST_REQUIRE_EQUAL(segments.size(), 3);

    {
        auto locks = storage::internal::write_lock_segments(
                       segments, std::chrono::seconds(1), 1)
                       .get0();
        BOOST_REQUIRE(locks.size() == segments.size());
    }

    {
        auto lock = segments[2]->write_lock().get0();
        BOOST_REQUIRE_THROW(
          storage::internal::write_lock_segments(
            segments, std::chrono::seconds(1), 1)
            .get0(),
          ss::semaphore_timed_out);
    }

    {
        auto locks = storage::internal::write_lock_segments(
                       segments, std::chrono::seconds(1), 1)
                       .get0();
        BOOST_REQUIRE(locks.size() == segments.size());
    }
}
FIXTURE_TEST(reader_reusability_test_parser_header, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();
    // first small batch
    append_exactly(log, 1, 128).get0();
    // then large batches
    append_exactly(log, 1, 128_KiB).get0();
    append_exactly(log, 1, 128_KiB).get0();

    storage::log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      0,
      4096,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    /**
     * Turn on strict max bytes to leave header in parser
     */
    reader_cfg.strict_max_bytes = true;

    model::offset next_to_read;
    {
        auto reader = log.make_reader(reader_cfg).get();

        auto rec = model::consume_reader_to_memory(
                     std::move(reader), model::no_timeout)
                     .get();
        next_to_read = rec.back().last_offset() + model::offset(1);
        BOOST_REQUIRE_EQUAL(rec.size(), 1);
    }
    {
        reader_cfg.start_offset = next_to_read;
        reader_cfg.max_bytes = 150_KiB;
        auto reader = log.make_reader(reader_cfg).get();

        auto rec = model::consume_reader_to_memory(
                     std::move(reader), model::no_timeout)
                     .get();

        BOOST_REQUIRE_EQUAL(rec.size(), 1);
    }
}

FIXTURE_TEST(compaction_backlog_calculation, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto disk_log = get_disk_log(log);

    // add a segment with random keys until a certain size
    auto add_segment = [&log, disk_log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (disk_log->segments().back()->size_bytes() < size);
    };

    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(5_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 5);

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    /**
     * Initially all compaction rations are equal to 1.0 so it is easy to
     * calculate backlog size.
     */
    auto& segments = disk_log->segments();
    auto backlog_size = log.compaction_backlog();
    size_t self_seg_compaction_sz = 0;
    for (auto& s : segments) {
        self_seg_compaction_sz += s->size_bytes();
    }
    BOOST_REQUIRE_EQUAL(
      backlog_size,
      3 * segments[0]->size_bytes() + 3 * segments[1]->size_bytes()
        + 2 * segments[2]->size_bytes() + segments[3]->size_bytes()
        + self_seg_compaction_sz);
    // self compaction steps
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 5);
    auto new_backlog_size = log.compaction_backlog();
    /**
     * after all self segments are compacted they shouldn't be included into the
     * backlog (only last segment is since it has appender and isn't self
     * compacted)
     */
    BOOST_REQUIRE_EQUAL(
      new_backlog_size,
      backlog_size - self_seg_compaction_sz + segments[4]->size_bytes());
}

FIXTURE_TEST(not_compacted_log_backlog, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::yes;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto disk_log = get_disk_log(log);

    // add a segment with random keys until a certain size
    auto add_segment = [&log, disk_log](size_t size, model::term_id term) {
        do {
            append_single_record_batch(log, 1, term, 16_KiB, true);
        } while (disk_log->segments().back()->size_bytes() < size);
    };

    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(2_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(5_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(16_KiB, model::term_id(1));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 5);

    BOOST_REQUIRE_EQUAL(log.compaction_backlog(), 0);
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

FIXTURE_TEST(disposing_in_use_reader, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();
    for (auto i = 1; i < 100; ++i) {
        append_single_record_batch(log, 1, model::term_id(1), 128, true);
    }
    log.flush().get();
    // read only up to 4096 bytes, this way a reader will still be in a cache
    storage::log_reader_config reader_cfg(
      model::offset(0),
      model::offset::max(),
      0,
      4096,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    auto truncate_f = ss::now();
    {
        auto reader = log.make_reader(reader_cfg).get();

        auto rec = copy_reader_to_memory(reader, model::no_timeout).get();

        BOOST_REQUIRE_EQUAL(rec.back().last_offset(), model::offset(17));
        truncate_f = log.truncate(storage::truncate_config(
          model::offset(5), ss::default_priority_class()));
        // yield to allow truncate fiber to reach waiting for a lock
        ss::sleep(200ms).get();
    }
    // yield again to allow truncate fiber to finish
    tests::cooperative_spin_wait_with_timeout(200ms, [&truncate_f]() {
        return truncate_f.available();
    }).get();

    // we should be able to finish truncate immediately since reader was
    // destroyed
    BOOST_REQUIRE(truncate_f.available());
    truncate_f.get();
}

model::record_batch make_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (auto i = 0; i < 2; ++i) {
        builder.add_raw_kv(
          bytes_to_iobuf(random_generators::get_bytes(128)),
          bytes_to_iobuf(random_generators::get_bytes(10_KiB)));
    }
    return std::move(builder).build();
}

FIXTURE_TEST(committed_offset_updates, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(500_MiB);
    cfg.sanitize_fileops = storage::debug_sanitize_files::no;
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto append = [&] {
        // Append single batch
        storage::log_appender appender = log.make_appender(
          storage::log_append_config{
            .should_fsync = storage::log_append_config::fsync::no,
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout});

        ss::circular_buffer<model::record_batch> batches;
        batches.push_back(
          model::test::make_random_batch(model::offset(0), 1, false));

        auto rdr = model::make_memory_record_batch_reader(std::move(batches));
        return std::move(rdr).for_each_ref(
          std::move(appender), model::no_timeout);
    };

    mutex write_mutex;
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
                  auto f = log.flush();
                  u.return_all();
                  return f.then([&, dirty = res.last_offset] {
                      auto lstats = log.offsets();
                      BOOST_REQUIRE_GE(lstats.committed_offset, dirty);
                  });
              });
        });
    };

    std::vector<ss::future<>> futures;
    futures.reserve(100);

    for (auto i = 0; i < 100; ++i) {
        futures.push_back(append_with_lock());
    }

    ss::when_all_succeed(futures.begin(), futures.end()).get();
}

FIXTURE_TEST(changing_cleanup_policy_back_and_forth, storage_test_fixture) {
    // issue: https://github.com/redpanda-data/redpanda/issues/2214
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::no;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto disk_log = get_disk_log(log);

    // add a segment, some of the record keys in batches are random and some of
    // them are the same to generate offset gaps after compaction
    auto add_segment = [&log, disk_log](size_t size, model::term_id term) {
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
                iobuf key = bytes_to_iobuf(bytes(key_str.c_str()));
                bytes val_bytes = random_generators::get_bytes(1024);

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
                  .io_priority = ss::default_priority_class(),
                  .timeout = model::no_timeout,
                };

                std::move(reader)
                  .for_each_ref(log.make_appender(cfg), cfg.timeout)
                  .get0();
            }
        } while (disk_log->segments().back()->size_bytes() < size);
    };
    // add 2 log segments
    add_segment(1_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(1_MiB, model::term_id(1));
    disk_log->force_roll(ss::default_priority_class()).get();
    add_segment(1_MiB, model::term_id(1));
    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 3);

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    // self compaction steps
    log.compact(c_cfg).get0();
    log.compact(c_cfg).get0();

    // read all batches
    auto first_read = read_and_validate_all_batches(log);

    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    // update cleanup policy to deletion
    log.update_configuration(overrides).get();

    // read all batches again
    auto second_read = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(first_read.size(), second_read.size());
}

ss::future<ss::circular_buffer<model::record_batch>>
copy_to_mem(model::record_batch_reader& reader) {
    using data_t = ss::circular_buffer<model::record_batch>;
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

FIXTURE_TEST(reader_prevents_log_shutdown, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();
    // append some batches
    append_exactly(log, 5, 128).get0();

    // drain whole log with reader so it is not reusable anymore.
    storage::log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<int64_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    auto f = ss::now();
    {
        auto reader = log.make_reader(reader_cfg).get();
        auto batches = copy_to_mem(reader).get();

        f = mgr.shutdown(ntp);
    }
    f.get();
}

FIXTURE_TEST(test_querying_term_last_offset, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();
    // append some baches in term 0
    append_random_batches(log, 10, model::term_id(0));
    auto lstats_term_0 = log.offsets();
    // append some batches in term 1
    append_random_batches(log, 10, model::term_id(1));
    {
        auto disk_log = get_disk_log(log);
        // force segment roll
        disk_log->force_roll(ss::default_priority_class()).get();
    }
    // append more batches in the same term
    append_random_batches(log, 10, model::term_id(1));
    auto lstats_term_1 = log.offsets();
    // append some batche sin term 2
    append_random_batches(log, 10, model::term_id(2));

    BOOST_REQUIRE_EQUAL(
      lstats_term_0.dirty_offset,
      log.get_term_last_offset(model::term_id(0)).value());
    BOOST_REQUIRE_EQUAL(
      lstats_term_1.dirty_offset,
      log.get_term_last_offset(model::term_id(1)).value());
    BOOST_REQUIRE_EQUAL(
      log.offsets().dirty_offset,
      log.get_term_last_offset(model::term_id(2)).value());

    BOOST_REQUIRE(!log.get_term_last_offset(model::term_id(3)).has_value());
    // prefix truncate log at end offset fo term 0

    log
      .truncate_prefix(storage::truncate_prefix_config(
        lstats_term_0.dirty_offset + model::offset(1),
        ss::default_priority_class()))
      .get();

    BOOST_REQUIRE(!log.get_term_last_offset(model::term_id(0)).has_value());
}

void write_batch(
  storage::log log,
  ss::sstring key,
  int value,
  model::record_batch_type batch_type) {
    storage::record_batch_builder builder(batch_type, model::offset(0));

    builder.add_raw_kv(serde::to_iobuf(std::move(key)), serde::to_iobuf(value));

    auto batch = std::move(builder).build();
    batch.set_term(model::term_id(0));
    auto reader = model::make_memory_record_batch_reader({std::move(batch)});
    storage::log_append_config cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout,
    };

    std::move(reader).for_each_ref(log.make_appender(cfg), cfg.timeout).get0();
}

absl::flat_hash_map<std::pair<model::record_batch_type, ss::sstring>, int>
compact_in_memory(storage::log log) {
    auto rdr = log
                 .make_reader(storage::log_reader_config(
                   model::offset(0),
                   model::offset::max(),
                   ss::default_priority_class()))
                 .get();

    absl::flat_hash_map<std::pair<model::record_batch_type, ss::sstring>, int>
      ret;
    auto batches = model::consume_reader_to_memory(
                     std::move(rdr), model::no_timeout)
                     .get();

    for (auto& b : batches) {
        b.for_each_record([&ret, bt = b.header().type](model::record r) {
            auto k = std::make_pair(
              bt, serde::from_iobuf<ss::sstring>(r.key().copy()));
            ret.insert_or_assign(k, serde::from_iobuf<int>(r.value().copy()));
        });
    }

    return ret;
}

FIXTURE_TEST(test_compacting_batches_of_different_types, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_compacted_segment_size = config::mock_binding<size_t>(100_MiB);
    cfg.cache = storage::with_cache::no;
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;

    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto disk_log = get_disk_log(log);

    // the same key but three different batch types
    write_batch(log, "key_1", 1, model::record_batch_type::raft_data);
    write_batch(log, "key_1", 10, model::record_batch_type::tm_update);
    write_batch(log, "key_1", 100, model::record_batch_type::tx_fence);

    write_batch(log, "key_1", 2, model::record_batch_type::raft_data);
    write_batch(log, "key_1", 3, model::record_batch_type::raft_data);
    write_batch(log, "key_1", 4, model::record_batch_type::raft_data);

    write_batch(log, "key_1", 20, model::record_batch_type::tm_update);
    write_batch(log, "key_1", 30, model::record_batch_type::tm_update);
    write_batch(log, "key_1", 40, model::record_batch_type::tm_update);

    write_batch(log, "key_1", 200, model::record_batch_type::tm_update);
    write_batch(log, "key_1", 300, model::record_batch_type::tm_update);
    write_batch(log, "key_1", 400, model::record_batch_type::tm_update);

    disk_log->force_roll(ss::default_priority_class()).get();

    log.flush().get0();

    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 2);

    storage::compaction_config c_cfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    auto before_compaction = compact_in_memory(log);

    BOOST_REQUIRE_EQUAL(before_compaction.size(), 3);
    // compact
    log.compact(c_cfg).get0();
    auto after_compaction = compact_in_memory(log);

    BOOST_REQUIRE(before_compaction == after_compaction);
}

FIXTURE_TEST(read_write_truncate, storage_test_fixture) {
    /**
     * Test validating concurrent reads, writes and truncations
     */
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();

    int cnt = 0;
    int max = 500;
    mutex log_mutex;
    auto produce = ss::do_until(
      [&] { return cnt > max; },
      [&log, &cnt, &log_mutex] {
          ss::circular_buffer<model::record_batch> batches;
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
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout,
          };
          info("append");
          return log_mutex
            .with([reader = std::move(reader), cfg, &log]() mutable {
                info("append_lock");
                return std::move(reader).for_each_ref(
                  log.make_appender(cfg), cfg.timeout);
            })
            .then([](storage::append_result res) {
                info("append_result: {}", res.last_offset);
            })
            .then([&log] { return log.flush(); })
            .finally([&cnt] { cnt++; });
      });

    auto read = ss::do_until(
      [&] { return cnt > max; },
      [&log, &cnt] {
          auto offset = log.offsets();
          storage::log_reader_config cfg(
            std::max(model::offset(0), offset.dirty_offset - model::offset(10)),
            cnt % 2 == 0 ? offset.dirty_offset - model::offset(2)
                         : offset.dirty_offset,
            ss::default_priority_class());
          auto start = ss::steady_clock_type::now();
          return log.make_reader(cfg)
            .then([start](model::record_batch_reader rdr) {
                // assert that creating a reader took less than 5 seconds
                BOOST_REQUIRE_LT(
                  (ss::steady_clock_type::now() - start) / 1ms, 5000);
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](ss::circular_buffer<model::record_batch> batches) {
                if (batches.empty()) {
                    info("read empty range");
                    return;
                }
                info(
                  "read range: {}, {}",
                  batches.front().base_offset(),
                  batches.back().last_offset());
            });
      });

    auto truncate = ss::do_until(
      [&] { return cnt > max; },
      [&log, &log_mutex] {
          auto offset = log.offsets();
          if (offset.dirty_offset <= model::offset(0)) {
              return ss::now();
          }
          return log_mutex.with([&log] {
              auto offset = log.offsets();
              info("truncate offsets: {}", offset);
              auto start = ss::steady_clock_type::now();
              return log
                .truncate(storage::truncate_config(
                  offset.dirty_offset, ss::default_priority_class()))
                .finally([start] {
                    // assert that truncation took less than 5 seconds
                    BOOST_REQUIRE_LT(
                      (ss::steady_clock_type::now() - start) / 1ms, 5000);
                    info("truncate_done");
                });
          });
      });

    produce.get();
    read.get();
    truncate.get();
}

FIXTURE_TEST(write_truncate_compact, storage_test_fixture) {
    /**
     * Test validating concurrent reads, writes and truncations
     */
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(1_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    auto ntp = model::ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    int cnt = 0;
    int max = 50;
    bool done = false;
    mutex log_mutex;
    auto produce
      = ss::do_until(
          [&] { return cnt > max || done; },
          [&log, &cnt, &log_mutex] {
              ss::circular_buffer<model::record_batch> batches;
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
                .io_priority = ss::default_priority_class(),
                .timeout = model::no_timeout,
              };
              return log_mutex
                .with([reader = std::move(reader), cfg, &log]() mutable {
                    return std::move(reader).for_each_ref(
                      log.make_appender(cfg), cfg.timeout);
                })
                .then([](storage::append_result res) {
                    info("append_result: {}", res.last_offset);
                })
                .then([&log] { return log.flush(); })
                .finally([&cnt] { cnt++; });
          })
          .finally([&] { done = true; });

    auto truncate
      = ss::do_until(
          [&] { return done; },
          [&log, &log_mutex] {
              auto offset = log.offsets();
              if (offset.dirty_offset <= model::offset(0)) {
                  return ss::now();
              }
              return log_mutex.with([&log] {
                  auto offset = log.offsets();
                  auto truncate_at = model::offset{
                    random_generators::get_int<int64_t>(
                      offset.dirty_offset() / 2, offset.dirty_offset)};
                  info(
                    "truncate offsets: {}, truncate at: {}",
                    offset,
                    truncate_at);

                  return log
                    .truncate(storage::truncate_config(
                      truncate_at, ss::default_priority_class()))
                    .then_wrapped([log, o = truncate_at](ss::future<> f) {
                        vassert(
                          !f.failed(),
                          "truncation failed with {}",
                          f.get_exception());
                        BOOST_REQUIRE_LE(
                          log.offsets().dirty_offset, model::prev_offset(o));
                    });
              });
          })
          .finally([&] { done = true; });

    auto compact = ss::do_until(
                     [&] { return done; },
                     [&log, &as] {
                         return log
                           .compact(storage::compaction_config(
                             model::timestamp::min(),
                             std::nullopt,
                             model::offset::max(),
                             ss::default_priority_class(),
                             as))
                           .handle_exception_type(
                             [](const storage::segment_closed_exception&) {

                             })
                           .handle_exception([](std::exception_ptr e) {
                               info("compaction exception - {}", e);
                           });
                     })
                     .finally([&] { done = true; });

    compact.get();
    info("compact_done");
    produce.get();
    info("produce_done");
    truncate.get();
    info("truncate_done");

    // Ensure we've cleaned up all our staging segments such that a removal of
    // the log results in nothing leftover.
    auto dir_path = log.config().work_directory();
    try {
        mgr.remove(ntp).get();
    } catch (...) {
        directory_walker walker;
        walker
          .walk(
            dir_path,
            [](const ss::directory_entry& de) {
                info("Leftover file: {}", de.name);
                return ss::make_ready_future<>();
            })
          .get();
        // TODO: re-enable. See:
        // https://github.com/redpanda-data/redpanda/issues/8153
        // throw;
    }
    // BOOST_REQUIRE_EQUAL(false, ss::file_exists(dir_path).get());
};

FIXTURE_TEST(compaction_truncation_corner_cases, storage_test_fixture) {
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

    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   mgr.config().base_dir,
                   std::make_unique<storage::ntp_config::default_overrides>(
                     overrides)))
                 .get0();

    auto large_batch = [](int key) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        builder.add_raw_kv(
          reflection::to_iobuf(ssx::sformat("key-{}", key)),
          bytes_to_iobuf(random_generators::get_bytes(33_KiB)));
        return std::move(builder).build();
    };

    auto write_and_compact =
      [&](ss::circular_buffer<model::record_batch> batches) {
          auto reader = model::make_memory_record_batch_reader(
            std::move(batches));

          storage::log_append_config appender_cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout,
          };

          std::move(reader)
            .for_each_ref(log.make_appender(appender_cfg), model::no_timeout)
            .discard_result()
            .then([&log] { return log.flush(); })
            .get();

          log
            .compact(storage::compaction_config(
              model::timestamp::min(),
              std::nullopt,
              model::offset::max(),
              ss::default_priority_class(),
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
        ss::circular_buffer<model::record_batch> batches;

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

        log
          .truncate(storage::truncate_config(
            truncate_offset, ss::default_priority_class()))
          .get();
        info("truncated at: {}, offsets: {}", truncate_offset, log.offsets());
        BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset(0));
    }

    {
        /**
         * Truncate with the first offset available in the log
         * segment: [batch: (base_offset: 11)]
         *
         */
        ss::circular_buffer<model::record_batch> batches;

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
        log
          .truncate_prefix(storage::truncate_prefix_config(
            truncate_offset, ss::default_priority_class()))
          .get();

        log
          .truncate(storage::truncate_config(
            truncate_offset, ss::default_priority_class()))
          .get();
        info("truncated at: {}, offsets: {}", truncate_offset, log.offsets());
        // empty log have a dirty offset equal to (start_offset - 1)
        BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, model::offset(10));
    }
}

static storage::log_gap_analysis analyze(storage::log::impl& log) {
    // TODO factor out common constant
    storage::log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      0,
      10_MiB,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    return storage::make_log_gap_analysis(
      log.make_reader(reader_cfg).get(), model::offset(0));
}

FIXTURE_TEST(test_max_compact_offset, storage_test_fixture) {
    // Test setup.
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    // (1) append some random data, with limited number of distinct keys, so
    // compaction can make progress.
    auto headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20);

    // (2) remember log offset, roll log, and produce more messages
    log.flush().get0();
    auto first_stats = log.offsets();
    info("Offsets to be compacted {}", first_stats);
    disk_log->force_roll(ss::default_priority_class()).get();
    headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20);

    // (3) roll log and trigger compaction, analyzing offset gaps before and
    // after, to observe compaction behavior.
    log.flush().get0();
    auto second_stats = log.offsets();
    auto pre_compact_gaps = analyze(*disk_log);
    disk_log->force_roll(ss::default_priority_class()).get();
    auto max_compact_offset = first_stats.committed_offset;
    storage::compaction_config ccfg(
      model::timestamp::max(), // no time-based deletion
      std::nullopt,
      max_compact_offset,
      ss::default_priority_class(),
      as);
    log.compact(ccfg).get0();
    auto final_stats = log.offsets();
    auto post_compact_gaps = analyze(*disk_log);

    // (4) check correctness.
    info("pre-compact gaps {}", pre_compact_gaps);
    info("post-compact gaps {}", post_compact_gaps);

    // Compaction doesn't change offset values, it creates holes in offset
    // space.
    BOOST_REQUIRE(
      final_stats.committed_offset == second_stats.committed_offset);

    // No gaps before compacting, and >0 gaps after.
    BOOST_REQUIRE_EQUAL(pre_compact_gaps.num_gaps, 0);
    BOOST_REQUIRE_GT(post_compact_gaps.num_gaps, 0);
    // Verify no compaction happened past the max_compactable_offset we
    // specified.
    BOOST_REQUIRE_LE(post_compact_gaps.first_gap_start, max_compact_offset);
    BOOST_REQUIRE_LE(post_compact_gaps.last_gap_end, max_compact_offset);
};

FIXTURE_TEST(test_self_compaction_while_reader_is_open, storage_test_fixture) {
    // Test setup.
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    // (1) append some random data, with limited number of distinct keys, so
    // compaction can make progress.
    auto headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20);

    // (2) remember log offset, roll log, and produce more messages
    log.flush().get0();

    disk_log->force_roll(ss::default_priority_class()).get();
    headers = append_random_batches<key_limited_random_batch_generator>(
      log, 20);

    // (3) roll log and trigger compaction, analyzing offset gaps before and
    // after, to observe compaction behavior.
    log.flush().get0();

    disk_log->force_roll(ss::default_priority_class()).get();
    storage::compaction_config ccfg(
      model::timestamp::max(), // no time-based deletion
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);
    auto& segment = *(disk_log->segments().begin());
    auto stream = segment
                    ->offset_data_stream(
                      model::offset(0), ss::default_priority_class())
                    .get();
    log.compact(std::move(ccfg)).get();
    stream.close().get();
};

FIXTURE_TEST(test_simple_compaction_rebuild_index, storage_test_fixture) {
    // Test setup.
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    // Append some linear kv ints
    int num_appends = 5;
    append_random_batches<linear_int_kv_batch_generator>(log, num_appends);
    log.flush().get0();
    disk_log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 2);

    // Remove compacted indexes to trigger a full index rebuild.
    auto index_path = disk_log->segments()[0]->path().to_compacted_index();
    BOOST_REQUIRE(std::filesystem::remove(index_path));

    auto batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(
      batches.size(),
      num_appends * linear_int_kv_batch_generator::batches_per_call);
    BOOST_REQUIRE(std::all_of(batches.begin(), batches.end(), [](auto& b) {
        return b.record_count()
               == linear_int_kv_batch_generator::records_per_batch;
    }));

    storage::compaction_config ccfg(
      model::timestamp::min(),
      std::nullopt,
      model::offset::max(),
      ss::default_priority_class(),
      as);

    log.compact(ccfg).get();

    batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(
      batches.size(),
      num_appends * linear_int_kv_batch_generator::batches_per_call);
    linear_int_kv_batch_generator::validate_post_compaction(std::move(batches));
};

struct compact_test_args {
    model::offset max_compact_offs;
    long num_compactable_msg;
    long msg_per_segment;
    long segments;
};

static void
do_compact_test(const compact_test_args args, storage_test_fixture& f) {
    // Test setup.
    auto cfg = f.default_log_config(f.test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10_MiB);
    ss::abort_source as;
    storage::log_manager mgr = f.make_log_manager(cfg);
    tlog.info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_cfg(
      ntp,
      mgr.config().base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(overrides));
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto disk_log = get_disk_log(log);

    auto append_batch = [](storage::log log, model::term_id term) {
        iobuf key = bytes_to_iobuf(bytes("key"));
        iobuf value = random_generators::make_iobuf(100);

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
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout,
        };

        std::move(reader)
          .for_each_ref(log.make_appender(cfg), cfg.timeout)
          .get();
    };

    for (int s = 0; s < args.segments; s++) {
        for (int i = 0; i < args.msg_per_segment; i++) {
            append_batch(log, model::term_id(0));
        }
        disk_log->force_roll(ss::default_priority_class()).get0();
    }
    append_batch(log, model::term_id(0)); // write single message for final
                                          // segment after last roll

    auto pre_gaps = analyze(*disk_log);
    auto pre_stats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      pre_stats.committed_offset, args.segments * args.msg_per_segment);
    BOOST_REQUIRE_EQUAL(pre_gaps.num_gaps, 0);
    tlog.info("pre-compact stats: {}, analysis: {}", pre_stats, pre_gaps);

    storage::compaction_config ccfg(
      model::timestamp::max(), // no time-based deletion
      std::nullopt,
      model::offset(args.max_compact_offs),
      ss::default_priority_class(),
      as);
    log.compact(ccfg).get0();
    auto final_stats = log.offsets();
    auto final_gaps = analyze(*disk_log);
    tlog.info("post-compact stats: {}, analysis: {}", final_stats, final_gaps);
    BOOST_REQUIRE_EQUAL(
      final_stats.committed_offset, args.segments * args.msg_per_segment);
    // we used the same key for all messages, so we should have one huge gap at
    // the beginning of the log
    BOOST_REQUIRE_EQUAL(final_gaps.num_gaps, 1);
    BOOST_REQUIRE_EQUAL(final_gaps.first_gap_start, model::offset(0));

    // If adjacent segment compaction worked in order from oldest to newest, we
    // could use this assert.
    // We can compact the whole first segment, ending at num_compactible_msg -
    // 1, but compaction leaves at least one message per key in the segment,
    // thus the - 2 here.
    //   BOOST_REQUIRE_EQUAL(
    //     final_gaps.last_gap_end, model::offset(args.num_compactable_msg -
    //     2));
    //  Instead, we use weaker assert for now:

    BOOST_REQUIRE_LE(final_gaps.last_gap_end, args.max_compact_offs);
}

FIXTURE_TEST(test_max_compact_offset_mid_segment, storage_test_fixture) {
    // Create a log with three segments. We will set max compactible offset to
    // be in the middle of the second segment. This should cause only the first
    // segment to be compacted, as we do not support partial compaction of a
    // segment.
    do_compact_test(
      {.max_compact_offs = model::offset(150),
       .num_compactable_msg = 100,
       .msg_per_segment = 100,
       .segments = 3},
      *this);
}

FIXTURE_TEST(test_max_compact_offset_unset, storage_test_fixture) {
    // Same as above, but leave max_compact_offset unset.
    do_compact_test(
      {.max_compact_offs = model::offset::max(),
       // XXX AJF: I expected this to be 300, because I force segment roll
       // after writing the third segment.
       .num_compactable_msg = 200,
       .msg_per_segment = 100,
       .segments = 3},
      *this);
}

FIXTURE_TEST(test_bytes_eviction_overrides, storage_test_fixture) {
    size_t batch_size = 128;
    size_t segment_size = 512_KiB;
    auto batches_per_segment = size_t(segment_size / batch_size);
    auto batch_cnt = batches_per_segment * 10 + 1;
    info(
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
    test_cases.push_back(test_case{
      std::nullopt,                   // default local
      std::nullopt,                   // default cloud
      tristate<size_t>(std::nullopt), // topic local
      tristate<size_t>(std::nullopt), // topic cloud
      false,
      batch_size * batch_cnt,
    });

    test_cases.push_back(test_case{
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
    test_cases.push_back(test_case{
      retain_segments(4),             // default local
      retain_segments(6),             // default cloud
      tristate<size_t>(std::nullopt), // topic local
      tristate<size_t>(std::nullopt), // topic cloud
      true,
      segment_size * 4 + batch_size,
    });

    // per topic configuration
    test_cases.push_back(test_case{
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
    test_cases.push_back(test_case{
      retain_segments(5),             // default local
      retain_segments(3),             // default cloud
      tristate<size_t>(std::nullopt), // topic local
      tristate<size_t>(std::nullopt), // topic cloud
      true,
      segment_size * 3 + batch_size,
    });

    // defaults, local retention is disabled, it should be replaced by cloud one
    test_cases.push_back(test_case{
      std::nullopt,                   // default local
      retain_segments(3),             // default cloud
      tristate<size_t>(std::nullopt), // topic local
      tristate<size_t>(std::nullopt), // topic cloud
      true,
      segment_size * 3 + batch_size,
    });

    // topic configuration, local retention is larger than remote one, it
    // should be capped
    test_cases.push_back(test_case{
      retain_segments(6),                   // default local
      retain_segments(8),                   // default cloud
      tristate<size_t>(retain_segments(5)), // topic local
      tristate<size_t>(retain_segments(2)), // topic cloud
      true,
      segment_size * 2 + batch_size,
    });
    //  topic configuration, local retention is disabled, it should be
    // replaced by cloud one
    test_cases.push_back(test_case{
      retain_segments(6),                   // default local
      retain_segments(8),                   // default cloud
      tristate<size_t>{},                   // topic local
      tristate<size_t>(retain_segments(2)), // topic cloud
      true,
      segment_size * 2 + batch_size,
    });

    // cloud storage disabled, use whatever is there in cloud settings
    test_cases.push_back(test_case{
      retain_segments(6),                   // default local
      retain_segments(8),                   // default cloud
      tristate<size_t>(retain_segments(2)), // topic local
      tristate<size_t>(retain_segments(5)), // topic cloud
      false,
      segment_size * 5 + batch_size,
    });

    test_cases.push_back(test_case{
      retain_segments(2),             // default local
      retain_segments(6),             // default cloud
      tristate<size_t>(std::nullopt), // topic local
      tristate<size_t>(std::nullopt), // topic cloud
      false,
      segment_size * 6 + batch_size,
    });

    size_t i = 0;
    for (auto& tc : test_cases) {
        info("Running case {}", i++);
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

        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = model::ntp(model::kafka_namespace, "test", 0);
        storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
        storage::ntp_config::default_overrides overrides;

        bool have_overrides = false;
        if (tc.cloud_storage) {
            have_overrides = true;
            overrides.shadow_indexing_mode = model::shadow_indexing_mode::full;
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

        auto log = mgr.manage(std::move(ntp_cfg)).get0();
        auto deferred_rm = ss::defer(
          [&mgr, ntp]() mutable { mgr.remove(ntp).get(); });

        for (int i = 0; i < batch_cnt; ++i) {
            append_exactly(log, 1, batch_size).get();
        }
        auto disk_log = get_disk_log(log);

        BOOST_REQUIRE_EQUAL(disk_log->segment_count(), 11);

        compact_and_prefix_truncate(
          *disk_log,
          storage::compaction_config(
            model::timestamp::min(),
            cfg.retention_bytes(),
            model::offset::max(),
            ss::default_priority_class(),
            as));
        // make sure we retain less than expected bytes
        BOOST_REQUIRE_LE(disk_log->size_bytes(), tc.expected_bytes_left);
        BOOST_REQUIRE_GT(
          disk_log->size_bytes(), tc.expected_bytes_left - segment_size);
    }
}

FIXTURE_TEST(issue_8091, storage_test_fixture) {
    /**
     * Test validating concurrent reads, writes and truncations
     */
    auto cfg = default_log_config(test_dir);
    cfg.cache = storage::with_cache::no;
    cfg.max_segment_size = config::mock_binding<size_t>(100_MiB);
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

    auto ntp = model::ntp(model::kafka_namespace, "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();

    int cnt = 0;
    int max = 500;
    mutex log_mutex;
    model::offset last_truncate;

    auto produce = ss::do_until(
      [&] { return cnt > max; },
      [&log, &cnt, &log_mutex] {
          ss::circular_buffer<model::record_batch> batches;
          auto bt = random_generators::random_choice(
            std::vector<model::record_batch_type>{
              model::record_batch_type::raft_data,
              model::record_batch_type::raft_configuration});

          // single batch
          storage::record_batch_builder builder(bt, model::offset(0));
          if (bt == model::record_batch_type::raft_data) {
              builder.add_raw_kv(
                reflection::to_iobuf("key"),
                bytes_to_iobuf(random_generators::get_bytes(16 * 1024)));
          } else {
              builder.add_raw_kv(
                std::nullopt,
                bytes_to_iobuf(random_generators::get_bytes(128)));
          }
          batches.push_back(std::move(builder).build());

          auto reader = model::make_memory_record_batch_reader(
            std::move(batches));

          storage::log_append_config cfg{
            .should_fsync = storage::log_append_config::fsync::no,
            .io_priority = ss::default_priority_class(),
            .timeout = model::no_timeout,
          };
          info("append");
          return log_mutex
            .with([reader = std::move(reader), cfg, &log]() mutable {
                info("append_lock");
                return std::move(reader)
                  .for_each_ref(log.make_appender(cfg), cfg.timeout)
                  .then([](storage::append_result res) {
                      info("append_result: {}", res.last_offset);
                  })
                  .then([&log] { return log.flush(); });
            })
            .finally([&cnt] { cnt++; });
      });

    auto read = ss::do_until(
      [&] { return cnt > max; },
      [&log, &last_truncate] {
          auto offset = log.offsets();
          storage::log_reader_config cfg(
            last_truncate - model::offset(1),
            offset.dirty_offset,
            ss::default_priority_class());
          cfg.type_filter = model::record_batch_type::raft_data;

          auto start = ss::steady_clock_type::now();
          return log.make_reader(cfg)
            .then([start](model::record_batch_reader rdr) {
                // assert that creating a reader took less than 5 seconds
                BOOST_REQUIRE_LT(
                  (ss::steady_clock_type::now() - start) / 1ms, 5000);
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](ss::circular_buffer<model::record_batch> batches) {
                if (batches.empty()) {
                    info("read empty range");
                    return;
                }
                info(
                  "read range: {}, {}",
                  batches.front().base_offset(),
                  batches.back().last_offset());
            });
      });

    auto truncate = ss::do_until(
      [&] { return cnt > max; },
      [&log, &log_mutex, &last_truncate] {
          auto offset = log.offsets();
          if (offset.dirty_offset <= model::offset(0)) {
              return ss::now();
          }
          return log_mutex
            .with([&log, &last_truncate] {
                auto offset = log.offsets();
                info("truncate offsets: {}", offset);
                auto start = ss::steady_clock_type::now();
                last_truncate = offset.dirty_offset;
                return log
                  .truncate(storage::truncate_config(
                    offset.dirty_offset, ss::default_priority_class()))
                  .finally([start] {
                      // assert that truncation took less than 5 seconds
                      BOOST_REQUIRE_LT(
                        (ss::steady_clock_type::now() - start) / 1ms, 5000);
                      info("truncate_done");
                  });
            })
            .then([] { return ss::sleep(10ms); });
      });

    produce.get();
    read.get();
    truncate.get();
    auto disk_log = get_disk_log(log);

    // at the end of this test there must be no batch parse errors
    BOOST_REQUIRE_EQUAL(disk_log->get_probe().get_batch_parse_errors(), 0);
}

FIXTURE_TEST(test_skipping_compaction_below_start_offset, log_builder_fixture) {
    using namespace storage;

    ss::abort_source abs;
    temporary_dir tmp_dir("storage_e2e");
    auto data_path = tmp_dir.get_path();

    storage::ntp_config config{{"test_ns", "test_tpc", 0}, {data_path}};

    storage::ntp_config::default_overrides overrides;
    overrides.retention_bytes = tristate<size_t>{1};
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction
        | model::cleanup_policy_bitflags::deletion;

    config.set_overrides(overrides);

    // Create a log and populate it with two segments,
    // while making sure to close the first segment before
    // opening the second.
    b | start(std::move(config));

    auto& log = b.get_disk_log_impl();

    b | add_segment(0) | add_random_batch(0, 100);

    log.force_roll(ss::default_priority_class()).get();

    b | add_segment(100) | add_random_batch(100, 100);

    BOOST_REQUIRE_EQUAL(log.segment_count(), 2);

    compaction_config cfg{
      model::timestamp::max(),
      1,
      model::offset::max(),
      ss::default_priority_class(),
      abs};

    // Call into `disk_log_impl::gc` and listen for the eviction
    // notification being created.
    auto eviction_future = log.monitor_eviction(abs);
    auto new_start_offset = b.apply_retention(cfg).get();
    BOOST_REQUIRE(new_start_offset);

    BOOST_REQUIRE_EQUAL(log.segment_count(), 2);

    // Grab the new start offset from the notification and
    // update the collectible offset and start offsets.
    auto evict_at_offset = eviction_future.get();
    BOOST_REQUIRE_EQUAL(*new_start_offset, model::next_offset(evict_at_offset));
    BOOST_REQUIRE(b.update_start_offset(*new_start_offset).get());

    // Call into `disk_log_impl::compact`. The only segment eligible for
    // compaction is the below the start offset and it should be ignored.
    auto& first_seg = log.segments().front();
    BOOST_REQUIRE_EQUAL(first_seg->finished_self_compaction(), false);

    b.apply_compaction(cfg, *new_start_offset).get();

    BOOST_REQUIRE_EQUAL(first_seg->finished_self_compaction(), false);

    b.stop().get();
}
