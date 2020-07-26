#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/log_manager.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/random_batch.h"
#include "storage/types.h"

#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <boost/test/tools/old/interface.hpp>

void validate_offsets(
  model::offset base,
  const std::vector<model::record_batch_header>& write_headers,
  const ss::circular_buffer<model::record_batch>& read_batches) {
    BOOST_REQUIRE_EQUAL(write_headers.size(), read_batches.size());
    auto it = read_batches.begin();
    model::offset next_base = base;
    for (auto const h : write_headers) {
        BOOST_REQUIRE_EQUAL(it->base_offset(), next_base);
        // last offset delta is inclusive (record with this offset belongs to
        // previous batch)
        next_base += (h.last_offset_delta + model::offset(1));
        it++;
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
    cfg.max_segment_size = 1_KiB;
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
    cfg.max_segment_size = 10;
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(log, 10, model::term_id(1), []() {
        ss::circular_buffer<model::record_batch> batches;
        batches.push_back(
          storage::test::make_random_batch(model::offset(0), 1, true));
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
    cfg.max_segment_size = 10 * 1024;
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
      []() {
          ss::circular_buffer<model::record_batch> batches;
          batches.push_back(
            storage::test::make_random_batch(model::offset(0), 1, true));
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
      []() {
          ss::circular_buffer<model::record_batch> batches;
          batches.push_back(
            storage::test::make_random_batch(model::offset(0), 1, true));
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
    model::offset current_offset = model::offset{0};
    ss::circular_buffer<model::record_batch> batches;
    std::vector<size_t> term_batches_counts;
    for (auto i = 0; i < 5; i++) {
        auto term_batches = storage::test::make_random_batches(
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
    auto res = std::move(reader)
                 .for_each_ref(
                   log.make_appender(append_cfg), append_cfg.timeout)
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

FIXTURE_TEST(test_time_based_eviction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = 10;
    cfg.stype = storage::log_config::storage_type::disk;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    model::timestamp gc_ts = headers.back().max_timestamp;
    auto lstats = log.offsets();
    info("Offsets to be evicted {}", lstats);
    headers = append_random_batches(log, 10);
    storage::compaction_config ccfg(
      gc_ts, std::nullopt, ss::default_priority_class(), as);

    log.compact(ccfg).get0();

    auto new_lstats = log.offsets();
    info("Final offsets {}", new_lstats);
    BOOST_REQUIRE_EQUAL(
      new_lstats.start_offset, lstats.dirty_offset + model::offset(1));
};

FIXTURE_TEST(test_size_based_eviction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = 10;
    cfg.stype = storage::log_config::storage_type::disk;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
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

    storage::compaction_config ccfg(
      model::timestamp::min(),
      (total_size - first_size) + 1,
      ss::default_priority_class(),
      as);

    log.compact(ccfg).get0();

    auto new_lstats = log.offsets();
    info("Final offsets {}", new_lstats);
    BOOST_REQUIRE_EQUAL(
      new_lstats.start_offset, lstats.dirty_offset + model::offset(1));
};

FIXTURE_TEST(test_eviction_notification, storage_test_fixture) {
    ss::promise<storage::eviction_range_lock> ev_range_lock;
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = 10;
    cfg.stype = storage::log_config::storage_type::disk;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    storage::ntp_config ntp_cfg(ntp, mgr.config().base_dir);
    auto log = mgr.manage(std::move(ntp_cfg)).get0();
    (void)log.monitor_eviction(as).then(
      [&ev_range_lock](storage::eviction_range_lock lock) mutable {
          ev_range_lock.set_value(std::move(lock));
      });

    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    model::timestamp gc_ts = headers.back().max_timestamp;
    auto lstats_before = log.offsets();
    info("Offsets to be evicted {}", lstats_before);
    headers = append_random_batches(log, 10);
    storage::compaction_config ccfg(
      gc_ts, std::nullopt, ss::default_priority_class(), as);

    auto compaction_future = log.compact(ccfg);
    auto lock = ev_range_lock.get_future().get0();

    auto lstats_after = log.offsets();

    ss::sleep(100ms).get0(); // sleep so that compaction could be scheduled
    BOOST_REQUIRE_EQUAL(lstats_before.start_offset, lstats_after.start_offset);
    // release lock
    lock.locks.clear();
    // wait for compaction
    compaction_future.get0();
    auto compacted_lstats = log.offsets();
    info("Compacted offsets {}", compacted_lstats);
    // check if compaction happend
    BOOST_REQUIRE_EQUAL(
      compacted_lstats.start_offset,
      lstats_before.dirty_offset + model::offset(1));
};