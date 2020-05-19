#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>
#include <seastar/util/defer.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <iostream>
#include <iterator>
#include <numeric>
#include <stdexcept>

using namespace storage; // NOLINT

constexpr model::offset expected_last(model::offset t_offset) {
    // if we truncate at 0 then the last_offset has to be invalidated as log
    // does not include any batches
    return t_offset > model::offset(0) ? t_offset - model::offset(1)
                                       : model::offset{};
}

FIXTURE_TEST(test_truncate_whole, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }
    model::offset truncate_offset{0};
    log
      .truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get0();

    auto read_batches = read_and_validate_all_batches(log);
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
}

FIXTURE_TEST(test_truncate_in_the_middle_of_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    append_random_batches(log, 6, model::term_id(0));
    log.flush().get0();

    auto all_batches = read_and_validate_all_batches(log);
    auto truncate_offset = all_batches[4].base_offset();

    // truncate in the middle
    info("Truncating at offset:{}", truncate_offset);
    log
      .truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get0();
    info("reading all batches");
    auto read_batches = read_and_validate_all_batches(log);

    // one less
    auto expected = all_batches[3].last_offset();

    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, expected);
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, expected);
    if (truncate_offset != model::offset(0)) {
        BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), expected);
    } else {
        BOOST_REQUIRE_EQUAL(read_batches.empty(), true);
    }
}

FIXTURE_TEST(test_truncate_empty_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    append_random_batches(log, 1, model::term_id(1));
    log.flush().get0();

    auto all_batches = read_and_validate_all_batches(log);
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.committed_offset, all_batches.back().last_offset());
}

FIXTURE_TEST(test_truncate_middle_of_old_segment, storage_test_fixture) {
    std::cout.setf(std::ios::unitbuf);
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();

    // Generate from 10 up to 100 batches
    for (auto i = 0; i < 10; i++) {
        auto part = append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }
    auto all_batches = read_and_validate_all_batches(log);

    for (size_t i = 0, max = (all_batches.size() / 2); i < max; ++i) {
        all_batches.pop_back();
    }
    // truncate @ offset that belongs to an old segment
    log
      .truncate(storage::truncate_config(
        all_batches.back().base_offset(), ss::default_priority_class()))
      .get0();
    all_batches.pop_back(); // we just removed the last one!
    auto final_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(all_batches.size(), final_batches.size());
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.committed_offset, all_batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, all_batches.back().last_offset());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      all_batches.begin(),
      all_batches.end(),
      final_batches.begin(),
      final_batches.end());
}

FIXTURE_TEST(truncate_whole_log_and_then_again, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }

    const auto truncate_offset = model::offset{0};
    log
      .truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get0();
    log
      .truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get0();

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
}

FIXTURE_TEST(truncate_before_read, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }
    storage::log_reader_config cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      ss::default_priority_class());

    // first create the reader
    auto reader_ptr = std::make_unique<model::record_batch_reader>(
      log.make_reader(std::move(cfg)).get0());
    // truncate
    auto f = log.truncate(
      storage::truncate_config(model::offset(0), ss::default_priority_class()));
    // Memory log works fine
    reader_ptr->consume(batch_validating_consumer{}, model::no_timeout).get0();
    reader_ptr = nullptr;
    f.get();
    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
}

FIXTURE_TEST(
  test_truncate_in_the_middle_of_segment_and_append, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    append_random_batches(log, 6, model::term_id(0));
    log.flush().get0();

    auto all_batches = read_and_validate_all_batches(log);
    auto truncate_offset = all_batches[4].base_offset();

    // truncate in the middle
    info("Truncating at offset:{}", truncate_offset);
    log
      .truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get0();
    info("reading all batches");
    auto read_batches = read_and_validate_all_batches(log);

    // one less
    auto expected = all_batches[3].last_offset();
    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, expected);
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, expected);
    if (truncate_offset != model::offset(0)) {
        BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), expected);
    } else {
        BOOST_REQUIRE_EQUAL(read_batches.empty(), true);
    }
    // Append new batches
    auto headers = append_random_batches(log, 6, model::term_id(0));
    log.flush().get0();
    auto read_after_append = read_and_validate_all_batches(log);
    // 4 batches were not truncated
    BOOST_REQUIRE_EQUAL(read_after_append.size(), headers.size() + 4);
}

FIXTURE_TEST(test_truncate_last_single_record_batch, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();
    auto headers = append_random_batches(log, 15, model::term_id(0), [] {
        ss::circular_buffer<model::record_batch> ret;
        ret.push_back(
          storage::test::make_random_batch(model::offset(0), 1, true));
        return ret;
    });
    log.flush().get0();

    for (auto lstats = log.offsets(); lstats.dirty_offset > model::offset{};
         lstats = log.offsets()) {
        auto truncate_offset = lstats.dirty_offset;
        log
          .truncate(storage::truncate_config(
            truncate_offset, ss::default_priority_class()))
          .get0();
        auto all_batches = read_and_validate_all_batches(log);
        auto expected = truncate_offset - headers.back().record_count;
        headers.pop_back();
        if (!headers.empty()) {
            BOOST_REQUIRE_EQUAL(all_batches.back().last_offset(), expected);
        } else {
            BOOST_REQUIRE_EQUAL(all_batches.empty(), true);
        }
    }
}

FIXTURE_TEST(
  test_truncate_whole_log_when_logs_are_garbadge_collected,
  storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.stype = storage::log_config::storage_type::disk;
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);

    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();

    overrides->cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    overrides->segment_size = 1024;

    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp, mgr.config().base_dir, std::move(overrides)))
                 .get0();
    append_random_batches(log, 10, model::term_id(0));
    append_random_batches(log, 10, model::term_id(0));
    log.flush().get0();
    auto ts = model::timestamp::now();
    append_random_batches(log, 10, model::term_id(0));
    log.flush().get0();
    // garbadge collect first append series
    log
      .compact(
        compaction_config(ts, std::nullopt, ss::default_priority_class()))
      .get0();
    // truncate at 0, offset earlier then the one present in log
    log
      .truncate(storage::truncate_config(
        model::offset(0), ss::default_priority_class()))
      .get0();

    auto lstats = log.offsets();
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
}

FIXTURE_TEST(test_truncate, storage_test_fixture) {
    storage::disk_log_builder builder;
    builder | storage::start() | storage::add_segment(0)
      | storage::add_random_batch(0, 1, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(1, 5, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(6, 14, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(20, 30, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(50, 18, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(68, 11, storage::maybe_compress_batches::yes)
      | storage::truncate_log(68)
      | storage::add_random_batch(68, 11, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(79, 13, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(92, 4, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(96, 12, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(108, 3, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(111, 25, storage::maybe_compress_batches::yes)
      | storage::truncate_log(79)
      | storage::add_random_batch(79, 13, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(92, 4, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(96, 12, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(108, 3, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(111, 25, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(136, 20, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(156, 7, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(163, 22, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(185, 29, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(214, 6, storage::maybe_compress_batches::yes)
      | storage::truncate_log(136)
      | storage::add_random_batch(136, 20, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(156, 7, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(163, 22, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(185, 29, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(214, 6, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(220, 20, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(240, 16, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(256, 23, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(279, 17, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(296, 7, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(303, 16, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(319, 2, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(321, 17, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(338, 27, storage::maybe_compress_batches::yes)
      | storage::add_random_batch(365, 2, storage::maybe_compress_batches::yes)
      | storage::truncate_log(220);

    BOOST_REQUIRE_EQUAL(
      builder.get_log().offsets().dirty_offset, model::offset(219));
    builder | storage::stop();
}
