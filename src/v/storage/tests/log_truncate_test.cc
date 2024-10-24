// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/disk_log_impl.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/segment.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <exception>
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
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log->flush().get();
    }
    model::offset truncate_offset{0};
    log
      ->truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get();

    auto read_batches = read_and_validate_all_batches(log);
    auto lstats = log->offsets();
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.start_offset, model::offset{});
}

FIXTURE_TEST(test_truncate_in_the_middle_of_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    append_random_batches(log, 6, model::term_id(0));
    log->flush().get();

    auto all_batches = read_and_validate_all_batches(log);
    auto truncate_offset = all_batches[4].base_offset();

    // truncate in the middle
    info("Truncating at offset:{}", truncate_offset);
    log
      ->truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get();
    info("reading all batches");
    auto read_batches = read_and_validate_all_batches(log);

    // one less
    auto expected = all_batches[3].last_offset();

    auto lstats = log->offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, expected);
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, expected);
    if (truncate_offset != model::offset(0)) {
        BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), expected);
    } else {
        BOOST_REQUIRE_EQUAL(read_batches.empty(), true);
    }
}

FIXTURE_TEST(test_truncate_in_the_middle_of_batch, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();

    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 10, true));
    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 5, true));

    auto truncate_at = [log](model::offset o) mutable {
        log->truncate(storage::truncate_config(o, ss::default_priority_class()))
          .get();
    };

    BOOST_CHECK_THROW(truncate_at(model::offset{7}), std::exception);
    BOOST_CHECK_THROW(truncate_at(model::offset{12}), std::exception);

    truncate_at(model::offset{20});
    BOOST_CHECK_EQUAL(log->offsets().dirty_offset, model::offset{14});
    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 2);
    BOOST_CHECK_EQUAL(read_batches[1].last_offset(), model::offset{14});
}

FIXTURE_TEST(test_truncate_empty_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    append_random_batches(log, 1, model::term_id(1));
    log->flush().get();

    auto all_batches = read_and_validate_all_batches(log);
    auto lstats = log->offsets();
    BOOST_REQUIRE_EQUAL(
      lstats.committed_offset, all_batches.back().last_offset());
}

FIXTURE_TEST(test_truncate_middle_of_old_segment, storage_test_fixture) {
    std::cout.setf(std::ios::unitbuf);
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();

    // Generate from 10 up to 100 batches
    for (auto i = 0; i < 10; i++) {
        auto part = append_random_batches(log, 1, model::term_id(i));
        log->flush().get();
    }
    auto all_batches = read_and_validate_all_batches(log);

    for (size_t i = 0, max = (all_batches.size() / 2); i < max; ++i) {
        all_batches.pop_back();
    }
    // truncate @ offset that belongs to an old segment
    log
      ->truncate(storage::truncate_config(
        all_batches.back().base_offset(), ss::default_priority_class()))
      .get();
    all_batches.pop_back(); // we just removed the last one!
    auto final_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(all_batches.size(), final_batches.size());
    auto lstats = log->offsets();
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
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log->flush().get();
    }

    const auto truncate_offset = model::offset{0};
    log
      ->truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get();
    log
      ->truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get();

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    auto lstats = log->offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
}

FIXTURE_TEST(truncate_before_read, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log->flush().get();
    }
    storage::log_reader_config cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      ss::default_priority_class());

    // first create the reader
    auto reader_ptr = std::make_unique<model::record_batch_reader>(
      log->make_reader(std::move(cfg)).get());
    // truncate
    auto f = log->truncate(
      storage::truncate_config(model::offset(0), ss::default_priority_class()));
    // Memory log works fine
    reader_ptr->consume(batch_validating_consumer{}, model::no_timeout).get();
    reader_ptr = nullptr;
    f.get();
    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    auto lstats = log->offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, model::offset{});
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, model::offset{});
}

FIXTURE_TEST(
  test_truncate_in_the_middle_of_segment_and_append, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    append_random_batches(log, 6, model::term_id(0));
    log->flush().get();

    auto all_batches = read_and_validate_all_batches(log);
    auto truncate_offset = all_batches[4].base_offset();

    // truncate in the middle
    info("Truncating at offset:{}", truncate_offset);
    log
      ->truncate(
        storage::truncate_config(truncate_offset, ss::default_priority_class()))
      .get();
    info("reading all batches");
    auto read_batches = read_and_validate_all_batches(log);

    // one less
    auto expected = all_batches[3].last_offset();
    auto lstats = log->offsets();
    BOOST_REQUIRE_EQUAL(lstats.committed_offset, expected);
    BOOST_REQUIRE_EQUAL(lstats.dirty_offset, expected);
    if (truncate_offset != model::offset(0)) {
        BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), expected);
    } else {
        BOOST_REQUIRE_EQUAL(read_batches.empty(), true);
    }
    // Append new batches
    auto headers = append_random_batches(log, 6, model::term_id(0));
    log->flush().get();
    auto read_after_append = read_and_validate_all_batches(log);
    // 4 batches were not truncated
    BOOST_REQUIRE_EQUAL(read_after_append.size(), headers.size() + 4);
}

FIXTURE_TEST(test_truncate_last_single_record_batch, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    auto headers = append_random_batches(
      log,
      15,
      model::term_id(0),
      [](std::optional<model::timestamp> ts = std::nullopt) {
          ss::circular_buffer<model::record_batch> ret;
          ret.push_back(model::test::make_random_batch(
            model::offset(0),
            1,
            true,
            model::record_batch_type::raft_data,
            std::nullopt,
            ts));
          return ret;
      });
    log->flush().get();

    for (auto lstats = log->offsets(); lstats.dirty_offset > model::offset{};
         lstats = log->offsets()) {
        auto truncate_offset = lstats.dirty_offset;
        log
          ->truncate(storage::truncate_config(
            truncate_offset, ss::default_priority_class()))
          .get();
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
  test_truncate_whole_log_when_logs_are_garbage_collected,
  storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);

    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();

    overrides->cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    overrides->segment_size = 1024;

    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp, mgr.config().base_dir, std::move(overrides)))
                 .get();
    append_random_batches(log, 10, model::term_id(0));
    append_random_batches(log, 10, model::term_id(0));
    log->flush().get();
    auto ts = now();
    append_random_batches(log, 10, model::term_id(0));
    log->flush().get();
    // garbadge collect first append series
    ss::abort_source as;
    log
      ->housekeeping(housekeeping_config(
        ts,
        std::nullopt,
        model::offset::max(),
        std::nullopt,
        ss::default_priority_class(),
        as))
      .get();
    // truncate at 0, offset earlier then the one present in log
    log
      ->truncate(storage::truncate_config(
        model::offset(0), ss::default_priority_class()))
      .get();

    auto lstats = log->offsets();
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
      builder.get_log()->offsets().dirty_offset, model::offset(219));
    builder | storage::stop();
}

FIXTURE_TEST(truncated_segment_recovery, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    auto ntp = model::ntp("default", "test", 0);
    std::vector<model::offset> truncate_offsets;

    {
        storage::log_manager mgr = make_log_manager(cfg);
        info("config: {}", mgr.config());
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });

        auto log = mgr.manage(storage::ntp_config(ntp, cfg.base_dir)).get();

        append_random_batches(log, 10, model::term_id(0));
        log->flush().get();

        // truncate in the middle
        auto all_batches = read_and_validate_all_batches(log);
        truncate_offsets.push_back(all_batches[4].base_offset());

        info("Truncating at offset:{}", truncate_offsets.back());
        log
          ->truncate(storage::truncate_config(
            truncate_offsets.back(), ss::default_priority_class()))
          .get();

        // force segment roll
        append_random_batches(log, 5, model::term_id(1));

        // truncate near the end of the segment (past the last index mark)
        all_batches = read_and_validate_all_batches(log);
        truncate_offsets.push_back(all_batches.back().base_offset());

        info("Truncating at offset:{}", truncate_offsets.back());
        log
          ->truncate(storage::truncate_config(
            truncate_offsets.back(), ss::default_priority_class()))
          .get();

        // force segment roll
        append_random_batches(log, 3, model::term_id(2));

        log->flush().get();
    }

    // recover log
    storage::log_manager rec_mgr = make_log_manager(cfg);

    auto rec_deferred = ss::defer(
      [&rec_mgr]() mutable { rec_mgr.stop().get(); });
    auto rec_log = rec_mgr.manage(storage::ntp_config(ntp, cfg.base_dir)).get();
    auto& impl = *rec_log;

    BOOST_REQUIRE_EQUAL(impl.segment_count(), 3);

    size_t i_seg = 0;
    for (auto seg_it = impl.segments().begin(); seg_it != impl.segments().end();
         ++seg_it, ++i_seg) {
        auto next = std::next(seg_it);
        if (next == impl.segments().end()) {
            // we are interested in all segments except the last.
            break;
        }

        info("segment: {}", *seg_it);
        auto offsets = (*seg_it)->offsets();
        auto truncate_offset = truncate_offsets[i_seg];

        BOOST_REQUIRE_EQUAL(
          offsets.get_dirty_offset(), truncate_offset - model::offset{1});

        auto next_offsets = (*next)->offsets();
        BOOST_REQUIRE_EQUAL(next_offsets.get_base_offset(), truncate_offset);
        // segment commited offset has to be lower than next segment base
        // offset
        BOOST_REQUIRE_LT(
          offsets.get_committed_offset(), next_offsets.get_base_offset());
    }
}

FIXTURE_TEST(test_concurrent_prefix_truncate_and_gc, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);

    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();

    overrides->cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion;
    overrides->segment_size = 1024;

    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp, mgr.config().base_dir, std::move(overrides)))
                 .get();

    append_random_batches(log, 10, model::term_id(0));
    auto lstats = log->offsets();

    append_random_batches(log, 10, model::term_id(1));
    log->flush().get();

    auto ts = now();

    append_random_batches(log, 10, model::term_id(2));
    log->flush().get();
    ss::abort_source as;

    // The call to 'compact' simply triggers a notification
    // for the log eviction stm with an offset until which
    // to evict. The test does not listen for the notification,
    // so this call is basically a no-op.
    auto f1 = log->housekeeping(housekeeping_config(
      ts,
      std::nullopt,
      model::offset::max(),
      std::nullopt,
      ss::default_priority_class(),
      as));

    auto f2 = log->truncate_prefix(storage::truncate_prefix_config(
      model::next_offset(lstats.dirty_offset), ss::default_priority_class()));

    f1.get();
    f2.get();

    BOOST_REQUIRE_EQUAL(
      (*log->segments().begin())->offsets().get_base_offset(),
      log->offsets().start_offset);
}

FIXTURE_TEST(test_concurrent_truncate_and_compaction, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    auto deferred = ss::defer([&]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);

    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();
    overrides->cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp, mgr.config().base_dir, std::move(overrides)))
                 .get();
    for (int seg = 0; seg < 2; seg++) {
        for (int i = 0; i < 5; i++) {
            append_random_batches(log, 1, model::term_id(0));
        }
        log->flush().get();
        log->force_roll(ss::default_priority_class()).get();
    }

    auto all_batches = read_and_validate_all_batches(log);
    auto truncate_idx = random_generators::get_int(
      0, int(all_batches.size() - 1));
    auto truncate_offset = all_batches[truncate_idx].base_offset();

    // Truncation deletes the compacted index of already compacted segments.
    // so start out by compacting our segments. We'll use adjacent merge
    // compaction, which initially self compacts one segment at a time, while
    // leaving room for further windowed compaction.
    ss::abort_source as;
    compaction_config compaction_cfg(
      model::offset::max(), std::nullopt, ss::default_priority_class(), as);
    auto& disk_log = *dynamic_cast<disk_log_impl*>(log.get());
    disk_log.adjacent_merge_compact(compaction_cfg).get();
    disk_log.adjacent_merge_compact(compaction_cfg).get();
    for (const auto& s : log->segments()) {
        if (s->has_appender()) {
            continue;
        }
        BOOST_REQUIRE(!s->finished_windowed_compaction());
        BOOST_REQUIRE(s->finished_self_compaction());
        BOOST_REQUIRE(s->is_compacted_segment());
    }

    // Now race windowed compaction and truncation.
    auto ts = now();
    auto sleep_ms1 = random_generators::get_int(0, 100);
    housekeeping_config housekeeping_cfg(
      ts,
      std::nullopt,
      model::offset::max(),
      std::nullopt,
      ss::default_priority_class(),
      as);
    auto f1 = ss::sleep(sleep_ms1 * 1ms).then([&] {
        return log->housekeeping(housekeeping_cfg);
    });
    auto sleep_ms2 = random_generators::get_int(0, 100);
    auto f2 = ss::sleep(sleep_ms2 * 1ms).then([&] {
        return log->truncate(truncate_config{
          model::offset{truncate_offset}, ss::default_priority_class()});
    });

    std::exception_ptr housekeeping_eptr;
    std::exception_ptr truncation_eptr;
    try {
        f1.get();
    } catch (...) {
        housekeeping_eptr = std::current_exception();
        info("Housekeeping error: {}", housekeeping_eptr);
    }
    try {
        f2.get();
    } catch (...) {
        truncation_eptr = std::current_exception();
        info("Truncation error: {}", truncation_eptr);
    }
    if (housekeeping_eptr) {
        BOOST_REQUIRE_THROW(
          std::rethrow_exception(housekeeping_eptr),
          storage::segment_closed_exception);
    }
    BOOST_REQUIRE(!truncation_eptr);

    // Previously, the above race could result in subsequent windowed
    // compactions not completing.
    log->housekeeping(housekeeping_cfg).get();
    for (const auto& s : log->segments()) {
        info("Resulting segment: {}", s);
        if (s->has_appender()) {
            continue;
        }
        BOOST_REQUIRE(s->finished_windowed_compaction());
    }
}

FIXTURE_TEST(
  test_prefix_truncate_in_the_middle_of_batch, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();

    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 10, true));
    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 5, true));

    auto truncate_at = [log](model::offset o) mutable {
        log
          ->truncate_prefix(
            storage::truncate_prefix_config(o, ss::default_priority_class()))
          .get();
    };

    truncate_at(model::offset{7});
    BOOST_CHECK_EQUAL(log->offsets().start_offset, model::offset{7});
    auto batches1 = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(batches1.size(), 2);
    BOOST_CHECK_EQUAL(batches1[0].base_offset(), model::offset{0});

    truncate_at(model::offset{12});
    BOOST_CHECK_EQUAL(log->offsets().start_offset, model::offset{12});
    auto batches2 = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(batches2.size(), 1);
    BOOST_CHECK_EQUAL(batches2[0].base_offset(), model::offset{10});

    truncate_at(model::offset{20});
    BOOST_CHECK_EQUAL(log->offsets().start_offset, model::offset{20});
    auto batches3 = read_and_validate_all_batches(log);
    BOOST_CHECK_EQUAL(batches3.size(), 0);

    truncate_at(model::offset{3});
    BOOST_CHECK_EQUAL(log->offsets().start_offset, model::offset{20});
    auto batches4 = read_and_validate_all_batches(log);
    BOOST_CHECK_EQUAL(batches4.size(), 0);
}

FIXTURE_TEST(test_prefix_truncate_then_truncate_all, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();

    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 10, true));
    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 5, true));

    log
      ->truncate_prefix(storage::truncate_prefix_config(
        model::offset{10}, ss::default_priority_class()))
      .get();
    log
      ->truncate(storage::truncate_config(
        model::offset{10}, ss::default_priority_class()))
      .get();

    // Check that even though the log is empty, start offset is saved.

    BOOST_CHECK_EQUAL(log->offsets().start_offset, model::offset{10});
    BOOST_CHECK_EQUAL(read_and_validate_all_batches(log).size(), 0);

    append_batch(
      log, model::test::make_random_batch(model::offset{0}, 3, true));
    BOOST_CHECK_EQUAL(log->offsets().start_offset, model::offset{10});
    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 1);
    BOOST_CHECK_EQUAL(read_batches[0].base_offset(), model::offset{10});
    BOOST_CHECK_EQUAL(read_batches[0].last_offset(), model::offset{12});
}

FIXTURE_TEST(test_index_max_timestamp_update, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr = make_log_manager(cfg);
    info("config: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get();
    append_batch(
      log,
      model::test::make_random_batch(
        model::offset{0},
        10,
        true,
        model::record_batch_type::raft_data,
        std::vector<size_t>(10, 1024),
        model::timestamp(10000)));
    // The max timestamp for this batch will be 20009
    // as there are 10 records in it and the base is 20000.
    append_batch(
      log,
      model::test::make_random_batch(
        model::offset{10},
        10,
        true,
        model::record_batch_type::raft_data,
        std::vector<size_t>(10, 1024),
        model::timestamp(20000)));
    append_batch(
      log,
      model::test::make_random_batch(
        model::offset{20},
        10,
        true,
        model::record_batch_type::raft_data,
        std::vector<size_t>(10, 1024),
        model::timestamp(30000)));

    log
      ->truncate(storage::truncate_config(
        model::offset{20}, ss::default_priority_class()))
      .get();

    auto& impl = *log;

    // The maximum timestamp in the index should be the maximum
    // timestamp of the batch preceeding the batch where the truncation
    // occurred. In this case, truncation happened in the last batch,
    // so we require the max timestmap to be that of the previous second
    // batch.
    BOOST_REQUIRE(impl.segment_count() == 1);
    const auto& seg = impl.segments().front();
    BOOST_REQUIRE(seg->index().max_timestamp() == model::timestamp{20009});
}
