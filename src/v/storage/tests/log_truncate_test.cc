#include "model/fundamental.h"
#include "model/record.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/tests/storage_test_fixture.h"
#include "test_utils/fixture.h"

#include <seastar/util/defer.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <iostream>
#include <iterator>
#include <numeric>

using namespace storage; // NOLINT

constexpr model::offset expected_last(model::offset t_offset) {
    // if we truncate at 0 then the last_offset has to be invalidated as log
    // does not include any batches
    return t_offset > model::offset(0) ? t_offset - model::offset(1)
                                       : model::offset{};
}

// FIXME: Add test for on disk implementation

FIXTURE_TEST(test_truncate_whole, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        std::vector<model::record_batch_header> headers;
        for (auto i = 0; i < 10; i++) {
            append_random_batches(log, 1, model::term_id(i));
            log.flush().get();
        }

        log.truncate(model::offset{0}).get0();

        auto read_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
        BOOST_REQUIRE_EQUAL(log.committed_offset(), model::offset{});
        BOOST_REQUIRE_EQUAL(log.max_offset(), model::offset{});
    }
}

FIXTURE_TEST(test_truncate_in_the_middle_of_segment, storage_test_fixture) {
    for (auto type : storage_types) {
        info("{}", type == log_manager::storage_type::disk ? "DISK" : "MEMORY");
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        append_random_batches(log, 6, model::term_id(0));
        log.flush().get0();

        auto all_batches = read_and_validate_all_batches(log);
        auto truncate_offset = all_batches[4].base_offset();

        // truncate in the middle
        info("Truncating at offset:{}", truncate_offset);
        log.truncate(truncate_offset).get0();
        info("reading all batches");
        auto read_batches = read_and_validate_all_batches(log);

        // one less
        auto expected = all_batches[3].last_offset();

        BOOST_REQUIRE_EQUAL(log.committed_offset(), expected);
        BOOST_REQUIRE_EQUAL(log.max_offset(), expected);
        if (truncate_offset != model::offset(0)) {
            BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), expected);
        } else {
            BOOST_REQUIRE_EQUAL(read_batches.empty(), true);
        }
    }
}

FIXTURE_TEST(test_truncate_empty_log, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        append_random_batches(log, 1, model::term_id(1));
        log.flush().get0();

        auto all_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), all_batches.back().last_offset());
    }
}

FIXTURE_TEST(test_truncate_middle_of_old_segment, storage_test_fixture) {
    std::cout.setf(std::ios::unitbuf);
    for (auto type : storage_types) {
        info("{}", type == log_manager::storage_type::disk ? "DISK" : "MEMORY");
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();

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
        log.truncate(all_batches.back().base_offset()).get0();
        all_batches.pop_back(); // we just removed the last one!
        auto final_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(all_batches.size(), final_batches.size());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), all_batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(log.max_offset(), all_batches.back().last_offset());
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
          all_batches.begin(),
          all_batches.end(),
          final_batches.begin(),
          final_batches.end());
    }
}

FIXTURE_TEST(truncate_whole_log_and_then_again, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        std::vector<model::record_batch_header> headers;
        for (auto i = 0; i < 10; i++) {
            append_random_batches(log, 1, model::term_id(i));
            log.flush().get();
        }

        log.truncate(model::offset{0}).get0();
        log.truncate(model::offset{0}).get0();

        auto read_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
        BOOST_REQUIRE_EQUAL(log.committed_offset(), model::offset{});
        BOOST_REQUIRE_EQUAL(log.max_offset(), model::offset{});
    }
}

FIXTURE_TEST(truncate_before_read, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
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
        auto reader = log.make_reader(std::move(cfg));
        // truncate
        log.truncate(model::offset{0}).get0();
        // consume
        reader.consume(batch_validating_consumer{}, model::no_timeout).get0();
        auto read_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
        BOOST_REQUIRE_EQUAL(log.committed_offset(), model::offset{});
        BOOST_REQUIRE_EQUAL(log.max_offset(), model::offset{});
    }
}

FIXTURE_TEST(
  test_truncate_in_the_middle_of_segment_and_append, storage_test_fixture) {
    for (auto type : storage_types) {
        info("{}", type == log_manager::storage_type::disk ? "DISK" : "MEMORY");
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        append_random_batches(log, 6, model::term_id(0));
        log.flush().get0();

        auto all_batches = read_and_validate_all_batches(log);
        auto truncate_offset = all_batches[4].base_offset();

        // truncate in the middle
        info("Truncating at offset:{}", truncate_offset);
        log.truncate(truncate_offset).get0();
        info("reading all batches");
        auto read_batches = read_and_validate_all_batches(log);

        // one less
        auto expected = all_batches[3].last_offset();

        BOOST_REQUIRE_EQUAL(log.committed_offset(), expected);
        BOOST_REQUIRE_EQUAL(log.max_offset(), expected);
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
}
