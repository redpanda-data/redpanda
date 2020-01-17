#include "model/fundamental.h"
#include "model/record.h"
#include "storage/log.h"
#include "storage/tests/storage_test_fixture.h"
#include "test_utils/fixture.h"

#include <seastar/util/defer.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <iterator>
#include <numeric>

using namespace storage; // NOLINT

FIXTURE_TEST(test_rolling_term, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 5; i++) {
        auto part = append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
        std::move(part.begin(), part.end(), std::back_inserter(headers));
    }

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(log.max_offset(), read_batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(
      log.committed_offset(), read_batches.back().last_offset());
};

#if 0
FIXTURE_TEST(test_truncate_whole, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }

    log.truncate(model::offset{}).get0();

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    BOOST_REQUIRE_EQUAL(log.committed_offset(), model::offset{});
    BOOST_REQUIRE_EQUAL(log.max_offset(), model::offset{});
}

FIXTURE_TEST(test_truncate_in_the_middle_of_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();
    append_random_batches(log, 1, model::term_id(0));
    log.flush().get0();

    auto all_batches = read_and_validate_all_batches(log);
    auto truncate_offset = all_batches[all_batches.size() / 2].last_offset();

    // truncate in the middle
    log.truncate(truncate_offset).get0();
    auto read_batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(log.committed_offset(), truncate_offset);
    BOOST_REQUIRE_EQUAL(log.max_offset(), truncate_offset);
    BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), truncate_offset);
}

FIXTURE_TEST(test_truncate_empty_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();
    append_random_batches(log, 1, model::term_id(1));
    log.flush().get0();

    auto all_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(
      log.committed_offset(), all_batches.back().last_offset());
}

FIXTURE_TEST(test_truncate_middle_of_old_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();

    model::offset truncate_offset{0};
    size_t expected_batches = 0;
    for (auto i = 0; i < 10; i++) {
        auto part = append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
        if (i < 4) {
            expected_batches += part.size();
            for (auto& h : part) {
                truncate_offset += h.last_offset_delta + 1;
            }
        }
        if (i == 4) {
            // truncate in the middle of 4th term
            expected_batches += part.size() / 2;
            for (auto& h :
                 boost::make_iterator_range_n(part.begin(), part.size() / 2)) {
                truncate_offset += h.last_offset_delta + 1;
            }
        }
    }
    truncate_offset = truncate_offset - model::offset(1);
    // truncate @ offset that belongs to an old segment
    log.truncate(truncate_offset).get0();

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), expected_batches);
    BOOST_REQUIRE_EQUAL(log.committed_offset(), truncate_offset);
    BOOST_REQUIRE_EQUAL(log.max_offset(), truncate_offset);
    BOOST_REQUIRE_EQUAL(read_batches.back().last_offset(), truncate_offset);

    mgr.stop().get0();
}

FIXTURE_TEST(truncate_whole_log_and_then_again, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }

    log.truncate(model::offset{}).get0();
    log.truncate(model::offset{}).get0();

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    BOOST_REQUIRE_EQUAL(log.committed_offset(), model::offset{});
    BOOST_REQUIRE_EQUAL(log.max_offset(), model::offset{});
}

FIXTURE_TEST(truncate_before_read, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr.manage(ntp).get0();
    std::vector<model::record_batch_header> headers;
    for (auto i = 0; i < 10; i++) {
        append_random_batches(log, 1, model::term_id(i));
        log.flush().get();
    }
    storage::log_reader_config cfg{
      .start_offset = model::offset(0),
      .max_bytes = std::numeric_limits<size_t>::max(),
      .min_bytes = 0,
      .prio = ss::default_priority_class(),
      .type_filter = {}};
    // first create the reader
    auto reader = log.make_reader(std::move(cfg));
    // truncate
    log.truncate(model::offset{}).get0();
    // consume
    reader.consume(batch_validating_consumer{}, model::no_timeout).get0();
    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(read_batches.size(), 0);
    BOOST_REQUIRE_EQUAL(log.committed_offset(), model::offset{});
    BOOST_REQUIRE_EQUAL(log.max_offset(), model::offset{});
}
#endif
