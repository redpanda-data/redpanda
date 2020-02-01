#include "storage/tests/storage_test_fixture.h"

#include <seastar/util/defer.hh>

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
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        auto headers = append_random_batches(log, 10);
        log.flush().get0();
        auto batches = read_and_validate_all_batches(log);

        BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
        BOOST_REQUIRE_EQUAL(log.max_offset(), batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), batches.back().last_offset());
        validate_offsets(model::offset(0), headers, batches);
    }
};

FIXTURE_TEST(append_twice_to_same_segment, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        auto headers = append_random_batches(log, 10);
        log.flush().get0();
        auto headers_2 = append_random_batches(log, 10);
        log.flush().get0();
        std::move(
          std::begin(headers_2),
          std::end(headers_2),
          std::back_inserter(headers));
        auto batches = read_and_validate_all_batches(log);

        BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
        BOOST_REQUIRE_EQUAL(log.max_offset(), batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), batches.back().last_offset());
    }
};

FIXTURE_TEST(test_assigning_offsets_in_multiple_segment, storage_test_fixture) {
    for (auto type : storage_types) {
        auto cfg = default_log_config(test_dir);
        cfg.max_segment_size = kbytes(1);
        storage::log_manager mgr = make_log_manager(std::move(cfg));
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        auto headers = append_random_batches(log, 10);
        log.flush().get0();
        auto batches = read_and_validate_all_batches(log);

        BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
        BOOST_REQUIRE_EQUAL(log.max_offset(), batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), batches.back().last_offset());
        validate_offsets(model::offset(0), headers, batches);
    }
};

FIXTURE_TEST(test_single_record_per_segment, storage_test_fixture) {
    for (auto type : storage_types) {
        auto cfg = default_log_config(test_dir);
        cfg.max_segment_size = 10;
        storage::log_manager mgr = make_log_manager(std::move(cfg));
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
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
        BOOST_REQUIRE_EQUAL(log.max_offset(), batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), batches.back().last_offset());
        validate_offsets(model::offset(0), headers, batches);
    }
};

FIXTURE_TEST(test_reading_range_from_a_log, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto headers = append_random_batches(log, 10);
        log.flush().get0();
        auto batches = read_and_validate_all_batches(log);

        // range from base of beging to last of end
        auto range = read_range_to_vector(
          log, batches[3].base_offset(), batches[7].last_offset());
        BOOST_REQUIRE_EQUAL(range.size(), 5);
        BOOST_REQUIRE_EQUAL(range.front().crc(), batches[3].crc());
        BOOST_REQUIRE_EQUAL(range.back().crc(), batches[7].crc());
        // Range is inclusive base offset points to batch[7] so it have to be
        // included
        range = read_range_to_vector(
          log, batches[3].base_offset(), batches[7].base_offset());
        BOOST_REQUIRE_EQUAL(range.size(), 5);
        BOOST_REQUIRE_EQUAL(range.front().crc(), batches[3].crc());
        BOOST_REQUIRE_EQUAL(range.back().crc(), batches[7].crc());

        range = read_range_to_vector(
          log, batches[3].last_offset(), batches[7].base_offset());
        BOOST_REQUIRE_EQUAL(range.size(), 5);
        BOOST_REQUIRE_EQUAL(range.front().crc(), batches[3].crc());
        BOOST_REQUIRE_EQUAL(range.back().crc(), batches[7].crc());
        // range from base of beging to the middle of end
        range = read_range_to_vector(
          log,
          batches[3].base_offset(),
          batches[7].base_offset() + model::offset(batches[7].size() / 2));
        BOOST_REQUIRE_EQUAL(range.size(), 5);
        BOOST_REQUIRE_EQUAL(range.front().crc(), batches[3].crc());
        BOOST_REQUIRE_EQUAL(range.back().crc(), batches[7].crc());
    }
};

FIXTURE_TEST(test_rolling_term, storage_test_fixture) {
    for (auto type : storage_types) {
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
        std::vector<model::record_batch_header> headers;
        model::offset current_offset = model::offset{0};
        for (auto i = 0; i < 5; i++) {
            auto part = append_random_batches(log, 1, model::term_id(i));
            for (auto h : part) {
                current_offset += h.last_offset_delta + 1;
            }
            log.flush().get();
            BOOST_REQUIRE_EQUAL(
              model::term_id(i),
              log.get_term(current_offset - model::offset(1)).value());
            std::move(part.begin(), part.end(), std::back_inserter(headers));
        }

        auto read_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(
          log.max_offset(), read_batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), read_batches.back().last_offset());
    }
};

FIXTURE_TEST(test_append_batches_from_multiple_terms, storage_test_fixture) {
    for (auto type : storage_types) {
        info("Testing type: {}", type);
        storage::log_manager mgr = make_log_manager();
        auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
        auto ntp = make_ntp("default", "test", 0);
        auto log = mgr.manage(ntp, type).get0();
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
        auto reader = model::make_memory_record_batch_reader(
          std::move(batches));
        auto res = std::move(reader)
                     .consume(log.make_appender(append_cfg), append_cfg.timeout)
                     .get0();
        log.flush().get();

        auto read_batches = read_and_validate_all_batches(log);
        BOOST_REQUIRE_EQUAL(
          log.max_offset(), read_batches.back().last_offset());
        BOOST_REQUIRE_EQUAL(
          log.committed_offset(), read_batches.back().last_offset());
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
}
