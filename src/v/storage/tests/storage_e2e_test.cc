#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/log_manager.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/random_batch.h"

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
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    BOOST_REQUIRE_EQUAL(log.dirty_offset(), batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(log.committed_offset(), batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

FIXTURE_TEST(append_twice_to_same_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto headers_2 = append_random_batches(log, 10);
    log.flush().get0();
    std::move(
      std::begin(headers_2), std::end(headers_2), std::back_inserter(headers));
    auto batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    BOOST_REQUIRE_EQUAL(log.dirty_offset(), batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(log.committed_offset(), batches.back().last_offset());
};

FIXTURE_TEST(test_assigning_offsets_in_multiple_segment, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = 1_KiB;
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
    auto headers = append_random_batches(log, 10);
    log.flush().get0();
    auto batches = read_and_validate_all_batches(log);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    BOOST_REQUIRE_EQUAL(log.dirty_offset(), batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(log.committed_offset(), batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

FIXTURE_TEST(test_single_record_per_segment, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = 10;
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
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
    BOOST_REQUIRE_EQUAL(log.dirty_offset(), batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(log.committed_offset(), batches.back().last_offset());
    validate_offsets(model::offset(0), headers, batches);
};

FIXTURE_TEST(test_reading_range_from_a_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Configuration: {}", mgr.config());
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
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
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
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
    BOOST_REQUIRE_EQUAL(log.dirty_offset(), read_batches.back().last_offset());
    BOOST_REQUIRE_EQUAL(
      log.committed_offset(), read_batches.back().last_offset());
};

FIXTURE_TEST(test_append_batches_from_multiple_terms, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    info("Testing type: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = make_ntp("default", "test", 0);
    auto log = mgr
                 .manage(storage::ntp_config(
                   ntp,
                   fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                 .get0();
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
                 .consume(log.make_appender(append_cfg), append_cfg.timeout)
                 .get0();
    log.flush().get();

    auto read_batches = read_and_validate_all_batches(log);
    BOOST_REQUIRE_EQUAL(log.dirty_offset(), read_batches.back().last_offset());
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

ss::logger wlog{"storage-workload"};
class storage_workload {
    using command_t = std::function<ss::future<>(storage::log)>;
    using commands_t = std::vector<command_t>;

public:
    explicit storage_workload(storage::log l, size_t ops_count)
      : l(l) {
        generate_workload(ops_count);
    }

    ss::future<> execute() {
        // execute commands in sequence
        return ss::do_for_each(workload, [this](command_t& c) { return c(l); });
    }

private:
    command_t random_cmd() {
        switch (random_generators::get_int(0, 4)) {
        case 0:
            return append;
        case 1:
            return truncate;
        case 2:
            return read_cmd;
        case 3:
            return flush;
        case 4:
            return term_roll;
        }
        return append;
    }

    void generate_workload(size_t count) {
        workload.reserve(count);
        for (auto i : boost::irange((size_t)0, count)) {
            workload.push_back(random_cmd());
        }
    }

    command_t append = [this](storage::log l) {
        storage::log_append_config append_cfg{
          storage::log_append_config::fsync::yes,
          ss::default_priority_class(),
          model::no_timeout};
        auto batches = storage::test::make_random_batches(model::offset(0), 10);
        for (auto& b : batches) {
            b.set_term(current_term);
        }
        auto reader = model::make_memory_record_batch_reader(
          std::move(batches));

        return std::move(reader)
          .consume(l.make_appender(append_cfg), model::no_timeout)
          .discard_result();
    };

    ss::future<ss::circular_buffer<model::record_batch>> read(storage::log l) {
        int64_t dirty_offset = l.dirty_offset()() >= 0 ? l.dirty_offset()() : 0;
        int64_t begin_offset = random_generators::get_int(
          (int64_t)0, dirty_offset);
        int64_t end_offset = random_generators::get_int(
          (int64_t)begin_offset, dirty_offset);

        storage::log_reader_config cfg(
          model::offset(begin_offset),
          model::offset(end_offset),
          ss::default_priority_class());
        wlog.info(
          "[{}] Read [{},{}]", l.config().ntp, begin_offset, end_offset);
        return l.make_reader(cfg).then([](model::record_batch_reader reader) {
            return std::move(reader).consume(
              storage_test_fixture::batch_validating_consumer{},
              model::no_timeout);
        });
    }

    command_t flush = [](storage::log l) {
        wlog.info("[{}] Flush", l.config().ntp);

        return l.flush();
    };
    command_t read_cmd = [this](storage::log l) {
        return read(l).discard_result();
    };

    command_t truncate = [this](storage::log l) {
        return read(l).then(
          [l](ss::circular_buffer<model::record_batch> b) mutable {
              auto t_offset = b.empty() ? model::offset(0)
                                        : b.begin()->base_offset();
              wlog.info("[{}] Truncate [{}]", l.config().ntp, t_offset);
              return l.truncate(t_offset);
          });
    };
    command_t term_roll = [this](storage::log l) {
        ++current_term;
        wlog.info("[{}] Term roll [{}]", l.config().ntp, current_term);
        return ss::make_ready_future<>();
    };

    model::term_id current_term = model::term_id(0);
    commands_t workload;
    storage::log l;
};

FIXTURE_TEST(test_random_workload, storage_test_fixture) {
    // FIXME: change to disk storage type when we fix data corruption bug

    storage::log_manager mgr = make_log_manager(storage::log_config(
      storage::log_config::storage_type::memory,
      std::move(test_dir),
      200_MiB,
      storage::log_config::debug_sanitize_files::no,
      storage::log_config::with_cache::yes));

    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    // Test parameters
    size_t ntp_count = 1;
    size_t ops_per_ntp = 1000;

    std::vector<storage_workload> workloads;
    workloads.reserve(ntp_count);
    for (auto i : boost::irange((size_t)0, ntp_count)) {
        auto ntp = make_ntp("default", "test", i);

        auto log = mgr
                     .manage(storage::ntp_config(
                       ntp,
                       fmt::format("{}/{}", mgr.config().base_dir, ntp.path())))
                     .get0();
        workloads.emplace_back(log, ops_per_ntp);
    }
    // Execute NTP workloads in parallel
    ss::parallel_for_each(workloads, [](storage_workload& w) {
        return w.execute();
    }).get0();
}
