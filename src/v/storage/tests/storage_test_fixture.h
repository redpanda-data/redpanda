#pragma once

#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "random/generators.h"
#include "storage/crc_record.h"
#include "storage/log_manager.h"
#include "storage/tests/random_batch.h"
#include "test_utils/fixture.h"

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

constexpr size_t kb = 1024;
constexpr size_t mb = 1024 * kb;
constexpr size_t gb = 1024 * mb;

constexpr size_t operator""_kb(unsigned long long val) { return val * kb; }
constexpr size_t operator""_mb(unsigned long long val) { return val * mb; }
constexpr size_t operator""_gb(unsigned long long val) { return val * gb; }

static ss::logger tlog{"test_log"};

inline void validate_batch_crc(model::record_batch& batch) {
    crc32 crc;
    storage::crc_batch_header(
      crc, batch.get_header_for_testing(), batch.size());
    if (batch.compressed()) {
        crc.extend(batch.get_compressed_records().records());
    } else {
        for (model::record& r : batch.get_uncompressed_records_for_testing()) {
            storage::crc_record_header_and_key(crc, r);
            crc.extend(r.packed_value_and_headers());
        }
    }

    BOOST_REQUIRE_EQUAL(batch.crc(), crc.value());
}

struct random_batches_generator {
    std::vector<model::record_batch> operator()() {
        return storage::test::make_random_batches(
          model::offset(0), random_generators::get_int(1, 10));
    }
};

class storage_test_fixture {
public:
    ss::sstring test_dir = "test_data_"
                           + random_generators::gen_alphanum_string(5);

    storage_test_fixture() { configure_unit_test_logging(); }

    void configure_unit_test_logging() {
        ss::global_logger_registry().set_all_loggers_level(
          ss::log_level::trace);
        ss::global_logger_registry().set_logger_level(
          "exception", ss::log_level::debug);

        ss::apply_logging_settings(ss::logging_settings{
          .logger_levels = {{"exception", ss::log_level::debug}},
          .default_level = ss::log_level::trace,
          .stdout_timestamp_style = ss::logger_timestamp_style::real});
    }

    /// Creates a log manager in test directory
    storage::log_manager make_log_manager(storage::log_config cfg) {
        return storage::log_manager(std::move(cfg));
    }

    /// Creates a log manager in test directory with default config
    storage::log_manager make_log_manager() {
        return storage::log_manager(default_log_config(test_dir));
    }

    storage::log_config default_log_config(ss::sstring test_dir) {
        return storage::log_config{
          test_dir, 200_mb, storage::log_config::sanitize_files::yes};
    }

    model::ntp
    make_ntp(ss::sstring ns, ss::sstring topic, size_t partition_id) {
        return model::ntp{
          .ns = model::ns(std::move(ns)),
          .tp = {.topic = model::topic(std::move(topic)),
                 .partition = model::partition_id(partition_id)}};
    }

    void
    create_topic_dir(ss::sstring ns, ss::sstring topic, size_t partition_id) {
        auto ntp = make_ntp(ns, topic, partition_id);
        ss::recursive_touch_directory(
          fmt::format("{}/{}", test_dir, ntp.path()))
          .wait();
    }

    struct batch_validating_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            tlog.debug(
              "Validating batch [{},{}] of size {} bytes and {} records, "
              "compressed {}, CRC: [{}] ",
              b.base_offset(),
              b.last_offset(),
              b.size_bytes(),
              b.size(),
              b.compressed(),
              b.crc());

            validate_batch_crc(b);
            tlog.debug("Finished validating crc");
            batches.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }

        std::vector<model::record_batch> end_of_stream() {
            return std::move(batches);
        }

        std::vector<model::record_batch> batches;
    };

    std::vector<model::record_batch>
    read_and_validate_all_batches(storage::log log) {
        storage::log_reader_config cfg{
          .start_offset = model::offset(0),
          .max_bytes = std::numeric_limits<size_t>::max(),
          .min_bytes = 0,
          .prio = ss::default_priority_class(),
          .type_filter = {}};

        auto reader = log.make_reader(std::move(cfg));
        return reader.consume(batch_validating_consumer{}, model::no_timeout)
          .get0();
    }

    // clang-format off
    template<typename T = random_batches_generator>
    CONCEPT(
        requires requires (T generator) {
            { generator() } -> std::vector<model::record_batch>;
        }
    )
    // clang-format on
    std::vector<model::record_batch_header> append_random_batches(
      storage::log log,
      int appends,
      model::term_id term = model::term_id(0),
      T batch_generator = T{},
      storage::log_append_config::fsync sync
      = storage::log_append_config::fsync::no) {
        storage::log_append_config append_cfg{
          sync, ss::default_priority_class(), model::no_timeout, term};

        model::offset base_offset = log.max_offset() < model::offset(0)
                                      ? model::offset(0)
                                      : log.max_offset() + model::offset(1);
        int64_t total_records = 0;
        std::vector<model::record_batch_header> headers;

        // do multiple append calls

        for (auto append : boost::irange(0, appends)) {
            auto batches = batch_generator();
            // Collect batches offsets
            for (auto& b : batches) {
                headers.push_back(b.get_header_for_testing());
                total_records += b.size();
            }
            // make expected offset inclusive
            auto reader = model::make_memory_record_batch_reader(
              std::move(batches));
            auto res = std::move(reader)
                         .consume(
                           log.make_appender(append_cfg), append_cfg.timeout)
                         .get0();

            // Check if after append offset was updated correctly
            auto expected_offset = model::offset(total_records - 1)
                                   + base_offset;
            BOOST_REQUIRE_EQUAL(log.max_offset(), res.last_offset);
            BOOST_REQUIRE_EQUAL(log.max_offset(), expected_offset);
        }

        return headers;
    }

    // model::offset start_offset;
    // size_t max_bytes;
    // size_t min_bytes;
    // io_priority_class prio;
    // std::vector<model::record_batch_type> type_filter;
    // model::offset max_offset = model::model_limits<model::offset>::max(); //
    // inclusive
    std::vector<model::record_batch> read_range_to_vector(
      storage::log log, model::offset start, model::offset end) {
        storage::log_reader_config cfg{
          .start_offset = start,
          .max_bytes = std::numeric_limits<size_t>::max(),
          .min_bytes = 0,
          .prio = ss::default_priority_class(),
          .type_filter = {},
          .max_offset = end};
        auto reader = log.make_reader(std::move(cfg));
        return reader.consume(batch_validating_consumer(), model::no_timeout)
          .get0();
    }
};
