/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"
#include "units.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

using namespace std::chrono_literals; // NOLINT

inline ss::logger tlog{"test_log"};

struct random_batches_generator {
    ss::circular_buffer<model::record_batch> operator()() {
        return storage::test::make_random_batches(
          model::offset(0), random_generators::get_int(1, 10));
    }
};

class storage_test_fixture {
public:
    ss::sstring test_dir;
    storage::kvstore kvstore;

    storage_test_fixture()
      : test_dir("test.data." + random_generators::gen_alphanum_string(10))
      , kvstore(storage::kvstore_config(
          1_MiB,
          config::mock_binding(10ms),
          test_dir,
          storage::debug_sanitize_files::yes)) {
        configure_unit_test_logging();
        // avoid double metric registrations
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").set_value(true);
        }).get0();
        kvstore.start().get();
    }

    ~storage_test_fixture() { kvstore.stop().get(); }

    void configure_unit_test_logging() { std::cout.setf(std::ios::unitbuf); }

    /// Creates a log manager in test directory
    storage::log_manager make_log_manager(storage::log_config cfg) {
        return storage::log_manager(std::move(cfg), kvstore);
    }

    /// Creates a log manager in test directory with default config
    storage::log_manager make_log_manager() {
        return storage::log_manager(default_log_config(test_dir), kvstore);
    }

    /// \brief randomizes the configuration options
    storage::log_config default_log_config(ss::sstring test_dir) {
        auto i = random_generators::get_int(0, 100);
        auto stype = i > 50 ? storage::log_config::storage_type::disk
                            : storage::log_config::storage_type::memory;

        auto cache = i > 50 ? storage::with_cache::yes
                            : storage::with_cache::no;
        return storage::log_config(
          stype,
          std::move(test_dir),
          200_MiB,
          storage::debug_sanitize_files::yes,
          ss::default_priority_class(),
          cache);
    }

    void
    create_topic_dir(ss::sstring ns, ss::sstring topic, int32_t partition_id) {
        auto ntp = model::ntp(std::move(ns), std::move(topic), partition_id);
        ss::recursive_touch_directory(
          fmt::format("{}/{}", test_dir, ntp.path()))
          .wait();
    }

    struct batch_validating_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            BOOST_REQUIRE_EQUAL(b.header().crc, model::crc_record_batch(b));
            batches.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }

        ss::circular_buffer<model::record_batch> end_of_stream() {
            return std::move(batches);
        }

        ss::circular_buffer<model::record_batch> batches;
    };

    ss::circular_buffer<model::record_batch>
    read_and_validate_all_batches(storage::log log) {
        auto lstats = log.offsets();
        storage::log_reader_config cfg(
          lstats.start_offset,
          lstats.committed_offset,
          ss::default_priority_class());
        auto reader = log.make_reader(std::move(cfg)).get0();
        return reader.consume(batch_validating_consumer{}, model::no_timeout)
          .get0();
    }

    // clang-format off
    template<typename T = random_batches_generator>
        requires requires(T generator) {
            { generator() } -> std::same_as<ss::circular_buffer<model::record_batch>>;
        }
    // clang-format on
    std::vector<model::record_batch_header> append_random_batches(
      storage::log log,
      int appends,
      model::term_id term = model::term_id(0),
      T batch_generator = T{},
      storage::log_append_config::fsync sync
      = storage::log_append_config::fsync::no,
      bool flush_after_append = true) {
        auto lstats = log.offsets();
        storage::log_append_config append_cfg{
          sync, ss::default_priority_class(), model::no_timeout};

        model::offset base_offset = lstats.dirty_offset < model::offset(0)
                                      ? model::offset(0)
                                      : lstats.dirty_offset + model::offset(1);
        int64_t total_records = 0;
        std::vector<model::record_batch_header> headers;

        // do multiple append calls

        for (auto append : boost::irange(0, appends)) {
            auto batches = batch_generator();
            // Collect batches offsets
            for (auto& b : batches) {
                headers.push_back(b.header());
                b.set_term(term);
                total_records += b.record_count();
            }
            // make expected offset inclusive
            auto reader = model::make_memory_record_batch_reader(
              std::move(batches));
            auto res = std::move(reader)
                         .for_each_ref(
                           log.make_appender(append_cfg), append_cfg.timeout)
                         .get0();
            if (flush_after_append) {
                log.flush().get();
            }
            // Check if after append offset was updated correctly
            auto expected_offset = model::offset(total_records - 1)
                                   + base_offset;
            BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, res.last_offset);
            BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, expected_offset);
        }

        return headers;
    }

    void append_batch(storage::log log, model::record_batch batch) {
        model::record_batch_reader::data_t buffer;
        buffer.push_back(std::move(batch));
        storage::log_append_config append_cfg{
          storage::log_append_config::fsync::no,
          ss::default_priority_class(),
          model::no_timeout};

        model::offset old_dirty_offset = log.offsets().dirty_offset;
        model::offset base_offset = old_dirty_offset < model::offset(0)
                                      ? model::offset(0)
                                      : old_dirty_offset + model::offset(1);
        auto expected_offset
          = base_offset + model::offset{batch.header().last_offset_delta};

        auto res = model::make_memory_record_batch_reader(std::move(buffer))
                     .for_each_ref(
                       log.make_appender(append_cfg), model::no_timeout)
                     .get();

        log.flush().get();

        BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, res.last_offset);
        BOOST_REQUIRE_EQUAL(log.offsets().dirty_offset, expected_offset);
    }

    // model::offset start_offset;
    // size_t max_bytes;
    // size_t min_bytes;
    // io_priority_class prio;
    // std::vector<model::record_batch_type> type_filter;
    // model::offset max_offset = model::model_limits<model::offset>::max(); //
    // inclusive
    ss::circular_buffer<model::record_batch> read_range_to_vector(
      storage::log log, model::offset start, model::offset end) {
        storage::log_reader_config cfg(
          start, end, ss::default_priority_class());
        tlog.info("read_range_to_vector: {}", cfg);
        auto reader = log.make_reader(std::move(cfg)).get0();
        return std::move(reader)
          .consume(batch_validating_consumer(), model::no_timeout)
          .get0();
    }
};
