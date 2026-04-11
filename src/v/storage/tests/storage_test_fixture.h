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

#include "base/seastarx.h"
#include "base/units.h"
#include "config/configuration.h"
#include "container/chunked_circular_buffer.h"
#include "features/feature_table.h"
#include "model/limits.h"
#include "raft/fundamental.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/tests/batch_generators.h"
#include "storage/types.h"
#include "test_utils/test_env.h"
#include "test_utils/test_macros.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

#include <boost/range/irange.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <optional>

using namespace std::chrono_literals; // NOLINT

inline ss::logger tlog{"test_log"};

class storage_test_fixture : public ::testing::Test {
public:
    ss::sstring test_dir;
    storage::kvstore kvstore;
    storage::storage_resources resources;
    ss::sharded<features::feature_table> feature_table;

    std::optional<model::timestamp> ts_cursor;

    storage_test_fixture()
      : test_dir(test_env::random_dir_path("test.data.", 10))
      , kvstore(
          storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            test_dir,
            storage::make_sanitized_file_config()),
          ss::this_shard_id(),
          resources,
          feature_table) {
        configure_unit_test_logging();
        // avoid double metric registrations - disk_log_builder and other
        // helpers also start a feature_table and other structs that register
        // metrics
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").set_value(true);
            config::shard_local_cfg()
              .get("disable_public_metrics")
              .set_value(true);
            config::shard_local_cfg()
              .get("log_segment_size_min")
              .set_value(std::optional<uint64_t>{});
        }).get();
        feature_table.start().get();
        feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();

        kvstore.start().get();
    }

    ~storage_test_fixture() {
        kvstore.stop().get();
        feature_table.stop().get();
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").reset();
            config::shard_local_cfg().get("disable_public_metrics").reset();
            config::shard_local_cfg().get("log_segment_size_min").reset();
        }).get();
    }

    /**
     * Only safe to call if you have generated some batches: this gives you
     * a timestamp ahead of the most recently appended batch
     */
    model::timestamp now() { return *ts_cursor; }

    void configure_unit_test_logging() { std::cout.setf(std::ios::unitbuf); }

    /// Creates a log manager in test directory
    storage::log_manager make_log_manager(storage::log_config cfg) {
        return storage::log_manager(
          std::move(cfg), kvstore, resources, feature_table);
    }

    /// Creates a log manager in test directory with default config
    storage::log_manager make_log_manager() {
        return storage::log_manager(
          default_log_config(test_dir), kvstore, resources, feature_table);
    }

    /// \brief randomizes the configuration options
    storage::log_config default_log_config(ss::sstring test_dir) {
        auto cache = storage::with_cache::yes;
        auto cfg = storage::log_config(
          std::move(test_dir),
          200_MiB,
          cache,
          storage::make_sanitized_file_config());
        return cfg;
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
            RPTEST_EXPECT_EQ(b.header().crc, model::crc_record_batch(b));
            batches.push_back(std::move(b));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }

        chunked_circular_buffer<model::record_batch> end_of_stream() {
            return std::move(batches);
        }

        chunked_circular_buffer<model::record_batch> batches;
    };

    chunked_circular_buffer<model::record_batch>
    read_and_validate_all_batches(ss::shared_ptr<storage::log> log) {
        return read_and_validate_all_batches(
          log, model::model_limits<model::offset>::max());
    }

    chunked_circular_buffer<model::record_batch> read_and_validate_all_batches(
      ss::shared_ptr<storage::log> log, model::offset max_offset) {
        auto lstats = log->offsets();
        storage::local_log_reader_config cfg(lstats.start_offset, max_offset);
        auto reader = log->make_reader(std::move(cfg)).get();
        return reader.consume(batch_validating_consumer{}, model::no_timeout)
          .get();
    }

    // clang-format off
    template<typename T = random_batches_generator>
        requires requires(T generator, std::optional<model::timestamp> ts) {
            { generator(ts) } -> std::same_as<chunked_circular_buffer<model::record_batch>>;
        }
    // clang-format on
    std::vector<model::record_batch_header> append_random_batches(
      ss::shared_ptr<storage::log> log,
      int appends,
      model::term_id term = model::term_id(0),
      std::optional<model::timestamp> ts_override = std::nullopt,
      T batch_generator = T{},
      storage::log_append_config::fsync sync
      = storage::log_append_config::fsync::no,
      bool flush_after_append = true) {
        auto lstats = log->offsets();
        storage::log_append_config append_cfg{sync, model::no_timeout};

        model::offset base_offset = lstats.dirty_offset < model::offset(0)
                                      ? model::offset(0)
                                      : lstats.dirty_offset + model::offset(1);
        int64_t total_records = 0;
        std::vector<model::record_batch_header> headers;

        // do multiple append calls

        for ([[maybe_unused]] auto append : boost::irange(0, appends)) {
            auto ts = ts_override.has_value() ? ts_override : ts_cursor;
            auto batches = batch_generator(ts);
            // Collect batches offsets
            for (auto& b : batches) {
                headers.push_back(b.header());
                b.set_term(term);
                total_records += b.record_count();
            }
            if (!ts_override.has_value()) {
                ts_cursor = model::timestamp{
                  batches.back().header().max_timestamp() + 1};
            }

            // make expected offset inclusive
            auto reader = model::make_memory_record_batch_reader(
              std::move(batches));
            auto res = std::move(reader)
                         .for_each_ref(
                           log->make_appender(append_cfg), append_cfg.timeout)
                         .get();
            if (flush_after_append) {
                log->flush().get();
            }
            // Check if after append offset was updated correctly
            auto expected_offset = model::offset(total_records - 1)
                                   + base_offset;
            RPTEST_EXPECT_EQ(log->offsets().dirty_offset, res.last_offset);
            RPTEST_EXPECT_EQ(log->offsets().dirty_offset, expected_offset);
        }

        return headers;
    }

    void
    append_batch(ss::shared_ptr<storage::log> log, model::record_batch batch) {
        model::record_batch_reader::data_t buffer;
        const auto last_offset_delta = model::offset(
          batch.header().last_offset_delta);
        buffer.push_back(std::move(batch));
        storage::log_append_config append_cfg{
          storage::log_append_config::fsync::no, model::no_timeout};

        model::offset old_dirty_offset = log->offsets().dirty_offset;
        model::offset base_offset = old_dirty_offset < model::offset(0)
                                      ? model::offset(0)
                                      : old_dirty_offset + model::offset(1);
        auto expected_offset = base_offset + last_offset_delta;

        auto res = model::make_memory_record_batch_reader(std::move(buffer))
                     .for_each_ref(
                       log->make_appender(append_cfg), model::no_timeout)
                     .get();

        log->flush().get();

        RPTEST_EXPECT_EQ(log->offsets().dirty_offset, res.last_offset);
        RPTEST_EXPECT_EQ(log->offsets().dirty_offset, expected_offset);
    }

    // model::offset start_offset;
    // size_t max_bytes;
    // size_t min_bytes;
    // std::vector<model::record_batch_type> type_filter;
    // model::offset max_offset = model::model_limits<model::offset>::max(); //
    // inclusive
    chunked_circular_buffer<model::record_batch> read_range_to_vector(
      ss::shared_ptr<storage::log> log,
      model::offset start,
      model::offset end) {
        storage::local_log_reader_config cfg(start, end);
        tlog.info("read_range_to_vector: {}", cfg);
        auto reader = log->make_reader(std::move(cfg)).get();
        return std::move(reader)
          .consume(batch_validating_consumer(), model::no_timeout)
          .get();
    }

    std::pair<ssize_t, ssize_t> expected_dirty_and_closed_segment_bytes(
      ss::shared_ptr<storage::log> log) const {
        ssize_t dirty{0};
        ssize_t closed{0};
        for (const auto& segment : log->segments()) {
            if (!segment->has_appender()) {
                if (!segment->has_clean_compact_timestamp()) {
                    dirty += segment->file_size();
                }
                closed += segment->file_size();
            }
        }
        return {dirty, closed};
    }

    // Assert that the book-kept dirty and closed bytes reflect the contents of
    // the log.
    void check_dirty_and_closed_segment_bytes(
      ss::shared_ptr<storage::log> log) const {
        auto expected = expected_dirty_and_closed_segment_bytes(log);
        tlog.trace(
          "Expect dirty bytes: {}, expect closed bytes: {}",
          expected.first,
          expected.second);
        RPTEST_EXPECT_EQ(log->dirty_segment_bytes(), expected.first);
        RPTEST_EXPECT_EQ(log->closed_segment_bytes(), expected.second);
    }

    // Utility class to enable/disable stm_hookset according to the log
    // lifetime. In production raft layer does it.
    //
    // Limitations;
    // 1) Other shared pointers to log (apart from the log manager) cannot
    // outlive the log holder. Log manager must still be managing the log when
    // the holder is destructed.
    // 2) Log holder cannot be copied, only moved.
    class log_holder final {
    public:
        log_holder() = default;
        explicit log_holder(ss::shared_ptr<storage::log>&& log_ptr)
          : _log_ptr(std::move(log_ptr)) {
            _log_ptr->stm_hookset()->start();
        }

        log_holder(const log_holder&) = delete;
        log_holder& operator=(const log_holder&) = delete;

        log_holder(log_holder&& o) = default;

        log_holder& operator=(log_holder&& o) noexcept {
            if (this != &o) {
                this->~log_holder();
                new (this) log_holder(std::move(o));
            }
            return *this;
        }

        ~log_holder() {
            if (!_log_ptr) {
                // empty holder, nothing to stop
                return;
            }
            // only us and log_manager, no escaped copies
            vassert(
              _log_ptr.use_count() == 2,
              "there are {} shared_ptrs to log, expected 2",
              _log_ptr.use_count());
            _log_ptr->stm_hookset()->stop();
        }

        operator const ss::shared_ptr<storage::log>&() const {
            return _log_ptr;
        }
        storage::log& operator*() const { return *_log_ptr; }
        storage::log* get() const { return _log_ptr.get(); }
        storage::log* operator->() const { return _log_ptr.get(); }

    private:
        ss::shared_ptr<storage::log> _log_ptr;
    };

    // Manage log and de-/activate stm_hookset for the log lifetime
    log_holder manage_log(storage::log_manager& mgr, storage::ntp_config cfg) {
        return log_holder(mgr.manage(std::move(cfg)).get());
    }

    log_holder manage_log(
      storage::log_manager& mgr,
      storage::ntp_config cfg,
      raft::group_id group_id,
      std::vector<model::record_batch_type> translator_batch_types) {
        return log_holder(
          mgr
            .manage(std::move(cfg), group_id, std::move(translator_batch_types))
            .get());
    }
};
