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

#include "compression/compression.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/types.h"
#include "test_utils/fixture.h"
#include "units.h"

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

#include <cstdint>
#include <optional>

using namespace std::chrono_literals; // NOLINT

inline ss::logger tlog{"test_log"};

struct random_batches_generator {
    ss::circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> base_ts = std::nullopt) {
        return model::test::make_random_batches(
          model::offset(0), random_generators::get_int(1, 10), true, base_ts);
    }
};

struct key_limited_random_batch_generator {
    static constexpr int cardinality = 10;

    ss::circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> ts = std::nullopt) {
        return model::test::make_random_batches(model::test::record_batch_spec{
          .allow_compression = true,
          .count = random_generators::get_int(1, 10),
          .max_key_cardinality = cardinality,
          .bt = model::record_batch_type::raft_data,
          .timestamp = ts});
    }
};

// Deterministic data generator that generates integer keys linearly with
// duplicates in each batch. Each batch contains a new integer key. With small
// enough batch sizes (not exceeding compaction budget), compaction should
// dedupe all the keys within a batch and the resulting log file should contain
// a single key per batch forming a sequence of integers. A handy validate()
// method is provided that validates the the batch state after compaction.
struct linear_int_kv_batch_generator {
    int _idx = 0;

    static constexpr int batches_per_call = 5;
    static constexpr int records_per_batch = 5;

    model::record_batch
    make_batch(model::test::record_batch_spec spec, int idx) {
        auto ts = spec.timestamp.value_or(
          model::timestamp(model::timestamp::now()() - (spec.count - 1)));
        auto max_ts = spec.timestamp.value_or(
          model::timestamp(ts.value() + spec.count - 1));
        auto header = model::record_batch_header{
          .size_bytes = 0, // computed later
          .base_offset = spec.offset,
          .type = spec.bt,
          .crc = 0, // we-reassign later
          .attrs = model::record_batch_attributes(
            random_generators::get_int<int16_t>(
              0, spec.allow_compression ? 4 : 0)),
          .last_offset_delta = spec.count - 1,
          .first_timestamp = ts,
          .max_timestamp = max_ts,
          .producer_id = spec.producer_id,
          .producer_epoch = spec.producer_epoch,
          .base_sequence = 0,
          .record_count = spec.count};

        if (spec.is_transactional) {
            header.attrs.set_transactional_type();
        }

        if (spec.enable_idempotence) {
            header.base_sequence = spec.base_sequence;
        }

        auto size = model::packed_record_batch_header_size;
        model::record_batch::records_type records;
        auto rs = model::record_batch::uncompressed_records();
        rs.reserve(spec.count);

        for (int i = 0; i < spec.count; i++) {
            rs.emplace_back(
              model::test::make_random_record(i, reflection::to_iobuf(idx)));
        }

        if (header.attrs.compression() != model::compression::none) {
            iobuf body;
            for (auto& r : rs) {
                model::append_record_to_buffer(body, r);
            }
            rs.clear();
            records = ::compression::compressor::compress(
              body, header.attrs.compression());
            size += std::get<iobuf>(records).size_bytes();
        } else {
            for (auto& r : rs) {
                size += r.size_bytes();
                size += vint::vint_size(r.size_bytes());
            }
            records = std::move(rs);
        }
        // TODO: expose term setting
        header.ctx = model::record_batch_header::context(
          model::term_id(0), ss::this_shard_id());
        header.size_bytes = size;
        auto batch = model::record_batch(header, std::move(records));
        batch.header().crc = model::crc_record_batch(batch);
        batch.header().header_crc = model::internal_header_only_crc(
          batch.header());
        return batch;
    }

    ss::circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> ts = std::nullopt) {
        ss::circular_buffer<model::record_batch> ret;
        ret.reserve(batches_per_call);
        auto batch_spec = model::test::record_batch_spec{
          .allow_compression = false,
          .count = records_per_batch,
          .bt = model::record_batch_type::raft_data,
          .timestamp = ts,
        };
        return operator()(batch_spec, batches_per_call);
    }

    ss::circular_buffer<model::record_batch>
    operator()(model::test::record_batch_spec spec, int num_batches) {
        ss::circular_buffer<model::record_batch> ret;
        ret.reserve(num_batches);
        for (int i = 0; i < num_batches; i++) {
            ret.push_back(make_batch(spec, _idx++));
        }
        return ret;
    }

    // Batches generated by this generator should all have 1 record
    // and the record should match the index of the batch if compaction
    // ran correctly.
    static void validate_post_compaction(
      ss::circular_buffer<model::record_batch>&& batches) {
        int idx = 0;
        for (const auto& batch : batches) {
            BOOST_REQUIRE_EQUAL(batch.record_count(), 1);
            batch.for_each_record([&idx](model::record rec) {
                BOOST_REQUIRE_EQUAL(
                  reflection::from_iobuf<int>(rec.release_key()), idx++);
            });
        }
    }
};

class storage_test_fixture {
public:
    ss::sstring test_dir;
    storage::kvstore kvstore;
    storage::storage_resources resources;
    ss::sharded<features::feature_table> feature_table;

    std::optional<model::timestamp> ts_cursor;

    storage_test_fixture()
      : test_dir("test.data." + random_generators::gen_alphanum_string(10))
      , kvstore(
          storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            test_dir,
            storage::debug_sanitize_files::yes),
          resources,
          feature_table) {
        configure_unit_test_logging();
        // avoid double metric registrations
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").set_value(true);
            config::shard_local_cfg()
              .get("log_segment_size_min")
              .set_value(std::optional<uint64_t>{});
        }).get0();
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
          storage::debug_sanitize_files::yes,
          ss::default_priority_class(),
          cache);
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
        requires requires(T generator, std::optional<model::timestamp> ts) {
            { generator(ts) } -> std::same_as<ss::circular_buffer<model::record_batch>>;
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

        for ([[maybe_unused]] auto append : boost::irange(0, appends)) {
            auto batches = batch_generator(ts_cursor);
            // Collect batches offsets
            for (auto& b : batches) {
                headers.push_back(b.header());
                b.set_term(term);
                total_records += b.record_count();
            }

            ts_cursor = model::timestamp{
              batches.back().header().max_timestamp() + 1};

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
