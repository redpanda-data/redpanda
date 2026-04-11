/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cluster/archival/async_data_uploader.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"
#include "storage/offset_to_filepos.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

#pragma once

namespace archival {

extern ss::logger adu_fixture_log;

static inline const auto test_namespace = model::kafka_namespace; // NOLINT
static inline const auto test_topic = model::topic("optic");      // NOLINT
static inline const auto test_partition = model::partition_id(0); // NOLINT
static inline const auto test_ntp = model::ntp(                   // NOLINT
  test_namespace,
  test_topic,
  test_partition);

struct random_records_generator {
    std::vector<tests::kv_t> operator()() {
        std::vector<tests::kv_t> batch;
        for (size_t i = 0; i < records_per_batch; i++) {
            auto key = get_next_key();
            vlog(adu_fixture_log.info, "Used key {}", key);
            auto record = random_generators::gen_alphanum_string(record_size);
            batch.emplace_back(std::move(key), std::move(record));
        }
        return batch;
    }
    size_t key_size{10};
    size_t record_size{1000};
    size_t records_per_batch{10};
    std::optional<int> key_space_size;

private:
    ss::sstring get_next_key() {
        if (key_space_size.has_value() && keys.empty()) {
            // Initialize keys list
            for (int i = 0; i < key_space_size.value(); i++) {
                auto key = random_generators::gen_alphanum_string(key_size);
                vlog(adu_fixture_log.info, "Generated key {}", key);
                keys.emplace_back(std::move(key));
            }
        }
        if (key_space_size.has_value()) {
            auto ix = random_generators::get_int(0, (int)keys.size() - 1);
            return keys.at(ix);
        }
        return random_generators::gen_alphanum_string(key_size);
    }
    std::vector<ss::sstring> keys;
};

inline void dump_to_disk(iobuf buf, ss::sstring fname) {
    auto file = ss::open_file_dma(
                  fname, ss::open_flags::create | ss::open_flags::wo)
                  .get();
    auto istr = make_iobuf_input_stream(std::move(buf));
    auto ostr = ss::make_file_output_stream(std::move(file)).get();
    ss::copy(istr, ostr).get();
    ostr.flush().get();
    ostr.close().get();
}

class async_data_uploader_fixture : public redpanda_thread_fixture {
public:
    async_data_uploader_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{}) {
        cfg.get("cloud_storage_disable_upload_loop_for_tests").set_value(true);
        wait_for_controller_leadership().get();
    }

    void create_topic(bool compacted = false) {
        cluster::topic_properties p;
        p.storage_mode = model::redpanda_storage_mode::tiered;
        if (compacted) {
            p.cleanup_policy_bitflags
              = model::cleanup_policy_bitflags::compaction;
            p.compaction_strategy = model::compaction_strategy::header;
        }
        add_topic(model::topic_namespace(test_namespace, test_topic), 1, p)
          .get();
        wait_for_leader(test_ntp).get();
    }

    template<class GenFunc>
    void produce_data(size_t num_batches, GenFunc generator) {
        auto partition = app.partition_manager.local().get(test_ntp);

        // Generate data
        auto client = make_kafka_client().get();
        tests::kafka_produce_transport producer(std::move(client));
        producer.start().get();
        auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

        for (size_t i = 0; i < num_batches; i++) {
            std::vector<tests::kv_t> records = generator();
            auto ts = _produce_timestamp;
            _produce_timestamp = model::timestamp(ts.value() + 1);
            producer
              .produce_to_partition(
                test_topic, model::partition_id(0), std::move(records), ts)
              .get();
        }
    }

    template<class GenFunc>
    void generate_partition(
      size_t num_batches, GenFunc generator, bool enable_compaction = false) {
        std::optional<cluster::topic_properties> props;
        if (enable_compaction) {
            cluster::topic_properties p;
            p.cleanup_policy_bitflags
              = model::cleanup_policy_bitflags::compaction;
            p.compaction_strategy = model::compaction_strategy::header;
            props = p;
        }
        add_topic(model::topic_namespace(test_namespace, test_topic), 1, props)
          .get();
        wait_for_leader(test_ntp).get();

        auto partition = app.partition_manager.local().get(test_ntp);

        // Generate data
        produce_data(num_batches, generator);
    }

    void roll_segment() {
        auto log = get_partition_log();
        log->flush().get();
        log->force_roll().get();
    }

    size_t get_on_disk_size() {
        auto log = get_partition_log();
        size_t s = 0;
        for (const auto& i : log->segments()) {
            s += i->size_bytes();
        }
        return s;
    }

    void compact_segments(model::offset max_collect_offset) {
        ss::abort_source as;
        storage::housekeeping_config cfg(
          model::timestamp::min(),
          std::nullopt,
          max_collect_offset,
          max_collect_offset,
          max_collect_offset,
          std::nullopt,
          std::nullopt,
          std::chrono::milliseconds{0},
          as);

        auto log = get_partition_log();
        log->housekeeping(cfg).get();
    }

    auto get_test_partition() {
        return app.partition_manager.local().get(test_ntp).get();
    }

    storage::disk_log_impl* get_partition_log() {
        auto slog = get_test_partition()->log();
        return dynamic_cast<storage::disk_log_impl*>(slog.get());
    }

    struct log_map {
        std::vector<model::offset> base_offsets;
        std::vector<model::offset> last_offsets;
        std::vector<model::timestamp> timestamps;
        std::vector<model::record_batch_type> batch_types;
        std::vector<size_t> batch_sizes;
    };
    /// Get start offset of every record batch in the partition
    log_map get_log_map() {
        log_map res;
        auto co = get_test_partition()->committed_offset();
        storage::local_log_reader_config cfg(model::offset(0), co);
        auto rdr = get_test_partition()->make_local_reader(cfg).get();

        class consumer {
        public:
            explicit consumer(log_map& m)
              : _map(m) {}
            ss::future<ss::stop_iteration>
            operator()(model::record_batch batch) {
                _map.base_offsets.push_back(batch.base_offset());
                _map.last_offsets.push_back(batch.header().last_offset());
                _map.timestamps.push_back(batch.header().first_timestamp);
                _map.batch_types.push_back(batch.header().type);
                _map.batch_sizes.push_back(batch.size_bytes());
                co_return ss::stop_iteration::no;
            }
            bool end_of_stream() const { return false; }

        private:
            log_map& _map;
        };
        rdr.consume(consumer(res), model::timeout_clock::now() + 1s).get();
        return res;
    }

    struct size_limited_upl_result {
        iobuf payload;
        inclusive_offset_range range;
        upload_reconciliation_result meta;
    };

    /// Load arbitrary offset range using segment_upload class
    void read_offset_range(
      size_limited_offset_range range,
      std::optional<size_limited_upl_result>& result,
      bool expect_compacted = false) {
        vlog(
          adu_fixture_log.info,
          "Query range: {}, max size {}, min size {}",
          range.base,
          range.min_size,
          range.max_size);

        auto partition = get_test_partition();
        const auto& offsets = partition->log()->offsets();

        vlog(
          adu_fixture_log.info,
          "Partition offset range: {}-{}",
          offsets.start_offset,
          offsets.committed_offset);

        for (const auto& s : get_partition_log()->segments()) {
            vlog(
              adu_fixture_log.info,
              "Segment {}, size {}, offsets {}",
              s.get()->filename(),
              s.get()->file_size(),
              s.get()->offsets());
        }

        iobuf actual;
        auto out_s = make_iobuf_ref_output_stream(actual);

        auto upl_res = segment_upload::make_segment_upload(
                         get_test_partition(),
                         range,
                         0x1000,
                         ss::default_scheduling_group(),
                         model::time_from_now(100ms))
                         .get();

        if (upl_res.has_failure()) {
            vlog(
              adu_fixture_log.info,
              "Failed to initialize segment_upload {}",
              upl_res.error().message());
            result = std::nullopt;
            return;
        }
        auto actual_offset_range = upl_res.value()->get_meta().offsets;

        ASSERT_NE(
          upl_res.value()->get_meta().base_timestamp,
          model::timestamp::missing());
        ASSERT_NE(
          upl_res.value()->get_meta().max_timestamp,
          model::timestamp::missing());

        ASSERT_EQ(
          upl_res.value()->get_meta().eligible_for_compacted_reupload,
          expect_compacted);

        auto meta_size_bytes = upl_res.value()->get_meta().size_bytes;
        // Check metadata first
        ASSERT_EQ(range.base, actual_offset_range.base);

        auto upl_size = upl_res.value()->get_size_bytes();
        auto meta = upl_res.value()->get_meta();
        auto inp_s = std::move(*upl_res.value()).detach_stream().get();
        auto closer = ss::defer([&] {
            out_s.close().get();
            inp_s.close().get();
        });

        ss::copy(inp_s, out_s).get();

        vlog(
          adu_fixture_log.info,
          "Actual size: {}, \n{}",
          actual.size_bytes(),
          actual.hexdump(1024));

        ASSERT_GT(actual.size_bytes(), 0);

        // Check size computation
        vlog(
          adu_fixture_log.info,
          "Computed size: {}, actual size: {}",
          upl_size,
          actual.size_bytes());
        ASSERT_EQ(actual.size_bytes(), upl_size);
        ASSERT_EQ(actual.size_bytes(), meta_size_bytes);

        result.emplace(
          size_limited_upl_result{
            .payload = std::move(actual),
            .range = actual_offset_range,
            .meta = std::move(meta),
          });
    }

    /// Load arbitrary offset range using segment_upload class
    void read_offset_range(
      inclusive_offset_range range,
      std::optional<iobuf>& result,
      bool expect_compacted = false) {
        auto partition = get_test_partition();
        const auto& offsets = partition->log()->offsets();
        vlog(
          adu_fixture_log.info,
          "Partition offset range: {}-{}",
          offsets.start_offset,
          offsets.committed_offset);
        for (const auto& s : get_partition_log()->segments()) {
            vlog(
              adu_fixture_log.info,
              "Segment {}, size {}, offsets {}",
              s.get()->filename(),
              s.get()->file_size(),
              s.get()->offsets());
        }
        storage::local_log_reader_config reader_cfg(range.base, range.last);
        reader_cfg.skip_batch_cache = true;

        auto reader = partition->make_local_reader(reader_cfg).get();

        iobuf actual;
        auto out_s = make_iobuf_ref_output_stream(actual);

        auto upl_res = segment_upload::make_segment_upload(
                         get_test_partition(),
                         range,
                         0x1000,
                         ss::default_scheduling_group(),
                         model::time_from_now(100ms))
                         .get();

        if (upl_res.has_failure()) {
            vlog(
              adu_fixture_log.info,
              "Failed to initialize segment_upload {}",
              upl_res.error().message());
            result = std::nullopt;
            return;
        }
        auto actual_offset_range = upl_res.value()->get_meta().offsets;

        // Check metadata first
        ASSERT_TRUE(range == actual_offset_range);

        ASSERT_NE(
          upl_res.value()->get_meta().base_timestamp,
          model::timestamp::missing());
        ASSERT_NE(
          upl_res.value()->get_meta().max_timestamp,
          model::timestamp::missing());

        ASSERT_EQ(
          upl_res.value()->get_meta().eligible_for_compacted_reupload,
          expect_compacted);

        auto meta_size_bytes = upl_res.value()->get_meta().size_bytes;

        auto upl_size = upl_res.value()->get_size_bytes();
        auto inp_s = std::move(*upl_res.value()).detach_stream().get();
        auto closer = ss::defer([&] {
            out_s.close().get();
            inp_s.close().get();
        });

        ss::copy(inp_s, out_s).get();

        vlog(
          adu_fixture_log.info,
          "Actual size: {}, \n{}",
          actual.size_bytes(),
          actual.hexdump(1024));

        ASSERT_TRUE(actual.size_bytes() > 0);

        // Check size computation
        vlog(
          adu_fixture_log.info,
          "Computed size: {}, actual size: {}",
          upl_size,
          actual.size_bytes());
        ASSERT_EQ(actual.size_bytes(), upl_size);
        ASSERT_EQ(actual.size_bytes(), meta_size_bytes);

        result = std::move(actual);
    }

    /// Load individual log segment as an iobuf
    auto load_log_segment(
      ss::lw_shared_ptr<storage::segment> s, inclusive_offset_range range) {
        // Copy the expected byte range from the segment file
        auto r_handle = s->offset_data_stream(range.base).get();
        iobuf buf;
        auto file_out_s = make_iobuf_ref_output_stream(buf);
        auto closer = ss::defer([&] {
            r_handle.close().get();
            file_out_s.close().get();
        });
        ss::copy(r_handle.stream(), file_out_s).get();

        vlog(
          adu_fixture_log.info,
          "Expected size: {}, \n{}",
          buf.size_bytes(),
          buf.hexdump(1024));
        return buf;
    }

    /// Load individual log segment as an iobuf
    auto load_log_segment_concat(inclusive_offset_range range) {
        std::vector<ss::lw_shared_ptr<storage::segment>> segments;
        for (auto& s : get_partition_log()->segments()) {
            auto o = s->offsets();
            if (o.get_base_offset() > o.get_committed_offset()) {
                // tail segment with no data. skip it.
                continue;
            }
            if (
              o.get_base_offset() >= range.base
              && o.get_committed_offset() <= range.last) {
                // s inside the range: r[...........]r
                //                         o[...]o
                segments.push_back(s);
            } else if (
              range.base >= o.get_base_offset()
              && range.last <= o.get_committed_offset()) {
                // range inside the segment: o[...........]o
                //                               r[...]r
                segments.push_back(s);
            } else if (
              range.base <= o.get_base_offset()
              && range.last >= o.get_base_offset()) {
                // begin overlaps: r[.........]r
                //                      o[........]o
                segments.push_back(s);
            } else if (
              o.get_base_offset() < range.base
              && o.get_committed_offset() >= range.base) {
                // begin overlaps: o[........]o
                //                      r[........]r
                segments.push_back(s);
            }
        }
        auto first_segm = segments.front();
        auto last_segm = segments.back();
        auto start = storage::convert_begin_offset_to_file_pos(
                       range.base, first_segm, model::timestamp{})
                       .get()
                       .assume_value()
                       .bytes;
        auto end = storage::convert_end_offset_to_file_pos(
                     range.last, last_segm, model::timestamp{})
                     .get()
                     .assume_value()
                     .bytes;

        auto cc_view = storage::concat_segment_reader_view(
          segments, start, end);

        // Copy the expected byte range from the segment file
        iobuf buf;
        auto file_out_s = make_iobuf_ref_output_stream(buf);
        auto cc_stream = std::move(cc_view).take_stream();
        ss::copy(cc_stream, file_out_s).get();
        auto closer = ss::defer([&] {
            cc_stream.close().get();
            file_out_s.close().get();
        });

        vlog(
          adu_fixture_log.info,
          "Expected size: {}, \n{}",
          buf.size_bytes(),
          buf.hexdump(1024));
        return buf;
    }

    model::timestamp _produce_timestamp{0};
    scoped_config cfg;
};

} // namespace archival
