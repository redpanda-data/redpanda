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
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"
#include "storage/log_reader.h"
#include "storage/ntp_config.h"
#include "storage/offset_to_filepos.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("async-uploader-test");

static const auto test_namespace = model::kafka_namespace; // NOLINT
static const auto test_topic = model::topic("optic");      // NOLINT
static const auto test_partition = model::partition_id(0); // NOLINT
static const auto test_ntp = model::ntp(                   // NOLINT
  test_namespace,
  test_topic,
  test_partition);

SEASTAR_THREAD_TEST_CASE(test_inclusive_offset_range1) {
    inclusive_offset_range range(model::offset(3), model::offset(3));
    std::vector<model::offset> res;
    for (const auto o : range) {
        res.push_back(o);
    }
    BOOST_REQUIRE(res.size() == 1);
    BOOST_REQUIRE(res.at(0) == model::offset(3));
}

SEASTAR_THREAD_TEST_CASE(test_inclusive_offset_range2) {
    inclusive_offset_range range(model::offset(3), model::offset(7));
    std::vector<model::offset> res;
    for (const auto o : range) {
        res.push_back(o);
    }
    BOOST_REQUIRE(res.size() == 5);
    std::vector<model::offset> expected(
      {model::offset(3),
       model::offset(4),
       model::offset(5),
       model::offset(6),
       model::offset(7)});
    BOOST_REQUIRE(res == expected);
}

class async_data_uploader_fixture : public redpanda_thread_fixture {
public:
    async_data_uploader_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{}) {
        wait_for_controller_leadership().get();
    }

    void create_topic(bool compacted = false) {
        std::optional<cluster::topic_properties> props;
        if (compacted) {
            cluster::topic_properties p;
            p.cleanup_policy_bitflags
              = model::cleanup_policy_bitflags::compaction;
            p.compaction_strategy = model::compaction_strategy::header;
            props = p;
        }
        add_topic(model::topic_namespace(test_namespace, test_topic), 1, props)
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
        log->force_roll(ss::default_priority_class()).get();
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
          std::nullopt,
          ss::default_priority_class(),
          as);

        auto log = get_partition_log();
        log->housekeeping(cfg).get();
    }

    auto get_test_partition() {
        return app.partition_manager.local().get(test_ntp);
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
        storage::log_reader_config cfg(
          model::offset(0), co, ss::default_priority_class());
        auto rdr = get_test_partition()->make_reader(cfg).get();

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
    std::optional<size_limited_upl_result>
    read_offset_range(size_limited_offset_range range) {
        vlog(
          test_log.info,
          "Query range: {}, max size {}, min size {}",
          range.base,
          range.min_size,
          range.max_size);

        auto partition = get_test_partition();
        const auto& offsets = partition->log()->offsets();

        vlog(
          test_log.info,
          "Partition offset range: {}-{}",
          offsets.start_offset,
          offsets.committed_offset);

        for (const auto& s : get_partition_log()->segments()) {
            vlog(
              test_log.info,
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
              test_log.info,
              "Failed to initialize segment_upload {}",
              upl_res.error().message());
            return std::nullopt;
        }
        auto actual_offset_range = upl_res.value()->get_meta().offsets;
        auto meta_size_bytes = upl_res.value()->get_meta().size_bytes;

        // Check metadata first
        BOOST_REQUIRE_EQUAL(range.base, actual_offset_range.base);

        auto upl_size = upl_res.value()->get_size_bytes();
        auto meta = upl_res.value()->get_meta();
        auto inp_s = std::move(*upl_res.value()).detach_stream().get();
        auto closer = ss::defer([&] {
            out_s.close().get();
            inp_s.close().get();
        });

        ss::copy(inp_s, out_s).get();

        vlog(
          test_log.info,
          "Actual size: {}, \n{}",
          actual.size_bytes(),
          actual.hexdump(1024));

        BOOST_REQUIRE_GT(actual.size_bytes(), 0);

        // Check size computation
        vlog(
          test_log.info,
          "Computed size: {}, actual size: {}",
          upl_size,
          actual.size_bytes());
        BOOST_REQUIRE_EQUAL(actual.size_bytes(), upl_size);
        BOOST_REQUIRE_EQUAL(actual.size_bytes(), meta_size_bytes);

        return size_limited_upl_result{
          .payload = std::move(actual),
          .range = actual_offset_range,
          .meta = std::move(meta),
        };
    }

    /// Load arbitrary offset range using segment_upload class
    std::optional<iobuf> read_offset_range(inclusive_offset_range range) {
        auto partition = get_test_partition();
        const auto& offsets = partition->log()->offsets();
        vlog(
          test_log.info,
          "Partition offset range: {}-{}",
          offsets.start_offset,
          offsets.committed_offset);
        for (const auto& s : get_partition_log()->segments()) {
            vlog(
              test_log.info,
              "Segment {}, size {}, offsets {}",
              s.get()->filename(),
              s.get()->file_size(),
              s.get()->offsets());
        }
        storage::log_reader_config reader_cfg(
          range.base, range.last, ss::default_priority_class());
        reader_cfg.skip_batch_cache = true;

        auto reader = partition->make_reader(reader_cfg).get();

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
              test_log.info,
              "Failed to initialize segment_upload {}",
              upl_res.error().message());
            return std::nullopt;
        }
        auto actual_offset_range = upl_res.value()->get_meta().offsets;
        auto meta_size_bytes = upl_res.value()->get_meta().size_bytes;

        // Check metadata first
        BOOST_REQUIRE(range == actual_offset_range);

        auto upl_size = upl_res.value()->get_size_bytes();
        auto inp_s = std::move(*upl_res.value()).detach_stream().get();
        auto closer = ss::defer([&] {
            out_s.close().get();
            inp_s.close().get();
        });

        ss::copy(inp_s, out_s).get();

        vlog(
          test_log.info,
          "Actual size: {}, \n{}",
          actual.size_bytes(),
          actual.hexdump(1024));

        BOOST_REQUIRE(actual.size_bytes() > 0);

        // Check size computation
        vlog(
          test_log.info,
          "Computed size: {}, actual size: {}",
          upl_size,
          actual.size_bytes());
        BOOST_REQUIRE_EQUAL(actual.size_bytes(), upl_size);
        BOOST_REQUIRE_EQUAL(actual.size_bytes(), meta_size_bytes);

        return actual;
    }

    /// Load individual log segment as an iobuf
    auto load_log_segment(
      ss::lw_shared_ptr<storage::segment> s, inclusive_offset_range range) {
        // Copy the expected byte range from the segment file
        auto r_handle = s->offset_data_stream(
                           range.base, ss::default_priority_class())
                          .get();
        iobuf buf;
        auto file_out_s = make_iobuf_ref_output_stream(buf);
        auto closer = ss::defer([&] {
            r_handle.close().get();
            file_out_s.close().get();
        });
        ss::copy(r_handle.stream(), file_out_s).get();

        vlog(
          test_log.info,
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
                       range.base,
                       first_segm,
                       model::timestamp{},
                       ss::default_priority_class())
                       .get()
                       .assume_value()
                       .bytes;
        auto end = storage::convert_end_offset_to_file_pos(
                     range.last,
                     last_segm,
                     model::timestamp{},
                     ss::default_priority_class())
                     .get()
                     .assume_value()
                     .bytes;

        auto cc_view = storage::concat_segment_reader_view(
          segments, start, end, ss::default_priority_class());

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
          test_log.info,
          "Expected size: {}, \n{}",
          buf.size_bytes(),
          buf.hexdump(1024));
        return buf;
    }

    model::timestamp _produce_timestamp{0};
};

inline upload_reconciliation_result
consume_segment_and_compute_metadata(const iobuf& s) {
    vlog(
      test_log.debug,
      "consume_segment_and_compute_metadata {} bytes",
      s.size_bytes());
    upload_reconciliation_result meta = {};
    meta.size_bytes = 0;
    iobuf out;
    auto dst = make_iobuf_ref_output_stream(out);
    auto src = make_iobuf_input_stream(s.copy());
    auto pred = [&meta](model::record_batch_header& hdr) {
        if (meta.offsets.base == model::offset()) {
            meta.offsets.base = hdr.base_offset;
        }
        meta.offsets.last = hdr.last_offset();
        meta.size_bytes += hdr.size_bytes;
        vlog(
          test_log.debug,
          "{} bytes consumed, O: {}-{}, T: {}-{}",
          meta.size_bytes,
          hdr.base_offset,
          hdr.last_offset(),
          hdr.first_timestamp,
          hdr.max_timestamp);
        return storage::batch_consumer::consume_result::accept_batch;
    };
    ss::abort_source as;
    auto len = storage::transform_stream(
                 std::move(src), std::move(dst), pred, as)
                 .get();
    BOOST_REQUIRE(len.has_value());
    BOOST_REQUIRE_EQUAL(len.value(), meta.size_bytes);
    return meta;
}

struct random_records_generator {
    std::vector<tests::kv_t> operator()() {
        std::vector<tests::kv_t> batch;
        for (size_t i = 0; i < records_per_batch; i++) {
            auto key = get_next_key();
            vlog(test_log.info, "Used key {}", key);
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
                vlog(test_log.info, "Generated key {}", key);
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

void dump_to_disk(iobuf buf, ss::sstring fname) {
    auto file = ss::open_file_dma(
                  fname, ss::open_flags::create | ss::open_flags::wo)
                  .get();
    auto istr = make_iobuf_input_stream(std::move(buf));
    auto ostr = ss::make_file_output_stream(std::move(file)).get();
    ss::copy(istr, ostr).get();
    ostr.flush().get();
    ostr.close().get();
}

FIXTURE_TEST(
  test_async_segment_upload_full_non_compacted, async_data_uploader_fixture) {
#ifdef NDEBUG
    random_records_generator generator;
    generate_partition(1000, generator);

    auto partition = get_test_partition();

    BOOST_REQUIRE(get_partition_log()->segments().size() == 1);

    // Produce upload based on reverse-consuming from the partition
    // and compare it to the log segment
    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    auto range = inclusive_offset_range(
      offsets.start_offset, offsets.committed_offset);
    auto actual = read_offset_range(range);
    auto expected = load_log_segment(
      get_partition_log()->segments().front(), range);

    BOOST_REQUIRE(actual.value() == expected);
#endif
}

FIXTURE_TEST(
  test_async_segment_upload_full_compacted, async_data_uploader_fixture) {
#ifdef NDEBUG
    random_records_generator generator;
    generator.key_space_size = 10;
    generate_partition(1000, generator, true);

    auto partition = get_test_partition();
    // Capture the offsets before the log is rolled or anything
    // is compacted.
    const auto& offsets = partition->log()->offsets();
    auto range = inclusive_offset_range(
      offsets.start_offset, offsets.committed_offset);

    vlog(
      test_log.info,
      "Offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    // Calculate original segment size to make sure that it's compacted.
    auto non_compacted_size = load_log_segment(
                                get_partition_log()->segments().front(), range)
                                .size_bytes();

    // Force roll and compaction
    roll_segment();
    compact_segments(range.last);

    auto actual = read_offset_range(range);
    auto expected = load_log_segment(
      get_partition_log()->segments().front(), range);

    vlog(
      test_log.info,
      "Non-compacted size: {}, compacted size: {}, upload size: {}",
      non_compacted_size,
      expected.size_bytes(),
      actual->size_bytes());

    BOOST_REQUIRE(actual.value() == expected);
#endif
}

FIXTURE_TEST(
  test_async_segment_upload_partial_non_compacted,
  async_data_uploader_fixture) {
#ifdef NDEBUG
    random_records_generator generator;
    generate_partition(1000, generator);
    auto log_map = get_log_map();
    auto partition = get_test_partition();

    BOOST_REQUIRE(get_partition_log()->segments().size() == 1);

    // Produce upload based on reverse-consuming from the partition
    // and compare it to the log segment
    const auto& offsets = partition->log()->offsets();

    auto f_it = log_map.base_offsets.begin();
    f_it++;
    f_it++;
    auto b_it = log_map.base_offsets.rbegin();
    b_it++;
    b_it++;

    auto range = inclusive_offset_range(*f_it, *b_it);

    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    vlog(test_log.info, "Upload offset range: {}-{}", range.base, range.last);

    auto actual = read_offset_range(range);
    auto expected = load_log_segment_concat(range);

    BOOST_REQUIRE(actual.value() == expected);
#endif
}

struct fuzz_test_case {
    model::offset base;
    model::offset last;

    size_t expected_size{0};
};

template<class Fn>
inline void get_random_test_cases_impl(
  std::vector<fuzz_test_case>& cases,
  async_data_uploader_fixture::log_map& map,
  int num_results,
  Fn pick_offsets) {
    // Generate a bunch of random test cases
    std::vector<size_t> ix;
    for (size_t i = 0; i < map.base_offsets.size(); i++) {
        ix.push_back(i);
        vlog(
          test_log.debug,
          "log_map {} - {} - {}",
          map.base_offsets[i],
          map.timestamps[i],
          map.batch_types[i]);
    }

    for (int i = 0; i < num_results; i++) {
        auto [ix_first, ix_second] = pick_offsets(ix.size());
        // compute size of the offset range
        size_t range_size = 0;
        for (size_t ix = ix_first; ix <= ix_second; ix++) {
            range_size += map.batch_sizes.at(ix);
        }
        fuzz_test_case tc{
          .base = map.base_offsets.at(ix_first),
          .last = map.last_offsets.at(ix_second),
          .expected_size = range_size,
        };
        vlog(
          test_log.debug,
          "Test case #{}/{}: offsets: {}-{}",
          i,
          num_results,
          tc.base,
          tc.last);
        cases.push_back(tc);
    }
}

/// Generate randomized test cases with arbitrary alignment
inline void get_random_test_cases(
  std::vector<fuzz_test_case>& out,
  async_data_uploader_fixture::log_map& map,
  int num_results) {
    auto fn = [&](size_t ix_size) {
        auto ix_first = random_generators::get_int((size_t)0, ix_size - 1);
        auto ix_second = random_generators::get_int(ix_first, ix_size - 1);
        return std::make_pair(ix_first, ix_second);
    };
    get_random_test_cases_impl(out, map, num_results, fn);
}

/// Generate randomized test cases that contains only single batches
inline void get_random_single_batch_test_cases(
  std::vector<fuzz_test_case>& out,
  async_data_uploader_fixture::log_map& map,
  int num_results) {
    auto fn = [&](size_t ix_size) {
        auto ix_first = random_generators::get_int((size_t)0, ix_size - 1);
        return std::make_pair(ix_first, ix_first);
    };
    get_random_test_cases_impl(out, map, num_results, fn);
}

inline void get_random_test_cases_that_start_on_config_batches(
  std::vector<fuzz_test_case>& out,
  async_data_uploader_fixture::log_map& map,
  int num_results) {
    std::vector<size_t> ix_config;
    for (size_t i = 0; i < map.base_offsets.size(); i++) {
        if (map.batch_types[i] != model::record_batch_type::raft_data) {
            ix_config.push_back(i);
        }
    }
    // Produce a test case that starts at config batch
    auto start_fn = [&](size_t ix_size) {
        auto i = random_generators::get_int((size_t)0, ix_config.size() - 1);
        auto ix_first = ix_config[i];
        auto ix_second = random_generators::get_int(ix_first, ix_size - 1);
        return std::make_pair(ix_first, ix_second);
    };
    // Produce a test case that end at config batch
    auto stop_fn = [&](size_t) {
        auto i = random_generators::get_int((size_t)0, ix_config.size() - 1);
        auto ix_second = ix_config[i];
        auto ix_first = random_generators::get_int((size_t)0, ix_second);
        return std::make_pair(ix_first, ix_second);
    };
    // Produce a test case that starts and stops at config batches
    auto full_fn = [&](size_t) {
        auto i = random_generators::get_int((size_t)0, ix_config.size() - 1);
        auto j = random_generators::get_int(i, ix_config.size() - 1);
        auto ix_first = ix_config[i];
        auto ix_second = ix_config[j];
        return std::make_pair(ix_first, ix_second);
    };
    get_random_test_cases_impl(out, map, num_results / 3, start_fn);
    get_random_test_cases_impl(out, map, num_results / 3, stop_fn);
    get_random_test_cases_impl(out, map, num_results / 3, full_fn);
}

FIXTURE_TEST(
  test_async_segment_upload_random_not_compacted, async_data_uploader_fixture) {
#ifdef NDEBUG
    vlog(
      test_log.info,
      "Seed used for the test: {}",
      random_generators::internal::seed);
    create_topic();
    for (int i = 0; i < 10; i++) {
        random_records_generator generator;
        produce_data(100, generator);
        ss::abort_source as;
        get_test_partition()
          ->archival_meta_stm()
          ->cleanup_archive(
            model::offset(0), 0, ss::lowres_clock::now() + 1s, as)
          .get();
        roll_segment();
    }
    auto log_map = get_log_map();
    auto partition = get_test_partition();

    std::vector<fuzz_test_case> cases;
    get_random_test_cases(cases, log_map, 100);
    get_random_single_batch_test_cases(cases, log_map, 100);
    get_random_test_cases_that_start_on_config_batches(cases, log_map, 100);

    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    for (auto tc : cases) {
        vlog(test_log.info, "Test case, offsets: {}-{}", tc.base, tc.last);

        auto range = inclusive_offset_range(tc.base, tc.last);
        auto actual = read_offset_range(range);
        auto expected = load_log_segment_concat(range);

        BOOST_REQUIRE(actual.has_value());
        BOOST_REQUIRE_EQUAL(actual.value(), expected);
        BOOST_REQUIRE_EQUAL(actual.value().size_bytes(), tc.expected_size);
    }
#endif
}

FIXTURE_TEST(
  test_async_segment_upload_random_compacted, async_data_uploader_fixture) {
#ifdef NDEBUG
    vlog(
      test_log.info,
      "Seed used for the test: {}",
      random_generators::internal::seed);
    create_topic(true);
    for (int i = 0; i < 10; i++) {
        random_records_generator generator;
        generator.key_space_size = 5;
        produce_data(100, generator);
        ss::abort_source as;
        get_test_partition()
          ->archival_meta_stm()
          ->cleanup_archive(
            model::offset(0), 0, ss::lowres_clock::now() + 1s, as)
          .get();
        roll_segment();
    }

    size_t pre_compaction_size = get_on_disk_size();

    vlog(
      test_log.info,
      "High watermark: {}",
      get_test_partition()->high_watermark());

    roll_segment();
    compact_segments(get_test_partition()->log()->offsets().committed_offset);

    size_t post_compaction_size = get_on_disk_size();

    vlog(
      test_log.info,
      "Non-compacted size: {}, compacted size: {}",
      pre_compaction_size,
      post_compaction_size);

    auto log_map = get_log_map();
    auto partition = get_test_partition();

    std::vector<fuzz_test_case> cases;
    get_random_test_cases(cases, log_map, 100);
    get_random_single_batch_test_cases(cases, log_map, 100);
    get_random_test_cases_that_start_on_config_batches(cases, log_map, 100);

    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    for (auto tc : cases) {
        vlog(test_log.info, "Test case, offsets: {}-{}", tc.base, tc.last);

        auto range = inclusive_offset_range(tc.base, tc.last);
        auto actual = read_offset_range(range);
        auto expected = load_log_segment_concat(range);

        BOOST_REQUIRE(actual.has_value());
        BOOST_REQUIRE(actual.value() == expected);
        BOOST_REQUIRE(actual.value().size_bytes() == tc.expected_size);
    }
#endif
}

FIXTURE_TEST(
  test_async_segment_upload_random_size_limited_not_compacted,
  async_data_uploader_fixture) {
#ifdef NDEBUG
    vlog(
      test_log.info,
      "Seed used for the test: {}",
      random_generators::internal::seed);
    create_topic();
    for (int i = 0; i < 10; i++) {
        random_records_generator generator;
        produce_data(100, generator);
        ss::abort_source as;
        get_test_partition()
          ->archival_meta_stm()
          ->cleanup_archive(
            model::offset(0), 0, ss::lowres_clock::now() + 1s, as)
          .get();
        roll_segment();
    }
    auto log_map = get_log_map();
    auto partition = get_test_partition();

    std::vector<fuzz_test_case> cases;
    get_random_test_cases(cases, log_map, 500);
    get_random_single_batch_test_cases(cases, log_map, 500);
    get_random_test_cases_that_start_on_config_batches(cases, log_map, 500);

    const auto& offsets = partition->log()->offsets();
    vlog(
      test_log.info,
      "Partition offset range: {}-{}",
      offsets.start_offset,
      offsets.committed_offset);

    size_t progress = 0;
    for (auto tc : cases) {
        vlog(test_log.info, "Test case, offsets: {}-{}", tc.base, tc.last);

        auto range = size_limited_offset_range(
          tc.base, tc.expected_size, tc.expected_size);
        auto actual = read_offset_range(range);

        if (!actual.has_value()) {
            vlog(
              test_log.info,
              "Test case ignored, offsets: {}-{}",
              tc.base,
              tc.last);
            continue;
        }

        auto i_range = actual->range;
        auto expected = load_log_segment_concat(i_range);

        BOOST_REQUIRE(actual.has_value());
        BOOST_REQUIRE_EQUAL(actual.value().payload, expected);
        BOOST_REQUIRE_EQUAL(
          actual.value().payload.size_bytes(), expected.size_bytes());
        progress++;
    }

    BOOST_REQUIRE(progress > 0);
#endif
}
