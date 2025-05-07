/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cluster/archival/adjacent_segment_merger.h"
#include "cluster/archival/archival_policy.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/tests/async_data_uploader_fixture.h"
#include "kafka/data/record_batcher.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/offset_interval.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/log_manager.h"
#include "storage/record_batch_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/archival.h"
#include "test_utils/scoped_config.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace archival;

inline ss::logger test_log("segment-reupload-fixture");

namespace {

static constexpr size_t max_upload_size{4096_KiB};
static constexpr ss::lowres_clock::duration segment_lock_timeout{60s};

const auto manifest_namespace = model::ns("test-ns");    // NOLINT
const auto manifest_topic = model::topic("test-topic");  // NOLINT
const auto manifest_partition = model::partition_id(42); // NOLINT
const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);

constexpr std::string_view manifest = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 39,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "max_timestamp": 1234567890
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "max_timestamp": 1234567890
        }
    }
})json";

cloud_storage::partition_manifest
get_partition_manifest(std::string_view json = manifest) {
    cloud_storage::partition_manifest m;
    m.update(cloud_storage::manifest_format::json, make_manifest_stream(json))
      .get();
    return m;
}

class SegmentReuploadFixture
  : public async_data_uploader_fixture
  , public ::testing::Test {
public:
    struct log_spec {
        model::offset start_offset{0};
        size_t num_segments{1};
        size_t num_batches{10};
        // always overshoot
        model::offset target_max_collectable{0};
        size_t records_per_batch{1};
        bool roll_last_segment{true};
    };

    static constexpr log_spec default_spec() { return log_spec{}; }

    segment_collector_mode new_upload() const {
        return segment_collector_mode::new_upload;
    }

    segment_collector_mode compacted_reupload() const {
        return segment_collector_mode::compacted_reupload;
    }

    segment_collector_mode non_compacted_reupload() const {
        return segment_collector_mode::non_compacted_reupload;
    }

    void populate_log(log_spec spec = default_spec()) {
        random_records_generator gen{};
        populate_log(spec, random_records_generator{});
    }

    template<typename GenFunc>
    void populate_log(
      log_spec spec,
      GenFunc generator,
      model::cleanup_policy_bitflags cleanup_override
      = model::cleanup_policy_bitflags::compaction
        | model::cleanup_policy_bitflags::deletion) {
        std::optional<cluster::topic_properties> props;
        // enable compaction
        cluster::topic_properties p;
        p.cleanup_policy_bitflags = cleanup_override;
        p.shadow_indexing = model::shadow_indexing_mode::disabled;
        p.compaction_strategy = model::compaction_strategy::offset;
        props = p;
        add_topic(model::topic_namespace(test_namespace, test_topic), 1, props)
          .get();
        wait_for_leader(test_ntp).get();

        bool produce_throwaway_seg = spec.start_offset > model::offset{0};
        if (produce_throwaway_seg) {
            // produce to the desired local start offset (we will truncate
            // later)
            produce_data(spec.start_offset - 1, 1, generator, true);
        } else if (spec.num_segments > 0) {
            // hack for aligning the first segment when we don't get to truncate
            produce_data(
              spec.num_batches * spec.records_per_batch - 1,
              1,
              generator,
              spec.num_segments > 1 || spec.roll_last_segment);
        } else {
            // nothing else to do
            ASSERT_EQ(spec.num_segments, 0);
            return;
        }

        for (size_t i = produce_throwaway_seg ? 0 : 1; i < spec.num_segments;
             ++i) {
            bool roll = i + 1 < spec.num_segments || spec.roll_last_segment;
            produce_data(
              spec.num_batches, spec.records_per_batch, generator, roll);
        }

        for (auto seg : get_partition_log()->segments()) {
            if (
              seg->offsets().get_base_offset() < spec.target_max_collectable) {
                seg->mark_as_compacted_segment();
                seg->index().maybe_set_self_compact_timestamp(
                  model::timestamp::now());
                seg->mark_as_finished_windowed_compaction();
            }
        }

        if (spec.start_offset > model::offset{0}) {
            truncate_log(spec.start_offset);
        }

        RPTEST_REQUIRE_EVENTUALLY(10s, [this, spec] {
            return get_partition_log()->segment_count()
                   == spec.num_segments + (spec.roll_last_segment ? 1 : 0);
        });

        log_segments();
    }

    void log_segments() {
        vlog(
          test_log.debug,
          "PART: {} - {}",
          get_test_partition()->raft_start_offset(),
          get_test_partition()->committed_offset());
        vlog(test_log.debug, "LOG: {}", get_partition_log()->offsets());
        for (const auto& s : get_partition_log()->segments()) {
            vlog(test_log.debug, "SEG: {}", *s);
        }
    }

    ss::lw_shared_ptr<storage::segment> get_segment(size_t i) {
        auto& segment_set = get_partition_log()->segments();
        vassert(i < segment_set.size(), "Index out of range");
        return *std::next(segment_set.begin(), i);
    }

    template<class GenFunc>
    void produce_data(
      size_t num_batches,
      size_t records_per_batch,
      GenFunc generator,
      bool roll) {
        if (num_batches > 0) {
            generator.records_per_batch = records_per_batch;
            async_data_uploader_fixture::produce_data(num_batches, generator);
        }
        if (roll) {
            roll_segment();
        }
    };

    // TODO(oren): replace with model::test::make_random_batches
    template<class GenFunc>
    void append_batch(
      model::offset base_offset, size_t num_records, GenFunc generator) {
        generator.records_per_batch = num_records;
        auto kvs = generator();
        kafka::data::record_batcher batcher{max_upload_size};
        for (const auto& kv : kvs) {
            auto k = iobuf::from(kv.key);
            auto v = iobuf::from(kv.val.value());
            batcher.append(std::move(k), std::move(v));
        }
        auto bs = batcher.finish();
        ASSERT_EQ(bs.size(), 1);
        model::record_batch b{std::move(bs.back())};
        b.header().base_offset = base_offset;
        get_partition_log()->segments().back()->append(std::move(b)).get();
    }

    void run_disk_log_housekeeping(
      model::offset max_collectable = model::offset::max()) {
        scoped_config cfg;
        // Make sure that the compaction always runs
        cfg.get("min_cleanable_dirty_ratio")
          .set_value(std::make_optional(double(0)));

        auto compacted_before
          = get_partition_log()->get_probe().get_segments_compacted();
        get_partition_log()
          ->housekeeping(
            storage::housekeeping_config{
              model::timestamp::max(),
              std::nullopt /* max_bytes_in_log */,
              max_collectable,
              std::nullopt /* tombstone_retention_ms */,
              std::nullopt /* tx_retention_ms */,
              std::chrono::milliseconds{0} /* min_lag_ms */,
              as()})
          .get();
        auto compacted_after
          = get_partition_log()->get_probe().get_segments_compacted();

        log_segments();

        ASSERT_GT(compacted_after, compacted_before);
    }

    void truncate_log(model::offset truncate_to) {
        auto kafka_truncate_to = model::offset_cast(
          get_partition_log()->from_log_offset(truncate_to));
        get_test_partition()
          ->prefix_truncate(
            truncate_to, kafka_truncate_to, ss::lowres_clock::now() + 10s)
          .get();

        RPTEST_REQUIRE_EVENTUALLY(10s, [this, truncate_to] {
            auto* log = get_partition_log();
            auto truncation_point = log->index_lower_bound(truncate_to);
            return truncation_point.has_value()
                   && log->offsets().start_offset >= truncation_point;
        });

        log_segments();
    }

    segment_collector_stream_result
    try_make_candidate(segment_collector& collector) {
        return collector
          .make_segment_upload_stream(
            *get_test_partition(), segment_lock_timeout, gate())
          .get();
    }

    template<typename T>
    T get(segment_collector_stream_result& r) {
        vassert(std::holds_alternative<T>(r), "Wrong type, got {}", r.index());
        return std::get<T>(std::move(r));
    }

    void check_skip(segment_collector& collector, skip_offset_range expected) {
        auto result = try_make_candidate(collector);
        auto skip = get<Skip>(result);
        ASSERT_EQ(skip.begin_offset, expected.begin_offset);
        ASSERT_EQ(skip.end_offset, expected.end_offset);
        ASSERT_EQ(skip.reason, expected.reason);
    }

    struct stream_descriptor {
        model::offset start_offset;
        model::offset end_offset;
        bool is_compacted;
        ss::noncopyable_function<void(size_t)> check_size{[](size_t) {}};
    };

    void check_stream(
      segment_collector& collector,
      stream_descriptor expected,
      bool compare_versions = true) {
        auto result = try_make_candidate(collector);
        auto stream = get<Stream>(result);
        ASSERT_EQ(stream.start_offset, expected.start_offset);
        ASSERT_EQ(stream.end_offset, expected.end_offset);
        ASSERT_EQ(stream.is_compacted, expected.is_compacted);
        expected.check_size(stream.size);

        if (compare_versions) {
            result = collector
                       .make_upload_candidate_stream(segment_lock_timeout)
                       .get();
            auto old_stream = get<Stream>(result);
            compare_streams(stream, old_stream);
        }
    }

    void
    check_err(segment_collector& collector, candidate_creation_error expected) {
        auto result = try_make_candidate(collector);
        ASSERT_EQ(get<Error>(result), expected);
    }

    size_t get_range_size(model::offset start, model::offset end) {
        auto range_size
          = get_partition_log()->offset_range_size(start, end).get();
        vassert(range_size.has_value(), "offset_range_size failed");
        return range_size.value().on_disk_size;
    }

    size_t get_range_size_no_throw(
      model::offset start, model::offset end, size_t lb = 0) {
        size_t result{lb};
        while (result <= lb) {
            try {
                result = get_range_size(start, end);
            } catch (const ss::semaphore_timed_out&) {
                continue;
            }
        }
        return result;
    }

    model::offset lso() {
        return std::min(
          get_test_partition()->last_stable_offset(),
          model::next_offset(get_test_partition()->committed_offset()));
    }

    void advance_term() {
        get_test_partition()->raft()->step_down("segment_reupload test").get();
        wait_for_leader(test_ntp, 10s).get();
    }

    void compare_streams(
      segment_collector_stream& s1, segment_collector_stream& s2) {
        ASSERT_EQ(s1.size, s2.size);
        ASSERT_EQ(s1.start_offset, s2.start_offset);
        ASSERT_EQ(s1.end_offset, s2.end_offset);
        ASSERT_EQ(s1.min_timestamp, s2.min_timestamp);
        ASSERT_EQ(s1.max_timestamp, s2.max_timestamp);

        std::vector<iobuf> data;
        for (auto s : std::array{&s1, &s2}) {
            iobuf d;
            auto is = s->create_input_stream();
            while (!is.eof()) {
                auto buf = is.read().get();
                if (buf.empty()) {
                    break;
                }
                d.append(std::move(buf));
            }
            is.close().get();
            ASSERT_EQ(d.size_bytes(), s->size);
            data.push_back(std::move(d));
        }
        ASSERT_EQ(data[0], data[1]);
    }

    ss::gate& gate() { return _gate; }
    ss::abort_source& as() { return _abort_source; }

    using Stream = segment_collector_stream;
    using Error = candidate_creation_error;
    using Skip = skip_offset_range;

private:
    ss::gate _gate;
    ss::abort_source _abort_source;
};
} // namespace

TEST_F(SegmentReuploadFixture, test_make_segment_upload_stream) {
    populate_log(
      log_spec{
        .start_offset = model::offset{10},
        .num_segments = 4,
        .num_batches = 10,
        .target_max_collectable = model::offset{40},
      });

    auto m = get_partition_manifest();
    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{4},
      m,
      *get_partition_log(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});

    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{10},
        .end_offset = model::offset{39},
        .is_compacted = true,
        .check_size = [](size_t s) { ASSERT_GE(s, 1); },
      });
}

TEST_F(SegmentReuploadFixture, test_make_segment_upload_skip_offsets) {
    populate_log(
      log_spec{
        .start_offset = model::offset{15},
        .num_segments = 3,
        .num_batches = 10,
        .target_max_collectable = model::offset{45},
        .records_per_batch = 10,
      });
    auto m = get_partition_manifest();
    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{4},
      m,
      *get_partition_log(),
      max_upload_size};

    collector.collect_segments();

    // Both upload boundaries, aligned to the manifest, will fall inside a
    // batch, so upload creation will return skip_offsets
    check_skip(
      collector,
      skip_offset_range{
        .begin_offset = model::offset{20},
        .end_offset = model::offset{39},
        .reason = candidate_creation_error::offset_inside_batch,
      });
}

struct consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) noexcept {
        auto interval = model::bounded_offset_interval::checked(
          b.base_offset(), b.last_offset());
        boundaries->push_back(interval);
        auto hdr_size = model::packed_record_batch_header_size;
        auto data_size = b.data().size_bytes();
        auto prev_size = running_sum->empty() ? 0 : running_sum->back();
        running_sum->push_back(prev_size + hdr_size + data_size);
        co_return ss::stop_iteration::no;
    }

    bool end_of_stream() const { return false; }

    std::vector<model::bounded_offset_interval>* boundaries;
    std::vector<size_t>* running_sum;
};

TEST_F(SegmentReuploadFixture, test_new_segment_upload_fuzz) {
    cloud_storage::partition_manifest m(
      manifest_ntp, model::initial_revision_id{0});

    populate_log(
      log_spec{
        .start_offset = model::offset{0},
        .num_segments = 0,
      });

    constexpr int max_records_per_batch = 100;
    constexpr int batches_per_segment = 100;
    constexpr int num_segments = 20;
    model::offset start_offset{0};
    model::term_id start_term{0};

    random_records_generator gen{};

    for (int segment_ix = 0; segment_ix < num_segments; segment_ix++) {
        auto num_batches = random_generators::get_int(1, batches_per_segment);
        auto records_per_batch = random_generators::get_int(
          1, max_records_per_batch);
        vlog(
          test_log.info,
          "Add segment{}: {} {}",
          segment_ix,
          start_offset,
          start_term);

        produce_data(num_batches, records_per_batch, gen, false);

        if (segment_ix < num_segments - 1) {
            roll_segment();
        }

        // TODO(oren): this doesn't work for some reason
        // if (random_generators::get_int(2) == 1) {
        //     advance_term();
        // }
    }

    std::vector<model::bounded_offset_interval> batch_boundaries;
    std::vector<model::bounded_offset_interval> segment_boundaries;
    std::vector<size_t> acc_batch_size;

    // Find all segment boundaries
    for (auto& s : get_partition_log()->segments()) {
        auto interval = model::bounded_offset_interval::checked(
          s->offsets().get_base_offset(), s->offsets().get_committed_offset());
        segment_boundaries.push_back(interval);
        vlog(
          test_log.info,
          "Segment boundaries: {} - {} (closed: {})",
          s->offsets().get_base_offset(),
          s->offsets().get_committed_offset(),
          s->is_closed());
        if (s->has_appender() && !s->is_closed()) {
            s->release_appender()->close().get();
        }
    }

    auto last_offset = model::prev_offset(lso());

    // Consume the log to find all batch boundaries
    auto reader = get_test_partition()
                    ->make_local_reader(
                      storage::local_log_reader_config{
                        model::offset{0}, last_offset})
                    .get();
    vlog(test_log.info, "Consuming from the log 0 - {}", last_offset);
    std::move(reader)
      .consume(
        consumer{
          .boundaries = &batch_boundaries, .running_sum = &acc_batch_size},
        model::no_timeout)
      .get();

    for (auto t : batch_boundaries) {
        vlog(test_log.info, "Batch boundaries: {} - {}", t.min(), t.max());
    }
    vlog(test_log.info, "Done consuming from the log");

    for (int i = 0; i < 25; i++) {
        auto ix_start = random_generators::get_int(
          0UL, batch_boundaries.size() - 1);
        auto ix_end = random_generators::get_int(
          0UL, batch_boundaries.size() - 1);
        if (ix_start == ix_end) {
            continue;
        } else if (ix_start > ix_end) {
            std::swap(ix_start, ix_end);
        }
        auto start_bound = batch_boundaries.at(ix_start);
        auto end_bound = batch_boundaries.at(ix_end);

        auto expected_size = get_range_size_no_throw(
          start_bound.min(), end_bound.max());

        // Case 1:
        // Specify the full range and expect the collector to collect
        // all batches in the range. The size should match the expectation
        // precisely.
        // This code path is used when we're reuploading the offset range
        // or when we're uploading new segment but forcing upload up to LSO.
        vlog(
          test_log.info,
          "Collecting {} bytes starting from the offset {}-{}",
          expected_size,
          start_bound.min(),
          end_bound.max());
        archival::segment_collector closed_range_collector{
          new_upload(),
          start_bound.min(),
          m,
          *get_partition_log(),
          /* Make sure the size limit is not affecting anything */
          expected_size * 10,
          end_bound.max() /* end_inclusive  */,
          lso(),        /* end_exclusive */
          std::nullopt, /* flush_offset */
        };

        closed_range_collector.collect_segments();

        check_stream(
          closed_range_collector,
          stream_descriptor{
            .start_offset = start_bound.min(),
            .end_offset = end_bound.max(),
            .check_size =
              [&expected_size](size_t s) { ASSERT_EQ(s, expected_size); },
          },
          true);

        // Case 2:
        // In this case the end offset is not specified. The collector
        // should be able to collect the segments in the range based on
        // size limit (size limit is set to match the expected size).
        // This code path is used when we're uploading new non compacted
        // segments.

        // Since this will upload only one segment we need to adjust the
        // end offset and the size.
        bool batch_found = false;
        for (auto s : segment_boundaries) {
            if (s.contains(start_bound.min())) {
                // The uploaded segment is found. Now we need to find the end
                // batch.
                ix_end = ix_start;
                for (auto i = ix_start; i < batch_boundaries.size(); i++) {
                    if (batch_boundaries.at(i).contains(s.max())) {
                        ix_end = i;
                        end_bound = batch_boundaries.at(i);
                        expected_size
                          = acc_batch_size.at(ix_end)
                            - (ix_start == 0 ? 0 : acc_batch_size.at(ix_start - 1));
                        auto chk = get_range_size_no_throw(
                          start_bound.min(), end_bound.max());
                        ASSERT_EQ(expected_size, chk);
                        batch_found = true;
                        vlog(
                          test_log.info,
                          "New range: {}-{}, new content length: {}",
                          start_bound.min(),
                          end_bound.max(),
                          expected_size);
                        break;
                    }
                }
                break;
            }
        }
        ASSERT_TRUE(batch_found);
        vlog(
          test_log.info,
          "Collecting {} bytes starting from the offset {}-{}",
          expected_size,
          start_bound.min(),
          end_bound.max());
        archival::segment_collector open_range_collector{
          new_upload(),
          start_bound.min(),
          m,
          *get_partition_log(),
          expected_size,
          std::nullopt,
          lso(),
          std::nullopt,
        };

        open_range_collector.collect_segments();
        check_stream(
          open_range_collector,
          stream_descriptor{
            .start_offset = start_bound.min(),
            .end_offset = end_bound.max(),
            .check_size =
              [&expected_size](size_t s) { ASSERT_EQ(s, expected_size); },
          },
          true);
    }
}
