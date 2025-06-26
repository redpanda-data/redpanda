/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 *
 */

#include "cloud_storage/partition_manifest.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/tests/async_data_uploader_fixture.h"
#include "cluster/partition.h"
#include "kafka/data/record_batcher.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/offset_interval.h"
#include "storage/types.h"
#include "test_utils/archival.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <gtest/gtest.h>

#include <vector>

using namespace archival;

namespace {

ss::logger test_log("segment-reupload-v2-test");

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

static constexpr std::string_view manifest_with_gaps = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 59,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 30,
            "committed_offset": 39,
            "max_timestamp": 1234567890
        },
        "50-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 50,
            "committed_offset": 59,
            "max_timestamp": 1234567890
        }
    }
})json";

static constexpr std::string_view test_manifest = R"json({
  "version": 2,
  "namespace": "test-ns",
  "topic": "test-topic",
  "partition": 1,
  "revision": 21,
  "last_offset": 211,
  "segments": {
    "0-1-v1.log": {
      "base_offset": 0,
      "committed_offset": 1,
      "is_compacted": false,
      "size_bytes": 200,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389191244,
      "max_timestamp": 1686389191244,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 1,
      "delta_offset_end": 0
    },
    "2-2-v1.log": {
      "base_offset": 2,
      "committed_offset": 103,
      "is_compacted": false,
      "size_bytes": 98014783,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389202577,
      "max_timestamp": 1686389230060,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    },
    "104-2-v1.log": {
      "base_offset": 104,
      "committed_offset": 113,
      "is_compacted": false,
      "size_bytes": 10001460,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    },
    "113-2-v1.log": {
      "base_offset": 113,
      "committed_offset": 115,
      "is_compacted": false,
      "size_bytes": 10001460,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    },
    "116-2-v1.log": {
      "base_offset": 116,
      "committed_offset": 211,
      "is_compacted": false,
      "size_bytes": 10001460,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    }
  }
})json";

const auto manifest_namespace = model::ns("test-ns");    // NOLINT
const auto manifest_topic = model::topic("test-topic");  // NOLINT
const auto manifest_partition = model::partition_id(42); // NOLINT
const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
const auto manifest_revision = model::initial_revision_id(1); // NOLINT
const ss::sstring manifest_url = ssx::sformat(                // NOLINT
  "/10000000/meta/{}_{}/manifest.bin",
  manifest_ntp.path(),
  manifest_revision());

constexpr size_t max_upload_size{4096_KiB};
constexpr ss::lowres_clock::duration segment_lock_timeout{60s};

cloud_storage::partition_manifest
get_partition_manifest(std::string_view json = manifest) {
    cloud_storage::partition_manifest m;
    m.update(cloud_storage::manifest_format::json, make_manifest_stream(json))
      .get();
    return m;
}

struct single_key_record_generator {
    std::vector<tests::kv_t> operator()() {
        std::vector<tests::kv_t> batch;
        for (size_t i = 0; i < records_per_batch; i++) {
            vlog(test_log.info, "Used key {}", key);
            auto record = random_generators::gen_alphanum_string(record_size);
            batch.emplace_back(key, std::move(record));
        }
        return batch;
    }
    ss::sstring key{"abcd"};
    size_t record_size{1000};
    size_t records_per_batch{10};
    std::optional<int> key_space_size;
};

class SegmentReuploadFixture
  : public async_data_uploader_fixture
  , public ::testing::TestWithParam<model::cloud_storage_segment_upload_mode> {
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

    bool is_v2() const {
        return GetParam() == model::cloud_storage_segment_upload_mode::v2;
    }

    segment_collector_mode new_upload() const {
        if (is_v2()) {
            return segment_collector_mode::new_upload_v2;
        }
        return segment_collector_mode::new_upload;
    }

    segment_collector_mode compacted_reupload() const {
        if (is_v2()) {
            return segment_collector_mode::compacted_reupload_v2;
        }
        return segment_collector_mode::compacted_reupload;
    }

    segment_collector_mode non_compacted_reupload() const {
        if (is_v2()) {
            return segment_collector_mode::non_compacted_reupload_v2;
        }
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
          ->housekeeping(storage::housekeeping_config{
            model::timestamp::max(),
            std::nullopt,
            max_collectable,
            std::nullopt,
            std::chrono::milliseconds{0},
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
        ASSERT_EQ(skip.start_offset, expected.start_offset);
        ASSERT_EQ(skip.end_offset, expected.end_offset);
        ASSERT_EQ(skip.reason, expected.reason);
    }

    struct stream_descriptor {
        model::offset start_offset;
        model::offset end_offset;
        bool is_compacted;
        ss::noncopyable_function<void(size_t)> check_size{[](size_t) {}};
    };

    void
    check_stream(segment_collector& collector, stream_descriptor expected) {
        auto result = try_make_candidate(collector);
        auto stream = get<Stream>(result);
        auto close = ss::defer([&stream]() mutable { stream.close().get(); });
        ASSERT_EQ(stream.start_offset, expected.start_offset);
        ASSERT_EQ(stream.end_offset, expected.end_offset);
        ASSERT_EQ(stream.is_compacted, expected.is_compacted);
        expected.check_size(stream.size);
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
        vassert(
          !range_size.value().boundary_in_batch,
          "Range boundary unexpectedly inside batch");
        return range_size.value().on_disk_size;
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

TEST_P(SegmentReuploadFixture, test_segment_collection) {
    populate_log(log_spec{
      .start_offset = model::offset{5},
      .num_segments = 4,
      .num_batches = 15,
      .target_max_collectable = model::offset{45},
    });

    auto m = get_partition_manifest();

    auto part = get_test_partition();

    archival::segment_collector collector{
      compacted_reupload(), model::offset{4}, m, *part->log(), max_upload_size};

    ASSERT_TRUE(collector.collect_segments());

    // We should collect some compacted data, with begin and end offsets aligned
    // to the partition manifest
    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});
    ASSERT_EQ(collector.segments().size(), 0);
}

TEST_P(SegmentReuploadFixture, test_make_segment_upload_stream) {
    populate_log(log_spec{
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

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});
    ASSERT_EQ(0, collector.segments().size());

    auto result = try_make_candidate(collector);

    auto strm = get<Stream>(result);
    ASSERT_GE(strm.size, size_t{1});

    // Read from candidate stream
    iobuf candidate_data;
    {
        auto is = strm.create_input_stream();
        while (!is.eof()) {
            auto buf = is.read().get();
            if (buf.empty()) {
                break;
            }
            candidate_data.append(std::move(buf));
        }
        is.close().get();
    }
    ASSERT_EQ(candidate_data.size_bytes(), strm.size);
}

TEST_P(SegmentReuploadFixture, test_make_segment_upload_skip_offsets) {
    populate_log(log_spec{
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

    ASSERT_TRUE(collector.collect_segments());

    auto result = try_make_candidate(collector);
    auto strm = get<Skip>(result);

    // Both upload boundaries, aligned to the manifest, will fall inside a
    // batch, so upload creation will return skip_offsets
    ASSERT_EQ(strm.start_offset, model::offset{20});
    ASSERT_EQ(strm.end_offset, model::offset{39});
    ASSERT_EQ(strm.reason, candidate_creation_error::offset_inside_batch);
}

TEST_P(SegmentReuploadFixture, test_start_ahead_of_manifest) {
    populate_log(log_spec{
      .start_offset = model::offset{0},
      .num_segments = 1,
      .num_batches = 10,
      .target_max_collectable = model::offset{0},
      .records_per_batch = 10,
    });

    auto m = get_partition_manifest();

    {
        // start ahead of manifest end, no collection happens.
        archival::segment_collector collector{
          compacted_reupload(),
          model::offset{400},
          m,
          *get_partition_log(),
          max_upload_size};

        ASSERT_TRUE(!collector.collect_segments());

        ASSERT_EQ(false, collector.should_replace_manifest_segment());
    }

    {
        // start at manifest end. the collector will advance it first to prevent
        // overlap. no collection happens.
        archival::segment_collector collector{
          compacted_reupload(),
          model::offset{39},
          m,
          *get_partition_log(),
          max_upload_size};

        ASSERT_TRUE(!collector.collect_segments());

        ASSERT_EQ(false, collector.should_replace_manifest_segment());
    }
}

TEST_P(SegmentReuploadFixture, test_empty_manifest) {
    cloud_storage::partition_manifest m{};

    populate_log();

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{2},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(!collector.collect_segments());

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
}

TEST_P(
  SegmentReuploadFixture,
  test_short_compacted_segment_inside_manifest_segment) {
    auto m = get_partition_manifest();
    // segment [12-14] lies inside manifest segment [10-19]. start offset 1 is
    // adjusted to start of the local log 12. Since this offset is in the middle
    // of a manifest segment, advance it again to the beginning of the next
    // manifest segment: 20. There's no local segment containing that
    // offset,so no segments are collected.
    populate_log(log_spec{
      .start_offset = model::offset{12},
      .num_segments = 1,
      .num_batches = 2,
      .target_max_collectable = model::offset{14},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{1},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(!collector.collect_segments());

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
    check_err(collector, candidate_creation_error::no_segments_collected);
}

TEST_P(
  SegmentReuploadFixture,
  test_compacted_segment_aligned_with_manifest_segment) {
    auto m = get_partition_manifest();
    populate_log(log_spec{
      .start_offset = model::offset{10},
      .num_segments = 4,
      .num_batches = 10,
      .target_max_collectable = model::offset{20},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{1},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());

    // we never actually ran compaction, just marked the segments as such, so
    // the result is a skip_range
    check_skip(
      collector,
      skip_offset_range{
        .start_offset = model::offset{10},
        .end_offset = model::offset{19},
        .reason = candidate_creation_error::upload_size_unchanged});
}

TEST_P(
  SegmentReuploadFixture,
  test_short_compacted_segment_aligned_with_manifest_segment) {
    auto m = get_partition_manifest();
    // compacted segment start aligned with manifest segment start, but segment
    // is too short.
    populate_log(log_spec{
      .start_offset = model::offset{10},
      .num_segments = 1,
      .num_batches = 5,
      .target_max_collectable = model::offset{15},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(!collector.collect_segments());

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
    check_err(collector, candidate_creation_error::no_segments_collected);
}

TEST_P(
  SegmentReuploadFixture,
  test_many_compacted_segments_make_up_to_manifest_segment) {
    auto m = get_partition_manifest();
    populate_log(log_spec{
      .start_offset = model::offset{10},
      .num_segments = 5,
      .num_batches = 3,
      .target_max_collectable = model::offset{20},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{19});

    check_skip(
      collector,
      skip_offset_range{
        .start_offset = model::offset{10},
        .end_offset = model::offset{19},
        .reason = candidate_creation_error::upload_size_unchanged,
      });
}

TEST_P(
  SegmentReuploadFixture, test_compacted_segment_larger_than_manifest_segment) {
    auto m = get_partition_manifest();
    populate_log(log_spec{
      .start_offset = model::offset{8},
      .num_segments = 1,
      .num_batches = 20,
      .target_max_collectable = model::offset{28},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{2},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());

    // Begin and end markers are aligned to manifest segment.
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{19});

    check_skip(
      collector,
      skip_offset_range{
        .start_offset = model::offset{10},
        .end_offset = model::offset{19},
        .reason = candidate_creation_error::upload_size_unchanged,
      });
}

TEST_P(SegmentReuploadFixture, test_collect_capped_by_size) {
    auto m = get_partition_manifest();

    populate_log(log_spec{
      .start_offset = model::offset{5},
      .num_segments = 6,
      .num_batches = 10,
      .target_max_collectable = model::offset{45},
    });

    // get size on disk from beginning of manifest to end of the third segment
    // note that this is not the last compacted offset in the log
    auto range_size = get_partition_log()
                        ->offset_range_size(
                          model::offset{10}, model::offset{34})
                        .get();

    ASSERT_TRUE(range_size.has_value());
    ASSERT_TRUE(!range_size.value().boundary_in_batch);

    auto max_size = range_size.value().on_disk_size;

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      max_size};

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());

    // we should  clamp to the end of the second segment in the
    // manifest based on our configured max size
    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{10},
        .end_offset = model::offset{29},
        .is_compacted = true,
        .check_size = [max_size](size_t s) { ASSERT_LT(s, max_size); },
      });
}

TEST_P(SegmentReuploadFixture, test_no_compacted_segments) {
    auto m = get_partition_manifest();

    populate_log(log_spec{
      .start_offset = model::offset{5},
      .num_segments = 6,
      .num_batches = 10,
      .target_max_collectable = model::offset{0},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{5},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(!collector.collect_segments());
    ASSERT_TRUE(!collector.should_replace_manifest_segment());

    check_err(collector, candidate_creation_error::no_segments_collected);
}

TEST_P(SegmentReuploadFixture, test_collected_segments_completely_cover_gap_1) {
    auto m = get_partition_manifest(manifest_with_gaps);

    // The manifest has gap from 20-29. It will be replaced by re-uploaded
    // data. The re-upload will end at the gap boundary due to adjustment of
    // end offset.
    populate_log(log_spec{
      .start_offset = model::offset{5},
      .num_segments = 6,
      .num_batches = 10,
      .target_max_collectable = model::offset{45},
    });

    // get size on disk from beginning of manifest to end of the third
    // segment
    auto max_size = get_range_size(model::offset{10}, model::offset{34});

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      max_size};

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());

    // Collection start aligned to manifest start at 10
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});

    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{10},
        .end_offset = model::offset{29},
        .is_compacted = true,
        .check_size = [max_size](size_t s) { ASSERT_LE(s, max_size); },
      });
}

TEST_P(SegmentReuploadFixture, test_collected_segments_completely_cover_gap_2) {
    auto m = get_partition_manifest(manifest_with_gaps);

    // Re-uploaded segments completely cover gap.
    populate_log(log_spec{
      .start_offset = model::offset{10},
      .num_segments = 6,
      .num_batches = 10,
      .target_max_collectable = model::offset{50},
    });

    // get size on disk from beginning of manifest to end of the third
    // segment

    auto max_size = get_range_size(model::offset{10}, model::offset{39});

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      max_size};

    ASSERT_TRUE(collector.collect_segments());

    ASSERT_TRUE(collector.should_replace_manifest_segment());

    // Collection start aligned to manifest start at 10
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});

    // And the size requirement winds us back to the end of the third segment,
    // as expected
    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{10},
        .end_offset = model::offset{39},
        .is_compacted = true,
        .check_size = [max_size](size_t s) { ASSERT_EQ(s, max_size); },
      });
}

TEST_P(SegmentReuploadFixture, test_compacted_segment_after_manifest_start) {
    auto m = get_partition_manifest();

    // manifest start: 10, compacted segment start: 15, search start: 0
    // begin offset will be realigned to end of segment 10-19 to avoid overlap.
    populate_log(log_spec{
      .start_offset = model::offset{15},
      .num_segments = 3,
      .num_batches = 30,
      .target_max_collectable = model::offset{45},
    });

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      max_upload_size};

    ASSERT_TRUE(collector.collect_segments());
    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{20});

    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{20},
        .end_offset = model::offset{39},
        .is_compacted = true,
      });
}

TEST_P(SegmentReuploadFixture, test_upload_candidate_generation) {
    auto m = get_partition_manifest();

    populate_log(log_spec{
      .start_offset = model::offset{5},
      .num_segments = 6,
      .num_batches = 10,
      .target_max_collectable = model::offset{45},
    });

    // size of first three segments on disk, aligned to start of first manifest
    // segment
    auto max_size = get_range_size(model::offset{10}, model::offset{34});

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{5},
      m,
      *get_partition_log(),
      max_size};

    ASSERT_TRUE(collector.collect_segments());
    ASSERT_TRUE(collector.should_replace_manifest_segment());

    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{10},
        .end_offset = model::offset{29},
        .is_compacted = true,
        .check_size = [max_size](size_t s) { ASSERT_LE(s, max_size); },
      });
}

TEST_P(SegmentReuploadFixture, test_upload_aligned_to_non_existent_offset) {
    auto m = get_partition_manifest();

    populate_log(
      log_spec{
        .start_offset = model::offset{0},
        .num_segments = 6,
        .num_batches = 15,
        .target_max_collectable = model::offset{0},
        .roll_last_segment = false,
      },
      single_key_record_generator{},
      model::cleanup_policy_bitflags::compaction);

    auto size_before = get_range_size(model::offset{10}, model::offset{39});

    // compact up to the end of the fourth segment
    // leaving the last two untouched
    run_disk_log_housekeeping(model::offset{60});

    auto max_size = get_range_size(model::offset{10}, model::offset{39});

    ASSERT_EQ(get_partition_log()->segments().size(), 3);
    ASSERT_LT(max_size, size_before);

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{5},
      m,
      *get_partition_log(),
      max_size};

    ASSERT_TRUE(collector.collect_segments());
    ASSERT_TRUE(collector.should_replace_manifest_segment());

    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{10},
        .end_offset = model::offset{39},
        .is_compacted = true,
        .check_size = [max_size](size_t s) { ASSERT_EQ(s, max_size); },
      });
}

TEST_P(SegmentReuploadFixture, test_same_size_reupload_skipped) {
    // 'segment_collector' should not propose the re-upload
    // of a segment if the compacted size is equal to
    // the size of the segment in the manifest. In that case,
    // the resulting addresable name in cloud storage would be the
    // same for the segment before and after compaction. This would
    // result in the deletion of the segment.
    //
    // This test checks the invariant above.

    populate_log(log_spec{
      .start_offset = model::offset{2},
      .num_segments = 1,
      .num_batches = 1,
      .target_max_collectable = model::offset{4},
      .records_per_batch = 2,
    });

    auto total_size = get_range_size(model::offset{2}, model::offset{3});

    cloud_storage::partition_manifest m{
      manifest_ntp, model::initial_revision_id{1}};
    m.add(
      segment_name("2-1-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = total_size,
        .base_offset = model::offset(2),
        .committed_offset = model::offset(3),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    {
        archival::segment_collector collector{
          compacted_reupload(),
          model::offset{0},
          m,
          *get_partition_log(),
          total_size};

        ASSERT_TRUE(collector.collect_segments());
        ASSERT_EQ(collector.begin_inclusive(), model::offset{2});
        ASSERT_TRUE(collector.should_replace_manifest_segment());

        check_skip(
          collector,
          skip_offset_range{
            .start_offset = model::offset{2},
            .end_offset = model::offset{3},
            .reason = candidate_creation_error::upload_size_unchanged,
          });
    }

    produce_data(1, 1, random_records_generator{}, true /* roll segment */);

    ASSERT_EQ(get_partition_log()->segment_count(), 3);

    // Mark the new segment as having completed self compaction
    // and collect for re-upload again. Again, the upload candidate
    // should be a no-op since the reupload of the two local segments
    // results in a segment of the same size as the one that should be replaced.
    get_segment(1)->mark_as_compacted_segment();
    get_segment(1)->index().maybe_set_self_compact_timestamp(
      model::timestamp::now());
    get_segment(1)->mark_as_finished_windowed_compaction();

    total_size = get_range_size(model::offset{2}, model::offset{5});

    m = cloud_storage::partition_manifest(
      manifest_ntp, model::initial_revision_id{1});
    m.add(
      segment_name("2-1-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = total_size,
        .base_offset = model::offset(2),
        .committed_offset = model::offset(5),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    archival::segment_collector collector{
      compacted_reupload(),
      model::offset{0},
      m,
      *get_partition_log(),
      total_size};

    ASSERT_TRUE(collector.collect_segments());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{2});
    ASSERT_TRUE(collector.should_replace_manifest_segment());

    check_skip(
      collector,
      skip_offset_range{
        .start_offset = model::offset{2},
        .end_offset = model::offset{5},
        .reason = candidate_creation_error::upload_size_unchanged,
      });
}

// TODO(oren): what is this actually testing?
TEST_P(SegmentReuploadFixture, test_do_not_reupload_self_concatenated) {
    populate_log(log_spec{
      .start_offset = model::offset{1000},
      .num_segments = 3,
      .num_batches = 1000,
      .target_max_collectable = model::offset{2000},
    });

    auto seg_size = get_range_size(model::offset{1000}, model::offset{1999});
    cloud_storage::partition_manifest m(
      manifest_ntp, model::initial_revision_id{1});
    m.add(
      segment_name("1000-1999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(1000),
        .committed_offset = model::offset(1999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("2000-2999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(2000),
        .committed_offset = model::offset(2999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("3000-3999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(3000),
        .committed_offset = model::offset(3999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    truncate_log(model::offset(3000));

    {
        archival::segment_collector collector{
          compacted_reupload(),
          model::offset{0},
          m,
          *get_partition_log(),
          seg_size * 10};

        ASSERT_TRUE(!collector.collect_segments());
        ASSERT_TRUE(!collector.should_replace_manifest_segment());
    }
}

TEST_P(SegmentReuploadFixture, test_do_not_reupload_prefix_truncated) {
    populate_log(log_spec{
      .start_offset = model::offset{0},
      .num_segments = 3,
      .num_batches = 2,
      .target_max_collectable = model::offset{3000},
      .records_per_batch = 500,
    });

    // Set up our manifest to look as if our local data is a compacted version
    // of what's in the cloud.
    auto seg_size = get_segment(0)->size_bytes();
    cloud_storage::partition_manifest m(
      manifest_ntp, model::initial_revision_id{1});
    m.add(
      segment_name("0-499-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(499),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("500-999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(500),
        .committed_offset = model::offset(999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("1000-1999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(1000),
        .committed_offset = model::offset(1999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("2000-2999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(2000),
        .committed_offset = model::offset(2999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    // Prefix truncate without aligning to a segment boundary, a la
    // delete-records.
    truncate_log(model::offset{100});

    {
        archival::segment_collector collector{
          compacted_reupload(),
          model::offset{0},
          m,
          *get_partition_log(),
          seg_size * 10};

        // Since we can't replace offsets starting at 0, the first remote
        // segment isn't eligible for reupload and we should start from the next
        // segment.
        ASSERT_TRUE(collector.collect_segments());
        ASSERT_EQ(collector.begin_inclusive(), model::offset{500});

        ASSERT_TRUE(collector.should_replace_manifest_segment());

        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{500},
            .end_offset = model::offset{2999},
            .is_compacted = true,
          });
    }

    {
        // Try collecting from the middle of a local segment that happens to
        // align with our manifest. The start offset of the upload candidate
        // should be aligned with our manifest. Same idea as above.
        archival::segment_collector collector{
          compacted_reupload(),
          model::offset{500},
          m,
          *get_partition_log(),
          seg_size * 10};

        // Since we can't replace offsets starting at 0, the first remote
        // segment isn't eligible for reupload and we should start from the next
        // segment.
        ASSERT_TRUE(collector.collect_segments());
        ASSERT_EQ(collector.begin_inclusive(), model::offset{500});

        ASSERT_TRUE(collector.should_replace_manifest_segment());

        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{500},
            .end_offset = model::offset{2999},
            .is_compacted = true,
          });
    }
}

TEST_P(SegmentReuploadFixture, test_adjacent_segment_collection) {
    /*
         +-----------------------------------++------------------------------+
 Local   |2                               115||116                        211|
         +-----------------------------------++------------------------------+
         +----------++----------++-----------++------------------------------+
 Cloud   |2      103||104    113||113     115||116                        211|
         +----------++----------++-----------++------------------------------+
    */

    populate_log(log_spec{
      .start_offset = model::offset{0},
      .num_segments = 1,
      .num_batches = 2,
      .target_max_collectable = model::offset{0},
      .records_per_batch = 1,
    });

    // TODO(oren): there's a difference in behavior here between old and new if
    // the batch boundaries don't align. i.e. if _begin_inclusive lies in a
    // batch the old version gives us a start offset of 3 or something, the
    // start of the batch. dunno what's going on here.
    random_records_generator gen{};
    produce_data(1, 102, gen, false /* roll segment */);
    produce_data(1, 12, gen, true /* roll segment */);
    produce_data(1, 96, gen, true /* roll segment */);

    auto m = get_partition_manifest(test_manifest);

    auto size = get_range_size(model::offset{104}, model::offset{115});

    archival::segment_collector collector{
      non_compacted_reupload(),
      model::offset{104},
      m,
      *get_partition_log(),
      size,
      model::offset{115}};
    ASSERT_TRUE(collector.collect_segments());
    check_stream(
      collector,
      stream_descriptor{
        .start_offset = model::offset{104},
        .end_offset = model::offset{115},
        .is_compacted = false,
        .check_size = [&size](size_t s) { ASSERT_EQ(s, size); },
      });
}

TEST_P(SegmentReuploadFixture, test_new_segment_upload) {
    cloud_storage::partition_manifest m(
      manifest_ntp, model::initial_revision_id{0});

    populate_log(log_spec{
      .start_offset = model::offset{0},
      .num_segments = 5,
      .num_batches = 10,
      .target_max_collectable = model::offset{0},
      .roll_last_segment = false,
    });

    // append one more batch directly to the tail of the log, without flushing,
    // to advance the dirty offset
    append_batch(model::offset{50}, 6, random_records_generator{});

    {
        // Upload the sealed portion of the log. This replicates the normal
        // upload flow in the ntp_archiver. The archiver uploads segments
        // when they are sealed.
        auto expect = get_range_size(model::offset{0}, model::offset{39});

        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          expect,
          std::nullopt, /* end_inclusive */
          lso(),        /* end_exclusive  */
          std::nullopt, /* flush_offset */
        };

        // LSO is 50, but we read up to 39 because the tail segment has
        // uncommitted data
        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::offset{39},
            .is_compacted = false,
            .check_size = [expect](size_t s) { ASSERT_EQ(s, expect); },
          });
    }

    {
        // try to upload the last segment all the way to the end. this will
        // fail because the segment has unstable records.
        archival::segment_collector collector{
          new_upload(),
          model::offset{40},
          m,
          *get_partition_log(),
          max_upload_size,
          std::nullopt, /* end_inclusive */
          model::next_offset(model::offset{55}) /* end_exclusive */,
          std::nullopt, /* flush_offset */
        };

        ASSERT_TRUE(collector.collect_segments());
        check_err(collector, candidate_creation_error::no_segments_collected);
    }

    {
        // time based uploads can force through a segment with uncommitted data
        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          max_upload_size,
          model::prev_offset(lso()),
          lso(),
          std::nullopt,
        };
        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::offset{model::prev_offset(lso())},
            .is_compacted = false,
          });
    }

    {
        // similar to the previous case but the operation starts and end in the
        // middle of the first and last segment, respectively
        archival::segment_collector collector{
          new_upload(),
          model::offset{8},
          m,
          *get_partition_log(),
          max_upload_size,
          model::offset{42},
          lso(),
          std::nullopt,
        };
        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{8},
            .end_offset = model::offset{42},
            .is_compacted = false,
          });
    }

    {
        // Upload that starts and ends in the unsealed segment
        archival::segment_collector collector{
          new_upload(),
          model::offset{42},
          m,
          *get_partition_log(),
          max_upload_size,
          model::offset{48}, /* end_inclusive */
          lso(),             /* end_exclusive */
          std::nullopt,      /* flush_offset */
        };
        ASSERT_TRUE(collector.collect_segments());

        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{42},
            .end_offset = model::offset{48},
            .is_compacted = false,
          });
    }

    {
        // size limited upload

        auto size = get_range_size(model::offset{0}, model::offset{29});
        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          size,
          std::nullopt,
          lso(),
          std::nullopt,
        };

        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::offset{29},
            .is_compacted = false,
            .check_size = [&size](size_t s) { ASSERT_EQ(s, size); },
          });
    }

    roll_segment();
    advance_term();
    produce_data(1, 10, random_records_generator{}, true /* roll segment */);

    {
        // upload stops at term boundary
        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          max_upload_size,
          std::nullopt,
          lso(),
          std::nullopt,
        };
        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::offset{55},
            .is_compacted = false,
          });
    }
}

TEST_P(SegmentReuploadFixture, test_new_segment_upload_off_by_one) {
    // Upload segments that contain only one record so begin_inclusive
    // is equal to end_inclusive.
    cloud_storage::partition_manifest m(
      manifest_ntp, model::initial_revision_id{0});

    // create three segments w/ one record each
    populate_log(log_spec{
      .start_offset = model::offset{0},
      .num_segments = 4,
      .num_batches = 1,
      .target_max_collectable = model::offset{0},
      .roll_last_segment = false,
    });

    append_batch(model::offset{4}, 1, random_records_generator{});

    auto max_size = get_range_size(model::offset{0}, model::offset{3});

    {
        // Upload first segment which contains only one record [0-0 offset
        // range]
        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          max_size,
          model::offset{0},
          lso(),
          std::nullopt};

        // This is an artifact of the test setup - the first segment contains a
        // single raft config batch, so technically we should skip it.
        // collect_segments reports this. However, this doesn't stop us from
        // constructing an upload stream for the sake of argument.
        ASSERT_TRUE(!collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::offset{0},
            .is_compacted = false,
          });
    }

    {
        // Upload second segment which contains only one record [1-1 offset
        // range]
        archival::segment_collector collector{
          new_upload(),
          model::offset{1},
          m,
          *get_partition_log(),
          max_size,
          model::offset{1},
          lso(),
          std::nullopt};

        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{1},
            .end_offset = model::offset{1},
            .is_compacted = false,
          });
    }

    {
        // upload a series of segments. it doesn't matter that the last segment
        // is not sealed, but note that we will not read past the stable offset
        // of the partition
        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          max_size,
          std::nullopt,
          model::next_offset(get_segment(3)->offsets().get_committed_offset()),
          std::nullopt};
        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::prev_offset(
              get_test_partition()->last_stable_offset()),
            .is_compacted = false,
          });
    }

    {
        // TODO(oren): this is basically the same as the prev case
        // Upload series of segments but include last segment which is not
        // sealed.
        // This should work because target_end_inclusive is set to 3.
        // The 'target_end_inclusive' is supposed to be used to pass LSO value
        // to the collector. This means that the collector now knows that it's
        // safe to read from unsealed segment below this offset.
        archival::segment_collector collector{
          new_upload(),
          model::offset{0},
          m,
          *get_partition_log(),
          max_size,
          model::offset{3},
          lso(),
          std::nullopt};
        ASSERT_TRUE(collector.collect_segments());
        check_stream(
          collector,
          stream_descriptor{
            .start_offset = model::offset{0},
            .end_offset = model::offset{3},
            .is_compacted = false,
          });
    }
}

// TODO(oren): dedupe with segment_reupload_test.cc
/// Collect all batch boundaries
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

TEST_P(SegmentReuploadFixture, test_new_segment_upload_fuzz) {
    cloud_storage::partition_manifest m(
      manifest_ntp, model::initial_revision_id{0});

    populate_log(log_spec{
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
                    ->make_reader(
                      storage::log_reader_config{model::offset{0}, last_offset})
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
        auto expected_size = get_range_size(start_bound.min(), end_bound.max());

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
          expected_size
            * 10, // Make sure the size limit is not affecting anything
          end_bound.max() /* end_inclusive  */,
          lso(),        /* end_exclusive */
          std::nullopt, /* flush_offset */
        };

        ASSERT_TRUE(closed_range_collector.collect_segments());

        check_stream(
          closed_range_collector,
          stream_descriptor{
            .start_offset = start_bound.min(),
            .end_offset = end_bound.max(),
            .check_size =
              [&expected_size](size_t s) { ASSERT_EQ(s, expected_size); },
          });

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
                        auto chk = get_range_size(
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

        ASSERT_TRUE(open_range_collector.collect_segments());
        auto r = try_make_candidate(open_range_collector);
        std::get<Stream>(r).close().get();
        check_stream(
          open_range_collector,
          stream_descriptor{
            .start_offset = start_bound.min(),
            .end_offset = end_bound.max(),
            .check_size =
              [&expected_size](size_t s) { ASSERT_EQ(s, expected_size); },
          });
    }
}

INSTANTIATE_TEST_SUITE_P(
  SegmentReuploadFixtureTest,
  SegmentReuploadFixture,
  ::testing::Values(
    // model::cloud_storage_segment_upload_mode::v1,
    model::cloud_storage_segment_upload_mode::v2));
