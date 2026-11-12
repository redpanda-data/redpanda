/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/tests/common_def.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "random/generators.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

static const model::ntp test_ntp(
  model::kafka_namespace, model::topic("test-topic"), model::partition_id(0));

BOOST_AUTO_TEST_CASE(remote_segment_index_search_test) {
    // This value is a power of two - 1 on purpose. This way we
    // will read from the compressed part and from the buffer of
    // recent values. This is because the row widht is 16 and the
    // buffer has 16 elements. 1024 is 64 rows and 1023 is 63
    // rows + almost full buffer.
    size_t segment_num_batches = 1023;
    model::offset segment_base_rp_offset{1234};
    kafka::offset segment_base_kaf_offset{1210};

    std::vector<model::offset> rp_offsets;
    std::vector<kafka::offset> kaf_offsets;
    std::vector<size_t> file_offsets;
    std::vector<model::timestamp> timestamps;
    int64_t rp = segment_base_rp_offset();
    int64_t kaf = segment_base_kaf_offset();
    size_t fpos = random_generators::get_int(1000, 2000);
    model::timestamp timestamp{123456};
    bool is_config = false;
    for (size_t i = 0; i < segment_num_batches; i++) {
        if (!is_config) {
            rp_offsets.push_back(model::offset(rp));
            kaf_offsets.push_back(kafka::offset(kaf));
            file_offsets.push_back(fpos);
            timestamps.push_back(timestamp);
        }
        // The test queries every element using the key that matches the element
        // exactly and then it queries the element using the key which is
        // smaller than the element. In order to do this we need a way to
        // guarantee that the distance between to elements in the sequence is at
        // least 2, so we can decrement the key safely.
        auto batch_size = random_generators::get_int(2, 100);
        is_config = random_generators::get_int(20) == 0;
        rp += batch_size;
        kaf += is_config ? batch_size - 1 : batch_size;
        fpos += random_generators::get_int(1000, 2000);
        timestamp = model::timestamp(timestamp.value() + 1);
    }

    offset_index tmp_index(
      segment_base_rp_offset,
      segment_base_kaf_offset,
      0U,
      1000,
      model::timestamp{0xdeadbeef});
    model::offset last;
    kafka::offset klast;
    size_t flast;
    for (size_t i = 0; i < rp_offsets.size(); i++) {
        tmp_index.add(
          rp_offsets.at(i),
          kaf_offsets.at(i),
          file_offsets.at(i),
          timestamps.at(i));
        last = rp_offsets.at(i);
        klast = kaf_offsets.at(i);
        flast = file_offsets.at(i);
    }

    offset_index index(
      segment_base_rp_offset,
      segment_base_kaf_offset,
      0U,
      1000,
      model::timestamp{0xdeadbeef});
    auto buf = tmp_index.to_iobuf();
    index.from_iobuf(std::move(buf));

    // Query element before the first one
    auto opt_first = index.find_rp_offset(
      segment_base_rp_offset - model::offset(1));
    BOOST_REQUIRE(!opt_first.has_value());

    auto kopt_first = index.find_kaf_offset(
      segment_base_kaf_offset - kafka::offset(1));
    BOOST_REQUIRE(!kopt_first.has_value());

    for (unsigned ix = 0; ix < rp_offsets.size(); ix++) {
        auto opt = index.find_rp_offset(rp_offsets[ix] + model::offset(1));
        auto [rp, kaf, fpos] = *opt;
        BOOST_REQUIRE_EQUAL(rp, rp_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kaf, kaf_offsets[ix]);
        BOOST_REQUIRE_EQUAL(fpos, file_offsets[ix]);

        auto kopt = index.find_kaf_offset(kaf_offsets[ix] + model::offset(1));
        BOOST_REQUIRE_EQUAL(kopt->rp_offset, rp_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kopt->kaf_offset, kaf_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kopt->file_pos, file_offsets[ix]);
    }

    // Query after the last element
    auto opt_last = index.find_rp_offset(last + model::offset(1));
    auto [rp_last, kaf_last, file_last] = *opt_last;
    BOOST_REQUIRE_EQUAL(rp_last, last);
    BOOST_REQUIRE_EQUAL(kaf_last, klast);
    BOOST_REQUIRE_EQUAL(file_last, flast);

    auto kopt_last = index.find_kaf_offset(klast + kafka::offset(1));
    BOOST_REQUIRE_EQUAL(kopt_last->rp_offset, last);
    BOOST_REQUIRE_EQUAL(kopt_last->kaf_offset, klast);
    BOOST_REQUIRE_EQUAL(kopt_last->file_pos, flast);
}

SEASTAR_THREAD_TEST_CASE(test_remote_segment_index_builder) {
    static const model::offset base_offset{100};
    static const kafka::offset kbase_offset{100};
    std::vector<batch_t> batches;
    for (int i = 0; i < 1000; i++) {
        auto num_records = random_generators::get_int(1, 20);
        std::vector<size_t> record_sizes;
        for (int i = 0; i < num_records; i++) {
            record_sizes.push_back(random_generators::get_int(1, 100));
        }
        batch_t batch = {
          .num_records = num_records,
          .type = model::record_batch_type::raft_data,
          .record_sizes = std::move(record_sizes),
        };
        batches.push_back(std::move(batch));
    }
    auto [segment, co] = generate_segment(base_offset, batches);
    auto is = make_iobuf_input_stream(std::move(segment));
    offset_index ix(
      base_offset, kbase_offset, 0, 0, model::timestamp{0xdeadbeef});
    auto parser = make_remote_segment_index_builder(
      test_ntp, std::move(is), ix, model::offset_delta(0), 0);
    auto result = parser->consume().get();
    BOOST_REQUIRE(result.has_value());
    BOOST_REQUIRE(result.value() != 0);
    parser->close().get();

    auto offset = base_offset;
    auto koffset = kbase_offset;
    for (const auto& batch : batches) {
        auto res = ix.find_rp_offset(offset + model::offset(1));
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res->rp_offset, offset);
        BOOST_REQUIRE_EQUAL(res->kaf_offset, koffset);

        offset += batch.num_records;
        koffset += batch.num_records;
    }
}

BOOST_AUTO_TEST_CASE(remote_segment_index_find_timestamp_test) {
    // 100 entries: 6 full compressed rows plus a partial write buffer, so
    // both storage tiers of the index are covered by the queries below.
    const size_t num_entries = 100;
    const int64_t base_ts = 1000;
    const int64_t step = 10;
    const model::offset base_rp{100};
    const kafka::offset base_kaf{50};

    offset_index index(base_rp, base_kaf, 0, 1000, model::timestamp{base_ts});
    for (size_t i = 0; i < num_entries; i++) {
        index.add(
          base_rp + model::offset(i),
          base_kaf + kafka::offset(i),
          static_cast<int64_t>((i + 1) * 2000),
          model::timestamp(base_ts + step * static_cast<int64_t>(i)));
    }

    // Nothing is provably below a timestamp at or before the first entry:
    // the caller has to scan from the start of the segment.
    BOOST_REQUIRE(!index.find_timestamp(model::timestamp{base_ts - 1}));
    BOOST_REQUIRE(!index.find_timestamp(model::timestamp{base_ts}));

    for (size_t i = 1; i < num_entries; i++) {
        // A query matching entry i exactly must start scanning at entry
        // i-1: entry values are running maxima including their own batch,
        // so the first matching record may sit in the gap before entry i.
        auto ts = model::timestamp(base_ts + step * static_cast<int64_t>(i));
        auto res = index.find_timestamp(ts);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res->rp_offset, base_rp + model::offset(i - 1));
        BOOST_REQUIRE_EQUAL(res->kaf_offset, base_kaf + kafka::offset(i - 1));
        BOOST_REQUIRE_EQUAL(res->file_pos, i * 2000);

        // A query between entries i and i+1 starts at entry i.
        auto mid = index.find_timestamp(model::timestamp(ts.value() + 1));
        BOOST_REQUIRE(mid.has_value());
        BOOST_REQUIRE_EQUAL(mid->rp_offset, base_rp + model::offset(i));
    }

    // A query above all entries starts at the last entry.
    auto last = index.find_timestamp(
      model::timestamp(base_ts + step * static_cast<int64_t>(num_entries)));
    BOOST_REQUIRE(last.has_value());
    BOOST_REQUIRE_EQUAL(
      last->rp_offset, base_rp + model::offset(num_entries - 1));
}

BOOST_AUTO_TEST_CASE(remote_segment_index_find_timestamp_non_monotonic_test) {
    // Indexes written before entries became running maxima may hold
    // non-monotonic per-batch timestamps. Such entries cannot bound the
    // scan start (a matching batch may hide in a sampling gap), so any
    // query whose scan observes a decrease must return nullopt.
    offset_index index(
      model::offset{0}, kafka::offset{0}, 0, 1000, model::timestamp{1000});
    const std::vector<int64_t> timestamps{1000, 1010, 990, 1020};
    for (size_t i = 0; i < timestamps.size(); i++) {
        index.add(
          model::offset(i),
          kafka::offset(i),
          static_cast<int64_t>((i + 1) * 2000),
          model::timestamp(timestamps[i]));
    }

    // Stops at entry 1 (1010 >= 1005) before observing the decrease:
    // entries 0..1 are trustworthy and bound the scan.
    auto res = index.find_timestamp(model::timestamp{1005});
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE_EQUAL(res->rp_offset, model::offset(0));

    // These scans reach the 1010 -> 990 decrease: bail out.
    BOOST_REQUIRE(!index.find_timestamp(model::timestamp{1015}));
    BOOST_REQUIRE(!index.find_timestamp(model::timestamp{2000}));

    // First entry is already at or above the query.
    BOOST_REQUIRE(!index.find_timestamp(model::timestamp{995}));
}

SEASTAR_THREAD_TEST_CASE(test_remote_segment_index_builder_running_max) {
    // A single batch far in the future (the "spike") followed by batches
    // that drop back below it, sized so that the sampling step skips the
    // spike batch entirely. The builder must index running maxima: the
    // first sampled batch after the spike then carries the spike's
    // timestamp, so a query between the pre-spike max and the spike
    // starts its scan before the spike batch. An index of raw per-batch
    // timestamps would never cross such a query and the scan would start
    // at the end of the segment, skipping the only matching record.
    static const model::offset base_offset{0};
    static const kafka::offset kbase_offset{0};
    constexpr int records_per_batch = 10;
    constexpr size_t spike_index = 10;
    constexpr size_t num_batches = 21;
    constexpr int64_t spike_ts = 50000;
    constexpr size_t sampling_step = 4000;

    std::vector<batch_t> batches;
    for (size_t i = 0; i < num_batches; i++) {
        // Batches 9-11 are far below the sampling step so the window
        // reset caused by sampling batch 9 leaves batches 10 (the spike)
        // and 11 unsampled; every other batch exceeds the step on its
        // own.
        const size_t record_size = (i >= 9 && i <= 11) ? 10 : 450;
        batches.push_back(
          batch_t{
            .num_records = records_per_batch,
            .type = model::record_batch_type::raft_data,
            .record_sizes = std::vector<size_t>(records_per_batch, record_size),
            .timestamp = i == spike_index
                           ? model::timestamp(spike_ts)
                           : model::timestamp(
                               1000 + 100 * static_cast<int64_t>(i)),
          });
    }
    auto [segment, co] = generate_segment(base_offset, batches);
    auto is = make_iobuf_input_stream(std::move(segment));
    offset_index ix(
      base_offset, kbase_offset, 0, 0, model::timestamp{0xdeadbeef});
    auto parser = make_remote_segment_index_builder(
      test_ntp, std::move(is), ix, model::offset_delta(0), sampling_step);
    auto pclose = ss::defer([&parser] { parser->close().get(); });
    auto result = parser->consume().get();
    BOOST_REQUIRE(result.has_value());

    // Query above every non-spike batch but below the spike: the first
    // matching record is in the (unsampled) spike batch, so the scan must
    // start at or before the last batch preceding it.
    auto res = ix.find_timestamp(model::timestamp{20000});
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE_LE(
      res->rp_offset,
      base_offset + model::offset((spike_index - 1) * records_per_batch));

    // Query above the spike (= above everything): the running max keeps
    // every entry below it, so the scan starts at the last entry and the
    // caller finds no match.
    auto above = ix.find_timestamp(model::timestamp{spike_ts + 100});
    BOOST_REQUIRE(above.has_value());
    BOOST_REQUIRE_GT(
      above->rp_offset,
      base_offset + model::offset(spike_index * records_per_batch));
}

SEASTAR_THREAD_TEST_CASE(test_remote_segment_build_coarse_index) {
    const model::offset base_offset{100};
    const kafka::offset kbase_offset{100};
    std::vector<batch_t> batches;
    model::offset expected_base_offset = base_offset, expected_last_offset;
    size_t expected_conf_records = 0;
    size_t expected_data_records = 0;
    for (int i = 0; i < 1000; i++) {
        auto num_records = random_generators::get_int(1, 20);
        std::vector<size_t> record_sizes;
        record_sizes.reserve(num_records);
        for (int i = 0; i < num_records; i++) {
            record_sizes.push_back(random_generators::get_int(1, 100));
        }
        batch_t batch = {
          .num_records = num_records,
          .type = model::record_batch_type::raft_data,
          .record_sizes = std::move(record_sizes),
        };
        batches.push_back(std::move(batch));
        expected_data_records += num_records;
    }
    expected_last_offset = base_offset
                           + model::offset(
                             expected_conf_records + expected_data_records - 1);
    auto [segment, so] = generate_segment(base_offset, batches);
    auto is = make_iobuf_input_stream(std::move(segment));
    offset_index ix(
      base_offset, kbase_offset, 0, 0, model::timestamp{0xdeadbeef});
    segment_record_stats stats{};
    auto parser = make_remote_segment_index_builder(
      test_ntp, std::move(is), ix, model::offset_delta(0), 0, std::ref(stats));
    auto pclose = ss::defer([&parser] { parser->close().get(); });
    auto result = parser->consume().get();
    BOOST_REQUIRE(result.has_value());
    BOOST_REQUIRE_NE(result.value(), 0);

    BOOST_REQUIRE_EQUAL(stats.total_conf_records, expected_conf_records);
    BOOST_REQUIRE_EQUAL(stats.total_data_records, expected_data_records);
    BOOST_REQUIRE_EQUAL(stats.base_rp_offset, expected_base_offset);
    BOOST_REQUIRE_EQUAL(stats.last_rp_offset, expected_last_offset);

    auto mini_ix = ix.build_coarse_index(100_KiB, "test");
    absl::btree_map<int64_t, kafka::offset> file_to_koffset;
    std::transform(
      std::make_move_iterator(mini_ix.begin()),
      std::make_move_iterator(mini_ix.end()),
      std::inserter(file_to_koffset, file_to_koffset.end()),
      [](auto pair) { return std::make_pair(pair.second, pair.first); });

    // Assert that all entries in the mini-map are approximately as far away as
    // step size. Additionally all kafka offsets should be ascending.
    for (auto it_a = file_to_koffset.cbegin(), it_b = std::next(it_a);
         it_b != file_to_koffset.cend();
         ++it_a, ++it_b) {
        BOOST_REQUIRE_GE(it_b->first - it_a->first, 100_KiB);
        BOOST_REQUIRE_GT(it_b->second, it_a->second);
    }
}

namespace cloud_storage {
class offset_index_accessor {
public:
    explicit offset_index_accessor(const offset_index& ix)
      : _ix{ix} {}

    size_t file_offset_index_size() const {
        return _ix._file_index.get_row_count();
    }

    size_t file_offset_array_size() const {
        auto sz = 0;
        for (const auto& offset : _ix._file_offsets) {
            if (offset != 0) {
                ++sz;
            }
        }
        return sz;
    }

private:
    const offset_index& _ix;
};
} // namespace cloud_storage

SEASTAR_THREAD_TEST_CASE(
  test_remote_segment_build_coarse_index_from_offset_fields) {
    // This test asserts that for an index where all the data is contained in
    // the `_offsets` arrays and the `_index` fields are empty, the coarse index
    // is still generated correctly.
    const model::offset base_offset{100};
    const kafka::offset kbase_offset{100};

    std::vector<batch_t> batches;
    // Create only 10 batches, so that the index fields in remote segment index
    // remain empty
    for (int i = 0; i < 10; i++) {
        auto num_records = random_generators::get_int(10, 20);
        std::vector<size_t> record_sizes;
        record_sizes.reserve(num_records);
        for (int i = 0; i < num_records; i++) {
            // The record size is large enough that we exceed the coarse
            // index threshold
            record_sizes.push_back(random_generators::get_int(950, 1000));
        }
        batch_t batch = {
          .num_records = num_records,
          .type = model::record_batch_type::raft_data,
          .record_sizes = std::move(record_sizes),
        };
        batches.push_back(std::move(batch));
    }
    auto [segment, co] = generate_segment(base_offset, batches);
    auto is = make_iobuf_input_stream(std::move(segment));
    offset_index ix(
      base_offset, kbase_offset, 0, 0, model::timestamp{0xdeadbeef});
    auto parser = make_remote_segment_index_builder(
      test_ntp, std::move(is), ix, model::offset_delta(0), 0);
    auto pclose = ss::defer([&parser] { parser->close().get(); });
    auto result = parser->consume().get();
    BOOST_REQUIRE(result.has_value());
    BOOST_REQUIRE_NE(result.value(), 0);

    offset_index_accessor acc{ix};
    BOOST_REQUIRE_EQUAL(acc.file_offset_index_size(), 0);
    BOOST_REQUIRE_GT(acc.file_offset_array_size(), 0);

    auto mini_ix = ix.build_coarse_index(70_KiB, "test");
    absl::btree_map<int64_t, kafka::offset> file_to_koffset;
    std::transform(
      std::make_move_iterator(mini_ix.begin()),
      std::make_move_iterator(mini_ix.end()),
      std::inserter(file_to_koffset, file_to_koffset.end()),
      [](auto pair) { return std::make_pair(pair.second, pair.first); });

    BOOST_REQUIRE_GT(file_to_koffset.size(), 0);
    for (auto it_a = file_to_koffset.cbegin(), it_b = std::next(it_a);
         it_b != file_to_koffset.cend();
         ++it_a, ++it_b) {
        BOOST_REQUIRE_GE(it_b->first - it_a->first, 70_KiB);
        BOOST_REQUIRE_GT(it_b->second, it_a->second);
    }
}
