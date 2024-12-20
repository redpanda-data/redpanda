// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "container/fragmented_vector.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/btree_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <exception>

using namespace std::chrono_literals;

namespace {
ss::logger cmp_testlog("cmp-fuzz");
} // anonymous namespace

static model::record_batch make_random_batch(
  model::offset offset,
  bool empty,
  model::record_batch_type type,
  std::vector<std::optional<ss::sstring>> keys,
  std::vector<std::optional<ss::sstring>> values,
  int num_records) {
    BOOST_REQUIRE(keys.size() == values.size());
    storage::record_batch_builder builder(type, offset);
    auto to_iobuf = [](std::optional<ss::sstring> x) {
        std::optional<iobuf> result;
        if (x.has_value()) {
            iobuf buf;
            buf.append(x->data(), x->size());
            result = std::move(buf);
        }
        return result;
    };
    if (!empty) {
        for (int i = 0; i < num_records; i++) {
            auto key = random_generators::random_choice(keys);
            auto val = random_generators::random_choice(values);
            builder.add_raw_kv(to_iobuf(key), to_iobuf(val));
        }
    }
    return std::move(builder).build();
}

static fragmented_vector<model::record_batch>
generate_random_record_batches(int num, int cardinality) {
    fragmented_vector<model::record_batch> result;
    std::vector<std::optional<ss::sstring>> keys;
    std::vector<std::optional<ss::sstring>> values;
    std::vector<model::record_batch_type> types{
      model::record_batch_type::raft_configuration,
      model::record_batch_type::raft_data,
      model::record_batch_type::archival_metadata,
    };
    for (int i = 0; i < cardinality; i++) {
        if (i == 0) {
            keys.emplace_back(std::nullopt);
        } else {
            keys.emplace_back(
              random_generators::gen_alphanum_string(20, false));
        }
        values.emplace_back(random_generators::gen_alphanum_string(20, false));
    }
    // Generate actual batches
    model::offset current{0};
    for (int i = 0; i < num; i++) {
        result.emplace_back(make_random_batch(
          current,
          false,
          random_generators::random_choice(types),
          keys,
          values,
          random_generators::get_int(1, 10)));
        current = model::next_offset(result.back().last_offset());
    }
    return result;
}

/// Offset translator state observed at some point in time
struct ot_state {
    std::deque<model::offset> gap_offset;
    std::deque<int64_t> gap_length;
};

/// Consumer that builds the map of all non-data
/// batches!
struct ot_state_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
        if (rb.header().type != model::record_batch_type::raft_data) {
            // save information about the non-data batch
            st->gap_offset.push_back(rb.base_offset());
            st->gap_length.push_back(rb.record_count());
        }
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    ot_state* st;
};

/// Insert data into the log and maintain particular
/// segment arrangement. The arrangement is defined
/// by the set of segment base offset values.
ss::future<ot_state> arrange_and_compact(
  const fragmented_vector<model::record_batch>& batches,
  std::deque<model::offset> arrangement) {
    std::sort(arrangement.begin(), arrangement.end());
    storage::disk_log_builder b1;
    std::exception_ptr error = nullptr;
    co_await b1.start();
    try {
        for (const auto& b : batches) {
            co_await b1.add_batch(b.copy());
            if (
              !arrangement.empty() && b.base_offset() >= arrangement.front()) {
                arrangement.pop_front();
                co_await b1.get_disk_log_impl().force_roll(
                  ss::default_priority_class());
            }
        }
        ss::abort_source as;
        co_await b1.apply_compaction(storage::compaction_config(
          batches.back().last_offset(),
          std::nullopt,
          ss::default_priority_class(),
          as));
    } catch (...) {
        error = std::current_exception();
    }
    auto reader = co_await b1.get_disk_log_impl().make_reader(
      storage::log_reader_config(
        model::offset{0}, model::offset::max(), ss::default_priority_class()));
    ot_state st{};
    co_await std::move(reader).consume(
      ot_state_consumer{.st = &st}, model::no_timeout);
    co_await b1.stop();
    if (error) {
        vlog(
          cmp_testlog.error,
          "Error triggered while appending or compacting: {}",
          error);
    }
    BOOST_REQUIRE(error == nullptr);
    co_return st;
}

/// This function generates random alignment based on the set of batches
/// that will be written into the log.
std::deque<model::offset> generate_random_arrangement(
  const fragmented_vector<model::record_batch>& batches, size_t num_segments) {
    BOOST_REQUIRE(num_segments <= batches.size());
    std::deque<model::offset> arr;
    // User reservoir sample to produce num_segments
    for (size_t i = 0; i < num_segments; i++) {
        arr.push_back(batches[i].base_offset());
    }
    for (size_t i = num_segments; i < batches.size(); i++) {
        auto r = random_generators::get_int<size_t>(0, i);
        if (r < num_segments) {
            arr[r] = batches[i].base_offset();
        }
    }
    return arr;
}

SEASTAR_THREAD_TEST_CASE(test_compaction_with_different_segment_arrangements) {
    auto batches = generate_random_record_batches(1000, 10);
    auto expected_ot
      = arrange_and_compact(batches, std::deque<model::offset>{}).get();
    std::vector<size_t> num_segments = {10, 100, 1000};
    for (auto num : num_segments) {
        auto arrangement = generate_random_arrangement(batches, num);
        auto actual_ot = arrange_and_compact(batches, arrangement).get();
        BOOST_REQUIRE(expected_ot.gap_offset == actual_ot.gap_offset);
        BOOST_REQUIRE(expected_ot.gap_length == actual_ot.gap_length);
    }
}
