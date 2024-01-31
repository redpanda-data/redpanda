/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/random.h"
#include "container/zip.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "transform/tests/test_fixture.h"
#include "transform/transform_processor.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <ranges>
#include <tuple>
#include <vector>

namespace transform {
namespace {
class ProcessorTestFixture : public ::testing::Test {
public:
    void SetUp() override {
        auto engine = ss::make_shared<testing::fake_wasm_engine>();
        _engine = engine.get();
        auto src = std::make_unique<testing::fake_source>();
        _src = src.get();
        auto sink = std::make_unique<testing::fake_sink>();
        std::vector<std::unique_ptr<transform::sink>> sinks;
        _sinks.push_back(sink.get());
        sinks.push_back(std::move(sink));
        auto offset_tracker = std::make_unique<testing::fake_offset_tracker>();
        _offset_tracker = offset_tracker.get();
        _probe.setup_metrics(testing::my_metadata);
        _p = std::make_unique<transform::processor>(
          testing::my_transform_id,
          testing::my_ntp,
          testing::my_metadata,
          std::move(engine),
          [this](auto, auto, processor::state state) {
              if (state == processor::state::errored) {
                  ++_error_count;
              }
          },
          std::move(src),
          std::move(sinks),
          std::move(offset_tracker),
          &_probe);
        _p->start().get();
        // Wait for the initial offset to be committed so we know that the
        // processor is actually ready, otherwise it could be possible that
        // the processor picks up after the initial records are added to the
        // partition.
        wait_for_committed_offset(kafka::offset{});
    }
    void TearDown() override { _p->stop().get(); }

    bool wait_for_committed_offset(model::offset o) {
        return wait_for_committed_offset(model::offset_cast(o));
    }
    bool wait_for_committed_offset(kafka::offset o) {
        try {
            _offset_tracker
              ->wait_for_committed_offset(model::output_topic_index(0), o)
              .get();
            return true;
        } catch (const ss::condition_variable_timed_out&) {
            return false;
        }
    }
    bool wait_for_all_committed() {
        return wait_for_committed_offset(kafka::prev_offset(_offset));
    }

    using transform_mode = testing::fake_wasm_engine::mode;
    void set_transform_mode(testing::fake_wasm_engine::mode m) {
        _engine->set_mode(m);
    }

    std::vector<model::record> make_records(size_t n) {
        std::vector<model::record> records;
        std::generate_n(std::back_inserter(records), n, [&records] {
            return model::test::make_random_record(
              int(records.size()), random_generators::make_iobuf());
        });
        return records;
    }

    void push_batch(const std::vector<model::record>& records) {
        ss::chunked_fifo<model::transformed_data> data;
        for (const auto& r : records) {
            data.push_back(model::transformed_data::from_record(r.copy()));
        }
        auto batch = model::transformed_data::make_batch(
          model::timestamp::now(), std::move(data));
        batch.header().base_offset = kafka::offset_cast(_offset);
        _offset += batch.record_count();
        _src->push_batch(std::move(batch)).get();
    }

    void push_record(const model::record& record) {
        std::vector<model::record> batch;
        batch.push_back(record.copy());
        push_batch(batch);
    }

    std::vector<model::record> read_records(size_t n) {
        std::vector<model::record> records;
        while (n > records.size()) {
            auto read = _sinks[0]->read().get().copy_records();
            std::move(read.begin(), read.end(), std::back_inserter(records));
        }
        return records;
    }
    uint64_t error_count() const { return _error_count; }
    int64_t lag() const { return _p->current_lag(); }

    void restart() {
        stop();
        start();
    }
    void stop() { _p->stop().get(); }
    void start() { _p->start().get(); }

private:
    static constexpr kafka::offset start_offset = kafka::offset(0);

    kafka::offset _offset = start_offset;
    std::unique_ptr<transform::processor> _p;
    testing::fake_wasm_engine* _engine;
    testing::fake_source* _src;
    testing::fake_offset_tracker* _offset_tracker;
    std::vector<testing::fake_sink*> _sinks;
    uint64_t _error_count = 0;
    probe _probe;
};

MATCHER(SameRecordEq, "") {
    const model::record& a = std::get<0>(arg);
    const model::record& b = std::get<1>(arg).get();
    if (a.key() != b.key()) {
        *result_listener << "expected same key: " << a.key() << " vs "
                         << b.key();
        return false;
    }
    if (a.value() != b.value()) {
        *result_listener << "expected same value: " << a.value() << " vs "
                         << b.value();
        return false;
    }
    if (a.headers() != b.headers()) {
        *result_listener << "expected same headers: " << a.headers() << " vs "
                         << b.headers();
        return false;
    }
    return true;
}

// A helper to ensure all records have the same key/value/headers (but doesn't
// check other metadata).
auto SameRecords(std::vector<model::record>& expected) {
    std::vector<std::reference_wrapper<const model::record>> expected_refs;
    expected_refs.reserve(expected.size());
    for (const auto& r : expected) {
        expected_refs.push_back(std::ref(r));
    }
    return ::testing::Pointwise(SameRecordEq(), expected_refs);
}

} // namespace

TEST_F(ProcessorTestFixture, HandlesDoubleStops) {
    stop();
    stop();
}

TEST_F(ProcessorTestFixture, HandlesDoubleStarts) { start(); }

TEST_F(ProcessorTestFixture, ProcessOne) {
    auto batch = make_records(1);
    push_batch(batch);
    auto returned = read_records(1);
    EXPECT_THAT(returned, SameRecords(batch));
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, ProcessMany) {
    constexpr size_t n = 32;
    auto batch = make_records(n);
    push_batch(batch);
    auto returned = read_records(n);
    EXPECT_THAT(returned, SameRecords(batch));
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, TracksOffsets) {
    constexpr int num_records = 32;
    auto first_batches = make_records(num_records);
    auto second_batches = make_records(num_records);
    for (auto& b : first_batches) {
        push_record(b.share());
    }
    auto returned = read_records(num_records);
    EXPECT_THAT(returned, SameRecords(first_batches)) << "first batch mismatch";
    // If we don't wait for the last commit to happen, it's possible that
    // we restart and get duplicates.
    ASSERT_TRUE(wait_for_all_committed());
    restart();
    for (auto& b : second_batches) {
        push_record(b.share());
    }
    returned = read_records(num_records);
    EXPECT_THAT(returned, SameRecords(second_batches))
      << "second batch mismatch";
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, HandlesEmptyBatches) {
    auto batch_one = make_records(1);
    push_batch(batch_one);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));

    auto batch_two = make_records(1);
    set_transform_mode(transform_mode::filter);
    push_batch(batch_two);
    // We never will read batch two, it was filtered out
    // but we should still get a commit for batch two
    ASSERT_TRUE(wait_for_all_committed());

    auto batch_three = make_records(1);
    set_transform_mode(transform_mode::noop);
    push_batch(batch_three);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_three));
}

TEST_F(ProcessorTestFixture, LagOffByOne) {
    EXPECT_EQ(lag(), 0);
    auto batch_one = make_records(1);
    push_batch(batch_one);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));
    EXPECT_EQ(lag(), 0);
}

TEST_F(ProcessorTestFixture, LagOverflowBug) {
    stop();
    auto batch_one = make_records(1);
    push_batch(batch_one);
    start();
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));
    EXPECT_EQ(lag(), 0);
}

} // namespace transform
