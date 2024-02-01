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
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "test_utils/async.h"
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
#include <unistd.h>
#include <vector>

namespace transform {
namespace {
class ProcessorTestFixture
  : public ::testing::TestWithParam<model::transform_metadata> {
public:
    void SetUp() override {
        auto engine = ss::make_shared<testing::fake_wasm_engine>();
        _engine = engine.get();
        auto src = std::make_unique<testing::fake_source>();
        _src = src.get();
        std::vector<std::unique_ptr<transform::sink>> sinks;
        for (size_t i = 0; i < GetParam().output_topics.size(); ++i) {
            auto sink = std::make_unique<testing::fake_sink>();
            _sinks.push_back(sink.get());
            sinks.push_back(std::move(sink));
        }
        auto offset_tracker = std::make_unique<testing::fake_offset_tracker>();
        _offset_tracker = offset_tracker.get();
        _probe.setup_metrics(GetParam());
        _p = std::make_unique<transform::processor>(
          testing::my_transform_id,
          testing::my_ntp,
          GetParam(),
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

    bool wait_for_committed_offset(model::output_topic_index idx) {
        return wait_for_committed_offset(idx, kafka::prev_offset(_offset));
    }
    bool wait_for_committed_offset(model::offset o) {
        return wait_for_committed_offset(model::offset_cast(o));
    }
    bool wait_for_committed_offset(kafka::offset o) {
        for (auto idx : output_topics()) {
            if (!wait_for_committed_offset(idx, o)) {
                return false;
            }
        }
        return true;
    }
    bool
    wait_for_committed_offset(model::output_topic_index i, model::offset o) {
        return wait_for_committed_offset(i, model::offset_cast(o));
    }
    bool
    wait_for_committed_offset(model::output_topic_index i, kafka::offset o) {
        try {
            _offset_tracker->wait_for_committed_offset(i, o).get();
            return true;
        } catch (const ss::condition_variable_timed_out&) {
            return false;
        }
    }
    bool wait_for_all_committed() {
        return wait_for_committed_offset(kafka::prev_offset(_offset));
    }
    auto committed_offsets() {
        return _offset_tracker->load_committed_offsets().get();
    }

    void set_default_output() { _engine->set_use_default_output_topic(); }
    void set_devnull_output() { _engine->set_output_topics({}); }
    void set_tee_output() {
        std::vector<model::topic> topics;
        for (const auto& tp_ns : GetParam().output_topics) {
            topics.push_back(tp_ns.tp);
        }
        _engine->set_output_topics(std::move(topics));
    }

    std::vector<model::record> make_records(size_t n) {
        std::vector<model::record> records;
        std::generate_n(std::back_inserter(records), n, [&records] {
            return model::test::make_random_record(
              int(records.size()), random_generators::make_iobuf());
        });
        return records;
    }

    // Push a batch into the source returning the current max offset of the
    // source after the push.
    kafka::offset push_batch(const std::vector<model::record>& records) {
        ss::chunked_fifo<model::transformed_data> data;
        for (const auto& r : records) {
            data.push_back(model::transformed_data::from_record(r.copy()));
        }
        auto batch = model::transformed_data::make_batch(
          model::timestamp::now(), std::move(data));
        batch.header().base_offset = kafka::offset_cast(_offset);
        _offset += batch.record_count();
        _src->push_batch(std::move(batch)).get();
        return kafka::prev_offset(_offset);
    }

    // Push a batch of size 1, returns the new max offset.
    kafka::offset push_record(const model::record& record) {
        std::vector<model::record> batch;
        batch.push_back(record.copy());
        return push_batch(batch);
    }

    std::vector<model::record>
    read_records(model::output_topic_index idx, size_t n) {
        std::vector<model::record> records;
        while (n > records.size()) {
            auto read = _sinks[idx()]->read().get().copy_records();
            std::move(read.begin(), read.end(), std::back_inserter(records));
        }
        return records;
    }
    std::vector<model::record> read_records(size_t n) {
        return read_records({}, n);
    }
    bool sink_empty(model::output_topic_index idx) {
        return _sinks[idx()]->empty();
    }
    uint64_t error_count() const { return _error_count; }
    int64_t lag() const { return _p->current_lag(); }

    void cork_sink(model::output_topic_index idx) { _sinks[idx()]->cork(); }
    void uncork_sink(model::output_topic_index idx) { _sinks[idx()]->uncork(); }

    std::vector<model::output_topic_index> output_topics() const {
        std::vector<model::output_topic_index> indexes;
        size_t size = GetParam().output_topics.size();
        indexes.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            indexes.emplace_back(i);
        }
        return indexes;
    }

    void restart() {
        stop();
        start();
    }
    void stop() { _p->stop().get(); }
    void start() { _p->start().get(); }

    ss::future<> initiate_stop() { return _p->stop(); }

private:
    static constexpr kafka::offset start_offset = kafka::offset(0);

    kafka::offset _offset = start_offset;
    std::unique_ptr<transform::processor> _p;
    testing::fake_wasm_engine* _engine = nullptr;
    testing::fake_source* _src = nullptr;
    testing::fake_offset_tracker* _offset_tracker = nullptr;
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

TEST_P(ProcessorTestFixture, HandlesDoubleStops) {
    stop();
    stop();
}

TEST_P(ProcessorTestFixture, HandlesDoubleStarts) { start(); }

TEST_P(ProcessorTestFixture, ProcessOne) {
    auto batch = make_records(1);
    push_batch(batch);
    auto returned = read_records(1);
    EXPECT_THAT(returned, SameRecords(batch));
    EXPECT_EQ(error_count(), 0);
}

TEST_P(ProcessorTestFixture, ProcessMany) {
    constexpr size_t n = 32;
    auto batch = make_records(n);
    push_batch(batch);
    auto returned = read_records(n);
    EXPECT_THAT(returned, SameRecords(batch));
    EXPECT_EQ(error_count(), 0);
}

TEST_P(ProcessorTestFixture, TracksOffsets) {
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

TEST_P(ProcessorTestFixture, HandlesEmptyBatches) {
    auto batch_one = make_records(1);
    push_batch(batch_one);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));

    auto batch_two = make_records(1);
    set_devnull_output();
    push_batch(batch_two);
    // We never will read batch two, it was filtered out
    // but we should still get a commit for batch two
    ASSERT_TRUE(wait_for_all_committed());

    auto batch_three = make_records(1);
    set_default_output();
    push_batch(batch_three);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_three));
}

TEST_P(ProcessorTestFixture, LagOffByOne) {
    EXPECT_EQ(lag(), 0);
    auto batch_one = make_records(1);
    push_batch(batch_one);
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));
    // With multiple output topics, we need to ensure all outputs have reported
    // their lag.
    tests::drain_task_queue().get();
    EXPECT_EQ(lag(), 0);
}

TEST_P(ProcessorTestFixture, LagOverflowBug) {
    stop();
    auto batch_one = make_records(1);
    push_batch(batch_one);
    start();
    ASSERT_TRUE(wait_for_all_committed());
    EXPECT_THAT(read_records(1), SameRecords(batch_one));
    // With multiple output topics, we need to ensure all outputs have reported
    // their lag.
    tests::drain_task_queue().get();
    EXPECT_EQ(lag(), 0);
}

INSTANTIATE_TEST_SUITE_P(
  GenericProcessorTest,
  ProcessorTestFixture,
  ::testing::Values(
    testing::my_single_output_metadata, testing::my_multiple_output_metadata));

// Alias the test name so that we can write specialized tests for multiple
// output topics.
using MultipleOutputsProcessorTestFixture = ProcessorTestFixture;

TEST_P(MultipleOutputsProcessorTestFixture, ProcessOne) {
    set_tee_output();
    auto batch = make_records(1);
    push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, 1);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    EXPECT_EQ(error_count(), 0);
}

TEST_P(MultipleOutputsProcessorTestFixture, ProcessMany) {
    constexpr size_t n = 32;
    set_tee_output();
    auto batch = make_records(n);
    push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, n);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    EXPECT_EQ(error_count(), 0);
}

using ::testing::Contains;
using ::testing::Pair;

TEST_P(MultipleOutputsProcessorTestFixture, TracksProcessPerOutput) {
    set_tee_output();
    auto batch = make_records(1);
    auto initial_batch_offset = push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, 1);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    ASSERT_TRUE(wait_for_committed_offset(initial_batch_offset));
    // Pause writes for the last output
    auto last = output_topics().back();
    cork_sink(last);
    // Push a batch that all sinks get, but the last sink pauses on
    auto corked_batch = make_records(1);
    auto corked_offset = push_batch(corked_batch);
    for (auto output : output_topics()) {
        if (output == last) {
            tests::drain_task_queue().get();
            // We didn't make progress because the write is blocked
            EXPECT_TRUE(sink_empty(output));
            EXPECT_THAT(
              committed_offsets(),
              Contains(Pair(output, initial_batch_offset)));
        } else {
            auto returned = read_records(output, 1);
            EXPECT_THAT(returned, SameRecords(corked_batch));
            EXPECT_TRUE(wait_for_committed_offset(output, corked_offset));
        }
    }
    // Make progress without the last sink, which is stuck.
    auto latest_batch = make_records(1);
    auto latest_offset = push_batch(latest_batch);
    for (auto output : output_topics()) {
        if (output == last) {
            tests::drain_task_queue().get();
            // We didn't make progress because the write is blocked
            EXPECT_TRUE(sink_empty(output));
            EXPECT_THAT(
              committed_offsets(),
              Contains(Pair(output, initial_batch_offset)));
        } else {
            auto returned = read_records(output, 1);
            EXPECT_THAT(returned, SameRecords(latest_batch));
            EXPECT_TRUE(wait_for_committed_offset(output, latest_offset));
        }
    }
    // Attempt to stop, making as much progress as we can, but we won't be able
    // to complete stop as a sink is still corked.
    auto stop_fut = initiate_stop();
    ::tests::drain_task_queue().get();
    // Uncork the sink so that last sink commits the batch it was stuck on.
    uncork_sink(last);
    stop_fut.get();
    bool last_did_commit = false;
    for (auto output : output_topics()) {
        if (output == last) {
            auto returned = read_records(output, 1);
            EXPECT_THAT(returned, SameRecords(corked_batch));
            // We can't ensure that the producer picked up the progress message
            // before it stopped.
            //
            // Pragmatically speaking debug mode will likely not commit and
            // release mode will likely commit, so we get coverage of both
            // cases.
            last_did_commit = wait_for_committed_offset(output, corked_offset);
        } else {
            EXPECT_TRUE(sink_empty(output));
            EXPECT_TRUE(wait_for_committed_offset(output, latest_offset));
        }
    }
    // Start it back up and the last process should catch back up.
    start();
    if (!last_did_commit) {
        // Then we will replay the corked batch.
        auto returned = read_records(last, 1);
        EXPECT_THAT(returned, SameRecords(corked_batch));
        EXPECT_TRUE(wait_for_committed_offset(last, corked_offset));
    }
    // The last record will catchup to the others
    auto returned = read_records(last, 1);
    EXPECT_THAT(returned, SameRecords(latest_batch));
    EXPECT_TRUE(wait_for_all_committed());

    // Other outputs don't emit duplicates
    batch = make_records(1);
    push_batch(batch);
    for (auto output : output_topics()) {
        auto returned = read_records(output, 1);
        EXPECT_THAT(returned, SameRecords(batch));
    }
    EXPECT_TRUE(wait_for_all_committed());
    EXPECT_EQ(error_count(), 0);
}

INSTANTIATE_TEST_SUITE_P(
  MultipleOutputsProcessorTest,
  MultipleOutputsProcessorTestFixture,
  ::testing::Values(testing::my_multiple_output_metadata));

} // namespace transform
