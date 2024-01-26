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

#include "container/zip.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "transform/tests/test_fixture.h"
#include "transform/transform_processor.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <ranges>
#include <tuple>

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

    void wait_for_committed_offset(model::offset o) {
        wait_for_committed_offset(model::offset_cast(o));
    }
    void wait_for_committed_offset(kafka::offset o) {
        _offset_tracker->wait_for_committed_offset(o).get();
    }

    using transform_mode = testing::fake_wasm_engine::mode;
    void set_transform_mode(testing::fake_wasm_engine::mode m) {
        _engine->set_mode(m);
    }

    model::record_batch make_tiny_batch() {
        return model::test::make_random_batch(model::test::record_batch_spec{
          .offset = kafka::offset_cast(_offset++),
          .allow_compression = false,
          .count = 1});
    }
    void push_batch(model::record_batch batch) {
        _src->push_batch(std::move(batch)).get();
    }
    model::record_batch read_batch() { return _sinks[0]->read().get(); }
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

MATCHER_P(SameBatchRef, ref, "") {
    const model::record_batch& batch = ref.get();
    const auto& a = arg.copy_records();
    const auto& b = batch.copy_records();
    if (a.size() != b.size()) {
        *result_listener << "expected same size: " << a.size() << " vs "
                         << b.size();
        return false;
    }
    for (const auto& [a, b] : container::zip(a, b)) {
        if (a.key() != b.key()) {
            return false;
        }
        if (a.value() != b.value()) {
            return false;
        }
        if (a.headers() != b.headers()) {
            return false;
        }
    }
    return true;
}

auto SameBatch(const model::record_batch& batch) {
    return SameBatchRef(std::ref(batch));
}

} // namespace

TEST_F(ProcessorTestFixture, HandlesDoubleStops) {
    stop();
    stop();
}

TEST_F(ProcessorTestFixture, HandlesDoubleStarts) { start(); }

TEST_F(ProcessorTestFixture, ProcessOne) {
    auto batch = make_tiny_batch();
    push_batch(batch.share());
    auto returned = read_batch();
    EXPECT_THAT(batch, SameBatch(returned));
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, ProcessMany) {
    std::vector<model::record_batch> batches;
    constexpr int num_batches = 32;
    std::generate_n(std::back_inserter(batches), num_batches, [this] {
        return make_tiny_batch();
    });
    for (auto& b : batches) {
        push_batch(b.share());
    }
    for (auto& b : batches) {
        auto returned = read_batch();
        EXPECT_THAT(b, SameBatch(returned));
    }
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, TracksOffsets) {
    constexpr int num_batches = 32;
    std::vector<model::record_batch> first_batches;
    std::generate_n(std::back_inserter(first_batches), num_batches, [this] {
        return make_tiny_batch();
    });
    std::vector<model::record_batch> second_batches;
    std::generate_n(std::back_inserter(second_batches), num_batches, [this] {
        return make_tiny_batch();
    });
    for (auto& b : first_batches) {
        push_batch(b.share());
    }
    restart();
    for (auto& b : first_batches) {
        auto returned = read_batch();
        EXPECT_THAT(b, SameBatch(returned));
    }
    restart();
    for (auto& b : second_batches) {
        push_batch(b.share());
    }
    for (auto& b : second_batches) {
        auto returned = read_batch();
        EXPECT_THAT(b, SameBatch(returned));
    }
    EXPECT_EQ(error_count(), 0);
}

TEST_F(ProcessorTestFixture, HandlesEmptyBatches) {
    auto batch_one = make_tiny_batch();
    push_batch(batch_one.copy());
    wait_for_committed_offset(batch_one.last_offset());
    EXPECT_THAT(read_batch(), SameBatch(batch_one));

    auto batch_two = make_tiny_batch();
    set_transform_mode(transform_mode::filter);
    push_batch(batch_two.copy());
    // We never will read batch two, it was filtered out
    // but we should still get a commit for batch two
    wait_for_committed_offset(batch_two.last_offset());

    auto batch_three = make_tiny_batch();
    set_transform_mode(transform_mode::noop);
    push_batch(batch_three.copy());
    wait_for_committed_offset(batch_three.last_offset());
    EXPECT_THAT(read_batch(), SameBatch(batch_three));
}

TEST_F(ProcessorTestFixture, LagOffByOne) {
    EXPECT_EQ(lag(), 0);
    auto batch_one = make_tiny_batch();
    push_batch(batch_one.copy());
    wait_for_committed_offset(batch_one.last_offset());
    EXPECT_THAT(read_batch(), SameBatch(batch_one));
    EXPECT_EQ(lag(), 0);
}

TEST_F(ProcessorTestFixture, LagOverflowBug) {
    stop();
    auto batch_one = make_tiny_batch();
    push_batch(batch_one.copy());
    start();
    wait_for_committed_offset(batch_one.last_offset());
    EXPECT_THAT(read_batch(), SameBatch(batch_one));
    EXPECT_EQ(lag(), 0);
}

} // namespace transform
