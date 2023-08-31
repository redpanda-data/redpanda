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

#include "transform/tests/test_fixture.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

#include <exception>

namespace transform::testing {
ss::future<> fake_sink::write(ss::chunked_fifo<model::record_batch> batches) {
    for (auto& batch : batches) {
        co_await _batches.push_eventually(std::move(batch));
    }
}
ss::future<model::record_batch> fake_sink::read() {
    return _batches.pop_eventually();
}
ss::future<model::offset> fake_source::load_latest_offset() {
    co_return _latest_offset;
}
ss::future<model::record_batch_reader>
fake_source::read_batch(model::offset offset, ss::abort_source* as) {
    EXPECT_EQ(offset, _latest_offset);
    if (!_batches.empty()) {
        model::record_batch_reader::data_t batches;
        while (!_batches.empty()) {
            batches.push_back(_batches.pop());
        }
        _latest_offset = model::next_offset(batches.back().last_offset());
        co_return model::make_memory_record_batch_reader(std::move(batches));
    }
    auto sub = as->subscribe(
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
      [this](const std::optional<std::exception_ptr>& ex) noexcept {
          _batches.abort(ex.value_or(
            std::make_exception_ptr(ss::abort_requested_exception())));
      });
    if (!sub) {
        _batches.abort(
          std::make_exception_ptr(ss::abort_requested_exception()));
    }
    auto batch = co_await _batches.pop_eventually();
    _latest_offset = model::next_offset(batch.last_offset());
    sub->unlink();
    co_return model::make_memory_record_batch_reader(std::move(batch));
}
ss::future<> fake_source::push_batch(model::record_batch batch) {
    co_await _batches.push_eventually(std::move(batch));
}
std::string_view fake_wasm_engine::function_name() const {
    return my_metadata.name();
}
uint64_t fake_wasm_engine::memory_usage_size_bytes() const {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    return 64_KiB;
};
ss::future<> fake_wasm_engine::start() { return ss::now(); }
ss::future<> fake_wasm_engine::initialize() { return ss::now(); }
ss::future<> fake_wasm_engine::stop() { return ss::now(); }
} // namespace transform::testing
