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

#include "model/fundamental.h"
#include "transform/logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

#include <exception>
#include <iostream>

namespace transform::testing {
ss::future<> fake_sink::write(ss::chunked_fifo<model::record_batch> batches) {
    for (auto& batch : batches) {
        _batches.push_back(std::move(batch));
    }
    _cond_var.broadcast();
    co_return;
}
ss::future<model::record_batch> fake_sink::read() {
    co_await _cond_var.wait(1s, [this] { return !_batches.empty(); });
    auto batch = std::move(_batches.front());
    _batches.pop_front();
    co_return batch;
}

kafka::offset fake_source::latest_offset() {
    if (_batches.empty()) {
        return kafka::offset(0);
    }
    return kafka::next_offset(_batches.rbegin()->first);
}

ss::future<model::record_batch_reader>
fake_source::read_batch(kafka::offset offset, ss::abort_source* as) {
    auto sub = as->subscribe([this]() noexcept { _cond_var.broadcast(); });
    co_await _cond_var.wait([this, as, offset] {
        if (as->abort_requested()) {
            return true;
        }
        auto it = _batches.lower_bound(offset);
        return it != _batches.end();
    });
    as->check();
    auto it = _batches.lower_bound(offset);
    co_return model::make_memory_record_batch_reader(it->second.copy());
}

ss::future<> fake_source::push_batch(model::record_batch batch) {
    _batches.emplace(model::offset_cast(batch.last_offset()), std::move(batch));
    _cond_var.broadcast();
    co_return;
}
ss::future<> fake_wasm_engine::start() { return ss::now(); }
ss::future<> fake_wasm_engine::stop() { return ss::now(); }

ss::future<> fake_offset_tracker::commit_offset(kafka::offset o) {
    _committed = o;
    _cond_var.broadcast();
    co_return;
}

ss::future<std::optional<kafka::offset>>
fake_offset_tracker::load_committed_offset() {
    co_return _committed;
}

ss::future<> fake_offset_tracker::wait_for_committed_offset(kafka::offset o) {
    return _cond_var.wait(
      1s, [this, o] { return _committed && *_committed >= o; });
}

} // namespace transform::testing
