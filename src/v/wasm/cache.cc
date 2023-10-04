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

#include "wasm/cache.h"

#include "wasm/logger.h"

#include <seastar/coroutine/as_future.hh>

namespace wasm {

namespace {

/**
 * Allows sharing an engine between multiple uses.
 *
 * Must live on a single core.
 */
class shared_engine : public engine {
public:
    explicit shared_engine(std::unique_ptr<engine> underlying)
      : _underlying(std::move(underlying)) {}

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        auto u = co_await _mu.get_units();
        auto fut = co_await ss::coroutine::as_future<model::record_batch>(
          _underlying->transform(std::move(batch), probe));
        if (!fut.failed()) {
            co_return fut.get();
        }
        // Restart the engine
        try {
            co_await _underlying->stop();
            co_await _underlying->start();
        } catch (...) {
            vlog(
              wasm_log.warn,
              "failed to restart wasm engine: {}",
              std::current_exception());
        }
        std::rethrow_exception(fut.get_exception());
    }

    ss::future<> start() override {
        auto u = co_await _mu.get_units();
        if (_ref_count++ == 0) {
            co_await _underlying->start();
        }
    }
    ss::future<> stop() override {
        vassert(
          _ref_count > 0, "expected a call to start before a call to stop");
        auto u = co_await _mu.get_units();
        if (--_ref_count == 0) {
            co_await _underlying->stop();
        }
    }

    uint64_t memory_usage_size_bytes() const override {
        return _underlying->memory_usage_size_bytes();
    }

private:
    mutex _mu;
    size_t _ref_count = 0;
    std::unique_ptr<engine> _underlying;
};
}


}
