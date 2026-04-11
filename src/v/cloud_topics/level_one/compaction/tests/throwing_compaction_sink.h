/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/sink.h"
#include "compaction/reducer.h"

#include <stdexcept>

namespace cloud_topics::l1 {

/// A test wrapper that composes a compaction_sink and injects controlled
/// object rolls and exceptions during operator(). Used to test the exceptional
/// path in compaction_sink::finalize(), where an inflight object must be
/// discarded rather than flushed.
class throwing_compaction_sink
  : public compaction::sliding_window_reducer::sink {
public:
    using predicate_t = ss::noncopyable_function<bool()>;

    /// \param inner        The compaction_sink to delegate to.
    /// \param should_roll  Called before each operator() delegation. If true,
    ///                     forces the inner sink to flush its current object
    ///                     and start a new one, simulating a roll due to
    ///                     max_object_size being exceeded.
    /// \param should_throw Called after each operator() delegation. If true,
    ///                     throws a std::runtime_error.
    throwing_compaction_sink(
      std::unique_ptr<compaction_sink> inner,
      predicate_t should_roll,
      predicate_t should_throw)
      : _inner(std::move(inner))
      , _should_roll(std::move(should_roll))
      , _should_throw(std::move(should_throw)) {}

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source& src) final {
        return _inner->initialize(src);
    }

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression c) final {
        if (_should_roll()) {
            auto next_offset = model::offset_cast(b.base_offset());
            auto prev_offset = kafka::prev_offset(next_offset);
            co_await _inner->flush(prev_offset);
            co_await _inner->initialize_builder(next_offset);
        }
        auto res = co_await (*_inner)(std::move(b), c);
        if (_should_throw()) {
            throw std::runtime_error(
              "throwing_compaction_sink: injected exception");
        }
        co_return res;
    }

    ss::future<> prepare_iteration(kafka::offset o) final {
        return _inner->prepare_iteration(o);
    }

    ss::future<>
    finish_iteration(kafka::offset base, kafka::offset last) final {
        return _inner->finish_iteration(base, last);
    }

    ss::future<> finalize(bool success) final {
        return _inner->finalize(success);
    }

private:
    std::unique_ptr<compaction_sink> _inner;
    predicate_t _should_roll;
    predicate_t _should_throw;
};

} // namespace cloud_topics::l1
