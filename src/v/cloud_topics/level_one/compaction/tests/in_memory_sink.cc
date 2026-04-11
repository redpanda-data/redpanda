/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/tests/in_memory_sink.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/compression.h"

namespace cloud_topics::l1 {

in_memory_sink::in_memory_sink(
  model::topic_id_partition tp,
  chunked_vector<object_output_t>* obj_sink,
  object_builder::options opts)
  : _tp(tp)
  , _obj_sink(obj_sink)
  , _opts(opts) {}

bool in_memory_sink::needs_roll() const { return !_active_output_buf; }

ss::future<> in_memory_sink::maybe_flush_object_builder() {
    if (!_builder) {
        co_return;
    }

    auto builder = std::exchange(_builder, nullptr);
    auto object_info = co_await builder->finish().finally(
      [&builder] { return builder->close(); });

    auto active_buf = std::exchange(_active_output_buf, std::nullopt).value();
    _obj_sink->emplace_back(std::move(object_info), std::move(active_buf));
}

ss::future<> in_memory_sink::maybe_roll() {
    if (!needs_roll()) {
        co_return;
    }

    co_await maybe_flush_object_builder();

    _active_output_buf = iobuf{};
    _builder = object_builder::create(
      make_iobuf_ref_output_stream(_active_output_buf.value()), _opts);

    co_await _builder->start_partition(_tp);

    co_return;
}

ss::future<bool>
in_memory_sink::initialize(compaction::sliding_window_reducer::source&) {
    co_return true;
}

ss::future<ss::stop_iteration>
in_memory_sink::operator()(model::record_batch b, model::compression c) {
    co_await maybe_roll();
    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }
    co_await _builder->add_batch(std::move(b));
    co_return ss::stop_iteration::no;
}

ss::future<> in_memory_sink::finalize(bool /*success*/) {
    co_await maybe_flush_object_builder();
    co_return;
}

} // namespace cloud_topics::l1
