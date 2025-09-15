/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/sink.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/compression.h"

namespace cloud_topics::l1 {

bool compaction_sink::needs_roll() const {
    // TODO: This needs to consider L1 object size and what-not eventually.
    return !_active_output_buf;
}

ss::future<> compaction_sink::maybe_flush_object_builder() {
    if (!_builder) {
        co_return;
    }

    auto builder = std::exchange(_builder, nullptr);
    auto object_info = co_await builder->finish().finally(
      [&builder] { return builder->close(); });

    auto active_buf = std::exchange(_active_output_buf, std::nullopt).value();
    _closed_objs.emplace_back(std::move(object_info), std::move(active_buf));
}

ss::future<> compaction_sink::maybe_roll() {
    if (!needs_roll()) {
        co_return;
    }

    co_await maybe_flush_object_builder();

    _active_output_buf = iobuf{};
    _builder = object_builder::create(
      make_iobuf_ref_output_stream(_active_output_buf.value()), _opts);

    co_await _builder->start_partition(_tidp);

    co_return;
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    co_await maybe_roll();
    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }
    co_await _builder->add_batch(std::move(b));
    co_return ss::stop_iteration::no;
}

ss::future<> compaction_sink::finalize() {
    // TODO: This is very temporary.
    co_await maybe_flush_object_builder();
    if (_obj_sink) {
        *_obj_sink = std::move(_closed_objs);
    }
    co_return;
}

} // namespace cloud_topics::l1
