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

#include "cloud_topics/level_one/common/object.h"
#include "compaction/reducer.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

class in_memory_sink : public compaction::sliding_window_reducer::sink {
public:
    struct object_output_t {
        object_builder::object_info info;
        iobuf obj;
    };

    in_memory_sink(
      model::topic_id_partition,
      chunked_vector<object_output_t>*,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;
    ss::future<ss::stop_iteration>
    operator()(model::record_batch, model::compression) final;
    ss::future<> finalize(bool) final;
    ss::future<> prepare_iteration(kafka::offset) final { co_return; }
    ss::future<> finish_iteration(kafka::offset, kafka::offset) final {
        co_return;
    }

private:
    bool needs_roll() const;

    ss::future<> maybe_flush_object_builder();

    ss::future<> maybe_roll();

private:
    model::topic_id_partition _tp;
    chunked_vector<object_output_t>* _obj_sink{nullptr};
    const object_builder::options _opts;

    std::optional<iobuf> _active_output_buf{std::nullopt};
    // Guaranteed to have a value iff _output_buf.has_value().
    std::unique_ptr<object_builder> _builder{nullptr};
};

} // namespace cloud_topics::l1
