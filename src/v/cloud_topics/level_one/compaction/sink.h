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

class compaction_sink : public compaction::sliding_window_reducer::sink {
public:
    struct object_output_t {
        object_builder::object_info info;
        iobuf obj;
    };

    compaction_sink(
      model::topic_id_partition tidp, object_builder::options opts = {})
      : _tidp(tidp)
      , _opts(opts) {}

    ss::future<ss::stop_iteration>
    operator()(model::record_batch b, model::compression c) final;
    ss::future<> finalize() final;

    void set_object_sink(chunked_vector<object_output_t>* obj_sink) {
        _obj_sink = obj_sink;
    }

private:
    bool needs_roll() const;

    ss::future<> maybe_flush_object_builder();

    ss::future<> maybe_roll();

    model::topic_id_partition _tidp;
    const object_builder::options _opts;

    std::optional<iobuf> _active_output_buf{std::nullopt};
    // Guaranteed to have a value iff _output_buf.has_value().
    std::unique_ptr<object_builder> _builder{nullptr};
    chunked_vector<object_output_t> _closed_objs;

    // TODO: This is very temporary.
    chunked_vector<object_output_t>* _obj_sink{nullptr};
};

} // namespace cloud_topics::l1
