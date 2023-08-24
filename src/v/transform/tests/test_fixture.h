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

#pragma once

#include "model/tests/randoms.h"
#include "model/transform.h"
#include "transform/io.h"
#include "units.h"
#include "utils/notification_list.h"
#include "wasm/api.h"
#include "wasm/probe.h"

namespace transform::testing {

constexpr model::transform_id my_transform_id{42};
// NOLINTBEGIN(cert-err58-cpp)
static const model::ntp my_ntp = model::random_ntp();
static const model::transform_metadata my_metadata{
  .name = model::transform_name("xform"),
  .input_topic = model::topic_namespace(my_ntp.ns, my_ntp.tp.topic),
  .output_topics = {model::random_topic_namespace()},
  .environment = {{"FOO", "bar"}},
  .uuid = uuid_t::create(),
  .source_ptr = model::offset(9)};
// NOLINTEND(cert-err58-cpp)

class fake_wasm_engine : public wasm::engine {
public:
    ss::future<model::record_batch>
    transform(model::record_batch batch, wasm::transform_probe*) override {
        co_return batch;
    }

    ss::future<> start() override;
    ss::future<> initialize() override;
    ss::future<> stop() override;

    std::string_view function_name() const override;
    uint64_t memory_usage_size_bytes() const override;
};

class fake_source : public source {
    static constexpr size_t max_queue_size = 64;

public:
    explicit fake_source(model::offset initial_offset)
      : _batches(max_queue_size)
      , _latest_offset(initial_offset) {}

    ss::future<model::offset> load_latest_offset() override;
    ss::future<model::record_batch_reader>
    read_batch(model::offset offset, ss::abort_source* as) override;

    ss::future<> push_batch(model::record_batch batch);

private:
    ss::queue<model::record_batch> _batches;
    model::offset _latest_offset;
};

class fake_sink : public sink {
    static constexpr size_t max_queue_size = 64;

public:
    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override;

    ss::future<model::record_batch> read();

private:
    ss::queue<model::record_batch> _batches{max_queue_size};
};

} // namespace transform::testing
