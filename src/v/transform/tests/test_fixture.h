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

#include "model/record.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "transform/io.h"
#include "units.h"
#include "utils/notification_list.h"
#include "wasm/api.h"
#include "wasm/probe.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>

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
    ss::future<> stop() override;

    uint64_t memory_usage_size_bytes() const override;
};

class fake_source : public source {
    static constexpr size_t max_queue_size = 64;

public:
    explicit fake_source() = default;

    kafka::offset latest_offset() override;
    ss::future<model::record_batch_reader>
    read_batch(kafka::offset offset, ss::abort_source* as) override;

    ss::future<> push_batch(model::record_batch batch);

private:
    absl::btree_map<kafka::offset, model::record_batch> _batches;
    ss::condition_variable _cond_var;
};

class fake_sink : public sink {
public:
    ss::future<> write(ss::chunked_fifo<model::record_batch> batches) override;

    ss::future<model::record_batch> read();

private:
    ss::chunked_fifo<model::record_batch> _batches;
    ss::condition_variable _cond_var;
};

class fake_offset_tracker : public offset_tracker {
public:
    ss::future<std::optional<kafka::offset>> load_committed_offset() override;

    ss::future<> commit_offset(kafka::offset o) override;

    ss::future<> wait_for_committed_offset(kafka::offset);

private:
    std::optional<kafka::offset> _committed;
    ss::condition_variable _cond_var;
};

} // namespace transform::testing
