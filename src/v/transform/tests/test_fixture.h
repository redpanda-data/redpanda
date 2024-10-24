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
#include "model/timestamp.h"
#include "model/transform.h"
#include "ssx/semaphore.h"
#include "transform/io.h"
#include "wasm/engine.h"
#include "wasm/transform_probe.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>

#include <optional>

namespace transform::testing {

constexpr model::transform_id my_transform_id{42};
// NOLINTBEGIN(cert-err58-cpp)
static const model::ntp my_ntp = model::random_ntp();
static const model::transform_metadata my_single_output_metadata{
  .name = model::transform_name("xform"),
  .input_topic = model::topic_namespace(my_ntp.ns, my_ntp.tp.topic),
  .output_topics = {model::random_topic_namespace()},
  .environment = {{"FOO", "bar"}},
  .uuid = uuid_t::create(),
  .source_ptr = model::offset(9)};
static const model::transform_metadata my_multiple_output_metadata{
  .name = model::transform_name("xform-multi-output"),
  .input_topic = model::topic_namespace(my_ntp.ns, my_ntp.tp.topic),
  .output_topics = {
    model::random_topic_namespace(),
    model::random_topic_namespace(), 
    model::random_topic_namespace(),
  },
  .environment = {{"FOO", "bar"}},
  .uuid = uuid_t::create(),
  .source_ptr = model::offset(10)};
// NOLINTEND(cert-err58-cpp)

class fake_wasm_engine : public wasm::engine {
public:
    ss::future<> transform(
      model::record_batch batch,
      wasm::transform_probe*,
      wasm::transform_callback) override;

    void set_output_topics(std::vector<model::topic> topics);
    void set_use_default_output_topic();

    ss::future<> start() override;
    ss::future<> stop() override;

private:
    bool _started = false;
    std::optional<std::vector<model::topic>> _output_topics;
};

class fake_source : public source {
    static constexpr size_t max_queue_size = 64;

public:
    explicit fake_source() = default;

    ss::future<> start() override;
    ss::future<> stop() override;
    kafka::offset latest_offset() override;
    ss::future<std::optional<kafka::offset>>
    offset_at_timestamp(model::timestamp, ss::abort_source*) override;
    kafka::offset start_offset() const override;
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

    ss::future<model::record> read();
    bool empty() const { return _records.empty(); }

    /**
     * Pause writes for this sink. All calls to `write` will not resolve until
     * `uncork` is called.
     */
    void cork();

    /**
     * Unpause a sink that was paused via `cork`.
     */
    void uncork();

private:
    ss::chunked_fifo<model::record> _records;
    ss::condition_variable _cond_var;
    ssx::semaphore _cork = {ssx::semaphore::max_counter(), "fake_sink"};
};

class fake_offset_tracker : public offset_tracker {
public:
    ss::future<> start() override;
    ss::future<> stop() override;
    ss::future<> wait_for_previous_flushes(ss::abort_source*) override;

    ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_committed_offsets() override;

    ss::future<>
      commit_offset(model::output_topic_index, kafka::offset) override;

    ss::future<>
      wait_for_committed_offset(model::output_topic_index, kafka::offset);

private:
    absl::flat_hash_map<model::output_topic_index, kafka::offset> _committed;
    ss::condition_variable _cond_var;
};

} // namespace transform::testing
