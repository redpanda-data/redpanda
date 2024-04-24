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

#include "base/seastarx.h"
#include "io.h"
#include "memory_limiter.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/transform.h"
#include "probe.h"
#include "transfer_queue.h"
#include "utils/prefix_logger.h"
#include "wasm/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/queue.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <variant>

namespace transform {

// The limits to various buffers for this shard within the transform
// subsystem. We have two main "buffers" we need to limit, the amount of
// data we have ingress to the Wasm VM, and the amount of data we have
// egress from the Wasm VM. We split statically the memory available between
// these buffers so that there are starvation issues where we have to write
// more data in order to get data out of our write buffers and the read
// buffers have taken up all the memory.
//
// In addition to these semaphores, we also reserve 10% of this subsystem's
// memory as flex space.
//
// All of these memory limits have tunable overrides in the case of a
// miscalculation causing memory pressure on shards.
struct memory_limits {
    struct config {
        size_t read;
        size_t write;
    };
    explicit memory_limits(config cfg)
      : read_buffer_semaphore(cfg.read)
      , write_buffer_semaphore(cfg.write) {}
    memory_limiter read_buffer_semaphore;
    memory_limiter write_buffer_semaphore;
};

/**
 * A holder of the result of a transform, which is either a batch of data or a
 * committed offset.
 */
struct transformed_output {
    std::variant<model::transformed_data, kafka::offset> data;

    // How much memory this object is using.
    size_t memory_usage() const;
};

/**
 * A processor is the driver of a transform for a single partition.
 *
 * At it's heart it's a fiber that reads->transforms->writes batches
 * from an input ntp to an output ntp.
 */
class processor {
public:
    using state = model::transform_report::processor::state;
    using state_callback
      = ss::noncopyable_function<void(model::transform_id, model::ntp, state)>;

    processor(
      model::transform_id,
      model::ntp,
      model::transform_metadata,
      ss::shared_ptr<wasm::engine>,
      state_callback,
      std::unique_ptr<source>,
      std::vector<std::unique_ptr<sink>>,
      std::unique_ptr<offset_tracker>,
      probe*,
      memory_limits*);
    processor(const processor&) = delete;
    processor(processor&&) = delete;
    processor& operator=(const processor&) = delete;
    processor& operator=(processor&&) = delete;
    virtual ~processor() = default;
    virtual ss::future<> start();
    virtual ss::future<> stop();

    bool is_running() const;
    model::transform_id id() const;
    const model::ntp& ntp() const;
    const model::transform_metadata& meta() const;
    int64_t current_lag() const;

private:
    ss::future<> run_consumer_loop(kafka::offset);
    ss::future<> run_transform_loop();
    ss::future<> run_all_producers(
      absl::flat_hash_map<model::output_topic_index, kafka::offset>);
    ss::future<> run_producer_loop(
      model::output_topic_index,
      transfer_queue<transformed_output>*,
      sink*,
      kafka::offset);
    ss::future<> poll_sleep();
    ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
    load_latest_committed();
    void report_lag(model::output_topic_index, int64_t);

    template<typename... Future>
    ss::future<> when_all_shutdown(Future&&...);
    ss::future<> handle_processor_task(ss::future<>);

    model::transform_id _id;
    model::ntp _ntp;
    model::transform_metadata _meta;
    ss::shared_ptr<wasm::engine> _engine;
    std::unique_ptr<source> _source;
    std::unique_ptr<offset_tracker> _offset_tracker;
    state_callback _state_callback;
    probe* _probe;

    static constexpr size_t buffer_chunk_size = 8;

    transfer_queue<model::record_batch, buffer_chunk_size>
      _consumer_transform_pipe;

    struct output {
        model::output_topic_index index;
        transfer_queue<transformed_output> queue;
        std::unique_ptr<sink> sink;
    };
    absl::flat_hash_map<model::topic, output> _outputs;
    output* _default_output = nullptr;

    ss::abort_source _as;
    ss::future<> _task;
    prefix_logger _logger;

    std::vector<int64_t> _last_reported_lag;
};
} // namespace transform
