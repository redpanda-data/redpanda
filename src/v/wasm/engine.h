/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "model/fundamental.h"
#include "model/record.h"
#include "model/transform.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "wasm/fwd.h"

#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <memory>

namespace wasm {

using write_success = ss::bool_class<struct write_success_t>;

/**
 * The callback for when data emitted from the transform.
 *
 * The topic is optional, and if omitted, then the "default" output topic should
 * be assumed.
 */
using transform_callback = ss::noncopyable_function<ss::future<write_success>(
  std::optional<model::topic_view>, model::transformed_data)>;

/**
 * A wasm engine is a running VM loaded with a user module and capable of
 * transforming batches.
 *
 * A wasm engine is local to the core it was created on.
 */
class engine {
public:
    virtual ss::future<>
    transform(model::record_batch, transform_probe*, transform_callback) = 0;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    engine() = default;
    virtual ~engine() = default;
    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;
    engine(engine&&) = default;
    engine& operator=(engine&&) = default;
};

/**
 * A factory is a compilation service that can create many engines from a single
 * transform metadata and wasm module.
 *
 * The idea is that factory has a cached version of the parsed module so the
 * parsing/validation of a wasm module can be only done once and then the
 * attaching to an engine becomes a very fast operation.
 *
 * This object is safe to use across multiple threads concurrently. It only uses
 * local state (and a few std::shared_ptr) to create engines.
 */
class factory {
public:
    factory() = default;
    factory(const factory&) = delete;
    factory& operator=(const factory&) = delete;
    factory(factory&&) = delete;
    factory& operator=(factory&&) = delete;
    virtual ss::future<ss::shared_ptr<engine>>
      make_engine(std::unique_ptr<wasm::logger>) = 0;
    virtual ~factory() = default;
};

/**
 * A wasm runtime is capable of creating engines.
 *
 * There should only be a single runtime for a given process.
 */
class runtime {
public:
    runtime() = default;
    runtime(const runtime&) = delete;
    runtime& operator=(const runtime&) = delete;
    runtime(runtime&&) = delete;
    runtime& operator=(runtime&&) = delete;

    struct config {
        struct heap_memory {
            // per core how many bytes to reserve
            size_t per_core_pool_size_bytes;
            // per engine the max amount of memory
            size_t per_engine_memory_limit;
        };
        heap_memory heap_memory;
        struct stack_memory {
            // Enable debugging of host function's stack usage.
            // These host functions are called on the VM stack, so we need to
            // ensure that we aren't susceptible to guests that use most of the
            // stack and then our host function overflowing the rest of the
            // stack.
            bool debug_host_stack_usage;
        };
        stack_memory stack_memory;
        struct cpu {
            // per transform timeout (CPU time) also is applied for startup
            // timeout.
            //
            // NOTE: This is translated into fuel (instruction count limits) so
            // it's an approximate limit in terms of actual timeout at the
            // moment.
            std::chrono::milliseconds per_invocation_timeout;
        };
        cpu cpu;
    };

    virtual ss::future<> start(config) = 0;
    virtual ss::future<> stop() = 0;
    /**
     * Create a factory for this transform and the corresponding source wasm
     * module.
     *
     * This must only be called on a single shard, but the resulting factory
     * can be used on any shard and is thread-safe.
     */
    virtual ss::future<ss::shared_ptr<factory>>
      make_factory(model::transform_metadata, model::wasm_binary_iobuf) = 0;

    /**
     * Verify a WebAssembly module is valid and loosely adheres to our ABI
     * format.
     *
     * Throws an exception when validation fails.
     */
    virtual ss::future<> validate(model::wasm_binary_iobuf) = 0;

    virtual ~runtime() = default;
};

} // namespace wasm
