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
#include "model/transform.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "seastarx.h"
#include "wasm/fwd.h"

#include <memory>

namespace wasm {

/**
 * A wasm engine is a running VM loaded with a user module and capable of
 * transforming batches.
 *
 * A wasm engine is local to the core it was created on.
 */
class engine {
public:
    virtual ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) = 0;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    virtual uint64_t memory_usage_size_bytes() const = 0;

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
    virtual ss::future<ss::shared_ptr<engine>> make_engine() = 0;
    virtual ~factory() = default;
};

/**
 * A wasm runtime is capable of creating engines.
 *
 * There should only be a single runtime for a given process.
 */
class runtime {
public:
    /**
     * Create the default runtime.
     */
    static std::unique_ptr<runtime>
    create_default(pandaproxy::schema_registry::api*);

    runtime() = default;
    runtime(const runtime&) = delete;
    runtime& operator=(const runtime&) = delete;
    runtime(runtime&&) = delete;
    runtime& operator=(runtime&&) = delete;
    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;
    /**
     * Create a factory for this transform and the corresponding source wasm
     * module.
     *
     * This must only be called on a single shard, but the resulting factory
     * can be used on any shard and is thread-safe.
     */
    virtual ss::future<ss::shared_ptr<factory>>
    make_factory(model::transform_metadata, iobuf, ss::logger*) = 0;
    virtual ~runtime() = default;
};

} // namespace wasm
