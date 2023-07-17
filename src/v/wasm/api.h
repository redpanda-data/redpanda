/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/types.h"
#include "model/record.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "seastarx.h"
#include "ssx/thread_worker.h"
#include "wasm/fwd.h"

#include <memory>

namespace wasm {

/**
 * A wasm engine is a running VM loaded with a user module and capable of
 * transforming batches.
 */
class engine {
public:
    virtual ss::future<model::record_batch>
    transform(const model::record_batch* batch, probe* probe) = 0;

    virtual ss::future<> start() = 0;
    virtual ss::future<> initialize() = 0;
    virtual ss::future<> stop() = 0;

    virtual std::string_view function_name() const = 0;

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
 * TODO: Is this too many layers of indirection?
 */
class factory {
public:
    factory() = default;
    factory(const factory&) = delete;
    factory& operator=(const factory&) = delete;
    factory(factory&&) = delete;
    factory& operator=(factory&&) = delete;
    virtual ss::future<std::unique_ptr<engine>> make_engine() = 0;
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
    create_default(ssx::thread_worker*, pandaproxy::schema_registry::api*);

    runtime() = default;
    runtime(const runtime&) = delete;
    runtime& operator=(const runtime&) = delete;
    runtime(runtime&&) = delete;
    runtime& operator=(runtime&&) = delete;
    /**
     * Create a factory for this transform and the corresponding source wasm
     * module.
     */
    virtual ss::future<std::unique_ptr<factory>>
    make_factory(cluster::transform_metadata, iobuf, ss::logger*) = 0;
    virtual ~runtime() = default;
};

} // namespace wasm
