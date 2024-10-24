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

#include "model/transform.h"
#include "utils/mutex.h"
#include "wasm/engine.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/btree_map.h>

namespace wasm {

class engine_cache;
class cached_factory;

/**
 * A runtime that reuses factories and caches them per process as to share the
 * executable memory.
 *
 * To enable this, this runtime can only create factories on a single shard
 * (the same shard that it is created on, which is probably shard zero).
 * However, factories that are created by this runtime can be used to create
 * engines for any shard.
 *
 * Additionally, engines from this runtime's factories are reused within a
 * single shard. Ramifications of this is that failures to a single engine cause
 * the engine to be restarted and all users of a given engine must wait until
 * it's restarted to use the engine.
 */
class caching_runtime : public runtime {
public:
    explicit caching_runtime(std::unique_ptr<runtime>);
    caching_runtime(
      std::unique_ptr<runtime>, ss::lowres_clock::duration gc_interval);
    caching_runtime(const caching_runtime&) = delete;
    caching_runtime(caching_runtime&&) = delete;
    caching_runtime& operator=(const caching_runtime&) = delete;
    caching_runtime& operator=(caching_runtime&&) = delete;
    ~caching_runtime() override;

    ss::future<> start(runtime::config) override;
    ss::future<> stop() override;

    /**
     * Create a factory, must be called only on a single shard.
     */
    ss::future<ss::shared_ptr<factory>> make_factory(
      model::transform_metadata, model::wasm_binary_iobuf) override;

    /**
     * If a factory exists in cached, return it without needing the binary.
     */
    ss::optimized_optional<ss::shared_ptr<factory>>
    get_cached_factory(const model::transform_metadata&);

    ss::future<> validate(model::wasm_binary_iobuf) override;

private:
    friend class WasmCacheTest;

    /**
     * GC factories and engines that are no longer in use.
     *
     * Return the number of entries deleted (for testing).
     */
    ss::future<int64_t> do_gc();
    ss::future<int64_t> gc_factories();
    ss::future<int64_t> gc_engines();

    /*
     * This map holds locks for creating factories.
     *
     * These mutexes are shortlived and should only live during the creation of
     * factories.
     */
    absl::btree_map<model::offset, std::unique_ptr<mutex>>
      _factory_creation_mu_map;
    std::unique_ptr<runtime> _underlying;
    absl::btree_map<model::offset, ss::weak_ptr<cached_factory>> _factory_cache;
    ss::sharded<engine_cache> _engine_caches;
    ss::lowres_clock::duration _gc_interval;
    ss::timer<ss::lowres_clock> _gc_timer;
    ss::gate _gate;
};

} // namespace wasm
