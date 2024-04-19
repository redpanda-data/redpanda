/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "ssx/event.h"
#include "wasm/api.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#pragma once

namespace transform {

namespace rpc {
class client;
}
namespace worker::rpc {
class client;
}

/**
 * Manages wasm workers on a remote node.
 *
 * TODO: Make clients interfaces for testing
 * TOOD: Allow manual clock for testing
 */
class remote_wasm_manager {
public:
    // The VM is key'd by the transform that is running and it's version.
    using vm_key = std::pair<model::transform_id, uuid_t>;

    using metadata_lookup_fn = ss::noncopyable_function<
      std::optional<model::transform_metadata>(model::transform_id)>;

    /**
     * Must be only used on core 0
     */
    constexpr static ss::shard_id shard = 0;

    remote_wasm_manager(
      rpc::client*, ss::sharded<worker::rpc::client>*, metadata_lookup_fn);
    remote_wasm_manager(const remote_wasm_manager&) = delete;
    remote_wasm_manager(remote_wasm_manager&&) = delete;
    remote_wasm_manager& operator=(const remote_wasm_manager&) = delete;
    remote_wasm_manager& operator=(remote_wasm_manager&&) = delete;
    ~remote_wasm_manager();

    ss::future<> start();
    ss::future<> stop();

    /**
     * Create a factory, must be called only on a single shard.
     *
     * @return a factory that can create engines on a specific shard.
     */
    ss::future<ss::shared_ptr<wasm::factory>>
      make_factory(model::transform_id, model::transform_metadata);

private:
    ss::future<> reconciliation_loop();
    ss::future<> do_reconciliation();
    ss::future<> start_vm(vm_key);
    ss::future<> stop_vm(vm_key);

    ssx::basic_event<ss::lowres_clock> _reconciliation_needed_event;
    ss::condition_variable _reconciliation_waiters;
    ss::gate _gate;
    // A hash map that ref counts
    chunked_hash_map<vm_key, size_t> _ref_counts;
    // A hash set of the latest running VMs according to the worker.
    chunked_hash_set<vm_key> _active_vms;
    rpc::client* _rpc_client;
    ss::sharded<worker::rpc::client>* _worker_client;
    metadata_lookup_fn _metadata_lookup;
};

} // namespace transform
