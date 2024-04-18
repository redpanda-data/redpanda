/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "container/chunked_hash_map.h"
#include "model/transform.h"
#include "transform/worker/rpc/control_plane.h"
#include "transform/worker/rpc/data_plane.h"
#include "transform/worker/rpc/rpc_service.h"
#include "wasm/cache.h"

#include <seastar/core/rwlock.hh>
#include <seastar/core/sharded.hh>

namespace transform::worker {

struct vm;
struct probe;

class local_service : ss::peering_sharded_service<local_service> {
public:
    explicit local_service(wasm::caching_runtime* wasm_runtime)
      : _runtime(wasm_runtime) {}
    local_service(const local_service&) = delete;
    local_service(local_service&&) = delete;
    local_service& operator=(const local_service&) = delete;
    local_service& operator=(local_service&&) = delete;
    ~local_service() = default;

    ss::future<rpc::current_state_reply>
      compute_current_state(rpc::current_state_request);

    ss::future<rpc::start_vm_reply> start_vm(rpc::start_vm_request);

    ss::future<rpc::stop_vm_reply> stop_vm(rpc::stop_vm_request);

    ss::future<rpc::transform_data_reply>
      transform_data(rpc::transform_data_request);

private:
    ss::future<rpc::transform_data_reply>
      do_local_transform_data(rpc::transform_data_request);
    ss::future<rpc::transform_data_reply>
    do_transform(vm*, rpc::transform_data_request);

    ss::future<ss::foreign_ptr<ss::shared_ptr<wasm::factory>>>
    load_factory(rpc::start_vm_request*);

    ss::future<>
    do_start_vm(model::transform_id, model::transform_metadata, wasm::factory*);
    ss::future<> do_stop_vm(model::transform_id, uuid_t);

    ss::lw_shared_ptr<probe> get_or_make_probe(const model::transform_name&);

    chunked_hash_map<std::pair<model::transform_id, uuid_t>, rpc::vm_state>
    do_compute_current_state();

    // This maps is guarded by _mutex
    chunked_hash_map<
      std::pair<model::transform_id, uuid_t>,
      std::unique_ptr<vm>>
      _vms;
    ss::rwlock _vm_mutex;

    // This map has no mutex it's not accessed outside of scheduling points
    // TODO(rockwood): This map needs GC
    chunked_hash_map<model::transform_name, ss::weak_ptr<probe>> _probes;
    // This runtime is only accessed on core 0
    wasm::caching_runtime* _runtime;
};

class network_service final : public rpc::transform_worker_service {
public:
    network_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<local_service>* service)
      : rpc::transform_worker_service{sc, ssg}
      , _service(service) {}

    ss::future<rpc::current_state_reply> compute_current_state(
      rpc::current_state_request, ::rpc::streaming_context&) override;

    ss::future<rpc::start_vm_reply>
    start_vm(rpc::start_vm_request, ::rpc::streaming_context&) override;

    ss::future<rpc::stop_vm_reply>
    stop_vm(rpc::stop_vm_request, ::rpc::streaming_context&) override;

    ss::future<rpc::transform_data_reply> transform_data(
      rpc::transform_data_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<local_service>* _service;
};

} // namespace transform::worker
