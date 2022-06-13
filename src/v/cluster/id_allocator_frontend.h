/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/types.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {

class id_allocator;

// id_allocator_frontend is an frontend of the id_allocator_stm,
// an engine behind the id_allocator service.
//
// when a client invokes id_allocator_frontend::allocate_id on a
// remote node the frontend redirects the request to the service
// located on the leading broker of the id_allocator's partition.
//
// when a client is located on the same node as the leader then the
// frontend bypasses the network and directly engages with the state
// machine (id_allocator_stm.cc)
//
// when the service recieves a call it triggers id_allocator_frontend
// which in its own turn pass the request to the id_allocator_stm
class id_allocator_frontend {
public:
    id_allocator_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      std::unique_ptr<cluster::controller>&);

    ss::future<allocate_id_reply>
    allocate_id(model::timeout_clock::duration timeout);

    ss::future<> stop() {
        _as.request_abort();
        return ss::make_ready_future<>();
    }

private:
    ss::abort_source _as;
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    std::unique_ptr<cluster::controller>& _controller;
    int16_t _metadata_dissemination_retries{1};
    std::chrono::milliseconds _metadata_dissemination_retry_delay_ms;

    ss::future<allocate_id_reply> dispatch_allocate_id_to_leader(
      model::node_id, model::timeout_clock::duration);

    ss::future<allocate_id_reply>
      do_allocate_id(model::timeout_clock::duration);

    ss::future<allocate_id_reply>
      do_allocate_id(ss::shard_id, model::timeout_clock::duration);

    ss::future<bool> try_create_id_allocator_topic();

    friend id_allocator;
};
} // namespace cluster
