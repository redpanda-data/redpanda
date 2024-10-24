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
#include "cluster/id_allocator_service.h"
#include "cluster/leader_router.h"
#include "cluster/types.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace cluster {

class id_allocator;

class allocate_id_handler {
public:
    explicit allocate_id_handler(
      ss::smp_service_group ssg, ss::sharded<partition_manager>& pm)
      : _ssg(ssg)
      , _partition_manager(pm) {}

    using proto_t = id_allocator_client_protocol;

    static ss::sstring process_name() { return "id allocation"; }

    static allocate_id_reply error_resp(cluster::errc e) {
        return allocate_id_reply{0, e};
    }

    static ss::future<result<rpc::client_context<allocate_id_reply>>> dispatch(
      id_allocator_client_protocol proto,
      allocate_id_request req,
      model::timeout_clock::duration timeout);

    ss::future<allocate_id_reply>
    process(ss::shard_id shard, allocate_id_request req);

private:
    ss::smp_service_group _ssg;
    ss::sharded<partition_manager>& _partition_manager;
};

class reset_id_handler {
public:
    explicit reset_id_handler(
      ss::smp_service_group ssg, ss::sharded<partition_manager>& pm)
      : _ssg(ssg)
      , _partition_manager(pm) {}

    using proto_t = id_allocator_client_protocol;

    static ss::sstring process_name() { return "id allocation"; }

    static reset_id_allocator_reply error_resp(cluster::errc e) {
        return reset_id_allocator_reply{e};
    }

    static ss::future<result<rpc::client_context<reset_id_allocator_reply>>>
    dispatch(
      id_allocator_client_protocol proto,
      reset_id_allocator_request req,
      model::timeout_clock::duration timeout);

    ss::future<reset_id_allocator_reply>
    process(ss::shard_id, reset_id_allocator_request req);

private:
    ss::smp_service_group _ssg;
    ss::sharded<partition_manager>& _partition_manager;
};

using allocate_router
  = leader_router<allocate_id_request, allocate_id_reply, allocate_id_handler>;
using reset_router = leader_router<
  reset_id_allocator_request,
  reset_id_allocator_reply,
  reset_id_handler>;
class allocate_id_router : public allocate_router {
public:
    allocate_id_router(
      ss::smp_service_group ssg,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id);
    ~allocate_id_router() = default;

private:
    allocate_id_handler _handler;
};

class reset_id_router : public reset_router {
public:
    reset_id_router(
      ss::smp_service_group ssg,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id);
    ~reset_id_router() = default;

private:
    reset_id_handler _handler;
};

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
      const model::node_id,
      std::unique_ptr<cluster::controller>&);

    ss::future<allocate_id_reply>
    allocate_id(model::timeout_clock::duration timeout);

    ss::future<reset_id_allocator_reply>
    reset_next_id(model::producer_id, model::timeout_clock::duration timeout);

    ss::future<> stop() { return _allocator_router.shutdown(); }

    allocate_id_router& allocator_router() { return _allocator_router; }
    reset_id_router& id_reset_router() { return _id_reset_router; }

private:
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    std::unique_ptr<cluster::controller>& _controller;

    allocate_id_router _allocator_router;
    reset_id_router _id_reset_router;

    // Sets the underlying stm's next id to the given id, returning an error if
    // there was a problem (e.g. not leader, timed out, etc).
    ss::future<allocate_id_reply>
      do_reset_next_id(int64_t, model::timeout_clock::duration);

    ss::future<bool> try_create_id_allocator_topic();
    ss::future<bool> ensure_id_allocator_topic_exists();

    friend id_allocator;
};
} // namespace cluster
