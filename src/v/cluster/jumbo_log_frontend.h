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
#include "base/outcome.h"
#include "cluster/jumbo_log_service.h"
#include "cluster/leader_router.h"
#include "errc.h"
#include "fwd.h"
#include "jumbo_log/metadata.h"
#include "jumbo_log/rpc.h"
#include "metadata_cache.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "partition_leaders_table.h"
#include "rpc/fwd.h"
#include "rpc/types.h"
#include "shard_table.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <memory>
#include <vector>

namespace cluster {

class jumbo_log_service;

class create_write_intent_handler {
public:
    explicit create_write_intent_handler(
      ss::smp_service_group ssg, ss::sharded<partition_manager>& pm)
      : _ssg(ssg)
      , _partition_manager(pm) {}

    using proto_t = jumbo_log::rpc::jumbo_log_client_protocol;

    static ss::sstring process_name() { return "create_write_intent"; }

    static jumbo_log::rpc::create_write_intent_reply
    error_resp(cluster::errc e) {
        return jumbo_log::rpc::create_write_intent_reply{
          jumbo_log::write_intent_id_t(0), e};
    }

    static ss::future<
      result<rpc::client_context<jumbo_log::rpc::create_write_intent_reply>>>
    dispatch(
      proto_t proto,
      jumbo_log::rpc::create_write_intent_request req,
      model::timeout_clock::duration timeout);

    ss::future<jumbo_log::rpc::create_write_intent_reply> process(
      ss::shard_id shard, jumbo_log::rpc::create_write_intent_request req);

private:
    ss::smp_service_group _ssg;
    ss::sharded<partition_manager>& _partition_manager;
};

using create_write_intent_router_base = leader_router<
  jumbo_log::rpc::create_write_intent_request,
  jumbo_log::rpc::create_write_intent_reply,
  create_write_intent_handler>;

class create_write_intent_router : public create_write_intent_router_base {
public:
    create_write_intent_router(
      ss::smp_service_group ssg,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id);
    ~create_write_intent_router() = default;

private:
    create_write_intent_handler _handler;
};

class get_write_intents_handler {
public:
    explicit get_write_intents_handler(
      ss::smp_service_group ssg, ss::sharded<partition_manager>& pm)
      : _ssg(ssg)
      , _partition_manager(pm) {}

    using proto_t = jumbo_log::rpc::jumbo_log_client_protocol;

    static ss::sstring process_name() { return "get_write_intents"; }

    static jumbo_log::rpc::get_write_intents_reply error_resp(cluster::errc e) {
        return jumbo_log::rpc::get_write_intents_reply{{}, e};
    }

    static ss::future<
      result<rpc::client_context<jumbo_log::rpc::get_write_intents_reply>>>
    dispatch(
      proto_t proto,
      jumbo_log::rpc::get_write_intents_request req,
      model::timeout_clock::duration timeout);

    ss::future<jumbo_log::rpc::get_write_intents_reply>
    process(ss::shard_id shard, jumbo_log::rpc::get_write_intents_request req);

private:
    ss::smp_service_group _ssg;
    ss::sharded<partition_manager>& _partition_manager;
};

using get_write_intents_router_base = leader_router<
  jumbo_log::rpc::get_write_intents_request,
  jumbo_log::rpc::get_write_intents_reply,
  get_write_intents_handler>;

class get_write_intents_router : public get_write_intents_router_base {
public:
    get_write_intents_router(
      ss::smp_service_group ssg,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id);
    ~get_write_intents_router() = default;

private:
    get_write_intents_handler _handler;
};

class jumbo_log_frontend {
public:
    jumbo_log_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id,
      std::unique_ptr<cluster::controller>&);

    ss::future<jumbo_log::rpc::create_write_intent_reply> create_write_intent(
      jumbo_log::rpc::create_write_intent_request,
      model::timeout_clock::duration);

    ss::future<jumbo_log::rpc::get_write_intents_reply> get_write_intents(
      jumbo_log::rpc::get_write_intents_request,
      model::timeout_clock::duration);

    ss::future<> stop() {
        co_await _create_write_intent_router.shutdown();
        co_await _get_write_intents_router.shutdown();
    }

    create_write_intent_router& get_create_write_intent_router() {
        return _create_write_intent_router;
    }

    get_write_intents_router& get_get_write_intents_router() {
        return _get_write_intents_router;
    }

private:
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    std::unique_ptr<cluster::controller>& _controller;

    create_write_intent_router _create_write_intent_router;
    get_write_intents_router _get_write_intents_router;

    ss::future<bool> try_create_jumbo_log_topic();
    ss::future<bool> ensure_jumbo_log_topic_exists();

    friend jumbo_log_service;
};
} // namespace cluster
