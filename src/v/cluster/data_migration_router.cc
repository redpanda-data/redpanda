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
#include "cluster/data_migration_router.h"

#include "cluster_utils.h"
#include "container/chunked_vector.h"
#include "errc.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "partition_leaders_table.h"
#include "partition_manager.h"
#include "rpc/connection_cache.h"
#include "ssx/abort_source.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/all.hh>

#include <fmt/ostream.h>

#include <chrono>

namespace cluster::data_migrations {

static constexpr std::chrono::milliseconds max_backoff(60);
static constexpr std::chrono::milliseconds request_timeout(10);
router::router(
  model::node_id self,
  group_proxy& group_proxy,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  ss::abort_source& as)
  : _group_proxy(group_proxy)
  , _get_group_offsets_handler{.parent = *this}
  , _get_group_offsets_router(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      _get_group_offsets_handler,
      self,
      10,
      max_backoff)
  , _set_group_offsets_handler{.parent = *this}
  , _set_group_offsets_router(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      _set_group_offsets_handler,
      self,
      10,
      max_backoff)
  , _as_sub(ssx::subscribe_or_trigger(as, [this] noexcept {
      _get_group_offsets_router.request_stop();
      _set_group_offsets_router.request_stop();
  })) {}

ss::future<> router::stop() {
    co_await _set_group_offsets_router.stop();
    co_await _get_group_offsets_router.stop();
}

ss::future<get_group_offsets_reply>
router::get_group_offsets(get_group_offsets_request&& req) {
    auto pid = req.co_partition;
    return _get_group_offsets_router.process_or_dispatch(
      std::move(req),
      model::ntp{
        model::kafka_namespace, model::kafka_consumer_offsets_topic, pid},
      request_timeout);
}

ss::future<set_group_offsets_reply>
router::set_group_offsets(set_group_offsets_request&& req) {
    auto pid = req.group_offsets.offsets_topic_pid;
    return _set_group_offsets_router.process_or_dispatch(
      std::move(req),
      model::ntp{
        model::kafka_namespace, model::kafka_consumer_offsets_topic, pid},
      request_timeout);
};

get_group_offsets_reply
router::get_group_offsets_handler::error_resp(cluster::errc e) {
    return {e, {}};
}

ss::future<result<rpc::client_context<get_group_offsets_reply>>>
router::get_group_offsets_handler::dispatch(
  proto_t proto,
  get_group_offsets_request req,
  model::timeout_clock::duration timeout) {
    return proto.get_group_offsets(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

ss::future<get_group_offsets_reply> router::get_group_offsets_handler::process(
  ss::shard_id shard, get_group_offsets_request req) {
    co_return co_await parent.container().invoke_on(
      shard, [req = std::move(req)](router& r) mutable {
          return r._group_proxy.get_group_offsets(std::move(req));
      });
}

set_group_offsets_reply
router::set_group_offsets_handler::error_resp(cluster::errc e) {
    return {.ec = e};
}

ss::future<result<rpc::client_context<set_group_offsets_reply>>>
router::set_group_offsets_handler::dispatch(
  proto_t proto,
  set_group_offsets_request req,
  model::timeout_clock::duration timeout) {
    return proto.set_group_offsets(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

ss::future<set_group_offsets_reply> router::set_group_offsets_handler::process(
  ss::shard_id shard, set_group_offsets_request req) {
    co_return co_await parent.container().invoke_on(
      shard, [req = std::move(req)](router& r) mutable {
          return r._group_proxy.set_group_offsets(std::move(req));
      });
}

} // namespace cluster::data_migrations
