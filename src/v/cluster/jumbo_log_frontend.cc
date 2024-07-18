// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/jumbo_log_frontend.h"

#include "base/outcome.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/jumbo_log_service.h"
#include "cluster/jumbo_log_stm.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "errc.h"
#include "jumbo_log/metadata.h"
#include "jumbo_log/rpc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
#include "topic_configuration.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <system_error>
#include <type_traits>
#include <vector>

namespace cluster {
using namespace std::chrono_literals;

cluster::errc map_errc_fixme(std::error_code ec);

ss::future<
  result<rpc::client_context<jumbo_log::rpc::create_write_intent_reply>>>
create_write_intent_handler::dispatch(
  jumbo_log::rpc::jumbo_log_client_protocol proto,
  jumbo_log::rpc::create_write_intent_request req,
  model::timeout_clock::duration timeout) {
    req.timeout = timeout;
    return proto.create_write_intent(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

ss::future<result<rpc::client_context<jumbo_log::rpc::get_write_intents_reply>>>
get_write_intents_handler::dispatch(
  jumbo_log::rpc::jumbo_log_client_protocol proto,
  jumbo_log::rpc::get_write_intents_request req,
  model::timeout_clock::duration timeout) {
    req.timeout = timeout;
    return proto.get_write_intents(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

static_assert(
  std::is_trivially_copyable<jumbo_log::rpc::create_write_intent_request>(),
  "The code bellow assumes that create_write_intent_request is trivially "
  "copyable. If this is not the case, the code should be updated to use "
  "std::move semantics.");

ss::future<jumbo_log::rpc::create_write_intent_reply>
create_write_intent_handler::process(
  ss::shard_id shard, jumbo_log::rpc::create_write_intent_request req) {
    return _partition_manager.invoke_on(
      shard, _ssg, [req](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::jumbo_log_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::jumbo_log_ntp);
              return ssx::now(error_resp(errc::topic_not_exists));
          }

          auto stm
            = partition->raft()->stm_manager()->get<cluster::jumbo_log_stm>();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get jumbo log stm of the {}' partition",
                model::id_allocator_ntp);

              return ssx::now(error_resp(errc::topic_not_exists));
          }

          return stm->create_write_intent(req.object, req.timeout)
            .then([](result<jumbo_log::write_intent_id_t> id_result) {
                if (!id_result) {
                    return error_resp(errc::topic_not_exists);
                }
                return jumbo_log::rpc::create_write_intent_reply{
                  id_result.value(), errc::success};
            });
      });
}

ss::future<jumbo_log::rpc::get_write_intents_reply>
get_write_intents_handler::process(
  ss::shard_id shard, jumbo_log::rpc::get_write_intents_request req) {
    return _partition_manager.invoke_on(
      shard, _ssg, [req](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::jumbo_log_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::jumbo_log_ntp);
              return ssx::now(error_resp(errc::topic_not_exists));
          }

          auto stm
            = partition->raft()->stm_manager()->get<cluster::jumbo_log_stm>();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get jumbo log stm of the {}' partition",
                model::id_allocator_ntp);
              return ssx::now(error_resp(errc::topic_not_exists));
          }

          return stm->get_write_intents(req.write_intent_ids, req.timeout)
            .then(
              [](result<std::vector<jumbo_log::write_intent_segment>> result) {
                  if (!result) {
                      return error_resp(errc::replication_error);
                  }
                  return jumbo_log::rpc::get_write_intents_reply{
                    std::move(result.assume_value()), errc::success};
              });
      });
}

create_write_intent_router::create_write_intent_router(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : create_write_intent_router_base(
    shard_table,
    metadata_cache,
    connection_cache,
    leaders,
    _handler,
    node_id,
    config::shard_local_cfg().metadata_dissemination_retries.value(),
    config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _handler(ssg, partition_manager) {}

get_write_intents_router::get_write_intents_router(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : get_write_intents_router_base(
    shard_table,
    metadata_cache,
    connection_cache,
    leaders,
    _handler,
    node_id,
    config::shard_local_cfg().metadata_dissemination_retries.value(),
    config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _handler(ssg, partition_manager) {}

jumbo_log_frontend::jumbo_log_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id,
  std::unique_ptr<cluster::controller>& controller)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _metadata_cache(metadata_cache)
  , _controller(controller)
  , _create_write_intent_router(
      ssg,
      partition_manager,
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      node_id)
  , _get_write_intents_router(
      ssg,
      partition_manager,
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      node_id) {}

ss::future<jumbo_log::rpc::create_write_intent_reply>
jumbo_log_frontend::create_write_intent(
  jumbo_log::rpc::create_write_intent_request req,
  model::timeout_clock::duration timeout) {
    if (!co_await ensure_jumbo_log_topic_exists()) {
        co_return jumbo_log::rpc::create_write_intent_reply{
          jumbo_log::write_intent_id_t(0), errc::topic_not_exists};
    }
    co_return co_await _create_write_intent_router.process_or_dispatch(
      std::move(req), model::jumbo_log_ntp, timeout);
}

ss::future<jumbo_log::rpc::get_write_intents_reply>
jumbo_log_frontend::get_write_intents(
  jumbo_log::rpc::get_write_intents_request req,
  model::timeout_clock::duration timeout) {
    if (!co_await ensure_jumbo_log_topic_exists()) {
        co_return jumbo_log::rpc::get_write_intents_reply{
          {}, errc::topic_not_exists};
    }
    co_return co_await _get_write_intents_router.process_or_dispatch(
      std::move(req), model::jumbo_log_ntp, timeout);
}

ss::future<bool> jumbo_log_frontend::try_create_jumbo_log_topic() {
    cluster::topic_configuration topic{
      model::jumbo_log_ntp.ns,
      model::jumbo_log_ntp.tp.topic,
      1,
      _controller->internal_topic_replication()};

    topic.properties.shadow_indexing = model::shadow_indexing_mode::disabled;
    topic.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::none;

    return _controller->get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              vlog(
                clusterlog.warn,
                "can not create {} topic - error: {}",
                model::jumbo_log_nt,
                cluster::make_error_code(res[0].ec).message());
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            clusterlog.warn,
            "can not create {} topic - error: {}",
            model::jumbo_log_nt,
            e);
          return false;
      });
}

ss::future<bool> jumbo_log_frontend::ensure_jumbo_log_topic_exists() {
    auto has_topic = true;

    if (!_metadata_cache.local().contains(
          model::jumbo_log_nt, model::partition_id(0))) {
        vlog(
          clusterlog.warn,
          "can't find {} in the metadata cache",
          model::jumbo_log_nt);
        has_topic = co_await try_create_jumbo_log_topic();
    }
    co_return has_topic;
}

} // namespace cluster
