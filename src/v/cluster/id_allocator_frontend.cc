// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/id_allocator_frontend.h"

#include "cluster/controller.h"
#include "cluster/id_allocator_service.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_helpers.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "rpc/connection_cache.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
using namespace std::chrono_literals;

cluster::errc map_errc_fixme(std::error_code ec);

allocate_id_router::allocate_id_router(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : allocate_router(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      _handler,
      node_id,
      config::shard_local_cfg().metadata_dissemination_retries.value(),
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _handler(ssg, partition_manager) {}

reset_id_router::reset_id_router(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : reset_router(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      _handler,
      node_id,
      config::shard_local_cfg().metadata_dissemination_retries.value(),
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _handler(ssg, partition_manager) {}

ss::future<result<rpc::client_context<allocate_id_reply>>>
allocate_id_handler::dispatch(
  id_allocator_client_protocol proto,
  allocate_id_request req,
  model::timeout_clock::duration timeout) {
    req.timeout = timeout;
    return proto.allocate_id(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

ss::future<result<rpc::client_context<reset_id_allocator_reply>>>
reset_id_handler::dispatch(
  id_allocator_client_protocol proto,
  reset_id_allocator_request req,
  model::timeout_clock::duration timeout) {
    req.timeout = timeout;
    return proto.reset_id_allocator(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

ss::future<reset_id_allocator_reply>
reset_id_handler::process(ss::shard_id shard, reset_id_allocator_request req) {
    auto timeout = req.timeout;
    return _partition_manager.invoke_on(
      shard,
      _ssg,
      [timeout, id = req.producer_id](partition_manager& mgr) mutable {
          auto partition = mgr.get(model::id_allocator_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::id_allocator_ntp);
              return ss::make_ready_future<reset_id_allocator_reply>(
                reset_id_allocator_reply(errc::topic_not_exists));
          }
          auto stm = partition->id_allocator_stm();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get id allocator stm of the {}' partition",
                model::id_allocator_ntp);
              return ss::make_ready_future<reset_id_allocator_reply>(
                reset_id_allocator_reply{errc::topic_not_exists});
          }
          return stm->reset_next_id(id, timeout)
            .then([](id_allocator_stm::stm_allocation_result r) {
                if (!r) {
                    vlog(
                      clusterlog.warn,
                      "allocate id stm call failed with {}",
                      r.assume_error().message());
                    return reset_id_allocator_reply{errc::replication_error};
                }

                return reset_id_allocator_reply{errc::success};
            });
      });
}

ss::future<allocate_id_reply>
allocate_id_handler::process(ss::shard_id shard, allocate_id_request req) {
    auto timeout = req.timeout;
    return _partition_manager.invoke_on(
      shard, _ssg, [timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::id_allocator_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::id_allocator_ntp);
              return ss::make_ready_future<allocate_id_reply>(
                0, errc::topic_not_exists);
          }
          auto stm = partition->id_allocator_stm();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get id allocator stm of the {}' partition",
                model::id_allocator_ntp);
              return ss::make_ready_future<allocate_id_reply>(
                0, errc::topic_not_exists);
          }
          return stm->allocate_id(timeout).then(
            [](id_allocator_stm::stm_allocation_result r) {
                if (!r) {
                    vlog(
                      clusterlog.warn,
                      "allocate id stm call failed with {}",
                      r.assume_error().message());
                    return allocate_id_reply{-1, errc::replication_error};
                }

                return allocate_id_reply{r.assume_value(), errc::success};
            });
      });
}

id_allocator_frontend::id_allocator_frontend(
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
  , _allocator_router(
      _ssg,
      partition_manager,
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      node_id)
  , _id_reset_router(
      _ssg,
      partition_manager,
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      node_id) {}

ss::future<allocate_id_reply>
id_allocator_frontend::allocate_id(model::timeout_clock::duration timeout) {
    if (!co_await ensure_id_allocator_topic_exists()) {
        co_return allocate_id_reply{0, errc::topic_not_exists};
    }
    co_return co_await _allocator_router.allocate_router::process_or_dispatch(
      allocate_id_request{timeout}, model::id_allocator_ntp, timeout);
}

ss::future<reset_id_allocator_reply> id_allocator_frontend::reset_next_id(
  model::producer_id pid, model::timeout_clock::duration timeout) {
    if (!co_await ensure_id_allocator_topic_exists()) {
        co_return reset_id_allocator_reply{errc::topic_not_exists};
    }
    co_return co_await _id_reset_router.reset_id_router::process_or_dispatch(
      reset_id_allocator_request{timeout, pid},
      model::id_allocator_ntp,
      timeout);
}

ss::future<bool> id_allocator_frontend::try_create_id_allocator_topic() {
    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::id_allocator_topic,
      1,
      _controller->internal_topic_replication()};

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
                "can not create {}/{} topic - error: {}",
                model::kafka_internal_namespace,
                model::id_allocator_topic,
                cluster::make_error_code(res[0].ec).message());
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            clusterlog.warn,
            "can not create {}/{} topic - error: {}",
            model::kafka_internal_namespace,
            model::id_allocator_topic,
            e);
          return false;
      });
}

ss::future<bool> id_allocator_frontend::ensure_id_allocator_topic_exists() {
    auto nt = model::topic_namespace(
      model::kafka_internal_namespace, model::id_allocator_topic);

    auto has_topic = true;

    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        vlog(clusterlog.warn, "can't find {} in the metadata cache", nt);
        has_topic = co_await try_create_id_allocator_topic();
    }
    co_return has_topic;
}

} // namespace cluster
