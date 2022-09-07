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
#include "vformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
using namespace std::chrono_literals;

cluster::errc map_errc_fixme(std::error_code ec);

id_allocator_frontend::id_allocator_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  std::unique_ptr<cluster::controller>& controller)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _controller(controller)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {
}

ss::future<allocate_id_reply>
id_allocator_frontend::allocate_id(model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(
      model::kafka_internal_namespace, model::id_allocator_topic);

    auto has_topic = true;

    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        has_topic = co_await try_create_id_allocator_topic();
    }

    if (!has_topic) {
        vlog(clusterlog.warn, "can't find {} in the metadata cache", nt);
        co_return allocate_id_reply{0, errc::topic_not_exists};
    }

    auto _self = _controller->self();

    auto r = allocate_id_reply{0, errc::not_leader};

    auto retries = _metadata_dissemination_retries;
    auto delay_ms = _metadata_dissemination_retry_delay_ms;
    std::optional<std::string> error;
    while (!_as.abort_requested() && 0 < retries--) {
        auto leader_opt = _leaders.local().get_leader(model::id_allocator_ntp);
        if (unlikely(!leader_opt)) {
            error = vformat(
              fmt::runtime("can't find {} in the leaders cache"),
              model::id_allocator_ntp);
            vlog(
              clusterlog.trace,
              "waiting for {} to fill leaders cache, retries left: {}",
              model::id_allocator_ntp,
              retries);
            co_await sleep_abortable(delay_ms, _as);
            continue;
        }
        auto leader = leader_opt.value();

        if (leader == _self) {
            r = co_await do_allocate_id(timeout);
        } else {
            vlog(
              clusterlog.trace,
              "dispatching id allocation from {} to {} ",
              _self,
              leader);
            r = co_await dispatch_allocate_id_to_leader(leader, timeout);
        }

        if (likely(r.ec == errc::success)) {
            error = std::nullopt;
            break;
        }

        if (likely(r.ec != errc::replication_error)) {
            error = vformat(fmt::runtime("id allocation failed with {}"), r.ec);
            break;
        }

        error = vformat(fmt::runtime("id allocation failed with {}"), r.ec);
        vlog(
          clusterlog.trace, "id allocation failed, retries left: {}", retries);
        co_await sleep_abortable(delay_ms, _as);
    }

    if (error) {
        vlog(clusterlog.warn, "{}", error.value());
    }

    co_return r;
}

ss::future<allocate_id_reply>
id_allocator_frontend::dispatch_allocate_id_to_leader(
  model::node_id leader, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<cluster::id_allocator_client_protocol>(
        _controller->self(),
        ss::this_shard_id(),
        leader,
        timeout,
        [timeout](id_allocator_client_protocol cp) {
            return cp.allocate_id(
              allocate_id_request{timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<allocate_id_reply>)
      .then([](result<allocate_id_reply> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote allocate_id",
                r.error().message());
              return allocate_id_reply{0, errc::timeout};
          }
          return r.value();
      });
}

ss::future<allocate_id_reply>
id_allocator_frontend::do_allocate_id(model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::id_allocator_ntp);

    if (unlikely(!shard)) {
        auto retries = _metadata_dissemination_retries;
        auto delay_ms = _metadata_dissemination_retry_delay_ms;
        auto aborted = false;
        while (!aborted && !shard && 0 < retries--) {
            aborted = !co_await sleep_abortable(delay_ms, _as);
            shard = _shard_table.local().shard_for(model::id_allocator_ntp);
        }

        if (!shard) {
            vlog(
              clusterlog.warn,
              "can't find {} in the shard table",
              model::id_allocator_ntp);
            co_return allocate_id_reply{0, errc::no_leader_controller};
        }
    }
    co_return co_await do_allocate_id(*shard, timeout);
}

ss::future<allocate_id_reply> id_allocator_frontend::do_allocate_id(
  ss::shard_id shard, model::timeout_clock::duration timeout) {
    return _partition_manager.invoke_on(
      shard, _ssg, [timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::id_allocator_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::id_allocator_ntp);
              return ss::make_ready_future<allocate_id_reply>(
                allocate_id_reply{0, errc::topic_not_exists});
          }
          auto stm = partition->id_allocator_stm();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get id allocator stm of the {}' partition",
                model::id_allocator_ntp);
              return ss::make_ready_future<allocate_id_reply>(
                allocate_id_reply{0, errc::topic_not_exists});
          }
          return stm->allocate_id(timeout).then(
            [](id_allocator_stm::stm_allocation_result r) {
                if (r.raft_status != raft::errc::success) {
                    vlog(
                      clusterlog.warn,
                      "allocate id stm call failed with {}",
                      raft::make_error_code(r.raft_status).message());
                    return allocate_id_reply{r.id, errc::replication_error};
                }

                return allocate_id_reply{r.id, errc::success};
            });
      });
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

} // namespace cluster
