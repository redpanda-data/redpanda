// Copyright 2020 Vectorized, Inc.
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
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

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
  , _controller(controller) {}

ss::future<allocate_id_reply>
id_allocator_frontend::allocate_id(model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(
      model::kafka_internal_namespace, model::id_allocator_topic);

    auto has_topic = ss::make_ready_future<bool>(true);

    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        has_topic = try_create_id_allocator_topic();
    }

    return has_topic.then([this, timeout](bool does_topic_exist) {
        if (!does_topic_exist) {
            return ss::make_ready_future<allocate_id_reply>(
              allocate_id_reply{0, errc::topic_not_exists});
        }

        auto leader = _leaders.local().get_leader(model::id_allocator_ntp);

        if (!leader) {
            vlog(
              clusterlog.warn,
              "can't find a leader for {}",
              model::id_allocator_ntp);
            return ss::make_ready_future<allocate_id_reply>(
              allocate_id_reply{0, errc::no_leader_controller});
        }

        auto _self = _controller->self();

        if (leader == _self) {
            return do_allocate_id(timeout);
        }

        vlog(
          clusterlog.trace,
          "dispatching allocate id to {} from {}",
          leader,
          _self);

        return dispatch_allocate_id_to_leader(leader.value(), timeout);
    });
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
                "got error {} on remote allocate id",
                r.error());
              return allocate_id_reply{0, errc::timeout};
          }

          return r.value();
      });
}

ss::future<allocate_id_reply>
id_allocator_frontend::do_allocate_id(model::timeout_clock::duration timeout) {
    auto shard = _shard_table.local().shard_for(model::id_allocator_ntp);

    if (shard == std::nullopt) {
        vlog(
          clusterlog.warn,
          "can't find a shard for {}",
          model::id_allocator_ntp);
        return ss::make_ready_future<allocate_id_reply>(
          allocate_id_reply{0, errc::no_leader_controller});
    }

    return _partition_manager.invoke_on(
      *shard, _ssg, [timeout](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::id_allocator_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::id_allocator_ntp);
              return ss::make_ready_future<allocate_id_reply>(
                allocate_id_reply{0, errc::topic_not_exists});
          }
          auto& stm = partition->id_allocator_stm();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get id allocator stm of the {}' partition",
                model::id_allocator_ntp);
              return ss::make_ready_future<allocate_id_reply>(
                allocate_id_reply{0, errc::topic_not_exists});
          }
          return stm
            ->allocate_id_and_wait(model::timeout_clock::now() + timeout)
            .then([](id_allocator_stm::stm_allocation_result r) {
                if (r.raft_status == raft::errc::success) {
                    return allocate_id_reply{r.id, errc::success};
                } else {
                    vlog(
                      clusterlog.trace,
                      "allocate id stm call failed with {}",
                      r.raft_status);
                    return allocate_id_reply{r.id, errc::replication_error};
                }
            });
      });
}

ss::future<bool> id_allocator_frontend::try_create_id_allocator_topic() {
    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::id_allocator_topic,
      1,
      config::shard_local_cfg().default_topic_replication()};

    topic.cleanup_policy_bitflags = model::cleanup_policy_bitflags::none;

    return _controller->get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(clusterlog.warn, "cant create allocate id stm topic {}", e);
          return false;
      });
}

} // namespace cluster
