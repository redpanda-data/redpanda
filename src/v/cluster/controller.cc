// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/logger.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/raft0_utils.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

namespace cluster {

controller::controller(
  ss::sharded<rpc::connection_cache>& ccache,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<storage::api>& storage)
  : _connections(ccache)
  , _partition_manager(pm)
  , _shard_table(st)
  , _storage(storage)
  , _tp_updates_dispatcher(_partition_allocator, _tp_state) {}

ss::future<> controller::wire_up() {
    return _as.start()
      .then([this] { return _members_table.start(); })
      .then([this] { return _partition_leaders.start(); })
      .then(
        [this] { return _partition_allocator.start_single(raft::group_id(0)); })
      .then([this] { return _tp_state.start(); });
}

ss::future<> controller::start() {
    std::vector<model::broker> initial_raft0_brokers;
    if (config::shard_local_cfg().seed_servers().empty()) {
        initial_raft0_brokers.push_back(
          cluster::make_self_broker(config::shard_local_cfg()));
    }
    return create_raft0(
             _partition_manager,
             _shard_table,
             config::shard_local_cfg().data_directory().as_sstring(),
             std::move(initial_raft0_brokers))
      .then([this](consensus_ptr c) { _raft0 = c; })
      .then([this] {
          return _members_manager.start_single(
            _raft0,
            std::ref(_members_table),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_storage),
            std::ref(_as));
      })
      .then([this] {
          // validate configuration invariants to exit early
          return _members_manager.local().validate_configuration_invariants();
      })
      .then([this] {
          return _stm.start_single(
            std::ref(clusterlog),
            _raft0.get(),
            raft::persistent_last_applied::yes,
            std::ref(_tp_updates_dispatcher));
      })
      .then([this] {
          return _tp_frontend.start(
            _raft0->self().id(),
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_partition_leaders),
            std::ref(_as));
      })
      .then([this] {
          return _backend.start(
            std::ref(_tp_state),
            std::ref(_shard_table),
            std::ref(_partition_manager),
            std::ref(_members_table),
            std::ref(_partition_leaders),
            std::ref(_tp_frontend),
            std::ref(_as));
      })
      .then([this] {
          return _stm.invoke_on(controller_stm_shard, &controller_stm::start);
      })
      .then([this] {
          return _members_manager.invoke_on(
            members_manager::shard, &members_manager::start);
      })
      .then([this] {
          return _stm.invoke_on(controller_stm_shard, [](controller_stm& stm) {
              // we do not have to use timeout in here as all the batches to
              // apply have to be accesssible
              return stm.wait(stm.bootstrap_last_applied(), model::no_timeout);
          });
      })
      .then(
        [this] { return _backend.invoke_on_all(&controller_backend::start); });
}

ss::future<> controller::shutdown_input() {
    _raft0->shutdown_input();
    return _as.invoke_on_all(&ss::abort_source::request_abort);
}

ss::future<> controller::stop() {
    auto f = ss::now();
    if (unlikely(!_raft0)) {
        return f;
    }

    if (!_as.local().abort_requested()) {
        f = shutdown_input();
    }

    return f.then([this] {
        return _backend.stop()
          .then([this] { return _tp_frontend.stop(); })
          .then([this] { return _stm.stop(); })
          .then([this] { return _tp_state.stop(); })
          .then([this] { return _members_manager.stop(); })
          .then([this] { return _partition_allocator.stop(); })
          .then([this] { return _partition_leaders.stop(); })
          .then([this] { return _members_table.stop(); })
          .then([this] { return _as.stop(); });
    });
}

} // namespace cluster
