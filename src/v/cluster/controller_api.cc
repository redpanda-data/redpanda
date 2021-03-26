/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/controller_api.h"

#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/node_hash_map.h>

namespace cluster {

controller_api::controller_api(
  model::node_id self,
  ss::sharded<controller_backend>& backend,
  ss::sharded<topic_table>& topics,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _backend(backend)
  , _topics(topics)
  , _shard_table(shard_table)
  , _as(as) {}

ss::future<std::vector<ntp_reconciliation_state>>
controller_api::get_reconciliation_state(std::vector<model::ntp> ntps) {
    return ssx::async_transform(ntps, [this](const model::ntp& ntp) {
        return get_reconciliation_state(ntp);
    });
}

bool has_node_local_replicas(
  model::node_id self, const partition_assignment& assignment) {
    return std::any_of(
      assignment.replicas.cbegin(),
      assignment.replicas.cend(),
      [self](const model::broker_shard& p_as) { return p_as.node_id == self; });
}

ss::future<result<std::vector<ntp_reconciliation_state>>>
controller_api::get_reconciliation_state(model::topic_namespace_view tp_ns) {
    using ret_t = result<std::vector<ntp_reconciliation_state>>;
    auto metadata = _topics.local().get_topic_metadata(tp_ns);
    if (!metadata) {
        co_return ret_t(errc::topic_not_exists);
    }
    std::vector<model::ntp> ntps;
    ntps.reserve(metadata->partitions.size());

    std::transform(
      metadata->partitions.cbegin(),
      metadata->partitions.cend(),
      std::back_inserter(ntps),
      [tp_ns](const model::partition_metadata& p_md) {
          return model::ntp(tp_ns.ns, tp_ns.tp, p_md.id);
      });

    co_return co_await get_reconciliation_state(std::move(ntps));
}

ss::future<std::vector<topic_table_delta>>
controller_api::get_remote_core_deltas(model::ntp ntp, ss::shard_id shard) {
    return _backend.invoke_on(
      shard, [ntp = std::move(ntp)](controller_backend& backend) {
          return backend.list_ntp_deltas(ntp);
      });
}

ss::future<ntp_reconciliation_state>
controller_api::get_reconciliation_state(model::ntp ntp) {
    if (_as.local().abort_requested()) {
        co_return ntp_reconciliation_state(std::move(ntp), errc::shutting_down);
    }
    vlog(clusterlog.trace, "getting reconciliation state for {}", ntp);
    auto target_assignment = _topics.local().get_partition_assignment(ntp);

    // partition not found, return error
    if (!target_assignment) {
        co_return ntp_reconciliation_state(
          std::move(ntp), errc::partition_not_exists);
    }
    // query controller backends for in progress operations
    std::vector<backend_operation> ops;
    const auto shards = boost::irange<ss::shard_id>(0, ss::smp::count);
    for (auto shard : shards) {
        auto local_deltas = co_await get_remote_core_deltas(
          std::move(ntp), shard);

        std::transform(
          local_deltas.begin(),
          local_deltas.end(),
          std::back_inserter(ops),
          [shard](topic_table_delta& delta) {
              return backend_operation{
                .source_shard = shard,
                .p_as = std::move(delta.new_assignment),
                .type = delta.type,
              };
          });
    }

    // having any deltas is sufficient to state that reconciliation is still
    // in progress
    if (!ops.empty()) {
        co_return ntp_reconciliation_state(
          std::move(ntp), std::move(ops), reconciliation_status::in_progress);
    }

    // deltas are empty, make sure that local node partitions are in align with
    // expected cluster state

    auto has_local_replicas = has_node_local_replicas(
      _self, *target_assignment);

    auto shard = _shard_table.local().shard_for(ntp);

    // shard not found for ntp and it is expected to have no local replicas
    if ((!shard && !has_local_replicas) || (shard && has_local_replicas)) {
        co_return ntp_reconciliation_state(
          std::move(ntp), {}, reconciliation_status::done);
    }

    // cluster & metadata state are inconsistent
    co_return ntp_reconciliation_state(
      std::move(ntp), {}, reconciliation_status::in_progress);
}
} // namespace cluster
