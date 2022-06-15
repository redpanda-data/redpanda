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
#include "rpc/connection_cache.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/node_hash_map.h>

namespace cluster {

controller_api::controller_api(
  model::node_id self,
  ss::sharded<controller_backend>& backend,
  ss::sharded<topic_table>& topics,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<rpc::connection_cache>& cache,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _backend(backend)
  , _topics(topics)
  , _shard_table(shard_table)
  , _connections(cache)
  , _as(as) {}

ss::future<std::vector<ntp_reconciliation_state>>
controller_api::get_reconciliation_state(std::vector<model::ntp> ntps) {
    return ss::do_with(std::move(ntps), [this](std::vector<model::ntp>& ntps) {
        return ssx::async_transform(ntps, [this](const model::ntp& ntp) {
            return get_reconciliation_state(ntp);
        });
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
    auto metadata = _topics.local().get_topic_metadata_ref(tp_ns);
    if (!metadata) {
        co_return ret_t(errc::topic_not_exists);
    }
    std::vector<model::ntp> ntps;
    ntps.reserve(metadata->get().get_assignments().size());

    std::transform(
      metadata->get().get_assignments().cbegin(),
      metadata->get().get_assignments().cend(),
      std::back_inserter(ntps),
      [tp_ns](const partition_assignment& p_as) {
          return model::ntp(tp_ns.ns, tp_ns.tp, p_as.id);
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
        auto local_deltas = co_await get_remote_core_deltas(ntp, shard);

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

    // if update is in progress return
    if (_topics.local().is_update_in_progress(ntp)) {
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

ss::future<result<std::vector<ntp_reconciliation_state>>>
controller_api::get_reconciliation_state(
  model::node_id target_id,
  std::vector<model::ntp> ntps,
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<ntp_reconciliation_state>>;
    if (target_id == _self) {
        return get_reconciliation_state(std::move(ntps))
          .then([](std::vector<ntp_reconciliation_state> ret) {
              return ret_t(std::move(ret));
          });
    }

    vlog(
      clusterlog.trace,
      "dispatching get ntps: {} reconciliation state request to {}",
      ntps,
      target_id);
    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        target_id,
        timeout,
        [timeout,
         ntps = std::move(ntps)](controller_client_protocol client) mutable {
            reconciliation_state_request req{.ntps = std::move(ntps)};
            return client.get_reconciliation_state(
              std::move(req), rpc::client_opts(timeout));
        })
      .then(&rpc::get_ctx_data<reconciliation_state_reply>)
      .then([](result<reconciliation_state_reply> reply) {
          if (reply) {
              return ret_t(std::move(reply.value().results));
          }
          return ret_t(reply.error());
      });
}

ss::future<result<ntp_reconciliation_state>>
controller_api::get_reconciliation_state(
  model::node_id id, model::ntp ntp, model::timeout_clock::time_point timeout) {
    using ret_t = result<ntp_reconciliation_state>;
    return get_reconciliation_state(
             id, std::vector<model::ntp>{std::move(ntp)}, timeout)
      .then([](result<std::vector<ntp_reconciliation_state>> result) {
          if (result.has_error()) {
              return ret_t(result.error());
          }
          vassert(result.value().size() == 1, "result MUST contain single ntp");

          return ret_t(result.value().front());
      });
}

// high level APIs
ss::future<std::error_code> controller_api::wait_for_topic(
  model::topic_namespace_view tp_ns, model::timeout_clock::time_point timeout) {
    auto metadata = _topics.local().get_topic_metadata_ref(tp_ns);
    if (!metadata) {
        vlog(clusterlog.trace, "topic {} does not exists", tp_ns);
        co_return make_error_code(errc::topic_not_exists);
    }

    absl::node_hash_map<model::node_id, std::vector<model::ntp>> requests;
    // collect ntps per node
    for (const auto& p_as : metadata->get().get_assignments()) {
        for (const auto& bs : p_as.replicas) {
            requests[bs.node_id].emplace_back(tp_ns.ns, tp_ns.tp, p_as.id);
        }
    }
    bool ready = false;
    while (!ready) {
        if (model::timeout_clock::now() > timeout) {
            co_return make_error_code(errc::timeout);
        }
        auto res = co_await are_ntps_ready(requests, timeout);
        ready = !res.has_error() && res.value();
        if (!ready) {
            static constexpr auto retry_period = std::chrono::milliseconds(100);
            if (model::timeout_clock::now() + retry_period > timeout) {
                co_return make_error_code(errc::timeout);
            } else {
                co_await ss::sleep_abortable(retry_period, _as.local());
            }
        }
    }

    co_return errc::success;
}

ss::future<result<bool>> controller_api::are_ntps_ready(
  absl::node_hash_map<model::node_id, std::vector<model::ntp>> requests,
  model::timeout_clock::time_point timeout) {
    std::vector<ss::future<result<bool>>> replies;
    replies.reserve(requests.size());

    for (auto& [id, ntps] : requests) {
        auto f = get_reconciliation_state(id, std::move(ntps), timeout)
                   .then([](result<std::vector<ntp_reconciliation_state>> res) {
                       if (!res) {
                           return result<bool>(res.error());
                       }
                       return result<bool>(std::all_of(
                         res.value().begin(),
                         res.value().end(),
                         [](const ntp_reconciliation_state& state) {
                             return state.status()
                                    == reconciliation_status::done;
                         }));
                   });
        replies.push_back(std::move(f));
    }

    auto r = co_await ss::when_all_succeed(replies.begin(), replies.end());

    co_return std::all_of(r.begin(), r.end(), [](result<bool> is_ready_result) {
        return !is_ready_result.has_error() && is_ready_result.value();
    });
}
} // namespace cluster
