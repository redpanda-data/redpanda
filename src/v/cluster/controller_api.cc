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

#include "cluster/cluster_utils.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/node_hash_map.h>

namespace cluster {

controller_api::controller_api(
  model::node_id self,
  ss::sharded<controller_backend>& backend,
  ss::sharded<topic_table>& topics,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<rpc::connection_cache>& cache,
  ss::sharded<health_monitor_frontend>& health_monitor,
  ss::sharded<members_table>& members,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _backend(backend)
  , _topics(topics)
  , _shard_table(shard_table)
  , _connections(cache)
  , _health_monitor(health_monitor)
  , _members(members)
  , _as(as) {}

ss::future<std::vector<ntp_reconciliation_state>>
controller_api::get_reconciliation_state(std::vector<model::ntp> ntps) {
    return ss::do_with(std::move(ntps), [this](std::vector<model::ntp>& ntps) {
        return ssx::async_transform(ntps, [this](const model::ntp& ntp) {
            return get_reconciliation_state(ntp);
        });
    });
}

ss::future<result<bool>>
controller_api::all_reconciliations_done(std::vector<model::ntp> ntps) {
    const size_t batch_size = 8192;
    // For a huge topic with e.g. 100k partitions, this will be a huge loop:
    // that means we need parallelism, but not so much that we totally
    // saturate inter-core queues.
    for (size_t i = 0; i < ntps.size(); i += batch_size) {
        auto this_batch = std::min(ntps.size() - i, batch_size);
        // Avoid allocating a vector of results, we only care about
        // the reduced values of 'were any unready' and 'did any error'.
        // Use a smart pointer to avoid tricky reasoning about
        // coroutines and captured references.
        struct reduced_state {
            bool complete{true};
            errc err{errc::success};
        };
        auto reduced = ss::make_lw_shared<reduced_state>();

        co_await ss::parallel_for_each(
          ntps.begin() + i,
          ntps.begin() + i + this_batch,
          [this, reduced](const model::ntp& ntp) {
              return get_reconciliation_state(ntp).then([reduced](auto status) {
                  if (
                    status.cluster_errc() != errc::success
                    && reduced->err == errc::success) {
                      reduced->err = status.cluster_errc();
                  }
                  if (status.status() != reconciliation_status::done) {
                      reduced->complete = false;
                  }
              });
          });

        // Return as soon as we have a conclusive result, avoid
        // spending CPU time hitting subsequent batches of partitions
        if (reduced->err != errc::success) {
            co_return reduced->err;
        } else {
            if (reduced->complete != true) {
                co_return false;
            }
        }
    }

    // Fall through: no ntps were incomplete
    co_return true;
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

ss::future<std::vector<controller_backend::delta_metadata>>
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
          [shard](controller_backend::delta_metadata& m) {
              return backend_operation{
                .source_shard = shard,
                .p_as = std::move(m.delta.new_assignment),
                .type = m.delta.type,
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
            co_await ss::sleep_abortable(
              std::chrono::milliseconds(100), _as.local());
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
        auto f = all_reconciliations_done(std::move(ntps));
        replies.push_back(std::move(f));
    }

    auto r = co_await ss::when_all_succeed(replies.begin(), replies.end());

    co_return std::all_of(r.begin(), r.end(), [](result<bool> is_ready_result) {
        return !is_ready_result.has_error() && is_ready_result.value();
    });
}

ss::future<result<std::vector<partition_reconfiguration_state>>>
controller_api::get_partitions_reconfiguration_state(
  std::vector<model::ntp> partitions,
  model::timeout_clock::time_point timeout) {
    auto& updates_in_progress = _topics.local().updates_in_progress();

    partitions_filter partitions_filter;
    absl::node_hash_map<model::ntp, partition_reconfiguration_state> states;
    for (auto& ntp : partitions) {
        auto progress_it = updates_in_progress.find(ntp);
        if (progress_it == updates_in_progress.end()) {
            continue;
        }
        auto p_as = _topics.local().get_partition_assignment(ntp);
        if (!p_as) {
            continue;
        }
        partition_reconfiguration_state state;
        state.ntp = ntp;

        state.current_assignment = std::move(p_as->replicas);
        state.previous_assignment = progress_it->second.get_previous_replicas();
        state.state = progress_it->second.get_state();
        states.emplace(ntp, std::move(state));

        auto [tp_it, _] = partitions_filter.namespaces.try_emplace(
          ntp.ns, partitions_filter::topic_map_t{});

        auto [p_it, inserted] = tp_it->second.try_emplace(
          ntp.tp.topic, partitions_filter::partitions_set_t{});

        p_it->second.emplace(ntp.tp.partition);
    }

    auto result = co_await _health_monitor.local().get_cluster_health(
      cluster_report_filter{
        .node_report_filter
        = node_report_filter{.ntp_filters = std::move(partitions_filter)}},
      force_refresh::no,
      timeout);

    if (!result) {
        co_return result.error();
    }

    auto& report = result.value();

    for (auto& node_report : report.node_reports) {
        for (auto& tp : node_report.topics) {
            for (auto& p : tp.partitions) {
                model::ntp ntp(tp.tp_ns.ns, tp.tp_ns.tp, p.id);
                auto it = states.find(ntp);
                if (it == states.end()) {
                    continue;
                }

                if (p.leader_id == node_report.id) {
                    it->second.current_partition_size = p.size_bytes;
                }
                const auto moving_to = moving_to_node(
                  node_report.id,
                  it->second.previous_assignment,
                  it->second.current_assignment);

                // node was added to replica set
                if (moving_to) {
                    it->second.already_transferred_bytes.emplace_back(
                      replica_bytes{
                        .node = node_report.id, .bytes = p.size_bytes});
                }

                co_await ss::maybe_yield();
            }
        }
    }
    std::vector<partition_reconfiguration_state> ret;
    ret.reserve(states.size());
    for (auto& [_, state] : states) {
        ret.push_back(std::move(state));
    }
    co_return ret;
}

ss::future<result<node_decommission_progress>>
controller_api::get_node_decommission_progress(
  model::node_id id, model::timeout_clock::time_point timeout) {
    node_decommission_progress ret{};
    auto node = _members.local().get_node_metadata_ref(id);

    if (!node) {
        auto removed_node = _members.local().get_removed_node_metadata_ref(id);
        // node was removed, decommissioning is done
        if (removed_node) {
            ret.finished = true;
            co_return ret;
        }
        co_return errc::node_does_not_exists;
    }

    if (
      node.value().get().state.get_membership_state()
      != model::membership_state::draining) {
        co_return errc::invalid_node_operation;
    }

    ret.replicas_left = _topics.local().get_node_partition_count(id);
    auto moving_from_node = _topics.local().ntps_moving_from_node(id);

    // replicas that are moving from decommissioned node are still present on a
    // node but their metadata is update, add them explicitly
    ret.replicas_left += moving_from_node.size();
    auto states = co_await get_partitions_reconfiguration_state(
      std::move(moving_from_node), timeout);

    if (states) {
        ret.current_reconfigurations = std::move(states.value());
    }

    co_return ret;
}

std::optional<ss::shard_id>
controller_api::shard_for(const raft::group_id& group) const {
    if (_shard_table.local().contains(group)) {
        return _shard_table.local().shard_for(group);
    } else {
        return std::nullopt;
    }
}

std::optional<ss::shard_id>
controller_api::shard_for(const model::ntp& ntp) const {
    return _shard_table.local().shard_for(ntp);
}

} // namespace cluster
