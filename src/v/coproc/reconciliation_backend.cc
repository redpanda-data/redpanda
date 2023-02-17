/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/reconciliation_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "coproc/partition_manager.h"
#include "coproc/script_database.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>

namespace {

using delta_op_type = cluster::topic_table_delta::op_type;
bool is_non_replicable_event(delta_op_type t) {
    return t == delta_op_type::add_non_replicable
           || t == delta_op_type::del_non_replicable;
}

std::vector<model::ntp> get_materialized_ntps(
  const cluster::topic_table& topics, const model::ntp& ntp) {
    std::vector<model::ntp> ntps;
    auto ps = topics.get_partition_assignment(ntp);
    if (ps) {
        auto found = topics.hierarchy_map().find(
          model::topic_namespace_view{ntp});
        if (found != topics.hierarchy_map().end()) {
            for (const auto& c : found->second) {
                ntps.emplace_back(c.ns, c.tp, ps->id);
            }
        }
    }
    return ntps;
}

coproc::pacemaker::script_inputs_t grab_metadata(
  coproc::wasm::script_database& sdb, model::topic_namespace_view tn_view) {
    coproc::pacemaker::script_inputs_t results;
    auto found = sdb.find(tn_view);
    if (found != sdb.inv_cend()) {
        auto& ids = found->second;
        for (auto id : ids) {
            auto policies = sdb.find(id)->second.inputs;
            for (auto& tnp : policies) {
                /// Modify topic policy to be earliest, since log must be
                /// re-processed
                tnp.policy = coproc::topic_ingestion_policy::earliest;
            }
            results.emplace(id, std::move(policies));
        }
    }
    return results;
}

} // namespace

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::partition_manager>& cluster_pm,
  ss::sharded<partition_manager>& coproc_pm,
  ss::sharded<pacemaker>& pacemaker,
  ss::sharded<wasm::script_database>& sdb) noexcept
  : _self(model::node_id(*config::node().node_id()))
  , _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
  , _shard_table(shard_table)
  , _cluster_pm(cluster_pm)
  , _coproc_pm(coproc_pm)
  , _pacemaker(pacemaker)
  , _sdb(sdb) {}

void reconciliation_backend::enqueue_events(
  cluster::topic_table::delta_range_t deltas) {
    for (auto& d : deltas) {
        if (is_non_replicable_event(d.type)) {
            _topic_deltas[d.ntp].push_back(d);
        } else {
            /// Obtain all child ntps from the source, and key
            /// by materialized ntps, otherwise out of order
            /// processing will occur
            auto materialized_ntps = get_materialized_ntps(
              _topics.local(), d.ntp);
            for (auto& ntp : materialized_ntps) {
                _topic_deltas[ntp].push_back(d);
            }
        }
    }
}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](cluster::topic_table::delta_range_t deltas) {
          if (!_gate.is_closed()) {
              enqueue_events(deltas);
          } else {
              vlog(
                coproclog.debug,
                "Gate closed encountered while handling delta notification");
          }
      });
    if (!_gate.is_closed()) {
        ssx::spawn_with_gate(_gate, [this] {
            return ss::do_until(
              [this] { return _as.abort_requested(); },
              [this] { return fetch_and_reconcile(std::move(_topic_deltas)); });
        });
    } else {
        vlog(
          coproclog.debug,
          "Gate closed encountered when attempting to start reconciliation "
          "backend");
    }
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _as.request_abort();
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<> reconciliation_backend::process_updates(
  model::ntp ntp, std::vector<update_t> updates) {
    auto itr = updates.begin();
    while (itr != updates.end()) {
        vlog(coproclog.trace, "executing ntp: {} op: {}", ntp, *itr);
        auto err = co_await process_update(ntp, *itr);
        vlog(coproclog.info, "partition operation {} result {}", *itr, err);
        /// This retry mechanism exists to replay events that must be
        /// executed before progress can be made.
        ///
        /// An example is creating a materialized partition. This action
        /// depends on the cluster::controller_backend having first
        /// created the source partition. This is an event that MUST have
        /// occurred first in absolute time otherwise an event to create a
        /// materialized log with that source as a parent would never have
        /// been fired.
        if (err != errc::partition_not_exists) {
            ++itr;
        } else {
            vlog(
              coproclog.warn,
              "Partition {} wasn't created after process_update call: {}",
              ntp,
              *itr);
            try {
                co_await ss::sleep_abortable(1s, _as);
            } catch (const ss::sleep_aborted& e) {
                vlog(coproclog.debug, "Aborted sleep due to shutdown");
                break;
            }
        }
    }
}

ss::future<>
reconciliation_backend::fetch_and_reconcile(events_cache_t deltas) {
    if (deltas.empty()) {
        /// Prevent spin looping when theres nothing to process
        try {
            co_await ss::sleep_abortable(1s, _as);
        } catch (const ss::sleep_aborted&) {
            vlog(coproclog.debug, "Aborted sleep due to shutdown");
        }
        co_return;
    }
    co_await ss::parallel_for_each(
      deltas.begin(),
      deltas.end(),
      [this](events_cache_t::value_type& p) -> ss::future<> {
          return process_updates(p.first, std::move(p.second));
      });
}

ss::future<std::error_code>
reconciliation_backend::process_update(model::ntp ntp, update_t delta) {
    using op_t = update_t::op_type;
    model::revision_id rev(delta.offset());
    switch (delta.type) {
    case op_t::add_non_replicable:
        if (!cluster::has_local_replicas(
              _self, delta.new_assignment.replicas)) {
            co_return errc::success;
        }
        co_return co_await create_non_replicable_partition(
          delta.ntp, rev, delta.new_assignment.replicas);
    case op_t::del_non_replicable:
        if (!cluster::has_local_replicas(
              _self, delta.new_assignment.replicas)) {
            co_return errc::success;
        }
        co_return co_await delete_non_replicable_partition(delta.ntp, rev);
    case op_t::update:
        if (!cluster::has_local_replicas(
              _self, delta.previous_replica_set.value())) {
            co_return errc::success;
        }
        co_return co_await process_shutdown(
          delta.ntp, ntp, rev, std::move(delta.new_assignment.replicas));
    case op_t::update_finished:
        if (!cluster::has_local_replicas(
              _self, delta.new_assignment.replicas)) {
            co_return errc::success;
        }
        co_return co_await process_restart(
          delta.ntp, ntp, rev, delta.new_assignment.replicas);
    case op_t::reset: {
        const auto& prev_replicas = delta.previous_replica_set.value();
        const auto& new_replicas = delta.new_assignment.replicas;
        if (
          !cluster::has_local_replicas(_self, prev_replicas)
          && !cluster::has_local_replicas(_self, new_replicas)) {
            co_return errc::success;
        }
        auto ec = co_await process_shutdown(delta.ntp, ntp, rev, prev_replicas);
        if (ec) {
            co_return ec;
        }
        co_return co_await process_restart(delta.ntp, ntp, rev, new_replicas);
    }
    case op_t::add:
    case op_t::del:
    case op_t::cancel_update:
    case op_t::force_abort_update:
    case op_t::update_properties:
        /// All other case statements are no-ops because those events are
        /// expected to be handled in cluster::controller_backend. Convsersely
        /// the controller_backend will not handle the types of events that
        /// reconciliation_backend is responsible for
        co_return errc::success;
    }
    __builtin_unreachable();
}

ss::future<std::error_code> reconciliation_backend::process_shutdown(
  model::ntp src,
  model::ntp ntp,
  model::revision_id rev,
  std::vector<model::broker_shard>) {
    vlog(coproclog.info, "Processing shutdown of: {}", ntp);
    auto ids = co_await _pacemaker.local().shutdown_partition(src);
    vlog(coproclog.debug, "Ids {} shutdown", ids);
    co_await _shard_table.invoke_on_all(
      [ntp, rev](cluster::shard_table& st) { st.erase(ntp, rev); });
    auto copro_partition = _coproc_pm.local().get(ntp);
    if (copro_partition && copro_partition->get_revision_id() < rev) {
        co_await _coproc_pm.local().remove(ntp);
    }
    co_return make_error_code(errc::success);
}

ss::future<std::error_code> reconciliation_backend::process_restart(
  model::ntp src,
  model::ntp ntp,
  model::revision_id rev,
  std::vector<model::broker_shard> new_replicas) {
    vlog(coproclog.info, "Processing restart of: {}", ntp);
    auto r = co_await create_non_replicable_partition(ntp, rev, new_replicas);
    if (r == errc::partition_not_exists) {
        co_return r;
    }

    /// Obtain all matching script_ids for the shutdown partition, they must all
    /// be restarted
    auto meta = co_await _sdb.invoke_on(
      wasm::script_database_main_shard, [src](wasm::script_database& sdb) {
          return grab_metadata(sdb, model::topic_namespace_view{src});
      });
    if (meta.empty()) {
        /// A move for an input has been detected for which doesn't exist
        /// in the script database. Its possible this is not an error in the
        /// case the coprocessor_internal_topic has compacted some keys and
        /// events for which warranted an action previously would no longer be
        /// necessary to perform.
        vlog(
          coproclog.warn,
          "Update detected for missing registered topic: {} - {}",
          src,
          ntp);
        co_return make_error_code(errc::topic_does_not_exist);
    }

    /// Ensure that materialized log creation occurs before this line, as
    /// restart_partition will start processing and coprocessors would find
    /// themselves in an odd state (i.e. non_replicated topic exists in topic
    /// metadata but no associated storage::log exists)
    auto results = co_await _pacemaker.local().restart_partition(
      src, std::move(meta));
    for (const auto& [id, errc] : results) {
        vlog(
          coproclog.info,
          "Upon restart of '{}' reported status: {}",
          src,
          errc);
    }
    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
reconciliation_backend::delete_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.trace, "removing {} from shard table at {}", ntp, rev);
    co_await _shard_table.invoke_on_all(
      [ntp, rev](cluster::shard_table& st) { st.erase(ntp, rev); });
    auto copro_partition = _coproc_pm.local().get(ntp);
    if (copro_partition && copro_partition->get_revision_id() < rev) {
        co_await _pacemaker.local().with_hold(
          copro_partition->source(), ntp, [this, ntp]() {
              return _coproc_pm.local().remove(ntp);
          });
    }
    co_return make_error_code(errc::success);
}

bool reconciliation_backend::stale_create_non_replicable_partition_request(
  const model::ntp& src_ntp,
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas) {
    /// If the source partition exists in the topics table, but not in
    /// the cluster::partition_manager, either this request occurred before the
    /// source partition has been inserted into the partition_manager and a
    /// retry of creating 'this' partition should occur (until success), OR the
    /// source topic has been moved to another broker_shard and this request is
    /// therefore stale
    auto src_assignments = _topics.local().get_topic_assignments(
      model::topic_namespace_view{src_ntp});
    vassert(
      src_assignments,
      "Inconsistent hierarchy detected in topics table: {}",
      src_ntp);
    auto src_partition_itr = std::find_if(
      src_assignments->begin(),
      src_assignments->end(),
      [&ntp](const cluster::partition_assignment& pa) {
          return pa.id == ntp.tp.partition;
      });
    vassert(
      src_partition_itr != src_assignments->end(),
      "Missing assignment for partition: {}",
      ntp);
    /// If this method returns true the op_t is essentially out-of-date
    /// This partition has been moved, reply with success to avoid retry.
    return !cluster::are_replica_sets_equal(
      replicas, src_partition_itr->replicas);
}

ss::future<std::error_code>
reconciliation_backend::create_non_replicable_partition(
  model::ntp ntp,
  model::revision_id rev,
  std::vector<model::broker_shard> replicas) {
    auto& topics_map = _topics.local().topics_map();
    auto tt_md = topics_map.find(model::topic_namespace_view(ntp));
    if (tt_md == topics_map.end()) {
        // partition was already removed, do nothing
        co_return errc::success;
    }
    vassert(
      !tt_md->second.is_topic_replicable(),
      "Expected to find non_replicable_topic instead: {}",
      ntp);
    auto copro_partition = _coproc_pm.local().get(ntp);
    if (likely(!copro_partition)) {
        // get_source_topic will assert if incorrect API is used
        model::ntp src_ntp(
          ntp.ns, tt_md->second.get_source_topic(), ntp.tp.partition);
        auto src_partition = _cluster_pm.local().get(src_ntp);
        if (!src_partition) {
            co_return stale_create_non_replicable_partition_request(
              src_ntp, ntp, replicas)
              ? errc::success
              : errc::partition_not_exists;
        }
        // Non-replicated topics are not integrated with shadow indexing yet,
        // so the initial_revision_id is set to invalid value.
        auto ntp_cfg = tt_md->second.get_configuration().make_ntp_config(
          _data_directory,
          ntp.tp.partition,
          rev,
          model::initial_revision_id{-1});
        co_await _coproc_pm.local().manage(std::move(ntp_cfg), src_partition);
    } else if (copro_partition->get_revision_id() < rev) {
        co_return errc::partition_already_exists;
    }
    co_await add_to_shard_table(std::move(ntp), ss::this_shard_id(), rev);
    co_return errc::success;
}

ss::future<> reconciliation_backend::add_to_shard_table(
  model::ntp ntp, ss::shard_id shard, model::revision_id revision) {
    vlog(
      coproclog.trace,
      "adding {} / {} to shard table at {}",
      revision,
      ntp,
      shard);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), shard, revision](cluster::shard_table& s) mutable {
          vassert(
            s.update_shard(ntp, shard, revision),
            "Newer revision for non-replicable ntp {} exists: {}",
            ntp,
            revision);
      });
}

} // namespace coproc
