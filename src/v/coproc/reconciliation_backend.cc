/*
 * Copyright 2021 Vectorized, Inc.
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
#include "storage/api.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::partition_manager>& cluster_pm,
  ss::sharded<partition_manager>& coproc_pm,
  ss::sharded<pacemaker>& pacemaker) noexcept
  : _self(model::node_id(config::node().node_id))
  , _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
  , _shard_table(shard_table)
  , _cluster_pm(cluster_pm)
  , _coproc_pm(coproc_pm)
  , _pacemaker(pacemaker) {
    _retry_timer.set_callback([this] {
        (void)within_context([this]() { return fetch_and_reconcile(); });
    });
}

template<typename Fn>
ss::future<> reconciliation_backend::within_context(Fn&& fn) {
    try {
        return ss::with_gate(
          _gate, [this, fn = std::forward<Fn>(fn)]() mutable {
              if (!_as.abort_requested()) {
                  return _mutex.with(
                    [fn = std::forward<Fn>(fn)]() mutable { return fn(); });
              }
              return ss::now();
          });
    } catch (const ss::gate_closed_exception& ex) {
        vlog(coproclog.debug, "Timer fired during shutdown: {}", ex);
    }
    return ss::now();
}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](std::vector<update_t> deltas) {
          return within_context([this, deltas = std::move(deltas)]() mutable {
              for (auto& d : deltas) {
                  auto ntp = d.ntp;
                  _topic_deltas[ntp].push_back(std::move(d));
              }
              return fetch_and_reconcile();
          });
      });
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _retry_timer.cancel();
    _as.request_abort();
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<std::vector<reconciliation_backend::update_t>>
reconciliation_backend::process_events_for_ntp(
  model::ntp ntp, std::vector<update_t> updates) {
    std::vector<update_t> retries;
    for (auto& d : updates) {
        vlog(coproclog.trace, "executing ntp: {} op: {}", d.ntp, d);
        auto err = co_await process_update(d);
        vlog(coproclog.info, "partition operation {} result {}", d, err);
        if (err == errc::partition_not_exists) {
            /// In this case the source topic exists but its
            /// associated partition doesn't, so try again
            retries.push_back(std::move(d));
        }
    }
    co_return retries;
}

ss::future<> reconciliation_backend::fetch_and_reconcile() {
    using deltas_cache = decltype(_topic_deltas);
    auto deltas = std::exchange(_topic_deltas, {});
    deltas_cache retry_cache;
    co_await ss::parallel_for_each(
      deltas.begin(),
      deltas.end(),
      [this, &retry_cache](deltas_cache::value_type& p) -> ss::future<> {
          auto retries = co_await process_events_for_ntp(p.first, p.second);
          if (!retries.empty()) {
              retry_cache[p.first] = std::move(retries);
          }
      });
    if (!retry_cache.empty()) {
        vlog(
          coproclog.warn,
          "There were recoverable errors when processing events, retrying");
        std::swap(_topic_deltas, retry_cache);
        if (!_retry_timer.armed()) {
            _retry_timer.arm(
              model::timeout_clock::now() + retry_timeout_interval);
        }
    }
}

ss::future<std::error_code>
reconciliation_backend::process_update(update_t delta) {
    using op_t = update_t::op_type;
    model::revision_id rev(delta.offset());
    switch (delta.type) {
    case op_t::add_non_replicable:
        if (!cluster::has_local_replicas(
              _self, delta.new_assignment.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_non_replicable_partition(delta.ntp, rev);
    case op_t::del_non_replicable:
        return delete_non_replicable_partition(delta.ntp, rev).then([] {
            return std::error_code(errc::success);
        });
    case op_t::add:
    case op_t::del:
    case op_t::update:
    case op_t::update_finished:
    case op_t::update_properties:
        /// All other case statements are no-ops because those events are
        /// expected to be handled in cluster::controller_backend. Convsersely
        /// the controller_backend will not handle the types of events that
        /// reconciliation_backend is responsible for
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    __builtin_unreachable();
}

ss::future<> reconciliation_backend::delete_non_replicable_partition(
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
}

ss::future<std::error_code>
reconciliation_backend::create_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
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
        auto src_partition = _cluster_pm.local().get(model::ntp(
          ntp.ns, tt_md->second.get_source_topic(), ntp.tp.partition));
        if (!src_partition) {
            co_return errc::partition_not_exists;
        }
        auto ntp_cfg = tt_md->second.get_configuration().cfg.make_ntp_config(
          _data_directory, ntp.tp.partition, rev);
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
