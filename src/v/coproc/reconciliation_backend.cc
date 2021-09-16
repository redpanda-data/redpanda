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
#include "cluster/shard_table.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "coproc/script_database.h"
#include "ssx/future-util.h"
#include "storage/log_manager.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<wasm::script_database>& sdb,
  ss::sharded<storage::api>& storage,
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<pacemaker>& pacemaker) noexcept
  : _self(config::shard_local_cfg().node_id())
  , _sdb(sdb)
  , _storage(storage)
  , _topics(topics)
  , _shard_table(shard_table)
  , _pacemaker(pacemaker) {}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](const std::vector<cluster::topic_table::delta>& deltas) {
          (void)process_updates(deltas);
      });
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<> reconciliation_backend::process_updates(
  std::vector<cluster::topic_table::delta> deltas) {
    return ss::do_with(std::move(deltas), [this](auto& deltas) {
        return ss::with_gate(_gate, [this, &deltas]() {
            return ss::with_semaphore(_sem, 1, [this, &deltas]() mutable {
                return ss::do_for_each(deltas, [this](auto& d) {
                    return process_update(std::move(d));
                });
            });
        });
    });
}

static std::optional<ss::shard_id>
new_shard(model::node_id self, std::vector<model::broker_shard> new_replicas) {
    auto rep = std::find_if(
      std::cbegin(new_replicas),
      std::cend(new_replicas),
      [self](const model::broker_shard& bs) { return bs.node_id == self; });
    return rep == new_replicas.end() ? std::nullopt
                                     : std::optional<ss::shard_id>(rep->shard);
}

static std::vector<storage::ntp_config> child_configs(
  const cluster::topic_table& topics,
  const storage::api& storage,
  const model::ntp& ntp) {
    std::vector<storage::ntp_config> configs;
    auto found = topics.hierarchy_map().find(model::topic_namespace_view{ntp});
    vassert(
      found != topics.hierarchy_map().end() && !found->second.empty(),
      "Missing corresponding hierarchy for source topic {}",
      ntp);
    for (const auto& c : found->second) {
        auto log = storage.log_mgr().get(
          model::ntp(ntp.ns, c.tp, ntp.tp.partition));
        if (log) {
            configs.emplace_back(log->config());
        }
    }
    return configs;
}

ss::future<> reconciliation_backend::process_shutdown(
  model::ntp ntp,
  model::revision_id rev,
  std::vector<model::broker_shard> new_replicas,
  std::vector<storage::ntp_config> configs) {
    vlog(coproclog.info, "Processing shutdown of: {}", ntp);
    auto errc_or_ctx = co_await _pacemaker.local().shutdown_partition(ntp);
    if (std::holds_alternative<errc>(errc_or_ctx)) {
        vlog(
          coproclog.error,
          "Failed to process shutdown of: {} reason: {}",
          ntp,
          std::get<errc>(errc_or_ctx));
        co_return;
    }
    auto offsets = std::move(
      std::get<ntp_context::offset_tracker>(errc_or_ctx));

    /// Remove from shard table so ntp is effectively deregistered
    co_await _shard_table.invoke_on_all(
      [configs, ntp, rev](cluster::shard_table& st) {
          for (const auto& c : configs) {
              st.erase(c.ntp(), rev);
          }
      });

    /// Either remove from disk or move...
    std::optional<ss::shard_id> shard = new_shard(_self, new_replicas);
    if (!shard) {
        /// Move is occuring on a new node, just remove the log on this node
        for (const auto& c : configs) {
            co_await _storage.local().log_mgr().remove(c.ntp());
            vlog(coproclog.info, "Materialized log {} removed", c.ntp());
        }
    } else {
        /// Move is occuring to a new shard on this node, call shutdown instead
        /// of remove to avoid erasing all log data
        for (const auto& c : configs) {
            co_await _storage.local().log_mgr().shutdown(c.ntp());
            vlog(coproclog.info, "Materialized log {} shutdown", c.ntp());
        }
        /// Store information needed to re-create the log on new shard
        co_await this->container().invoke_on(
          *shard,
          [ntp,
           rev,
           configs = std::move(configs),
           offsets = std::move(offsets)](reconciliation_backend& be) mutable {
              auto [itr, success] = be._saved_ctxs.emplace(
                std::move(ntp),
                state_revision{std::move(offsets), std::move(configs), rev});
              if (!success) {
                  if (itr->second.r_id < rev) {
                      itr->second = state_revision{
                        .offsets = std::move(offsets),
                        .configs = std::move(configs),
                        .r_id = rev};
                  } else {
                      vlog(
                        coproclog.warn,
                        "Ignoring older request ntp: {} old: {} new: {}",
                        ntp,
                        itr->second.r_id,
                        rev);
                  }
              }
          });
    }
}

static std::vector<std::pair<script_id, std::vector<topic_namespace_policy>>>
grab_metadata(wasm::script_database& sdb, model::topic_namespace_view tn_view) {
    std::vector<std::pair<script_id, std::vector<topic_namespace_policy>>>
      results;
    auto found = sdb.find(tn_view);
    auto& ids = found->second;
    for (auto id : ids) {
        auto id_found = sdb.find(id);
        results.emplace_back(id, id_found->second.inputs);
    }
    return results;
}

ss::future<> reconciliation_backend::process_restart(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.info, "Processing restart of: {}", ntp);

    state_revision sr;
    auto found = _saved_ctxs.extract(ntp);
    vassert(!found.empty(), "missing matching op_t::update event {}", ntp);
    sr = std::move(found.mapped());
    vassert(
      sr.r_id < rev, "Older rev - ntp: {} old: {} new: {}", ntp, sr.r_id, rev);

    /// Obtain all matching script_ids for the shutdown partition, they must all
    /// be restarted
    auto meta = co_await _sdb.invoke_on(
      wasm::script_database_main_shard, [ntp](wasm::script_database& sdb) {
          return grab_metadata(sdb, model::topic_namespace_view{ntp});
      });

    std::vector<model::ntp> ntps;
    for (auto& c : sr.configs) {
        ntps.push_back(c.ntp());
        vlog(coproclog.info, "Re-creating materialized log: {}", c.ntp());
        co_await _storage.local()
          .log_mgr()
          .manage(std::move(c))
          .discard_result();
    }

    auto shard = ss::this_shard_id();
    co_await _shard_table.invoke_on_all(
      [ntps = std::move(ntps), shard, ntp, rev](
        cluster::shard_table& shard_table) mutable {
          for (auto& ntp : ntps) {
              shard_table.update_shard(std::move(ntp), shard, rev);
          }
      });

    /// Ensure that materialized log creation occurs before this line, as
    /// restart_partition will start processing and coprocessors would find
    /// themselves in an odd state (i.e. non_replicated topic exists in topic
    /// metadata but no associated storage::log exists)
    auto results = co_await _pacemaker.local().restart_partition(
      ntp, std::move(meta), std::move(sr.offsets));
    for (const auto& [id, errc] : results) {
        vlog(
          coproclog.info,
          "Upon restart of '{}' script_id '{}' reported status: {}",
          ntp,
          errc);
    }
}

ss::future<>
reconciliation_backend::process_update(cluster::topic_table::delta d) {
    using op_t = cluster::topic_table::delta::op_type;
    if (!(d.type == op_t::update || d.type == op_t::update_finished)) {
        /// ignore requests that have nothing to do with moving topics
        co_return;
    }
    const auto& tmap = _topics.local().topics_map();
    auto tp_md = tmap.find(model::topic_namespace_view{d.ntp});
    vassert(tp_md != tmap.end(), "ntp missing topic metadata: {}", d.ntp);
    if (!tp_md->second.is_topic_replicable()) {
        /// ignore requests that have nothing to do with replicable_topics
        co_return;
    }
    model::revision_id rev(d.offset());
    if (d.type == op_t::update) {
        if (cluster::has_local_replicas(
              _self, d.previous_assignment->replicas)) {
            /// Grab the topic hierarchy of the source topic
            auto configs = child_configs(
              _topics.local(), _storage.local(), d.ntp);
            vassert(
              !configs.empty(), "Missing config source hierarchy: {}", d.ntp);
            co_await process_shutdown(
              d.ntp,
              rev,
              std::move(d.new_assignment.replicas),
              std::move(configs));
        }
    } else if (d.type == op_t::update_finished) {
        if (cluster::has_local_replicas(_self, d.new_assignment.replicas)) {
            co_await process_restart(d.ntp, rev);
        }
    }
}

} // namespace coproc
