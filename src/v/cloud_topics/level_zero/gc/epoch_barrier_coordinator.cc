/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier_coordinator.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/logger.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/partition_manager.h"
#include "model/namespace.h"

namespace cloud_topics::l0::gc {

namespace {

class partition_manager_source
  : public epoch_barrier_coordinator::partition_source {
public:
    explicit partition_manager_source(cluster::partition_manager& pm)
      : _pm(pm) {}

    chunked_vector<std::pair<model::ntp, info>>
    cloud_topic_partitions() const override {
        chunked_vector<std::pair<model::ntp, info>> result;
        for (const auto& [ntp, partition] : _pm.partitions()) {
            auto ctp_stm
              = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
            if (ntp.ns != model::kafka_namespace || !ctp_stm) {
                continue;
            }
            result.emplace_back(
              ntp,
              info{
                .committed_offset = partition->committed_offset(),
                .term = partition->term(),
                .is_leader = partition->is_leader(),
                .last_reconciled_log_offset
                = ctp_stm->state().get_last_reconciled_log_offset(),
              });
        }
        return result;
    }

    std::optional<info> get(const model::ntp& ntp) const override {
        auto p = _pm.get(ntp);
        auto ctp_stm = p->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
        if (!p || !ctp_stm) {
            return std::nullopt;
        }
        return info{
          .committed_offset = p->committed_offset(),
          .term = p->term(),
          .is_leader = p->is_leader(),
          .last_reconciled_log_offset
          = ctp_stm->state().get_last_reconciled_log_offset(),
        };
    }

private:
    cluster::partition_manager& _pm;
};

} // namespace

// TODO(oren): error handling in here is really bad

std::unique_ptr<epoch_barrier_coordinator::partition_source>
epoch_barrier_coordinator::make_default_partition_source(
  cluster::partition_manager& pm) {
    return std::make_unique<partition_manager_source>(pm);
}

epoch_barrier_coordinator::epoch_barrier_coordinator(
  ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>& epoch_service,
  data_plane_api& data_plane,
  std::unique_ptr<partition_source> partitions)
  : _epoch_service(epoch_service)
  , _data_plane(data_plane)
  , _partitions(std::move(partitions)) {}

ss::future<> epoch_barrier_coordinator::start() { return ss::now(); }

ss::future<> epoch_barrier_coordinator::stop() { return ss::now(); }

ss::future<std::expected<std::monostate, ::rpc::errc>>
epoch_barrier_coordinator::invalidate(cluster_epoch candidate) {
    vlog(
      cd_log.debug,
      "Epoch barrier: invalidating epoch cache for candidate {}",
      candidate);

    // Reset round state so the next poll_drain re-drains and re-collects
    // seal points with current leadership.
    co_await container().invoke_on_all(
      [](epoch_barrier_coordinator& c) { c._round.reset(); });

    co_await _epoch_service.local().force_epoch_update(candidate());
    co_await _epoch_service.local().invalidate_epoch_cache(
      cluster_epoch::max());
    co_return std::monostate{};
}

epoch_barrier_coordinator::seal_point* epoch_barrier_coordinator::find_seal(
  const model::topic& topic, model::partition_id pid) {
    auto topic_it = _round->seals.find(topic);
    if (topic_it == _round->seals.end()) {
        return nullptr;
    }
    auto pid_it = topic_it->second.find(pid);
    if (pid_it == topic_it->second.end()) {
        return nullptr;
    }
    return &pid_it->second;
}

void epoch_barrier_coordinator::upsert_seal(
  const model::topic& topic, model::partition_id pid, seal_point sp) {
    _round->seals[topic][pid] = sp;
}

void epoch_barrier_coordinator::collect_local_seal_points(
  cluster_epoch candidate) {
    _round = round_state{.candidate = candidate};
    for (const auto& [ntp, pinfo] : _partitions->cloud_topic_partitions()) {
        if (!pinfo.is_leader) {
            continue;
        }
        upsert_seal(
          ntp.tp.topic,
          ntp.tp.partition,
          seal_point{.committed = pinfo.committed_offset, .term = pinfo.term});
    }
}

epoch_barrier_coordinator::seal_check_result
epoch_barrier_coordinator::check_local_seal_points() {
    if (!_round) {
        return seal_check_result::reconciled;
    }

    auto result = seal_check_result::reconciled;

    // Check existing seal points.
    for (auto& [topic, partitions] : _round->seals) {
        for (auto& [pid, seal] : partitions) {
            auto ntp = model::ntp(model::kafka_namespace, topic, pid);
            auto pinfo = _partitions->get(ntp);
            if (!pinfo || !pinfo->is_leader) {
                // Sealed partition lost leadership or was removed.
                // The seal table is stale — tell the caller to reset
                // and redrain. No point checking the rest.
                return seal_check_result::stale;
            }
            if (pinfo->term != seal.term) {
                // Term changed — push the seal point forward to the
                // current committed offset. The new offset is >= the old
                // one, so we're not regressing the target.
                seal.committed = pinfo->committed_offset;
                seal.term = pinfo->term;
                result = seal_check_result::pending;
                continue;
            }
            auto lrlo = pinfo->last_reconciled_log_offset;
            if (!lrlo || *lrlo < seal.committed) {
                result = seal_check_result::pending;
            }
        }
    }

    // Ensure every leader cloud topic partition on this shard has a seal
    // point. Partitions that gained leadership since initial collection
    // need to be tracked too.
    for (const auto& [ntp, pinfo] : _partitions->cloud_topic_partitions()) {
        if (!pinfo.is_leader) {
            continue;
        }
        if (find_seal(ntp.tp.topic, ntp.tp.partition)) {
            continue;
        }
        upsert_seal(
          ntp.tp.topic,
          ntp.tp.partition,
          seal_point{.committed = pinfo.committed_offset, .term = pinfo.term});
        result = seal_check_result::pending;
    }

    return result;
}

ss::future<std::expected<bool, ::rpc::errc>>
epoch_barrier_coordinator::poll_drain(cluster_epoch candidate) {
    // Drain and collect seal points once per round. The round state is
    // reset by invalidate(), so this runs once per barrier round.
    if (!_round || _round->candidate != candidate) {
        vlog(
          cd_log.debug,
          "Epoch barrier: draining writes for candidate {}",
          candidate);
        co_await _data_plane.drain_inflight_writes();

        // Collect seal points on ALL shards.
        co_await container().invoke_on_all(
          [candidate](epoch_barrier_coordinator& c) {
              c.collect_local_seal_points(candidate);
          });
        vlog(
          cd_log.debug,
          "Epoch barrier: drain complete, checking reconciliation for {}",
          candidate);
    }

    // Check reconciliation progress on ALL shards. The reducer picks
    // the worst outcome: stale beats pending beats reconciled.
    auto check = co_await container().map_reduce0(
      [](epoch_barrier_coordinator& c) { return c.check_local_seal_points(); },
      seal_check_result::reconciled,
      [](seal_check_result a, seal_check_result b) {
          if (a == seal_check_result::stale || b == seal_check_result::stale) {
              return seal_check_result::stale;
          }
          if (
            a == seal_check_result::pending
            || b == seal_check_result::pending) {
              return seal_check_result::pending;
          }
          return seal_check_result::reconciled;
      });

    if (check == seal_check_result::stale) {
        // At least one shard's seal table is stale (a sealed partition
        // lost leadership). Reset all shards so the next poll re-drains
        // and builds a fresh seal table.
        vlog(
          cd_log.debug,
          "Epoch barrier: seal table stale for {}, will redrain",
          candidate);
        co_await container().invoke_on_all(
          [](epoch_barrier_coordinator& c) { c._round.reset(); });
        co_return false;
    }

    if (check == seal_check_result::pending) {
        vlog(
          cd_log.debug,
          "Epoch barrier: reconciliation not yet complete for {}",
          candidate);
    }
    co_return check == seal_check_result::reconciled;
}

ss::future<std::expected<std::monostate, ::rpc::errc>>
epoch_barrier_coordinator::publish_safe_epoch(cluster_epoch safe_epoch) {
    vlog(cd_log.debug, "Epoch barrier: publishing safe epoch {}", safe_epoch);
    co_await container().invoke_on_all(
      [safe_epoch](epoch_barrier_coordinator& c) {
          c._safe_epoch = safe_epoch;
      });
    co_return std::monostate{};
}

} // namespace cloud_topics::l0::gc
