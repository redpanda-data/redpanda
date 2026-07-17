/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/log_info_collector.h"

#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "compaction/utils.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

namespace cloud_topics::l1 {

namespace {

inline bool needs_compaction(
  const metastore::compaction_info_response& info,
  const cluster::topic_configuration& topic_cfg) {
    if (!topic_cfg.is_compacted()) {
        return false;
    }
    auto& topic_mcdr = topic_cfg.properties.min_cleanable_dirty_ratio;
    auto min_cleanable_dirty_ratio
      = topic_mcdr.has_optional_value()
          ? topic_mcdr.value()
          : config::shard_local_cfg().min_cleanable_dirty_ratio().value_or(0.0);
    auto& topic_mcl = topic_cfg.properties.max_compaction_lag_ms;
    auto max_compaction_lag_ms
      = topic_mcl.has_value()
          ? topic_mcl.value()
          : config::shard_local_cfg().max_compaction_lag_ms();
    return compaction::log_needs_compaction(
      info.dirty_ratio,
      min_cleanable_dirty_ratio,
      info.earliest_dirty_ts,
      max_compaction_lag_ms);
}

} // namespace

topic_cfg_provider_impl::topic_cfg_provider_impl(
  cluster::metadata_cache* metadata_cache)
  : _metadata_cache(metadata_cache) {}

std::optional<std::reference_wrapper<const cluster::topic_configuration>>
topic_cfg_provider_impl::get_topic_cfg(model::topic_namespace_view tp) const {
    auto topic_md_ref = _metadata_cache->get_topic_metadata_ref(tp);
    if (!topic_md_ref.has_value()) {
        return std::nullopt;
    }

    return topic_md_ref.value().get().get_configuration();
}

max_compactible_offset_provider_impl::max_compactible_offset_provider_impl(
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::partition_manager>* partition_manager)
  : _shard_table(shard_table)
  , _partition_manager(partition_manager) {}

ss::future<> max_compactible_offset_provider_impl::fill_max_compactible_offsets(
  chunked_hash_map<model::ntp, kafka::offset>& ntp_to_max_compactible_offset)
  const {
    // Group NTPs by their owning shard to batch cross-shard calls.
    chunked_hash_map<ss::shard_id, chunked_vector<model::ntp>> ntps_by_shard;
    for (const auto& [ntp, _] : ntp_to_max_compactible_offset) {
        auto shard_opt = _shard_table->local().shard_for(ntp);
        if (shard_opt) {
            ntps_by_shard[*shard_opt].push_back(ntp);
        }
    }

    for (auto& [shard, shard_ntps] : ntps_by_shard) {
        auto shard_results = co_await _partition_manager->invoke_on(
          shard,
          [ntps = std::move(shard_ntps)](
            const cluster::partition_manager& pm) mutable {
              chunked_hash_map<model::ntp, kafka::offset> results;
              for (auto& ntp : ntps) {
                  auto p = pm.get(ntp);
                  if (!p) {
                      continue;
                  }
                  auto lowest_pinned = p->raft()
                                         ->log()
                                         ->stm_hookset()
                                         ->lowest_pinned_data_offset();
                  auto max_compactible = lowest_pinned.has_value()
                                           ? kafka::prev_offset(
                                               lowest_pinned.value())
                                           : kafka::offset::max();
                  results.insert_or_assign(std::move(ntp), max_compactible);
              }
              return results;
          });

        for (auto& [ntp, offset] : shard_results) {
            ntp_to_max_compactible_offset.insert_or_assign(
              std::move(ntp), offset);
        }
    }
}

log_info_collector::log_info_collector(
  metastore* metastore,
  std::unique_ptr<topic_cfg_provider> tp_metadata_provider,
  std::unique_ptr<max_compactible_offset_provider>
    max_compactible_offset_provider)
  : _metastore(metastore)
  , _topic_metadata_provider(std::move(tp_metadata_provider))
  , _max_compactible_offset_provider(
      std::move(max_compactible_offset_provider)) {}

ss::future<> log_info_collector::collect_compaction_info(
  log_set_t& logs_set,
  log_list_t& logs_list,
  compaction_queue& compaction_queue) const {
    auto now = model::timestamp::now();

    auto specs = build_compaction_specs(logs_list, logs_set.size(), now);

    if (specs.empty()) {
        co_return;
    }

    auto compaction_infos_res = co_await _metastore->get_compaction_infos(
      specs);
    if (!compaction_infos_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Failed to retrieve compaction info from metastore: {}",
          compaction_infos_res.error());
        co_return;
    }

    auto compaction_infos = std::move(compaction_infos_res).value();

    // Collect NTPs that need max compactible offset lookups.
    chunked_hash_map<model::ntp, kafka::offset> ntp_to_max_compactible_offset;
    for (const auto& log : logs_list) {
        // We have to iterate over logs_list and perform a look-up in
        // compaction_infos unfortunately due to grouping by tidp, but needing
        // to look up compactible_offsets by ntp. If shard_table offered a way
        // to look up by tidp, this wouldn't be pessimized.
        if (compaction_infos.contains(log.tidp)) {
            // Use kafka::offset::min() as a placeholder; real values are filled
            // in by fill_max_compactible_offsets below.
            ntp_to_max_compactible_offset.insert_or_assign(
              log.ntp, kafka::offset::min());
        }
    }

    co_await _max_compactible_offset_provider->fill_max_compactible_offsets(
      ntp_to_max_compactible_offset);

    populate_logs_with_compaction_info(
      compaction_infos,
      logs_set,
      logs_list,
      compaction_queue,
      ntp_to_max_compactible_offset,
      now);
}

chunked_vector<metastore::compaction_info_spec>
log_info_collector::build_compaction_specs(
  log_list_t& logs_list,
  size_t size,
  model::timestamp collection_timestamp) const {
    chunked_vector<metastore::compaction_info_spec> specs;

    specs.reserve(size);

    for (const auto& log : logs_list) {
        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        // All cloud topics are managed, but only compacted ones are sampled
        // for compaction; non-compacted topics are managed solely for leveling.
        if (!topic_cfg_opt.value().get().is_compacted()) {
            continue;
        }

        if (log.compaction.inflight_shard.has_value()) {
            // No need to sample inflight logs
            vlog(
              compaction_log.debug,
              "Skipping info collection for CTP {}, compaction is inflight",
              log.ntp);
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get();
        auto tombstone_removal_ts =
          [&topic_cfg, collection_timestamp]() -> model::timestamp {
            // Cleaned ranges with tombstones that were cleaned at or below
            // tombstone_removal_upper_bound_ts are eligible to have tombstones
            // entirely removed.
            auto delete_retention_ms
              = config::shard_local_cfg().tombstone_retention_ms();
            if (topic_cfg.properties.delete_retention_ms.has_optional_value()) {
                delete_retention_ms
                  = topic_cfg.properties.delete_retention_ms.value();
            }

            if (topic_cfg.properties.delete_retention_ms.is_disabled()) {
                delete_retention_ms = std::nullopt;
            }

            return delete_retention_ms.has_value()
                     ? collection_timestamp
                         - model::timestamp(delete_retention_ms->count())
                     : model::timestamp::min();
        }();
        vlog(
          compaction_log.debug,
          "Sampling CTP {} with tombstone removal upper bound timestamp {}",
          log.ntp,
          tombstone_removal_ts);

        specs.emplace_back(log.tidp, tombstone_removal_ts);
    }

    specs.shrink_to_fit();
    return specs;
}

void log_info_collector::populate_logs_with_compaction_info(
  metastore::compaction_info_map& compaction_infos,
  log_set_t& logs_set,
  log_list_t& logs_list,
  compaction_queue& compaction_queue,
  const chunked_hash_map<model::ntp, kafka::offset>&
    ntp_to_max_compactible_offset,
  model::timestamp collection_timestamp) const {
    for (auto& log : logs_list) {
        if (log.compaction.inflight_shard.has_value()) {
            // Don't step on compaction info that is actively being used.
            continue;
        }

        auto it = compaction_infos.find(log.tidp);
        if (it == compaction_infos.end()) {
            // Likely this log was not sampled because the log was previously
            // sampled less than `gather_interval` time ago.
            continue;
        }

        auto& compaction_info = it->second;

        if (!compaction_info.has_value()) {
            // Minimize logging on benign `missing_ntp` errors in case
            // the `metastore` does not yet have any reconciled data for the log
            // in question.
            auto err = compaction_info.error();
            auto lvl = err == metastore::errc::missing_ntp
                           && !log.has_seen_reconciled_data
                         ? ss::log_level::debug
                         : ss::log_level::warn;

            vlogl(
              compaction_log,
              lvl,
              "Failed to collect compaction info for CTP {}: {}",
              log.ntp,
              err);
            continue;
        }

        auto offset_it = ntp_to_max_compactible_offset.find(log.ntp);
        if (offset_it == ntp_to_max_compactible_offset.end()) {
            // Likely this log was concurrently removed during some scheduling
            // point.
            continue;
        }

        auto max_compactible_offset = offset_it->second;

        log.has_seen_reconciled_data = true;
        auto info_and_ts = compaction_info_and_timestamp{
          .info = std::move(compaction_info).value(),
          .collected_at = collection_timestamp,
          .max_compactible_offset = max_compactible_offset};

        vlog(
          compaction_log.debug,
          "Compaction info for CTP {} returned {} with max_compactible_offset: "
          "{}",
          log.ntp,
          info_and_ts.info,
          max_compactible_offset);

        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get();

        if (!needs_compaction(info_and_ts.info, topic_cfg)) {
            continue;
        }

        auto ptr_it = logs_set.find(log.tidp);
        if (ptr_it == logs_set.end()) {
            continue;
        }

        // (Re)build the job from this fresh sample and enqueue it. If the CTP
        // is already queued, `push` replaces its job in place at the new
        // priority; an inflight CTP is skipped above, so its job is never
        // disturbed.
        auto job = ss::make_lw_shared<compaction_job>(
          *ptr_it, std::move(info_and_ts));
        compaction_queue.push(std::move(job));
    }
}

chunked_vector<metastore::leveling_info_spec>
log_info_collector::build_leveling_specs(log_list_t& logs_list) const {
    auto target_size
      = config::shard_local_cfg().cloud_topics_reconciliation_max_object_size();
    auto ratio
      = config::shard_local_cfg().cloud_topics_leveling_min_extent_size_ratio();
    auto min_acceptable = static_cast<size_t>(
      static_cast<double>(target_size) * ratio);

    chunked_vector<metastore::leveling_info_spec> specs;
    for (auto& log : logs_list) {
        if (log.compaction.inflight_shard.has_value()) {
            continue;
        }
        specs.emplace_back(
          metastore::leveling_info_spec{log.tidp, min_acceptable});
    }
    return specs;
}

ss::future<> log_info_collector::collect_leveling_info(
  log_set_t& logs_set,
  log_list_t& logs_list,
  leveling_queue& leveling_queue) const {
    auto now = model::timestamp::now();

    auto specs = build_leveling_specs(logs_list);

    if (specs.empty()) {
        co_return;
    }

    auto leveling_infos_res = co_await _metastore->get_leveling_infos(specs);
    if (!leveling_infos_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Failed to retrieve leveling info from metastore: {}",
          leveling_infos_res.error());
        co_return;
    }

    auto leveling_infos = std::move(leveling_infos_res).value();

    populate_logs_with_leveling_info(
      leveling_infos, logs_set, leveling_queue, now);
}

void log_info_collector::populate_logs_with_leveling_info(
  metastore::leveling_info_map& leveling_infos,
  log_set_t& logs_set,
  leveling_queue& leveling_queue,
  model::timestamp collection_timestamp) const {
    for (auto& [tidp, leveling_info] : leveling_infos) {
        auto log_it = logs_set.find(tidp);
        if (log_it == logs_set.end()) {
            // CTP was concurrently unmanaged while the RPC was in flight.
            continue;
        }
        auto& log = *log_it;
        if (log->compaction.inflight_shard.has_value()) {
            continue;
        }
        if (!leveling_info.has_value()) {
            // Minimize logging on benign `missing_ntp` errors in case
            // the `metastore` does not yet have any reconciled data for the log
            // in question.
            auto err = leveling_info.error();
            auto lvl = err == metastore::errc::missing_ntp
                           && !log->has_seen_reconciled_data
                         ? ss::log_level::debug
                         : ss::log_level::warn;

            vlogl(
              compaction_log,
              lvl,
              "Failed to collect leveling info for CTP {}: {}",
              log->ntp,
              err);
            continue;
        }

        log->has_seen_reconciled_data = true;
        auto info = std::move(leveling_info).value();

        vlog(
          compaction_log.debug,
          "Leveling info for CTP {} returned {}",
          log->ntp,
          info);

        // This fresh metastore sample supersedes whatever we previously queued
        // for the CTP, so drop its existing queue and rebuild it below from the
        // newly collected ranges.
        leveling_queue.clear(tidp);

        // Consult the CTP's inflight ranges (dequeued for leveling but not yet
        // committed) when rebuilding the queue.
        auto& inflight = log->leveling.inflight_ranges;

        // First, evict entries whose completion timestamp came before
        // collection_timestamp, since the metastore logically knows about these
        // updates already.
        offset_interval_map<std::optional<model::timestamp>> retained;
        auto inflight_stream = inflight.make_stream();
        while (inflight_stream.has_next()) {
            auto range = inflight_stream.next();
            const auto& committed_at = range.value;
            const bool expired
              = committed_at.has_value()
                && (collection_timestamp > committed_at.value());
            if (!expired) {
                retained.insert(
                  range.base_offset, range.last_offset, committed_at);
            }
        }
        inflight = std::move(retained);

        for (auto& range : info.ranges) {
            // Skip any range that overlaps one already inflight; its rewrite
            // has not yet committed, so the metastore still reports it as
            // levelable. Inflight ranges are recorded on dequeue, not here.
            if (inflight.overlaps(range.base_offset, range.last_offset)) {
                continue;
            }
            auto job = ss::make_lw_shared<leveling_job>(log, range, info.epoch);
            leveling_queue.push(std::move(job));
        }
    }
}

log_info_collector make_default_log_info_collector(
  metastore* metastore,
  cluster::metadata_cache* metadata_cache,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::partition_manager>* partition_manager) {
    return log_info_collector(
      metastore,
      std::make_unique<topic_cfg_provider_impl>(metadata_cache),
      std::make_unique<max_compactible_offset_provider_impl>(
        shard_table, partition_manager));
}

} // namespace cloud_topics::l1
