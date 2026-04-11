/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_info_collector.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "compaction/utils.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

namespace {

inline bool needs_compaction(
  const log_compaction_meta& log,
  const cluster::topic_configuration& topic_cfg) {
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
      log.info_and_ts->info.dirty_ratio,
      min_cleanable_dirty_ratio,
      log.info_and_ts->info.earliest_dirty_ts,
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

ss::future<> log_info_collector::collect_info_for_logs(
  log_set_t& logs_set,
  log_list_t& logs_list,
  log_compaction_queue& compaction_queue) const {
    auto now = model::timestamp::now();

    auto to_collect = get_logs_to_collect(logs_list, logs_set.size(), now);

    auto compaction_infos_res = co_await _metastore->get_compaction_infos(
      to_collect);
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
        if (log.link.is_linked() && compaction_infos.contains(log.tidp)) {
            // Use kafka::offset::min() as a placeholder; real values are filled
            // in by fill_max_compactible_offsets below.
            ntp_to_max_compactible_offset.insert_or_assign(
              log.ntp, kafka::offset::min());
        }
    }

    co_await _max_compactible_offset_provider->fill_max_compactible_offsets(
      ntp_to_max_compactible_offset);

    populate_log_infos(
      compaction_infos,
      logs_set,
      logs_list,
      compaction_queue,
      ntp_to_max_compactible_offset,
      now);
}

chunked_vector<metastore::compaction_info_spec>
log_info_collector::get_logs_to_collect(
  log_list_t& logs_list,
  size_t size,
  model::timestamp collection_timestamp) const {
    chunked_vector<metastore::compaction_info_spec> to_collect;

    to_collect.reserve(size);

    for (const auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (log.state == log_compaction_meta::log_state::inflight) {
            // No need to sample inflight logs
            vlog(
              compaction_log.debug,
              "Skipping info collection for CTP {}, compaction is inflight",
              log.ntp);
            continue;
        }

        if (log.info_and_ts.has_value()) {
            auto sample_interval
              = config::shard_local_cfg().cloud_topics_compaction_interval_ms();
            auto delta = to_time_point(collection_timestamp)
                         - to_time_point(log.info_and_ts->collected_at);
            if (delta <= sample_interval) {
                vlog(
                  compaction_log.debug,
                  "Skipping info collection for CTP {}, delta is less than "
                  "sample interval.",
                  log.ntp);

                continue;
            }
        }

        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
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

        to_collect.emplace_back(log.tidp, tombstone_removal_ts);
    }

    to_collect.shrink_to_fit();
    return to_collect;
}

void log_info_collector::populate_log_infos(
  metastore::compaction_info_map& compaction_infos,
  log_set_t& logs_set,
  log_list_t& logs_list,
  log_compaction_queue& compaction_queue,
  const chunked_hash_map<model::ntp, kafka::offset>&
    ntp_to_max_compactible_offset,
  model::timestamp collection_timestamp) const {
    for (auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (log.state == log_compaction_meta::log_state::inflight) {
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
              "Failed to collect compaction info for CTP {} during compaction: "
              "{}",
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
        log.info_and_ts = compaction_info_and_timestamp{
          .info = std::move(compaction_info).value(),
          .collected_at = collection_timestamp,
          .max_compactible_offset = max_compactible_offset};

        vlog(
          compaction_log.debug,
          "Compaction info for CTP {} returned {} with max_compactible_offset: "
          "{}",
          log.ntp,
          log.info_and_ts->info,
          max_compactible_offset);

        if (log.state != log_compaction_meta::log_state::idle) {
            // We don't need to queue an already queued log.
            continue;
        }

        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get();

        if (needs_compaction(log, topic_cfg)) {
            auto ptr_it = logs_set.find(log.tidp);
            if (ptr_it != logs_set.end()) {
                log.state = log_compaction_meta::log_state::queued;
                compaction_queue.push(*ptr_it);
            }
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
