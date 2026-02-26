/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/log_info_collector.h"

#include "base/units.h"
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
  const log_maintenance_meta& log,
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
      log.compaction_info_and_ts->info.dirty_ratio,
      min_cleanable_dirty_ratio,
      log.compaction_info_and_ts->info.earliest_dirty_ts,
      max_compaction_lag_ms);
}

inline bool needs_leveling(const log_maintenance_meta& log) {
    const auto& info = log.leveling_info_and_ts->info;
    if (info.leveling_ranges.empty()) {
        return false;
    }
    auto min_ratio
      = config::shard_local_cfg().cloud_topics_min_levelable_ratio();
    return info.levelable_ratio >= min_ratio;
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
  log_compaction_queue& compaction_queue) const {
    auto now = model::timestamp::now();
    auto specs = get_compaction_specs(logs_list, logs_set.size(), now);

    if (specs.empty()) {
        co_return;
    }

    metastore::compaction_info_map compaction_infos;
    auto res = co_await _metastore->get_compaction_infos(specs);
    if (!res.has_value()) {
        vlog(
          maintenance_log.warn,
          "Failed to retrieve compaction info from metastore: {}",
          res.error());
        co_return;
    }
    compaction_infos = std::move(res).value();

    // Collect NTPs that need max compactible offset lookups. We have to
    // iterate over logs_list and perform a look-up in compaction_infos
    // unfortunately due to grouping by tidp, but needing to look up
    // compactible_offsets by ntp. If shard_table offered a way to look up
    // by tidp, this wouldn't be pessimized.
    chunked_hash_map<model::ntp, kafka::offset> ntp_to_max_compactible_offset;
    for (const auto& log : logs_list) {
        if (log.link.is_linked() && compaction_infos.contains(log.tidp)) {
            ntp_to_max_compactible_offset.insert_or_assign(
              log.ntp, kafka::offset::min());
        }
    }

    if (!ntp_to_max_compactible_offset.empty()) {
        co_await _max_compactible_offset_provider->fill_max_compactible_offsets(
          ntp_to_max_compactible_offset);
    }

    populate_compaction_infos(
      compaction_infos,
      logs_set,
      logs_list,
      compaction_queue,
      ntp_to_max_compactible_offset,
      now);
}

ss::future<> log_info_collector::collect_leveling_info(
  log_set_t& logs_set,
  log_list_t& logs_list,
  log_leveling_queue& leveling_queue) const {
    auto now = model::timestamp::now();
    auto specs = get_leveling_specs(logs_list, logs_set.size(), now);

    if (specs.empty()) {
        co_return;
    }

    metastore::leveling_info_map leveling_infos;
    auto res = co_await _metastore->get_leveling_infos(specs);
    if (!res.has_value()) {
        vlog(
          maintenance_log.warn,
          "Failed to retrieve leveling info from metastore: {}",
          res.error());
        co_return;
    }
    leveling_infos = std::move(res).value();

    populate_leveling_infos(
      leveling_infos, logs_set, logs_list, leveling_queue, now);
}

chunked_vector<metastore::compaction_info_spec>
log_info_collector::get_compaction_specs(
  log_list_t& logs_list,
  size_t size,
  model::timestamp collection_timestamp) const {
    chunked_vector<metastore::compaction_info_spec> specs;
    specs.reserve(size);

    auto compaction_interval
      = config::shard_local_cfg().cloud_topics_compaction_interval_ms();

    for (const auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (log.state == log_maintenance_meta::log_state::inflight) {
            vlog(
              maintenance_log.debug,
              "Skipping compaction info collection for CTP {}, maintenance "
              "is inflight",
              log.ntp);
            continue;
        }

        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get();
        if (!topic_cfg.is_compacted()) {
            continue;
        }

        if (log.compaction_info_and_ts.has_value()) {
            auto delta = to_time_point(collection_timestamp)
                         - to_time_point(
                           log.compaction_info_and_ts->collected_at);
            if (delta <= compaction_interval) {
                vlog(
                  maintenance_log.debug,
                  "Skipping compaction info collection for CTP {}, delta is "
                  "less than sample interval.",
                  log.ntp);
                continue;
            }
        }

        auto tombstone_removal_ts =
          [&topic_cfg, collection_timestamp]() -> model::timestamp {
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
          maintenance_log.debug,
          "Sampling CTP {} with tombstone removal upper bound timestamp {}",
          log.ntp,
          tombstone_removal_ts);

        specs.emplace_back(log.tidp, tombstone_removal_ts);
    }

    specs.shrink_to_fit();
    return specs;
}

chunked_vector<metastore::leveling_info_spec>
log_info_collector::get_leveling_specs(
  log_list_t& logs_list,
  size_t size,
  model::timestamp collection_timestamp) const {
    chunked_vector<metastore::leveling_info_spec> specs;
    specs.reserve(size);

    auto leveling_interval
      = config::shard_local_cfg().cloud_topics_leveling_interval_ms();

    for (const auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (log.state == log_maintenance_meta::log_state::inflight) {
            vlog(
              maintenance_log.debug,
              "Skipping leveling info collection for CTP {}, maintenance "
              "is inflight",
              log.ntp);
            continue;
        }

        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get();
        if (topic_cfg.is_compacted()) {
            continue;
        }

        if (log.leveling_info_and_ts.has_value()) {
            auto delta = to_time_point(collection_timestamp)
                         - to_time_point(
                           log.leveling_info_and_ts->collected_at);
            if (delta <= leveling_interval) {
                vlog(
                  maintenance_log.debug,
                  "Skipping leveling info collection for CTP {}, delta is "
                  "less than sample interval.",
                  log.ntp);
                continue;
            }
        }

        auto object_size_threshold
          = config::shard_local_cfg()
              .cloud_topics_leveling_object_size_threshold();
        auto max_object_size = config::shard_local_cfg()
                                 .cloud_topics_reconciliation_max_object_size();
        auto removed_data_threshold
          = config::shard_local_cfg()
              .cloud_topics_leveling_removed_data_threshold();
        specs.push_back(
          metastore::leveling_info_spec{
            .tidp = log.tidp,
            .min_acceptable_object_size = static_cast<size_t>(
              object_size_threshold * static_cast<double>(max_object_size)),
            .removed_data_threshold = removed_data_threshold});
    }

    specs.shrink_to_fit();
    return specs;
}

void log_info_collector::populate_compaction_infos(
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

        if (log.state == log_maintenance_meta::log_state::inflight) {
            continue;
        }

        auto it = compaction_infos.find(log.tidp);
        if (it == compaction_infos.end()) {
            continue;
        }

        auto& compaction_info = it->second;

        if (!compaction_info.has_value()) {
            auto err = compaction_info.error();
            auto lvl = err == metastore::errc::missing_ntp
                           && !log.has_seen_reconciled_data
                         ? ss::log_level::debug
                         : ss::log_level::warn;

            vlogl(
              maintenance_log,
              lvl,
              "Failed to collect compaction info for CTP {} during "
              "compaction: {}",
              log.ntp,
              err);
            continue;
        }

        auto offset_it = ntp_to_max_compactible_offset.find(log.ntp);
        if (offset_it == ntp_to_max_compactible_offset.end()) {
            continue;
        }

        auto max_compactible_offset = offset_it->second;

        log.has_seen_reconciled_data = true;
        log.compaction_info_and_ts = compaction_info_and_timestamp{
          .info = std::move(compaction_info).value(),
          .collected_at = collection_timestamp,
          .max_compactible_offset = max_compactible_offset};

        vlog(
          maintenance_log.debug,
          "Compaction info for CTP {} returned {} with "
          "max_compactible_offset: {}",
          log.ntp,
          log.compaction_info_and_ts->info,
          max_compactible_offset);

        if (log.state != log_maintenance_meta::log_state::idle) {
            continue;
        }

        auto topic_cfg_opt = _topic_metadata_provider->get_topic_cfg(
          model::topic_namespace_view(log.ntp));
        if (
          topic_cfg_opt.has_value()
          && needs_compaction(log, topic_cfg_opt.value().get())) {
            auto ptr_it = logs_set.find(log.tidp);
            if (ptr_it != logs_set.end()) {
                log.state = log_maintenance_meta::log_state::queued;
                compaction_queue.push(*ptr_it);
            }
        }
    }
}

void log_info_collector::populate_leveling_infos(
  metastore::leveling_info_map& leveling_infos,
  log_set_t& logs_set,
  log_list_t& logs_list,
  log_leveling_queue& leveling_queue,
  model::timestamp collection_timestamp) const {
    for (auto& log : logs_list) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (log.state == log_maintenance_meta::log_state::inflight) {
            continue;
        }

        auto it = leveling_infos.find(log.tidp);
        if (it == leveling_infos.end()) {
            continue;
        }

        auto& leveling_info = it->second;

        if (!leveling_info.has_value()) {
            auto err = leveling_info.error();
            auto lvl = err == metastore::errc::missing_ntp
                           && !log.has_seen_reconciled_data
                         ? ss::log_level::debug
                         : ss::log_level::warn;
            vlogl(
              maintenance_log,
              lvl,
              "Failed to collect leveling info for CTP {} during "
              "leveling: {}",
              log.ntp,
              err);
            continue;
        }

        log.has_seen_reconciled_data = true;
        log.leveling_info_and_ts = leveling_info_and_timestamp{
          .info = std::move(leveling_info).value(),
          .collected_at = collection_timestamp};

        vlog(
          maintenance_log.debug,
          "Leveling info for CTP {} returned {}",
          log.ntp,
          log.leveling_info_and_ts->info);

        if (log.state != log_maintenance_meta::log_state::idle) {
            continue;
        }

        if (needs_leveling(log)) {
            auto ptr_it = logs_set.find(log.tidp);
            if (ptr_it != logs_set.end()) {
                log.state = log_maintenance_meta::log_state::queued;
                leveling_queue.push(*ptr_it);
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
