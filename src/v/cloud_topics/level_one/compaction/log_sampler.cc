/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_sampler.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

log_sampler::log_sampler(
  metastore* metastore, cluster::metadata_cache* metadata_cache)
  : _metastore(metastore)
  , _metadata_cache(metadata_cache) {}

ss::future<chunked_vector<log_info_and_meta>> log_sampler::sample_logs(
  log_list_t& logs, std::optional<size_t> size_hint) const {
    chunked_vector<metastore::compaction_sample_spec> to_sample;

    if (size_hint.has_value()) {
        to_sample.reserve(size_hint.value());
    }

    auto now = model::timestamp::now();
    for (const auto& log : logs) {
        if (!log.link.is_linked()) {
            continue;
        }

        auto topic_cfg_opt = _metadata_cache->get_topic_metadata_ref(
          model::topic_namespace_view(log.ntp.ns, log.ntp.tp.topic));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        const auto& topic_cfg = topic_cfg_opt.value().get().get_configuration();
        auto tombstone_removal_ts = [&topic_cfg, now]() -> model::timestamp {
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
                     ? now - model::timestamp(delete_retention_ms->count())
                     : model::timestamp::max();
        }();
        to_sample.emplace_back(log.tidp, tombstone_removal_ts);
    }

    to_sample.shrink_to_fit();
    auto samples = co_await _metastore->get_compaction_infos(to_sample);

    chunked_vector<log_info_and_meta> ret;
    ret.reserve(samples.size());
    for (auto& log : logs) {
        if (!log.link.is_linked()) {
            continue;
        }

        auto& sample = samples.at(log.tidp);

        if (!sample.has_value()) {
            vlog(
              compaction_log.warn,
              "Failed to collect sample for CTP {} during compaction: {}",
              log.tidp,
              sample.error());
            continue;
        }

        ret.emplace_back(std::move(sample).value(), &log);
    }

    ret.shrink_to_fit();
    co_return ret;
}

} // namespace cloud_topics::l1
