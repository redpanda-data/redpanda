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
  metastore* metastore, ss::sharded<cluster::topic_table>* topic_table)
  : _metastore(metastore)
  , _topic_table(topic_table) {}

ss::future<chunked_vector<log_info_and_meta>>
log_sampler::sample_logs(log_list_t& logs) const {
    chunked_vector<metastore::to_sample_info> to_sample;
    for (const auto& log : logs) {
        if (!log.link.is_linked()) {
            continue;
        }

        // TODO: replace with `ntp_config`? Don't override default
        // `delete_retention_ms` here (user could describe topic and see empty)
        auto topic_cfg_opt = _topic_table->local().get_topic_cfg(
          model::topic_namespace_view(log.ntp.ns, log.ntp.tp.topic));

        if (!topic_cfg_opt.has_value()) {
            continue;
        }

        auto& topic_cfg = topic_cfg_opt.value();
        auto delete_retention_ms = [&topic_cfg]() {
            if (topic_cfg.properties.delete_retention_ms.has_optional_value()) {
                return topic_cfg.properties.delete_retention_ms.value();
            } else {
                static constexpr std::chrono::milliseconds
                  default_delete_retention_ms
                  = 86400000ms;
                return config::shard_local_cfg()
                  .tombstone_retention_ms()
                  .value_or(default_delete_retention_ms);
            }
        }();
        auto tombstone_removal_ts = model::timestamp::now()
                                    - model::timestamp(
                                      delete_retention_ms.count());
        to_sample.emplace_back(log.tid_p, tombstone_removal_ts);
    }

    auto samples = co_await _metastore->get_compaction_infos(to_sample);

    vassert(
      samples.size() == logs.size(),
      "Sizes of collected samples and container of logs differ");

    chunked_vector<log_info_and_meta> ret;
    ret.reserve(samples.size());
    for (auto&& [log, sample] : std::views::zip(logs, samples)) {
        if (!log.link.is_linked()) {
            continue;
        }

        if (!sample.has_value()) {
            vlog(
              compaction_log.debug,
              "Failed to collect sample for ntp {} during compaction: {}",
              log.ntp,
              sample.error());
            continue;
        }

        ret.emplace_back(std::move(sample).value(), &log);
    }

    ret.shrink_to_fit();
    co_return ret;
}

} // namespace cloud_topics::l1
