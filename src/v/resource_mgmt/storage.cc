/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage.h"

#include "cloud_storage/cache_service.h"
#include "cluster/partition_manager.h"
#include "storage/disk_log_impl.h"
#include "utils/human.h"
#include "vlog.h"

#include <seastar/util/log.hh>

static ss::logger rlog("resource_mgmt");

namespace storage {

disk_space_manager::disk_space_manager(
  config::binding<bool> enabled,
  config::binding<std::optional<uint64_t>> log_storage_target_size,
  ss::sharded<storage::api>* storage,
  ss::sharded<storage::node>* storage_node,
  ss::sharded<cloud_storage::cache>* cache,
  ss::sharded<cluster::partition_manager>* pm)
  : _enabled(std::move(enabled))
  , _storage(storage)
  , _storage_node(storage_node)
  , _cache(cache->local_is_initialized() ? cache : nullptr)
  , _pm(pm)
  , _log_storage_target_size(std::move(log_storage_target_size)) {
    _enabled.watch([this] {
        vlog(
          rlog.info,
          "{} disk space manager control loop",
          _enabled() ? "Enabling" : "Disabling");
        _control_sem.signal();
    });
}

ss::future<> disk_space_manager::start() {
    vlog(
      rlog.info,
      "Starting disk space manager service ({})",
      _enabled() ? "enabled" : "disabled");
    if (ss::this_shard_id() == run_loop_core) {
        ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
        _cache_disk_nid = _storage_node->local().register_disk_notification(
          node::disk_type::cache,
          [this](node::disk_space_info info) { _cache_disk_info = info; });
        _data_disk_nid = _storage_node->local().register_disk_notification(
          node::disk_type::data,
          [this](node::disk_space_info info) { _data_disk_info = info; });
    }
    co_return;
}

ss::future<> disk_space_manager::stop() {
    vlog(rlog.info, "Stopping disk space manager service");
    if (ss::this_shard_id() == run_loop_core) {
        _storage_node->local().unregister_disk_notification(
          node::disk_type::cache, _cache_disk_nid);
        _storage_node->local().unregister_disk_notification(
          node::disk_type::data, _data_disk_nid);
    }
    _control_sem.broken();
    co_await _gate.close();
}

ss::future<> disk_space_manager::run_loop() {
    vassert(ss::this_shard_id() == run_loop_core, "Run on wrong core");

    while (!_gate.is_closed()) {
        try {
            if (_enabled()) {
                co_await _control_sem.wait(
                  config::shard_local_cfg().retention_local_trim_interval(),
                  std::max(_control_sem.current(), size_t(1)));
            } else {
                co_await _control_sem.wait();
            }
        } catch (const ss::semaphore_timed_out&) {
            // time for some controlling
        }

        if (!_enabled()) {
            continue;
        }

        if (_log_storage_target_size().has_value()) {
            co_await manage_data_disk(_log_storage_target_size().value());
        }
    }
}

void eviction_policy::schedule::seek(size_t cursor) {
    vassert(sched_size > 0, "Seek cannot be called on an empty schedule");
    cursor = cursor % sched_size;

    // reset iterator
    shard_idx = 0;
    partition_idx = 0;

    for (; shard_idx < shards.size(); ++shard_idx) {
        const auto count = shards[shard_idx].partitions.size();
        if (cursor < count) {
            partition_idx = cursor;
            return;
        }
        cursor -= count;
    }

    vassert(false, "Seek could not find cursor location");
}

void eviction_policy::schedule::next() {
    ++partition_idx;
    while (true) {
        if (partition_idx >= shards[shard_idx].partitions.size()) {
            shard_idx = (shard_idx + 1) % shards.size();
            partition_idx = 0;
        } else {
            break;
        }
    }
}

eviction_policy::partition* eviction_policy::schedule::current() {
    return &shards[shard_idx].partitions[partition_idx];
}

ss::future<eviction_policy::schedule> eviction_policy::create_new_schedule() {
    auto shards = co_await _pm->map([this](auto& /* ignored */) {
        /*
         * the partition_manager reference is ignored since
         * collect_reclaimable_offsets already has access to a
         * sharded<partition_manager>.
         */
        return collect_reclaimable_offsets().then([](auto partitions) {
            return shard_partitions{
              .shard = ss::this_shard_id(),
              .partitions = std::move(partitions),
            };
        });
    });

    // compute the size of the schedule
    const auto size = std::accumulate(
      shards.cbegin(),
      shards.cend(),
      size_t{0},
      [](size_t acc, const shard_partitions& shard) {
          return acc + shard.partitions.size();
      });

    vlog(rlog.debug, "Created new eviction schedule with {} partitions", size);
    co_return schedule(std::move(shards), size);
}

ss::future<fragmented_vector<eviction_policy::partition>>
eviction_policy::collect_reclaimable_offsets() {
    /*
     * build a lightweight copy to avoid invalidations during iteration
     */
    fragmented_vector<ss::lw_shared_ptr<cluster::partition>> partitions;
    for (const auto& p : _pm->local().partitions()) {
        if (!p.second->remote_partition()) {
            continue;
        }
        partitions.push_back(p.second);
    }

    /*
     * retention settings mirror settings found in housekeeping()
     */
    const auto collection_threshold = [this] {
        const auto& lm = _storage->local().log_mgr();
        if (!lm.config().delete_retention().has_value()) {
            return model::timestamp(0);
        }
        const auto now = model::timestamp::now().value();
        const auto retention = lm.config().delete_retention().value().count();
        return model::timestamp(now - retention);
    };

    gc_config cfg(
      collection_threshold(),
      _storage->local().log_mgr().config().retention_bytes());

    /*
     * in smallish batches partitions are queried for their reclaimable
     * segments. all of this information is bundled up and returned.
     */
    fragmented_vector<partition> res;
    co_await ss::max_concurrent_for_each(
      partitions.begin(), partitions.end(), 20, [&res, cfg](const auto& p) {
          auto log = dynamic_cast<storage::disk_log_impl*>(p->log().get_impl());
          return log->get_reclaimable_offsets(cfg)
            .then([&res, group = p->group()](auto offsets) {
                res.push_back({
                  .group = group,
                  .offsets = std::move(offsets),
                });
            })
            .handle_exception_type([](const ss::gate_closed_exception&) {})
            .handle_exception([ntp = p->ntp()](std::exception_ptr e) {
                vlog(
                  rlog.debug,
                  "Error collecting reclaimable offsets from {}: {}",
                  ntp,
                  e);
            });
      });

    /*
     * sorting by raft::group_id would provide a more stable round-robin
     * evaluation of partitions in later processing of the result, but on small
     * timescales we don't expect enough churn to make a difference, nor with a
     * large number of partitions on the system would the non-determinism
     * in the parallelism above in max_concurrent_for_each be problematic.
     */

    vlog(rlog.trace, "Reporting reclaim offsets for {} partitions", res.size());
    co_return res;
}

ss::future<> eviction_policy::install_schedule(schedule schedule) {
    /*
     * install the schedule on each core in parallel
     */
    auto total = co_await ss::map_reduce(
      schedule.shards,
      [this](auto& shard) { return install_schedule(std::move(shard)); },
      size_t(0),
      std::plus<>());

    vlog(rlog.info, "Requested truncation from {} cloud partitions", total);
}

// the per-shard counterpart of install_schedule(schedule)
ss::future<size_t> eviction_policy::install_schedule(shard_partitions shard) {
    const auto shard_id = shard.shard;
    return _pm->invoke_on(shard_id, [shard = std::move(shard)](auto& pm) {
        size_t decisions = 0;
        for (const auto& partition : shard.partitions) {
            if (!partition.decision.has_value()) {
                continue;
            }

            auto p = pm.partition_for(partition.group);
            if (!p) {
                continue;
            }

            auto log = dynamic_cast<storage::disk_log_impl*>(
              p->log().get_impl());
            log->set_cloud_gc_offset(partition.decision.value());
            ++decisions;

            vlog(
              rlog.trace,
              "Marking offset {} in cloud partition {} group {} for estimated "
              "{} reclaim",
              partition.decision.value(),
              partition.group,
              p->ntp(),
              human::bytes(partition.total));
        }
        return decisions;
    });
}

size_t eviction_policy::evict_balanced_from_level(
  schedule& sched,
  size_t target_excess,
  std::string_view level_name,
  const level_selector& selector) {
    size_t level_total = 0;
    bool progress = false;
    auto partition = sched.current();
    const auto first = partition;

    while (true) {
        /*
         * if this is the first time visiting this partition and this level then
         * initialize the level iterator at the first segment.
         */
        auto level = selector(partition);
        if (partition->level != level) {
            partition->level = level;
            partition->iter = level->begin();
            vlog(
              rlog.trace,
              "Initializing level {} iterator for partition {} with {} "
              "candidate segments",
              level_name,
              partition->group,
              level->size());
        }

        if (partition->iter.has_value()) {
            if (partition->iter.value() == level->end()) {
                // suppress further messages about reaching end of level
                partition->iter.reset();
                vlog(
                  rlog.trace,
                  "Finished level {} iteration for partition {}",
                  level_name,
                  partition->group);
            } else {
                /*
                 * we haven't reached the end of the candidate set so schedule
                 * the current segment from this partition for removal.
                 */
                partition->decision = partition->iter.value()->offset;
                partition->total += partition->iter.value()->size;
                level_total += partition->iter.value()->size;
                progress = true;

                vlog(
                  rlog.trace,
                  "Mark partition {} at offset {} for {} removal total {} "
                  "level {} total {}",
                  partition->group,
                  partition->decision.value(),
                  human::bytes(partition->iter.value()->size),
                  human::bytes(partition->total),
                  level_name,
                  human::bytes(level_total));

                ++partition->iter.value();
            }
        }

        ++_cursor;
        sched.next();

        if (level_total > target_excess) {
            break;
        }

        partition = sched.current();

        // end level iteration if no progress is made
        if (partition == first) {
            if (!progress) {
                vlog(
                  rlog.trace,
                  "Ending level {} with no more progress possible",
                  level_name);
                break;
            }
            vlog(rlog.trace, "Restarting level {} iteration", level_name);
            progress = false;
        }
    }

    vlog(
      rlog.info,
      "Marked {} for removal with target {} in level {}",
      human::bytes(level_total),
      human::bytes(target_excess),
      level_name);

    return level_total;
}

size_t eviction_policy::evict_until_local_retention(
  schedule& sched, size_t target_excess) {
    return evict_balanced_from_level(
      sched, target_excess, "local-retention", [](auto group) {
          return &group->offsets.effective_local_retention;
      });
}

ss::future<> disk_space_manager::manage_data_disk(uint64_t target_size) {
    /*
     * query log storage usage across all cores
     */
    storage::usage_report usage;
    try {
        usage = co_await _storage->local().disk_usage();
    } catch (...) {
        vlog(
          rlog.info,
          "Unable to collect log storage usage. Skipping management tick: "
          "{}",
          std::current_exception());
        co_return;
    }

    /*
     * how much data from log storage do we need to remove from disk to be able
     * to stay below the current target size?
     */
    const auto target_excess = usage.usage.total() < target_size
                                 ? 0
                                 : usage.usage.total() - target_size;
    if (target_excess <= 0) {
        vlogl(
          rlog,
          _previous_reclaim ? ss::log_level::info : ss::log_level::debug,
          "Log storage usage {} <= target size {}. No work to do.",
          human::bytes(usage.usage.total()),
          human::bytes(target_size));
        _previous_reclaim = false;
        co_return;
    }
    _previous_reclaim = true;

    /*
     * when log storage has exceeded the target usage, then there are some knobs
     * that can be adjusted to help stay below this target.
     *
     * the first knob is to prioritize garbage collection. this is normally a
     * periodic task performed by the storage layer. if it hasn't run recently,
     * or it has been spending most of its time doing low impact compaction
     * work, then we can instead immediately begin applying retention rules.
     *
     * the effect of gc is defined entirely by topic configuration and current
     * state of storage. so in order to turn the first knob we need only trigger
     * gc across shards.
     *
     * however, if current retention is not sufficient to bring usage below the
     * target, then a second knob must be turned: overriding local retention
     * targets for cloud-enabled topics, removing data that has been backed up
     * into the cloud.
     */
    if (target_excess > usage.reclaim.retention) {
        vlog(
          rlog.info,
          "Log storage usage {} > target size {} by {}. Garbage collection "
          "expected to recover {}. Overriding tiered storage retention to "
          "recover {}. Total estimated available to recover {}",
          human::bytes(usage.usage.total()),
          human::bytes(target_size),
          human::bytes(target_excess),
          human::bytes(usage.reclaim.retention),
          human::bytes(target_excess - usage.reclaim.retention),
          human::bytes(usage.reclaim.available));
    } else {
        vlog(
          rlog.info,
          "Log storage usage {} > target size {} by {}. Garbage collection "
          "expected to recover {}.",
          human::bytes(usage.usage.total()),
          human::bytes(target_size),
          human::bytes(target_excess),
          human::bytes(usage.reclaim.retention));
    }

    /*
     * ask storage across all nodes to apply retention rules asap.
     */
    co_await _storage->invoke_on_all([](api& api) { api.trigger_gc(); });
}

} // namespace storage
