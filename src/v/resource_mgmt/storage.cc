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

    /*
     * In the short term this frequency should be low enough to provide decent
     * reactivity to changes in space usage, but not too low as to do an
     * excessive amount of work. Medium term we want storage to trigger an
     * upcall to start the monitor loop when it appears that we are getting
     * close to an important threshold.
     */
    constexpr auto frequency = std::chrono::seconds(5);

    while (!_gate.is_closed()) {
        try {
            if (_enabled()) {
                co_await _control_sem.wait(
                  frequency, std::max(_control_sem.current(), size_t(1)));
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

/*
 * Attempts to set retention offset for cloud enabled topics such that the
 * target amount of bytes will be recovered when garbage collection is applied.
 *
 * This is greedy approach which will recover as much as possible from each
 * partition before moving on to the next.
 */
static ss::future<size_t>
set_partition_retention_offsets(cluster::partition_manager& pm, size_t target) {
    // build a lightweight copy to avoid invalidations during iteration
    fragmented_vector<ss::lw_shared_ptr<cluster::partition>> partitions;
    for (const auto& p : pm.partitions()) {
        if (!p.second->remote_partition()) {
            continue;
        }
        partitions.push_back(p.second);
    }

    size_t partitions_total = 0;
    for (const auto& p : partitions) {
        if (partitions_total >= target) {
            break;
        }

        auto log = dynamic_cast<storage::disk_log_impl*>(p->log().get_impl());
        // make sure housekeeping doesn't delete the log from below us
        auto gate = log->gate().hold();

        auto segments = log->cloud_gc_eligible_segments();
        if (segments.empty()) {
            continue;
        }

        size_t log_total = 0;
        auto offset = segments.front()->offsets().committed_offset;
        for (const auto& seg : segments) {
            auto usage = co_await seg->persistent_size();
            log_total += usage.total();
            if (log_total >= target) {
                break;
            }
        }

        vlog(
          rlog.info,
          "Setting retention offset override {} estimated reclaim of {} for "
          "cloud topic {}",
          offset,
          log_total,
          p->ntp());

        log->set_cloud_gc_offset(offset);
        partitions_total += log_total;
    }

    co_return partitions_total;
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
        vlog(
          rlog.info,
          "Log storage usage {} <= target size {}. No work to do.",
          human::bytes(usage.usage.total()),
          human::bytes(target_size));
        co_return;
    }

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

        /*
         * This is a simple greedy approach. It will attempt to reclaim as much
         * data as possible from each partition on each core, stopping once
         * enough space has been reclaimed to meet the current target.
         */
        size_t total = 0;
        for (auto shard : ss::smp::all_cpus()) {
            auto goal = target_excess - total;
            total += co_await _pm->invoke_on(shard, [goal](auto& pm) {
                return set_partition_retention_offsets(pm, goal);
            });
            if (total >= target_excess) {
                break;
            }
        }
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
