/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/materialized_segments.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_segment.h"
#include "ssx/future-util.h"
#include "vlog.h"

#include <chrono>

namespace cloud_storage {

using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;

materialized_segments::materialized_segments()
  : _stm_jitter(stm_jitter_duration) {}

ss::future<> materialized_segments::stop() {
    _stm_timer.cancel();
    _cvar.broken();

    co_await _gate.close();

    // Do the last pass over the eviction list to stop remaining items returned
    // from readers after the eviction loop stopped.
    for (auto& rs : _eviction_list) {
        co_await std::visit([](auto&& rs) { return rs->stop(); }, rs);
    }
}
ss::future<> materialized_segments::start() {
    // Fiber that consumes from _eviction_list and calls stop
    // on items before destroying them
    ssx::spawn_with_gate(_gate, [this] { return run_eviction_loop(); });

    // Timer to invoke TTL eviction of segments
    _stm_timer.set_callback([this] {
        gc_stale_materialized_segments(false);
        _stm_timer.rearm(_stm_jitter());
    });
    _stm_timer.rearm(_stm_jitter());

    return ss::now();
}

void materialized_segments::evict_reader(
  std::unique_ptr<remote_segment_batch_reader> reader) {
    _eviction_list.push_back(std::move(reader));
    _cvar.signal();
}
void materialized_segments::evict_segment(
  ss::lw_shared_ptr<remote_segment> segment) {
    _eviction_list.push_back(std::move(segment));
    _cvar.signal();
}

ss::future<> materialized_segments::run_eviction_loop() {
    // Evict readers asynchronously
    while (true) {
        co_await _cvar.wait([this] { return !_eviction_list.empty(); });
        auto tmp_list = std::exchange(_eviction_list, {});
        for (auto& rs : tmp_list) {
            co_await std::visit([](auto&& rs) { return rs->stop(); }, rs);
        }
    }
}

void materialized_segments::register_segment(materialized_segment_state& s) {
    _materialized.push_back(s);
}

void materialized_segments::maybe_trim() {
    // TODO: implement trimming to bound resource consumption
}

void materialized_segments::gc_stale_materialized_segments(
  bool force_collection) {
    // TODO: augment the log messages in these functions with ntp

    // The remote_segment instances are materialized on demand. They are
    // collected after some period of inactivity.
    // To prevent high memory consumption in some corner cases the
    // materialization of the new remote_segment triggers GC. The idea is
    // that remote_partition should have only one remote_segment in materialized
    // state when it's constantly in use and zero if not in use.
    vlog(
      cst_log.debug,
      "collecting stale materialized segments, {} segments materialized",
      _materialized.size());

    auto now = ss::lowres_clock::now();
    auto max_idle = force_collection ? 0ms : stm_max_idle_time;

    // These pointers are safe because there are no scheduling points
    // between here and the ultimate eviction at end of function.
    std::vector<std::pair<materialized_segment_state*, kafka::offset>>
      to_offload;

    for (auto& st : _materialized) {
        auto deadline = st.atime + max_idle;
        if (now >= deadline && !st.segment->download_in_progress()) {
            if (st.segment.owned()) {
                vlog(
                  cst_log.debug,
                  "reader for segment with base offset {} is stale",
                  st.offset_key);
                // this will delete and unlink the object from
                // _materialized collection
                if (st.parent) {
                    to_offload.push_back(std::make_pair(&st, st.offset_key));
                } else {
                    // This cannot happen, because materialized_segment_state
                    // is only instantiated by remote_partition and will
                    // be disposed before the remote_partition it points to.
                    vassert(
                      false,
                      "materialized_segment_state outlived remote_partition");
                }
            } else {
                vlog(
                  cst_log.debug,
                  "Materialized segment with base-offset {} is not stale: {} "
                  "{} {} {} readers={}",
                  st.base_rp_offset,
                  now - st.atime > stm_max_idle_time,
                  st.segment->download_in_progress(),
                  st.segment.owned(),
                  st.segment.use_count(),
                  st.readers.size());

                // Readers hold a reference to the segment, so for the
                // segment.owned() check to pass, we need to clear them out.
                while (!st.readers.empty()) {
                    evict_reader(std::move(st.readers.front()));
                    st.readers.pop_front();
                }
            }
        }
    }

    vlog(cst_log.debug, "found {} eviction candidates ", to_offload.size());
    for (const auto& i : to_offload) {
        remote_partition* p = i.first->parent.get();

        // Should not happen because we handled the case of parent==null
        // above, before inserting into to_offload
        vassert(p, "Unexpected orphan segment!");

        p->offload_segment(i.second);
    }
}

} // namespace cloud_storage
