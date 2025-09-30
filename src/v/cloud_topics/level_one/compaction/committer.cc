/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/committer.h"

#include "cloud_topics/level_one/compaction/committing_policy.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

compaction_committer::compaction_committer(
  std::unique_ptr<committing_policy> policy, metastore* metastore, io* io)
  : _policy(std::move(policy))
  , _metastore(metastore)
  , _io(io) {
    start_bg_loop();
}

void compaction_committer::start_bg_loop() {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return committing_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
              vlogl(
                compaction_log,
                log_level,
                "Encountered exception in main loop: {}",
                e);
          });
    });
}

ss::future<> compaction_committer::stop() {
    _as.request_abort();
    _sem.broken();
    co_await _gate.close();
}

ss::future<> compaction_committer::committing_loop() {
    while (!_gate.is_closed() && !_as.abort_requested()) {
        constexpr std::chrono::seconds poll_frequency(10);
        try {
            co_await _sem.wait(
              poll_frequency, std::max(_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (_updates.empty()) {
            continue;
        }

        if (_policy->should_commit()) {
            auto updates = std::exchange(_updates, {});
            co_await commit_some(std::move(updates));
        }
    }
}

void compaction_committer::push_update(object_output_t update) {
    _updates.push_back(std::move(update));
    auto update_response = _policy->on_update(_updates.back());
    if (update_response == committing_policy::update_response::preempt) {
        _sem.signal();
    }
}

ss::future<chunked_vector<compaction_committer::built_object>>
compaction_committer::build_objects([[maybe_unused]] updates_t updates) {
    chunked_vector<built_object> ret;
    // TODO: Here, we may also want to make decisions about how to group
    // together partitions/updates in L1. We could, for example, do a best
    // effort isolation of partition data in L1 objects. Building a
    // metadata_builder per update is silly, but we also may have to implement
    // new primitives for concatenating together L1 staging files.
    //
    // Ultimately this is a similar function to `reconciler::build_object()` and
    // may be worth abstracting out somehow, though perhaps with a different
    // heuristic for batching L1 updates here.
    co_return ret;
}

ss::future<>
compaction_committer::commit_some([[maybe_unused]] updates_t updates) {
    // auto objects_fut = co_await ss::coroutine::as_future(
    //   build_objects(std::move(updates)));

    co_return;
}

} // namespace cloud_topics::l1
