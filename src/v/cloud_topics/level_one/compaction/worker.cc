/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "compaction/reducer.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

compaction_worker::compaction_worker(io* io, compaction_committer* committer)
  : _io(io)
  , _committer(committer) {}

ss::future<>
compaction_worker::compact(log_info_and_meta log, ss::abort_source& as) {
    const auto& tp = log.meta->tid_p;
    vlog(compaction_log.info, "Compacting ntp {}", log.meta->ntp);
    if (_stopped) {
        co_return;
    }

    _state = compaction_job_state::running;
    _ntp = log.meta->ntp;

    auto src = std::make_unique<compaction_source>(tp, as, _state);
    auto sink = std::make_unique<compaction_sink>(_io, _committer, tp);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    auto compact_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (compact_fut.failed()) {
        auto eptr = compact_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::warn
                                                        : ss::log_level::debug;
        vlogl(
          compaction_log,
          log_lvl,
          "Caught exception {} while compacting ntp {}.",
          eptr,
          log.meta->ntp);
    }

    _state = compaction_job_state::idle;
    _ntp.reset();
}

void compaction_worker::request_stop_compact(model::ntp expected_ntp) {
    if (_ntp == expected_ntp && _state == compaction_job_state::running) {
        _state = compaction_job_state::stopped;
    }
}

void compaction_worker::set_stopped() {
    _stopped = true;
    _state = compaction_job_state::stopped;
}

} // namespace cloud_topics::l1
