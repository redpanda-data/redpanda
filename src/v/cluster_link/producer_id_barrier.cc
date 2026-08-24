/**
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/producer_id_barrier.h"

#include "cluster_link/logger.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

#include <limits>

namespace cluster_link {

void producer_id_barrier_impl::shutdown() noexcept {
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
}

ss::future<result<::model::producer_id, errc>>
producer_id_barrier_impl::advance() noexcept {
    if (_pid_exhausted) {
        // Short-circuit rather than repeat an all-broker scan every
        // reconciliation tick. Recovery from this state is a forced failover
        // (`rpk shadow delete --force`).
        co_return errc::producer_id_exhausted;
    }
    if (_as.abort_requested()) {
        // The abortable semaphore wait below only aborts *waiters*; an
        // immediately-available unit would let a post-shutdown advance run
        // the scan anyway.
        co_return errc::service_shutting_down;
    }
    auto f = co_await ss::coroutine::as_future(do_serialized_advance());
    if (f.failed()) {
        auto eptr = f.get_exception();
        if (ssx::is_shutdown_exception(eptr)) {
            vlog(cllog.debug, "Producer ID barrier aborted: {}", eptr);
            co_return errc::service_shutting_down;
        }
        vlog(cllog.info, "Producer ID barrier failed: {}", eptr);
        co_return errc::rpc_error;
    }
    auto res = f.get();
    if (res.has_error() && res.error() == errc::producer_id_exhausted) {
        _pid_exhausted = true;
    }
    co_return res;
}

ss::future<result<::model::producer_id, errc>>
producer_id_barrier_impl::do_serialized_advance() {
    // Single-flight: the barrier is cluster-scoped but every link runs its
    // own reconciler fiber, so concurrent links would otherwise duplicate
    // all-broker scans and equal-value resets — each an extra quorum write.
    // Abortable so shutdown doesn't wait on queued barriers.
    auto units = co_await ss::get_units(_sem, 1, _as);
    co_return co_await do_advance();
}

ss::future<result<::model::producer_id, errc>>
producer_id_barrier_impl::do_advance() {
    auto scanned = co_await _ops->scan_highest_pid();
    if (scanned.has_error()) {
        co_return scanned.error();
    }
    // Always reset, including when the observed maximum is 0: producer ID 0
    // is valid (a fresh allocator hands it out first), and the scan cannot
    // distinguish it from "no producers found".
    //
    // The margin cushions a scan that undercounts slightly in this narrow
    // window (producer IDs granted but not yet produced with, batches still
    // in flight). It is NOT a general correctness guarantee and must not be
    // read as covering an arbitrarily late batch.
    constexpr int64_t barrier_margin = 1000;
    auto raw = scanned.value()();
    if (raw > std::numeric_limits<int64_t>::max() - barrier_margin) {
        vlog(
          cllog.error,
          "Cluster highest producer ID {} is too close to the int64 maximum "
          "to advance the ID allocator by {}",
          raw,
          barrier_margin);
        co_return errc::producer_id_exhausted;
    }
    auto ec = co_await _ops->reset_next_id(
      ::model::producer_id{raw + barrier_margin});
    if (ec != errc::success) {
        co_return ec;
    }
    co_return scanned.value();
}

} // namespace cluster_link
