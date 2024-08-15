/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/vassert.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

/// This tool can be used to detect anomalously long
/// running operations or stuck async loops.
/// You can create an instance of 'watchdog' and pass
/// a timestamp and a callback. The callback will be
/// invoked when the timeout expires.
///
/// Code sample below:
/// \code
///   ss::future<> service::close() {
///     watchdog wd(10s, [] { vlog(log.error, "service::close hang"); });
///     _as.request_abort();
///     co_await _gate.close(); // op. can potentially hang
///   }
/// \endcode
///
/// Note that you can't pass an abort_source. It's expected that when
/// the subsystem protected by the watchdog is shutting down using its
/// own abort_source the 'watchdog' instance will be quickly disposed.
/// If the shutdown process is broken we want the 'watchdog' to detect
/// this.
///
/// \warning The watchdog is not supposed to be used to invoke a lot of
/// logic inside the callback (e.g. for general control flow). Its supposed to
/// trigger logging or metric update. The lifetime of the underlying background
/// fiber is not restricted by the lifetime of the watchdog so the callback
/// should either be very simple (logging) or implement concurrency control
/// externally using gate. Also, note that it's not possible to pass the
/// abort_source. This is by design because we don't want to defuse the watchdog
/// during shutdown because a lot of hangs happen during shutdown.
class watchdog {
public:
    /// Create watchdog instance
    ///
    /// \param timeout defines time interval after which the watchdog will be
    ///                triggered
    /// \param deadline_reached is a callback that will be called when
    ///                         the watchdog is triggered
    /// \note The callback may outlive the watchdog
    /// instance. To prevent lifetime issues one could use external
    /// synchronization (hold a gate in the callback and close the gate outside
    /// of the callback) or alternatively (and preferably) the callback should
    /// be very simple, for instance, it should just log an error message.
    watchdog(
      seastar::lowres_clock::duration timeout,
      seastar::noncopyable_function<void()> deadline_reached) {
        start_waiting(timeout, std::move(deadline_reached));
    }
    // D-tor defuses the watchdog. The callback won't be called after this.
    ~watchdog() {
        // Cancellation happens asynchronously but its guaranteed that
        // a. The background task never touches any fields of the 'watchdog'
        //    which allows it to outlive the 'watchdog' instance.
        // b. After cancellation it will never invoke any code other than code
        //    which is used to ignore exceptions.
        cancel();
    }
    watchdog(const watchdog&) = delete;
    watchdog(watchdog&&) = delete;
    watchdog& operator=(const watchdog&) = delete;
    watchdog& operator=(watchdog&&) = delete;

private:
    void start_waiting(
      seastar::lowres_clock::duration timeout,
      seastar::noncopyable_function<void()> deadline_reached) {
        ssx::background = ssx::ignore_shutdown_exceptions(
          seastar::sleep_abortable(timeout, _as)
            // copy the callback to the local state of the background fiber
            .then([cb = std::move(deadline_reached)] { cb(); }));
    }
    void cancel() { _as.request_abort(); }
    seastar::abort_source _as;
};
