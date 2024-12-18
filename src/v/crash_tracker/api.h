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

#include "base/seastarx.h"

#include <seastar/core/future.hh>

namespace crash_tracker {

/// Should be called on startup before any call to record_crash, otherwise
/// record_crash is a noop.
/// Here we check for too many consecutive unclean
/// shutdowns/crashes and abort the startup sequence if the limit exceeds
/// crash_loop_limit until the operator intervenes. Crash tracking
/// is reset if the node configuration changes or its been 1h since
/// the broker last failed to start. This metadata is tracked in the
/// tracker file. This is to prevent on disk state from piling up in
/// each unclean run and creating more state to recover for the next run.
ss::future<> initialize();

/// On a clean shutdown,
/// the tracker file should be deleted thus reseting the crash count on the
/// next run. In case of an unclean shutdown, we already bumped
/// the crash count and that should be taken into account in the
/// next run.
ss::future<> record_clean_shutdown();

/// Async-signal safe
void record_sigsegv_crash();

/// Async-signal safe
void record_sigabrt_crash();

/// Async-signal safe
void record_sigill_crash();

} // namespace crash_tracker
