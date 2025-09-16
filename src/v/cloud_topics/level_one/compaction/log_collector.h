/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l1 {

class compaction_scheduler;

// Responsible for pushing CTPs/logs that require compaction to the
// `compaction_scheduler` for managing, as well as their removal. Provides a
// very limited interface to the user- implementors require only adding
// `start_collecting_logs()` and `stop_collecting_logs()` functions in their
// concrete classes.
class log_collector {
public:
    log_collector(compaction_scheduler*);

    virtual ~log_collector() noexcept = default;

    // Starts the `log_collector` by calling `manage_logs()`, allowing it to
    // push CTPs that require compaction to the `compaction_scheduler`, or
    // remove CTPs that no longer require compaction from the
    // `compaction_scheduler`.
    ss::future<> start();

    // Stops the `log_collector` by calling `unmanage_logs()`. The
    // `compaction_scheduler` will no longer receive updates on which CTPs
    // require managing. This should only be invoked during application
    // shutdown.
    ss::future<> stop();

protected:
    // Called during start-up. Initiates log collection, allowing for
    // registration/deregistration of logs with the `compaction_scheduler`. For
    // example, setting up a callback with a cluster object that pushes newly
    // managed CTPs to the `compaction_scheduler`.
    virtual ss::future<> start_collecting_logs() = 0;

    // Called during tear-down. Stops the `log_collector` from making further
    // registrations/deregistrations of logs with the `compaction_scheduler`
    // (stopping call-backs, destructing objects or closing background loops,
    // etc.)
    virtual ss::future<> stop_collecting_logs() = 0;

protected:
    // Owned by `app`.
    compaction_scheduler* _scheduler;
};

} // namespace cloud_topics::l1
