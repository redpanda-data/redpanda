/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/fwd.h"
#include "seastarx.h"
#include "storage/backlog_controller.h"
#include "utils/named_type.h"
#include "utils/prefix_logger.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sharded.hh>

#include <filesystem>

namespace archival {

/// PID controller to control compaction scheduling and IO shares
/// Modeled after compaction_controller.
class upload_controller {
public:
    upload_controller(
      ss::sharded<cluster::partition_manager>&,
      storage::backlog_controller_config);

    ss::future<> start() { return _ctrl.start(); }
    ss::future<> stop() { return _ctrl.stop(); }

private:
    storage::backlog_controller _ctrl;
};

} // namespace archival
