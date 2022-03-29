/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "storage/backlog_controller.h"
#include "storage/fwd.h"

#include <seastar/core/sharded.hh>

namespace storage {

struct compaction_backlog_sampler : public backlog_controller::sampler {
    explicit compaction_backlog_sampler(ss::sharded<api>& api)
      : _api(api) {}

    ss::future<int64_t> sample_backlog() final;

private:
    ss::sharded<api>& _api;
};
/**
 * PID controller to controll compaction scheduling and IO shares
 */
class compaction_controller {
public:
    compaction_controller(ss::sharded<api>&, backlog_controller_config);

    ss::future<> start() { return _ctrl.start(); }
    ss::future<> stop() { return _ctrl.stop(); }

private:
    backlog_controller _ctrl;
};

} // namespace storage
