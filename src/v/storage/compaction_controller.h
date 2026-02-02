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

#include <functional>

namespace storage {

using backlog_fn = std::function<int64_t()>;

struct compaction_backlog_sampler : public backlog_controller::sampler {
    compaction_backlog_sampler(
      ss::sharded<api>& api, backlog_fn cloud_topics_backlog = nullptr)
      : _api(api)
      , _cloud_topics_backlog(std::move(cloud_topics_backlog)) {}

    ss::future<int64_t> sample_backlog() final;

private:
    ss::sharded<api>& _api;
    // Returns the cloud topics compaction backlog in bytes.
    // Called on shard 0 where the cloud compaction scheduler runs.
    backlog_fn _cloud_topics_backlog;
};
/**
 * PID controller to controll compaction scheduling and IO shares
 */
class compaction_controller {
public:
    compaction_controller(
      ss::sharded<api>&,
      backlog_controller_config,
      backlog_fn cloud_topics_backlog = nullptr);

    ss::future<> start() { return _ctrl.start(); }
    ss::future<> stop() { return _ctrl.stop(); }

private:
    backlog_controller _ctrl;
};

} // namespace storage
