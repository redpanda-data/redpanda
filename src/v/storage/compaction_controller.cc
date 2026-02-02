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

#include "storage/compaction_controller.h"

#include "storage/api.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace storage {
static ss::logger compaction_log{"compaction_ctrl"};

ss::future<int64_t> compaction_backlog_sampler::sample_backlog() {
    int64_t local_backlog = _api.local().log_mgr().compaction_backlog();
    int64_t cloud_backlog = 0;
    if (_cloud_topics_backlog) {
        auto fn = _cloud_topics_backlog;
        cloud_backlog = co_await ss::smp::submit_to(0, std::move(fn));
    }
    co_return local_backlog + cloud_backlog;
}

compaction_controller::compaction_controller(
  ss::sharded<api>& api,
  backlog_controller_config cfg,
  backlog_fn cloud_backlog)
  : _ctrl(
      std::make_unique<compaction_backlog_sampler>(
        api, std::move(cloud_backlog)),
      compaction_log,
      std::move(cfg)) {
    _ctrl.setup_metrics("storage:compaction");
}

} // namespace storage
