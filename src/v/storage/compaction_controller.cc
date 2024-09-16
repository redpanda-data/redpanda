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

namespace storage {
static ss::logger compaction_log{"compaction_ctrl"};

ss::future<int64_t> compaction_backlog_sampler::sample_backlog() {
    co_return _api.local().log_mgr().compaction_backlog();
}

compaction_controller::compaction_controller(
  ss::sharded<api>& api, backlog_controller_config cfg)
  : _ctrl(
      std::make_unique<compaction_backlog_sampler>(api), compaction_log, cfg) {
    _ctrl.setup_metrics("storage:compaction");
}

} // namespace storage
