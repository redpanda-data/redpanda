/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/upload_controller.h"

#include "archival/logger.h"
#include "cluster/partition_manager.h"

#include <fmt/format.h>

namespace archival {

struct upload_backlog_sampler : public storage::backlog_controller::sampler {
    explicit upload_backlog_sampler(
      ss::sharded<cluster::partition_manager>& partition_manager)
      : _partition_manager(partition_manager) {}

    ss::future<int64_t> sample_backlog() final {
        co_return _partition_manager.local().upload_backlog_size();
    }

private:
    ss::sharded<cluster::partition_manager>& _partition_manager;
};

upload_controller::upload_controller(
  ss::sharded<cluster::partition_manager>& partition_manager,
  storage::backlog_controller_config cfg)
  : _ctrl(
    std::make_unique<upload_backlog_sampler>(partition_manager),
    upload_ctrl_log,
    cfg) {
    _ctrl.setup_metrics("archival:upload");
}

} // namespace archival
