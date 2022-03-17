/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/upload_controller.h"

#include "archival/logger.h"

#include <fmt/format.h>

namespace archival {

struct upload_backlog_sampler : public storage::backlog_controller::sampler {
    explicit upload_backlog_sampler(ss::sharded<scheduler_service>& api)
      : _service(api) {}

    ss::future<int64_t> sample_backlog() final {
        co_return _service.local().estimate_backlog_size();
    }

private:
    ss::sharded<scheduler_service>& _service;
};

upload_controller::upload_controller(
  ss::sharded<scheduler_service>& api, storage::backlog_controller_config cfg)
  : _ctrl(std::make_unique<upload_backlog_sampler>(api), upload_ctrl_log, cfg) {
    _ctrl.setup_metrics("archival:upload");
}

} // namespace archival
