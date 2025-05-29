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

#include "kafka/server/datalake_usage_api.h"

namespace cluster {
class controller;
}

namespace datalake {

class disabled_datalake_usage_api_impl final
  : public kafka::datalake_usage_api {
public:
    disabled_datalake_usage_api_impl(cluster::controller*);

    ss::future<usage_stats> compute_usage(ss::abort_source&) final;

private:
    cluster::controller* _controller{nullptr};
};
} // namespace datalake
