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

#include "cloud_topics/cluster_services.h"

#include <seastar/core/coroutine.hh>

namespace experimental::cloud_topics {

class ephemeral_cluster_services : public cluster_services {
public:
    seastar::future<cluster_epoch>
    current_epoch(seastar::abort_source*) override {
        co_return epoch_++;
    }

private:
    cluster_epoch epoch_{0};
};

} // namespace experimental::cloud_topics
