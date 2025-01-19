/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace cluster {
class partition_manager;
}

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_storage {
class cache;
}

namespace experimental::cloud_topics {

class app {
    class impl;

public:
    app(
      seastar::sharded<cluster::partition_manager>*,
      seastar::sharded<cloud_io::remote>*,
      seastar::sharded<cloud_storage::cache>*,
      cloud_storage_clients::bucket_name bucket);

    app(const app&) = delete;
    app& operator=(const app&) = delete;
    app(app&&) noexcept = delete;
    app& operator=(app&&) noexcept = delete;
    ~app();

    seastar::future<> start();
    seastar::future<> stop();

private:
    std::unique_ptr<impl> _impl;
};

} // namespace experimental::cloud_topics
