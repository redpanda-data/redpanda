// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cloud_io/remote.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/configuration.h"

namespace cloud_io {

// Convenience wrapper around the client pool and remote, initializing them
// together, and destructing them in the proper order.
struct scoped_remote {
    static std::unique_ptr<scoped_remote>
    create(size_t pool_size, cloud_storage_clients::s3_configuration);
    ~scoped_remote();
    void request_stop();

    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<remote> remote;

private:
    scoped_remote() = default;
};

} // namespace cloud_io
