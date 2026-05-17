/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "cloud_io/scheduler_types.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/configuration.h"
#include "config/property.h"

namespace cloud_storage {

struct configuration {
    /// Client configuration
    cloud_storage_clients::client_configuration client_config;
    /// Number of simultaneous client uploads
    connection_limit connection_limit;
    /// Admission scheduler configuration applied to the per-shard
    /// cloud_storage_clients::client_pool. Populated once at startup
    /// from cluster config; passed by value down to the pool and
    /// scheduler so neither re-reads cluster state.
    cloud_io::scheduler_config scheduler;
    /// The S3 bucket or ABS container to use
    cloud_storage_clients::bucket_name bucket_name;

    model::cloud_credentials_source cloud_credentials_source;

    fmt::iterator format_to(fmt::iterator it) const;

    static ss::future<configuration> get_config();
    static ss::future<configuration> get_s3_config();
    static ss::future<configuration> get_abs_config();
    static const config::property<std::optional<ss::sstring>>&
    get_bucket_config();
};

} // namespace cloud_storage
