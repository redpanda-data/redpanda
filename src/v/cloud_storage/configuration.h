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

#include "cloud_storage/types.h"
#include "cloud_storage_clients/configuration.h"

namespace cloud_storage {

struct configuration {
    /// Client configuration
    cloud_storage_clients::client_configuration client_config;
    /// Number of simultaneous client uploads
    connection_limit connection_limit;
    /// The S3 bucket or ABS container to use
    cloud_storage_clients::bucket_name bucket_name;

    model::cloud_credentials_source cloud_credentials_source;

    friend std::ostream& operator<<(std::ostream& o, const configuration& cfg);

    static ss::future<configuration> get_config();
    static ss::future<configuration> get_s3_config();
    static ss::future<configuration> get_abs_config();
    static const config::property<std::optional<ss::sstring>>&
    get_bucket_config();
};

} // namespace cloud_storage
