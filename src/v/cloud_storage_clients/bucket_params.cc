/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/bucket_params.h"

namespace cloud_storage_clients {

std::expected<bucket_params, std::string>
extract_bucket_params(const bucket_name& name) {
    const auto& name_str = name();

    // For now, we only support plain bucket names without connection
    // parameters.
    if (name_str.find('?') != ss::sstring::npos) {
        return std::unexpected(
          "bucket name parsing with connection parameters is not supported");
    }

    bucket_params params;
    params.plain_name = plain_bucket_name{name_str};

    return params;
}

} // namespace cloud_storage_clients
