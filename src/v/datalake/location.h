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

#include "cloud_io/provider.h"
#include "datalake/base_types.h"
#include "iceberg/uri.h"
#include "model/fundamental.h"

#include <optional>

namespace datalake {

class location_provider {
public:
    location_provider(
      cloud_io::provider provider, cloud_storage_clients::bucket_name bucket)
      : uri_converter_(std::move(provider))
      , bucket_(std::move(bucket)) {}

public:
    std::optional<remote_path> from_uri(const iceberg::uri& uri) const {
        auto maybe_path = uri_converter_.from_uri(bucket_, uri);
        if (!maybe_path) {
            return std::nullopt;
        }

        return remote_path(std::move(maybe_path.value()));
    }

private:
    iceberg::uri_converter uri_converter_;
    cloud_storage_clients::bucket_name bucket_;
};

} // namespace datalake
