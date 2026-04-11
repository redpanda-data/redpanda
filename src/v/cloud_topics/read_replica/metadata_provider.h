/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/read_replica/partition_metadata.h"
#include "model/fundamental.h"

#include <optional>

namespace cloud_topics::read_replica {

// Abstract interface for accessing partition metadata.
// Implemented by metadata_manager to provide read-only metadata queries.
class metadata_provider {
public:
    virtual ~metadata_provider() = default;
    virtual ss::future<> stop() = 0;

    // Query metadata for a partition.
    // Returns std::nullopt if partition is not registered.
    virtual std::optional<partition_metadata>
    get_metadata(const model::ntp&) const = 0;
};

} // namespace cloud_topics::read_replica
