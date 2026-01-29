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

#include <cstdint>

namespace cloud_topics::l0 {

/// This struct is used to track per-request resource
/// consumption.
struct micro_probe {
    // Number of times the data was read from cache
    uint16_t num_cache_reads{0};
    // Number of times the data was written to the cache
    uint16_t num_cache_writes{0};
    // Number of times data was downloaded from the cloud
    uint16_t num_cloud_reads{0};
    // Number of times data was uploaded to the cloud
    uint16_t num_cloud_writes{0};
    // Number of bytes read from cache
    uint64_t cache_read_bytes{0};
    // Number of bytes written to cache
    uint64_t cache_write_bytes{0};
    // Number of bytes downloaded from the cloud
    uint64_t cloud_read_bytes{0};
    // Number of bytes uploaded to the cloud
    uint64_t cloud_write_bytes{0};

    micro_probe& operator+=(const micro_probe& t) {
        num_cache_reads += t.num_cache_reads;
        num_cache_writes += t.num_cache_writes;
        num_cloud_reads += t.num_cloud_reads;
        num_cloud_writes += t.num_cloud_writes;
        cache_read_bytes += t.cache_read_bytes;
        cache_write_bytes += t.cache_write_bytes;
        cloud_read_bytes += t.cloud_read_bytes;
        cloud_write_bytes += t.cloud_write_bytes;
        return *this;
    }
};

} // namespace cloud_topics::l0
