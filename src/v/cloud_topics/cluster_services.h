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

#include "cloud_topics/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace cloud_topics {

/*
 * This interface defines access to cluster-level services, such as a global
 * epoch source and partition directory. Take care when introducing a new
 * dependency, because it is expected that these services will be implemented
 * outside of the cloud topics sub-system.
 */
class cluster_services {
public:
    cluster_services() = default;
    cluster_services(const cluster_services&) = delete;
    cluster_services& operator=(const cluster_services&) = delete;
    cluster_services(cluster_services&&) = delete;
    cluster_services& operator=(cluster_services&&) = delete;
    virtual ~cluster_services() = default;

    /*
     * Return the current cluster epoch value.
     *
     * The epoch is a monotonically increasing value that is unique across the
     * cluster. The rate at which the epoch is incremented is implementation
     * specific.
     */
    virtual seastar::future<cluster_epoch>
    current_epoch(seastar::abort_source*) = 0;

    /*
     * Invalidatae any epoch caching that maybe happening is below
     */
    virtual seastar::future<> invalidate_epoch_below(cluster_epoch) = 0;
};

} // namespace cloud_topics
