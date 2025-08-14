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

#include "base/seastarx.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace cluster {
class controller;
}

namespace experimental::cloud_topics::l1 {
class domain_manager;

// Responsible for creating and managing domain managers on the leaders of the
// L1 topic partitions.
class domain_supervisor {
    class impl;

public:
    explicit domain_supervisor(cluster::controller*);
    domain_supervisor(const domain_supervisor&) = delete;
    domain_supervisor(domain_supervisor&&) = delete;
    domain_supervisor& operator=(const domain_supervisor&) = delete;
    domain_supervisor& operator=(domain_supervisor&&) = delete;
    ~domain_supervisor();

    ss::future<> start();
    ss::future<> stop();

    // Returns nullopt if the domain manager for the given L1 metastore NTP, if
    // one exists (e.g. if it is currently leader and has processed the
    // appropriate leadership notification).
    ss::lw_shared_ptr<domain_manager> get(const model::ntp&) const;

    // Creates the L1 metastore topic, returning false if there was an issue
    // while creating.
    ss::future<bool> maybe_create_metastore_topic();

private:
    std::unique_ptr<impl> _impl;
};

} // namespace experimental::cloud_topics::l1
