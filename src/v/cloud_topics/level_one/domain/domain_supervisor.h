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
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/optimized_optional.hh>

#include <filesystem>

namespace cluster {
class controller;
class partition;
} // namespace cluster

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_topics::l1 {
class domain_manager;
class io;

// Responsible for creating and managing domain managers on the leaders of the
// L1 topic partitions.
class domain_supervisor {
    class impl;

public:
    explicit domain_supervisor(
      cluster::controller*,
      io*,
      std::filesystem::path staging_dir,
      cloud_io::remote*,
      cloud_storage_clients::bucket_name bucket,
      ss::scheduling_group sg);
    domain_supervisor(const domain_supervisor&) = delete;
    domain_supervisor(domain_supervisor&&) = delete;
    domain_supervisor& operator=(const domain_supervisor&) = delete;
    domain_supervisor& operator=(domain_supervisor&&) = delete;
    ~domain_supervisor();

    ss::future<> start();
    ss::future<> stop();

    // This is expected to be called when leadership for a L1 domain is called
    // to notify the domain_supervisor if there is a manager that needs to be
    // created (or removed if partition is nullptr).
    void on_domain_leadership_change(
      const model::ntp&,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>>);

    // Returns nullopt if the domain manager for the given L1 metastore NTP, if
    // one exists (e.g. if it is currently leader and has processed the
    // appropriate leadership notification).
    ss::shared_ptr<domain_manager> get(const model::ntp&) const;

    // Creates the L1 metastore topic, returning false if there was an issue
    // while creating.
    ss::future<bool>
    maybe_create_metastore_topic(std::optional<int> num_partitions);

private:
    std::unique_ptr<impl> _impl;
};

} // namespace cloud_topics::l1
