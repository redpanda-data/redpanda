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

#include "model/fundamental.h"

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace cluster {
class partition_manager;
template<typename Clock>
class cluster_epoch_service;
} // namespace cluster

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_storage {
class cache;
}

namespace storage {
class api;
}

namespace experimental::cloud_topics {

class cluster_services;
class data_plane_api;

ss::future<std::unique_ptr<data_plane_api>> make_data_plane(
  ss::sstring logger_name,
  seastar::sharded<cloud_io::remote>*,
  seastar::sharded<cloud_storage::cache>*,
  cloud_storage_clients::bucket_name bucket,
  seastar::sharded<storage::api>* log_manager,
  seastar::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>*
    cluster_services);

} // namespace experimental::cloud_topics
