/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "cloud_topics/api.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace cluster {
class partition_manager;
}

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_storage {
class cache;
}

namespace experimental::cloud_topics {

ss::shared_ptr<api> make_app(
  seastar::sharded<cluster::partition_manager>*,
  seastar::sharded<cloud_io::remote>*,
  seastar::sharded<cloud_storage::cache>*,
  cloud_storage_clients::bucket_name bucket);

} // namespace experimental::cloud_topics
