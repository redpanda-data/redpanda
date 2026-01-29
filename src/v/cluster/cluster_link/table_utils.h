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
#include "cluster_link/model/types.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/future.hh>

namespace cluster::cluster_link {

ss::future<chunked_hash_map<
  ::cluster_link::model::id_t,
  ::cluster_link::model::metadata>>
copy_links_for_snapshot(
  chunked_hash_map<
    ::cluster_link::model::id_t,
    ::cluster_link::model::metadata_ptr> links);

ss::future<chunked_hash_map<
  ::cluster_link::model::id_t,
  ::cluster_link::model::metadata_ptr>>
copy_links_from_snapshot(
  const chunked_hash_map<
    ::cluster_link::model::id_t,
    ::cluster_link::model::metadata>& links);
} // namespace cluster::cluster_link
