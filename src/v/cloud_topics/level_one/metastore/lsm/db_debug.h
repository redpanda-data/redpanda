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

#include "lsm/lsm.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l1 {

/// Iterates all rows in the given database snapshot and logs each partition's
/// metadata and extents at debug level. Directly decodes keys and values from
/// the raw iterator.
ss::future<> dump_partition_state(lsm::snapshot& snap);

} // namespace cloud_topics::l1
