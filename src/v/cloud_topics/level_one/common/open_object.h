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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "cloud_topics/level_one/common/object_id.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <expected>
#include <memory>

namespace cloud_topics::l1 {

/// Open an object for reading through `io`, returning a handle that exposes the
/// object's index (seek by offset/timestamp) and opens readers at a seek point.
/// `skip_cache` has the same meaning as for io::read_object and is propagated
/// to the reads the handle performs.
///
/// What the extent locates depends on the format it describes (see
/// object_extent). For a native L1 object, the range is the footer. For an
/// imported tiered-storage segment, it's the whole segment as the index is a
/// distinct `.index` object.
ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>> open_object(
  io&,
  object_extent,
  ss::abort_source*,
  cloud_io::group_id,
  bool skip_cache = false);

} // namespace cloud_topics::l1
