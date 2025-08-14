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

#include "utils/named_type.h"

#include <cstdint>

namespace experimental::cloud_topics {

/// A version number for MVCC semantics.
using dl_version = named_type<int64_t, struct dl_version_tag>;

} // namespace experimental::cloud_topics
