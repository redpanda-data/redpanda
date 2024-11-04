// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "utils/named_type.h"

#include <cstdint>

namespace experimental::cloud_topics {

/// A version number for MVCC semantics.
using dl_version = named_type<int64_t, struct dl_version_tag>;

} // namespace experimental::cloud_topics
