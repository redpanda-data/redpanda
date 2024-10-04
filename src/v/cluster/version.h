/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "utils/named_type.h"

namespace cluster {

// A cluster version is a logical protocol version describing the content
// of the raft0 on disk structures, and available features.  These are
// passed over the network via the health_manager, and persisted in
// the feature_manager
using cluster_version = named_type<int64_t, struct cluster_version_tag>;
inline constexpr cluster_version invalid_version = cluster_version{-1};

} // namespace cluster
