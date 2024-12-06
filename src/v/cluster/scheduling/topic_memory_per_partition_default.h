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

#include "base/units.h"

#include <cstddef>

namespace cluster {

// Default value for `topic_memory_per_partition`. In a constant here such that
// it can easier be referred to.
inline constexpr size_t DEFAULT_TOPIC_MEMORY_PER_PARTITION = 200_KiB;

// DO NOT CHANGE
//
// Original default for `topic_memory_per_partition` when we made it memory
// group aware. This property is used for heuristics to determine if the user
// had overridden the default when it was non-mmemory group aware. Hence, this
// value should NOT be changed and stay the same even if the (above) default is
// changed.
inline constexpr size_t ORIGINAL_MEMORY_GROUP_AWARE_TMPP = 200_KiB;

} // namespace cluster
