/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "units.h"

namespace cluster::node {

// Naming is a little confusing: these are min/max values we will allow for
// config parameters that specify minimum free space thresholds.

static constexpr unsigned int min_min_percent_free_alert = 0;
static constexpr unsigned int max_min_percent_free_alert = 50;

static constexpr size_t min_min_bytes_free_alert = 0;

// Point at which we become degraded, blocking writes.
static constexpr size_t min_min_bytes_free = 1_GiB;

} // namespace cluster::node