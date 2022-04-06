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

// If you want a smaller threshold, use bytes field.
// We don't want alert to be disabled, as setting 0% would.
static constexpr unsigned int min_percent_free_threshold = 1;
static constexpr unsigned int max_percent_free_threshold = 50;
static constexpr size_t min_bytes_free_threshold = 1_GiB;

} // namespace cluster::node