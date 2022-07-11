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

#include <cinttypes>

// Some cluster limts.
//
// These are not hard limits in any sense but soft targets. Clusters may fail
// for a variety of reasons before hitting these limits and also may work beyond
// these limits. Rather, they are here to track our current assumptions about
// what "should" work and for use in calculations or heuristics relating to
// maximum sizes.
//
// For exmaple, when we accept a metadata request we don't know how large the
// response will be as it depends on the total number of partitions and topics
// in the system, so we can use the values here as soft upper bounds on the
// relevant values.
//

namespace kafka {

/**
 * @brief Soft limit on the highest number of partitions across all topics in a
 * cluster.
 */
static constexpr uint32_t max_clusterwide_partitions = 40000;

} // namespace kafka
