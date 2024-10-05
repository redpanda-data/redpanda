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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

namespace cluster {

/// Names of snapshot files used by stm's
inline const ss::sstring archival_stm_snapshot = "archival_metadata.snapshot";
inline const ss::sstring rm_stm_snapshot = "tx.snapshot";
inline const ss::sstring tm_stm_snapshot = "tx.coordinator.snapshot";
inline const ss::sstring id_allocator_snapshot = "id.snapshot";
inline const ss::sstring partition_properties_stm_snapshot
  = "partition_properties.snapshot";

} // namespace cluster
