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

namespace cluster {

/// Names of snapshot files used by stm's
static const ss::sstring archival_stm_snapshot = "archival_metadata.snapshot";
static const ss::sstring rm_stm_snapshot = "tx.snapshot";
static const ss::sstring tm_stm_snapshot = "tx.coordinator.snapshot";
static const ss::sstring id_allocator_snapshot = "id.snapshot";

} // namespace cluster
