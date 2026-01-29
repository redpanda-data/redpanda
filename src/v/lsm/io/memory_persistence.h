/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "lsm/io/persistence.h"

namespace lsm::io {

struct memory_persistence_controller {
    bool should_fail{false};
};

// Create an in memory ephemeral data persistence layer for testing.
std::unique_ptr<data_persistence>
make_memory_data_persistence(memory_persistence_controller* = nullptr);

// Create an in memory ephemeral metadata persistence layer for testing.
std::unique_ptr<metadata_persistence>
make_memory_metadata_persistence(memory_persistence_controller* = nullptr);

} // namespace lsm::io
