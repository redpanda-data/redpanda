/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <iostream>

namespace storage {

enum class disk_space_alert { ok = 0, low_space = 1, degraded = 2 };

inline disk_space_alert max_severity(disk_space_alert a, disk_space_alert b) {
    return std::max(a, b);
}

std::ostream& operator<<(std::ostream& o, const storage::disk_space_alert d);

} // namespace storage