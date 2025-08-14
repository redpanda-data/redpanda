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

#include "serde/protobuf/field_mask.h"

namespace serde::pb {

fmt::iterator field_mask::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{paths: {}}}", paths);
}

} // namespace serde::pb
