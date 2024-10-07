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

namespace raft {
using group_manager_notification_id
  = named_type<int32_t, struct raft_group_manager_notification_id>;
inline constexpr group_manager_notification_id notification_id_type_invalid{-1};

} // namespace raft
