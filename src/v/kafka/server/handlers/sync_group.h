/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/sync_group.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using sync_group_handler = two_phase_handler<sync_group_api, 0, 4>;

} // namespace kafka
