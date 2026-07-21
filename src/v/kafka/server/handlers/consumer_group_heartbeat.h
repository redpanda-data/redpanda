/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using consumer_group_heartbeat_handler
  = single_stage_handler<consumer_group_heartbeat_api, 0, 1>;

} // namespace kafka
