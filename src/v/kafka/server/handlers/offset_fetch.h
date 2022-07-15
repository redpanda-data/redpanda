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
#include "kafka/protocol/offset_fetch.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

// in version 0 kafka stores offsets in zookeeper. if we ever need to
// support version 0 then we need to do some code review to see if this has
// any implications on semantics.
using offset_fetch_handler = single_stage_handler<offset_fetch_api, 1, 7>;

} // namespace kafka
