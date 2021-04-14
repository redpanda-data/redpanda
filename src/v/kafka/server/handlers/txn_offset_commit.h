/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/txn_offset_commit.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using txn_offset_commit_handler = handler<txn_offset_commit_api, 0, 2>;

}
