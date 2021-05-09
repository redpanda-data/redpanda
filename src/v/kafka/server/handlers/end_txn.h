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
#include "kafka/protocol/end_txn.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using end_txn_handler = handler<end_txn_api, 0, 2>;

}
