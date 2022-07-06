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
#include "kafka/protocol/init_producer_id.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using init_producer_id_handler = handler<init_producer_id_api, 0, 3>;

}
