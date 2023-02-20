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
#include "kafka/protocol/delete_acls.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using delete_acls_handler = single_stage_handler<delete_acls_api, 0, 2>;

}
