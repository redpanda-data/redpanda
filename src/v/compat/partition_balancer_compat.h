/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "compat/check.h"
#include "compat/partition_balancer_generator.h"

namespace compat {

EMPTY_COMPAT_CHECK_SERDE_ONLY(cluster::partition_balancer_overview_request);

} // namespace compat
