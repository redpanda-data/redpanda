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

#include "cluster/partition_balancer_types.h"
#include "compat/check.h"
#include "compat/json.h"
#include "compat/partition_balancer_generator.h"

namespace compat {

EMPTY_COMPAT_CHECK_SERDE_ONLY(cluster::partition_balancer_overview_request);

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::partition_balancer_overview_reply,
  {
      json_write(error);
      json_write(last_tick_time);
      json_write(status);
      json_write(violations);
      json_write(decommission_realloc_failures);
      json_write(partitions_pending_force_recovery_count);
      json_write(partitions_pending_force_recovery_sample);
  },
  {
      json_read(error);
      json_read(last_tick_time);
      json_read(status);
      json_read(violations);
      json_read(decommission_realloc_failures);
      json_read(partitions_pending_force_recovery_count);
      json_read(partitions_pending_force_recovery_sample);
  })

} // namespace compat
