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

#include "cluster/types.h"
#include "compat/check.h"
#include "compat/get_cluster_health_generator.h"
#include "compat/json.h"

namespace compat {

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::get_cluster_health_request,
  {
      json_write(filter);
      json_write(refresh);
      json_write(decoded_version);
  },
  {
      json_read(filter);
      json_read(refresh);
      json_read(decoded_version);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::get_cluster_health_reply,
  {
      json_write(error);
      json_write(report);
  },
  {
      json_read(error);
      json_read(report);
  });

}; // namespace compat
