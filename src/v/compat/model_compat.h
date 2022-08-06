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
#include "compat/json.h"
#include "compat/model_generator.h"
#include "model/metadata.h"

namespace compat {

GEN_COMPAT_CHECK(
  model::partition_metadata,
  {
      json_write(id);
      json_write(replicas);
      json_write(leader_node);
  },
  {
      json_read(id);
      json_read(replicas);
      json_read(leader_node);
  })

GEN_COMPAT_CHECK(
  model::topic_metadata,
  {
      json_write(tp_ns);
      json_write(partitions);
  },
  {
      json_read(tp_ns);
      json_read(partitions);
  });

} // namespace compat
