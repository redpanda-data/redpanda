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
#include "compat/commit_group_tx_generator.h"
#include "compat/json.h"

namespace compat {

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::commit_group_tx_request,
  {
      json_write(ntp);
      json_write(pid);
      json_write(tx_seq);
      json_write(group_id);
      json_write(timeout);
  },
  {
      json_read(ntp);
      json_read(pid);
      json_read(tx_seq);
      json_read(group_id);
      json_read(timeout);
  });

GEN_COMPAT_CHECK_SERDE_ONLY(
  cluster::commit_group_tx_reply, { json_write(ec); }, { json_read(ec); });

}; // namespace compat
