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
#include "compat/acls_generator.h"
#include "compat/check.h"
#include "compat/json.h"

namespace compat {

GEN_COMPAT_CHECK(
  cluster::create_acls_request,
  {
      json_write(data);
      json_write(timeout);
  },
  {
      json_read(data);
      json_read(timeout);
  });

GEN_COMPAT_CHECK(
  cluster::create_acls_reply,
  { json_write(results); },
  { json_read(results); });

GEN_COMPAT_CHECK(
  cluster::delete_acls_reply,
  { json_write(results); },
  { json_read(results); });

GEN_COMPAT_CHECK(
  cluster::delete_acls_request,
  {
      json_write(data);
      json_write(timeout);
  },
  {
      json_read(data);
      json_read(timeout);
  });

}; // namespace compat