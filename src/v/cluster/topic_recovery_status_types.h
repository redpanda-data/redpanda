/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "serde/envelope.h"

#include <compare>
#include <type_traits>

namespace cluster {

struct status_request
  : serde::
      envelope<status_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
};

struct status_response
  : serde::
      envelope<status_response, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    bool is_active;
};

} // namespace cluster
